package applebackup

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/pbkdf2"
	"howett.net/plist"
)

// Test fixtures use deliberately tiny PBKDF2 iteration counts; production
// keybags carry their own counts (typically 10M), which the parser reads
// from the keybag rather than hardcoding.
const (
	testPassword = "correct horse battery staple"
	testDPIC     = 137
	testITER     = 211
)

// --- fixture builders -------------------------------------------------

func tlv(tag string, value []byte) []byte {
	out := make([]byte, 0, 8+len(value))
	out = append(out, tag...)
	out = binary.BigEndian.AppendUint32(out, uint32(len(value)))
	return append(out, value...)
}

func u32be(v uint32) []byte {
	return binary.BigEndian.AppendUint32(nil, v)
}

func randBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return b
}

// aesKeyWrap is the RFC 3394 forward wrap, used only to build fixtures
// that the production unwrap path must invert.
func aesKeyWrap(t *testing.T, kek, key []byte) []byte {
	t.Helper()
	block, err := aes.NewCipher(kek)
	require.NoError(t, err)
	require.Zero(t, len(key)%8)

	n := len(key) / 8
	a := make([]byte, 8)
	for i := range a {
		a[i] = 0xA6
	}
	r := make([][]byte, n+1)
	for i := 1; i <= n; i++ {
		r[i] = make([]byte, 8)
		copy(r[i], key[(i-1)*8:i*8])
	}

	buf := make([]byte, 16)
	for j := 0; j <= 5; j++ {
		for i := 1; i <= n; i++ {
			copy(buf[:8], a)
			copy(buf[8:], r[i])
			block.Encrypt(buf, buf)
			tt := uint64(n*j + i)
			binary.BigEndian.PutUint64(a, binary.BigEndian.Uint64(buf[:8])^tt)
			copy(r[i], buf[8:])
		}
	}

	out := make([]byte, 0, (n+1)*8)
	out = append(out, a...)
	for i := 1; i <= n; i++ {
		out = append(out, r[i]...)
	}
	return out
}

// derivePasscodeKey mirrors the iOS 10.2+ double PBKDF2 derivation so
// fixtures wrap class keys with the same key the production unlock derives.
func derivePasscodeKey(password string, dpsl []byte, dpic int, salt []byte, iter int) []byte {
	intermediate := pbkdf2.Key([]byte(password), dpsl, dpic, 32, sha256.New)
	return pbkdf2.Key(intermediate, salt, iter, 32, sha1.New)
}

type testKeybag struct {
	blob      []byte
	classKeys map[uint32][]byte
}

// buildTestKeybag creates a keybag blob holding passcode-wrapped keys for
// the given protection classes.
func buildTestKeybag(t *testing.T, classes ...uint32) *testKeybag {
	t.Helper()
	salt := randBytes(t, 20)
	dpsl := randBytes(t, 20)
	passcodeKey := derivePasscodeKey(testPassword, dpsl, testDPIC, salt, testITER)

	blob := make([]byte, 0, 512)
	blob = append(blob, tlv("VERS", u32be(3))...)
	blob = append(blob, tlv("TYPE", u32be(1))...)
	blob = append(blob, tlv("UUID", randBytes(t, 16))...)
	blob = append(blob, tlv("HMCK", randBytes(t, 40))...)
	blob = append(blob, tlv("WRAP", u32be(1))...)
	blob = append(blob, tlv("SALT", salt)...)
	blob = append(blob, tlv("ITER", u32be(testITER))...)
	blob = append(blob, tlv("DPWT", u32be(0))...)
	blob = append(blob, tlv("DPIC", u32be(testDPIC))...)
	blob = append(blob, tlv("DPSL", dpsl)...)

	kb := &testKeybag{classKeys: map[uint32][]byte{}}
	for _, class := range classes {
		key := randBytes(t, 32)
		kb.classKeys[class] = key
		blob = append(blob, tlv("UUID", randBytes(t, 16))...)
		blob = append(blob, tlv("CLAS", u32be(class))...)
		blob = append(blob, tlv("WRAP", u32be(wrapPasscode))...)
		blob = append(blob, tlv("KTYP", u32be(0))...)
		blob = append(blob, tlv("WPKY", aesKeyWrap(t, passcodeKey, key))...)
	}
	kb.blob = blob
	return kb
}

func buildFileMetadataBlob(t *testing.T, size int64, class uint32, keyBlob []byte) []byte {
	t.Helper()
	fileDict := map[string]interface{}{
		"Size":            size,
		"ProtectionClass": int(class),
		"Mode":            0o100644,
	}
	objects := []interface{}{"$null", fileDict}
	if keyBlob != nil {
		fileDict["EncryptionKey"] = plist.UID(2)
		objects = append(objects, map[string]interface{}{"NS.data": keyBlob})
	}
	archive := map[string]interface{}{
		"$version":  100000,
		"$archiver": "NSKeyedArchiver",
		"$top":      map[string]interface{}{"root": plist.UID(1)},
		"$objects":  objects,
	}
	data, err := plist.Marshal(archive, plist.BinaryFormat)
	require.NoError(t, err)
	return data
}

func fileIDFor(domain, relativePath string) string {
	sum := sha1.Sum([]byte(domain + "-" + relativePath))
	return hex.EncodeToString(sum[:])
}

type manifestFile struct {
	domain       string
	relativePath string
	flags        int64
	content      []byte
}

// buildManifestDB creates a real Manifest.db SQLite file and returns its
// bytes along with the metadata used per file.
func buildManifestDB(t *testing.T, dir string, files []manifestFile, fileKeyBlob func(f manifestFile) []byte) string {
	t.Helper()
	dbPath := filepath.Join(dir, "manifest-src.db")
	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	_, err = db.Exec(`CREATE TABLE Files (
		fileID TEXT PRIMARY KEY,
		domain TEXT,
		relativePath TEXT,
		flags INTEGER,
		file BLOB
	)`)
	require.NoError(t, err)

	for _, f := range files {
		var blob []byte
		if f.flags == 1 {
			var keyBlob []byte
			if fileKeyBlob != nil {
				keyBlob = fileKeyBlob(f)
			}
			class := uint32(0)
			if keyBlob != nil {
				class = binary.LittleEndian.Uint32(keyBlob[:4])
			}
			blob = buildFileMetadataBlob(t, int64(len(f.content)), class, keyBlob)
		}
		_, err = db.Exec(
			`INSERT INTO Files (fileID, domain, relativePath, flags, file)
			 VALUES (?, ?, ?, ?, ?)`,
			fileIDFor(f.domain, f.relativePath), f.domain, f.relativePath,
			f.flags, blob,
		)
		require.NoError(t, err)
	}
	return dbPath
}

func writeManifestPlist(t *testing.T, dir string, m map[string]interface{}) {
	t.Helper()
	data, err := plist.Marshal(m, plist.BinaryFormat)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "Manifest.plist"), data, 0600))
}

func writeBackupFile(t *testing.T, dir, fileID string, content []byte) {
	t.Helper()
	sub := filepath.Join(dir, fileID[:2])
	require.NoError(t, os.MkdirAll(sub, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(sub, fileID), content, 0600))
}

func encryptCBC(t *testing.T, key, plaintext []byte) []byte {
	t.Helper()
	padded := make([]byte, (len(plaintext)+aes.BlockSize-1)/aes.BlockSize*aes.BlockSize)
	copy(padded, plaintext)
	block, err := aes.NewCipher(key)
	require.NoError(t, err)
	iv := make([]byte, aes.BlockSize)
	out := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(out, padded)
	return out
}

// buildUnencryptedBackup writes a complete unencrypted backup directory.
func buildUnencryptedBackup(t *testing.T, files []manifestFile) string {
	t.Helper()
	dir := t.TempDir()
	srcDB := buildManifestDB(t, t.TempDir(), files, nil)
	dbBytes, err := os.ReadFile(srcDB)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "Manifest.db"), dbBytes, 0600))
	writeManifestPlist(t, dir, map[string]interface{}{
		"IsEncrypted": false,
		"Version":     "10.0",
	})
	for _, f := range files {
		if f.flags == 1 {
			writeBackupFile(t, dir, fileIDFor(f.domain, f.relativePath), f.content)
		}
	}
	return dir
}

// buildEncryptedBackup writes a complete encrypted backup directory: keybag
// in Manifest.plist, CBC-encrypted Manifest.db, and per-file keys wrapped
// with a class key.
func buildEncryptedBackup(t *testing.T, files []manifestFile) string {
	t.Helper()
	dir := t.TempDir()

	const fileClass = uint32(3)
	const manifestClass = uint32(4)
	kb := buildTestKeybag(t, fileClass, manifestClass)

	fileKeys := map[string][]byte{}
	srcDB := buildManifestDB(t, t.TempDir(), files, func(f manifestFile) []byte {
		key := randBytes(t, 32)
		fileKeys[f.relativePath] = key
		blob := binary.LittleEndian.AppendUint32(nil, fileClass)
		return append(blob, aesKeyWrap(t, kb.classKeys[fileClass], key)...)
	})

	dbBytes, err := os.ReadFile(srcDB)
	require.NoError(t, err)
	manifestDBKey := randBytes(t, 32)
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "Manifest.db"),
		encryptCBC(t, manifestDBKey, dbBytes), 0600,
	))

	manifestKeyBlob := binary.LittleEndian.AppendUint32(nil, manifestClass)
	manifestKeyBlob = append(
		manifestKeyBlob,
		aesKeyWrap(t, kb.classKeys[manifestClass], manifestDBKey)...,
	)
	writeManifestPlist(t, dir, map[string]interface{}{
		"IsEncrypted":  true,
		"BackupKeyBag": kb.blob,
		"ManifestKey":  manifestKeyBlob,
		"Version":      "10.0",
	})

	for _, f := range files {
		if f.flags != 1 {
			continue
		}
		writeBackupFile(
			t, dir, fileIDFor(f.domain, f.relativePath),
			encryptCBC(t, fileKeys[f.relativePath], f.content),
		)
	}
	return dir
}

// --- tests ------------------------------------------------------------

func testVoicemailFiles() []manifestFile {
	return []manifestFile{
		{
			domain:       "HomeDomain",
			relativePath: "Library/Voicemail",
			flags:        2, // directory
		},
		{
			domain:       "HomeDomain",
			relativePath: "Library/Voicemail/voicemail.db",
			flags:        1,
			content:      []byte("SQLite format 3\x00 pretend voicemail db body"),
		},
		{
			domain:       "HomeDomain",
			relativePath: "Library/Voicemail/17.amr",
			flags:        1,
			content:      []byte("#!AMR\nsynthetic audio payload that is not block aligned"),
		},
		{
			domain:       "HomeDomain",
			relativePath: "Library/Preferences/other.plist",
			flags:        1,
			content:      []byte("unrelated"),
		},
	}
}

func TestOpenUnencryptedBackup(t *testing.T) {
	dir := buildUnencryptedBackup(t, testVoicemailFiles())

	b, err := Open(dir, "")
	require.NoError(t, err)
	defer func() { require.NoError(t, b.Close()) }()

	assert.False(t, b.Encrypted())

	files, err := b.ListFiles("HomeDomain", "Library/Voicemail/")
	require.NoError(t, err)
	require.Len(t, files, 2, "directory rows must be skipped")
	assert.Equal(t, "Library/Voicemail/17.amr", files[0].RelativePath)
	assert.Equal(t, "Library/Voicemail/voicemail.db", files[1].RelativePath)

	content, err := b.ReadFile(files[0])
	require.NoError(t, err)
	assert.Equal(t, []byte("#!AMR\nsynthetic audio payload that is not block aligned"), content)
}

func TestOpenEncryptedBackup(t *testing.T) {
	files := testVoicemailFiles()
	dir := buildEncryptedBackup(t, files)

	b, err := Open(dir, testPassword)
	require.NoError(t, err)
	defer func() { require.NoError(t, b.Close()) }()

	assert.True(t, b.Encrypted())

	records, err := b.ListFiles("HomeDomain", "Library/Voicemail/")
	require.NoError(t, err)
	require.Len(t, records, 2)

	byPath := map[string]FileRecord{}
	for _, r := range records {
		byPath[r.RelativePath] = r
	}

	audio, err := b.ReadFile(byPath["Library/Voicemail/17.amr"])
	require.NoError(t, err)
	assert.Equal(t,
		[]byte("#!AMR\nsynthetic audio payload that is not block aligned"),
		audio,
		"decrypted content must be truncated to the recorded plaintext size",
	)

	db, err := b.ReadFile(byPath["Library/Voicemail/voicemail.db"])
	require.NoError(t, err)
	assert.Equal(t, []byte("SQLite format 3\x00 pretend voicemail db body"), db)
}

func TestOpenEncryptedBackupPasswordErrors(t *testing.T) {
	dir := buildEncryptedBackup(t, testVoicemailFiles())

	_, err := Open(dir, "")
	assert.ErrorIs(t, err, ErrPasswordRequired)

	_, err = Open(dir, "wrong password")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "wrong backup password")
}

func TestDiscover(t *testing.T) {
	root := t.TempDir()

	older := filepath.Join(root, "00000000-0000000000000001")
	newer := filepath.Join(root, "00000000-0000000000000002")
	for _, dir := range []string{older, newer} {
		require.NoError(t, os.MkdirAll(dir, 0700))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "Manifest.db"), []byte("x"), 0600))
	}
	past := time.Now().Add(-time.Hour)
	require.NoError(t, os.Chtimes(older, past, past))

	got, err := Discover(root)
	require.NoError(t, err)
	assert.Equal(t, newer, got, "most recently modified backup wins")

	got, err = Discover(newer)
	require.NoError(t, err)
	assert.Equal(t, newer, got, "a backup directory resolves to itself")

	_, err = Discover(t.TempDir())
	assert.Error(t, err)
}

func TestKeybagRejectsGarbage(t *testing.T) {
	_, err := parseKeybag([]byte("not a keybag"))
	assert.Error(t, err)

	_, err = parseKeybag(tlv("VERS", u32be(3)))
	assert.Error(t, err, "keybag without class keys must be rejected")
}

func TestAESKeyWrapRoundTrip(t *testing.T) {
	kek := randBytes(t, 32)
	key := randBytes(t, 32)
	wrapped := aesKeyWrap(t, kek, key)
	require.Len(t, wrapped, wrappedKeyLen)

	got, err := aesKeyUnwrap(kek, wrapped)
	require.NoError(t, err)
	assert.Equal(t, key, got)

	_, err = aesKeyUnwrap(randBytes(t, 32), wrapped)
	assert.Error(t, err, "unwrap with the wrong KEK must fail the integrity check")
}

func TestListFilesEscapesLikeMetacharacters(t *testing.T) {
	files := []manifestFile{{
		domain:       "HomeDomain",
		relativePath: "Library/Voice_mail/x.amr",
		flags:        1,
		content:      []byte("a"),
	}}
	dir := buildUnencryptedBackup(t, files)
	b, err := Open(dir, "")
	require.NoError(t, err)
	defer func() { require.NoError(t, b.Close()) }()

	got, err := b.ListFiles("HomeDomain", "Library/VoiceXmail/")
	require.NoError(t, err)
	assert.Empty(t, got, "underscore in stored path must not act as a wildcard match target")

	got, err = b.ListFiles("HomeDomain", "Library/Voice_mail/")
	require.NoError(t, err)
	assert.Len(t, got, 1)
}
