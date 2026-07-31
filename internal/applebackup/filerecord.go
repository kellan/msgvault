package applebackup

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"

	"howett.net/plist"
)

// FileRecord is one row of Manifest.db's Files table: a file captured in
// the backup, addressed on disk as <backupDir>/<fileID[:2]>/<fileID>.
type FileRecord struct {
	FileID       string
	Domain       string
	RelativePath string
	Flags        int64

	// Size is the plaintext length recorded in the file's metadata plist
	// (encrypted backups pad ciphertext to the AES block size).
	Size int64
	// protectionClass and wrappedKey decrypt the file's contents in an
	// encrypted backup. wrappedKey is empty for unencrypted backups and
	// for non-file entries (directories, symlinks).
	protectionClass uint32
	wrappedKey      []byte
}

// parseFileMetadata extracts Size, ProtectionClass, and the wrapped
// per-file encryption key from the NSKeyedArchiver plist stored in
// Manifest.db's Files.file column.
func parseFileMetadata(blob []byte) (size int64, class uint32, wrappedKey []byte, err error) {
	var archive struct {
		Objects []interface{}          `plist:"$objects"`
		Top     map[string]interface{} `plist:"$top"`
	}
	if _, err := plist.Unmarshal(blob, &archive); err != nil {
		return 0, 0, nil, fmt.Errorf("decode file metadata plist: %w", err)
	}

	root, err := derefArchiveObject(archive.Objects, archive.Top["root"])
	if err != nil {
		return 0, 0, nil, fmt.Errorf("file metadata root: %w", err)
	}
	rootDict, ok := root.(map[string]interface{})
	if !ok {
		return 0, 0, nil, errors.New("file metadata root is not a dictionary")
	}

	size = archiveInt(rootDict["Size"])
	class = uint32(archiveInt(rootDict["ProtectionClass"]))

	if ref, ok := rootDict["EncryptionKey"]; ok {
		keyObj, err := derefArchiveObject(archive.Objects, ref)
		if err != nil {
			return 0, 0, nil, fmt.Errorf("file metadata EncryptionKey: %w", err)
		}
		if keyDict, ok := keyObj.(map[string]interface{}); ok {
			if data, ok := keyDict["NS.data"].([]byte); ok {
				wrappedKey = data
			}
		}
	}

	return size, class, wrappedKey, nil
}

// derefArchiveObject resolves a $objects reference: plist UIDs point into
// the $objects array; anything else is already a value.
func derefArchiveObject(objects []interface{}, ref interface{}) (interface{}, error) {
	uid, ok := ref.(plist.UID)
	if !ok {
		return ref, nil
	}
	if int(uid) >= len(objects) {
		return nil, fmt.Errorf("archive UID %d out of range", uid)
	}
	return objects[uid], nil
}

func archiveInt(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case uint64:
		return int64(n)
	case int:
		return int64(n)
	case float64:
		return int64(n)
	}
	return 0
}

// unwrapFileKey splits the EncryptionKey NS.data payload (4-byte
// little-endian protection class followed by the wrapped key) and unwraps
// it with the matching class key.
func unwrapFileKey(kb *keybag, wrappedWithClass []byte) ([]byte, error) {
	if len(wrappedWithClass) < 4+wrappedKeyLen {
		return nil, fmt.Errorf(
			"encryption key blob too short (%d bytes)", len(wrappedWithClass),
		)
	}
	class := binary.LittleEndian.Uint32(wrappedWithClass[:4])
	return kb.unwrapKeyForClass(class, wrappedWithClass[4:])
}

// decryptCBC decrypts AES-256-CBC ciphertext with a zero IV, the scheme
// iOS backups use for both Manifest.db and file contents.
func decryptCBC(key, ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("file cipher: %w", err)
	}
	if len(ciphertext)%aes.BlockSize != 0 {
		return nil, fmt.Errorf(
			"ciphertext length %d is not a multiple of the AES block size",
			len(ciphertext),
		)
	}
	iv := make([]byte, aes.BlockSize)
	out := make([]byte, len(ciphertext))
	cipher.NewCBCDecrypter(block, iv).CryptBlocks(out, ciphertext)
	return out, nil
}

// manifestPlist is the subset of Manifest.plist applebackup needs.
type manifestPlist struct {
	BackupKeyBag []byte `plist:"BackupKeyBag"`
	ManifestKey  []byte `plist:"ManifestKey"`
	IsEncrypted  bool   `plist:"IsEncrypted"`
}

func parseManifestPlist(data []byte) (*manifestPlist, error) {
	var m manifestPlist
	if _, err := plist.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("decode Manifest.plist: %w", err)
	}
	return &m, nil
}

// decryptManifestDB unwraps the ManifestKey (4-byte little-endian class +
// wrapped key) and decrypts the Manifest.db ciphertext. The decrypted
// output may retain AES padding past the SQLite page data, which SQLite
// ignores.
func decryptManifestDB(kb *keybag, manifestKey, ciphertext []byte) ([]byte, error) {
	if len(manifestKey) < 4+wrappedKeyLen {
		return nil, errors.New("ManifestKey too short")
	}
	class := binary.LittleEndian.Uint32(manifestKey[:4])
	key, err := kb.unwrapKeyForClass(class, manifestKey[4:])
	if err != nil {
		return nil, fmt.Errorf("unwrap manifest key: %w", err)
	}
	plaintext, err := decryptCBC(key, ciphertext)
	if err != nil {
		return nil, err
	}
	if !bytes.HasPrefix(plaintext, []byte("SQLite format 3\x00")) {
		return nil, errors.New(
			"decrypted Manifest.db is not a SQLite database (wrong backup password?)",
		)
	}
	return plaintext, nil
}
