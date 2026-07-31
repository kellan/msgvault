// Package applebackup reads iTunes/Finder-style iPhone backups: it parses
// Manifest.plist and Manifest.db, and decrypts password-protected backups
// (keybag key derivation plus per-file AES keys) so callers can locate and
// extract individual files by domain and relative path without decrypting
// the whole backup.
package applebackup

import (
	"bytes"
	"crypto/aes"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"golang.org/x/crypto/pbkdf2"
)

// wrapPasscode marks a class key as wrapped with the passcode-derived key.
// (Bit 0x1 marks device-key wrapping, which cannot be undone off-device;
// backup keybags use passcode wrapping only.)
const wrapPasscode = 2

const (
	passcodeKeyLen = 32
	wrappedKeyLen  = 40 // 32-byte AES key + 8-byte RFC 3394 integrity block
)

// classKey is one protection-class entry in a keybag.
type classKey struct {
	class   uint32
	wrap    uint32
	wrapped []byte // WPKY: wrapped key material
	key     []byte // unwrapped key, populated by unlock
}

// keybag is a parsed BackupKeyBag blob from Manifest.plist.
type keybag struct {
	attrs     map[string][]byte
	classKeys map[uint32]*classKey
	unlocked  bool
}

// parseKeybag parses the TLV-encoded BackupKeyBag blob: a sequence of
// records with a 4-byte ASCII tag, a 4-byte big-endian length, and a value.
// Header attributes (SALT, ITER, DPSL, DPIC, ...) come first; each
// subsequent UUID tag starts a new protection-class block (CLAS, WRAP,
// WPKY, ...).
func parseKeybag(data []byte) (*keybag, error) {
	kb := &keybag{
		attrs:     map[string][]byte{},
		classKeys: map[uint32]*classKey{},
	}

	var current map[string][]byte
	flushCurrent := func() error {
		if current == nil {
			return nil
		}
		clas, ok := current["CLAS"]
		if !ok || len(clas) != 4 {
			return errors.New("keybag class block missing CLAS")
		}
		ck := &classKey{class: binary.BigEndian.Uint32(clas)}
		if w, ok := current["WRAP"]; ok && len(w) == 4 {
			ck.wrap = binary.BigEndian.Uint32(w)
		}
		if wpky, ok := current["WPKY"]; ok {
			ck.wrapped = wpky
		}
		kb.classKeys[ck.class] = ck
		current = nil
		return nil
	}

	seenHeaderUUID := false
	for off := 0; off < len(data); {
		if off+8 > len(data) {
			return nil, errors.New("truncated keybag record header")
		}
		tag := string(data[off : off+4])
		length := int(binary.BigEndian.Uint32(data[off+4 : off+8]))
		off += 8
		if length < 0 || off+length > len(data) {
			return nil, fmt.Errorf("truncated keybag record %q", tag)
		}
		value := data[off : off+length]
		off += length

		switch {
		case tag == "UUID" && !seenHeaderUUID:
			seenHeaderUUID = true
			kb.attrs[tag] = value
		case tag == "UUID":
			if err := flushCurrent(); err != nil {
				return nil, err
			}
			current = map[string][]byte{"UUID": value}
		case current != nil:
			current[tag] = value
		default:
			kb.attrs[tag] = value
		}
	}
	if err := flushCurrent(); err != nil {
		return nil, err
	}
	if len(kb.classKeys) == 0 {
		return nil, errors.New("keybag contains no class keys")
	}
	return kb, nil
}

// unlock derives the passcode key from the backup password and unwraps
// every passcode-wrapped class key.
//
// iOS 10.2+ keybags carry DPSL/DPIC and use a double derivation:
// PBKDF2-SHA256(password, DPSL, DPIC) feeds PBKDF2-SHA1(·, SALT, ITER).
// Older keybags use the SHA1 stage only.
func (kb *keybag) unlock(password string) error {
	salt, ok := kb.attrs["SALT"]
	if !ok {
		return errors.New("keybag missing SALT")
	}
	iter, err := kb.attrUint32("ITER")
	if err != nil {
		return err
	}

	pass := []byte(password)
	if dpsl, ok := kb.attrs["DPSL"]; ok {
		dpic, err := kb.attrUint32("DPIC")
		if err != nil {
			return err
		}
		pass = pbkdf2.Key(pass, dpsl, int(dpic), passcodeKeyLen, sha256.New)
	}
	passcodeKey := pbkdf2.Key(pass, salt, int(iter), passcodeKeyLen, sha1.New)

	unwrappedAny := false
	for _, ck := range kb.classKeys {
		if ck.wrap&wrapPasscode == 0 || len(ck.wrapped) == 0 {
			continue
		}
		key, err := aesKeyUnwrap(passcodeKey, ck.wrapped)
		if err != nil {
			return fmt.Errorf(
				"unwrap class %d key: %w (wrong backup password?)",
				ck.class, err,
			)
		}
		ck.key = key
		unwrappedAny = true
	}
	if !unwrappedAny {
		return errors.New("keybag has no passcode-wrapped class keys")
	}
	kb.unlocked = true
	return nil
}

// unwrapKeyForClass unwraps a per-file (or manifest) key that was wrapped
// with the given protection class's key.
func (kb *keybag) unwrapKeyForClass(class uint32, wrapped []byte) ([]byte, error) {
	if !kb.unlocked {
		return nil, errors.New("keybag is locked")
	}
	ck, ok := kb.classKeys[class]
	if !ok || ck.key == nil {
		return nil, fmt.Errorf("no unwrapped key for protection class %d", class)
	}
	return aesKeyUnwrap(ck.key, wrapped)
}

func (kb *keybag) attrUint32(tag string) (uint32, error) {
	v, ok := kb.attrs[tag]
	if !ok || len(v) != 4 {
		return 0, fmt.Errorf("keybag missing %s", tag)
	}
	return binary.BigEndian.Uint32(v), nil
}

// aesKeyUnwrap implements RFC 3394 AES key unwrapping with the default IV.
func aesKeyUnwrap(kek, wrapped []byte) ([]byte, error) {
	if len(wrapped) < 16 || len(wrapped)%8 != 0 {
		return nil, fmt.Errorf("invalid wrapped key length %d", len(wrapped))
	}
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, fmt.Errorf("wrap cipher: %w", err)
	}

	n := len(wrapped)/8 - 1
	a := make([]byte, 8)
	copy(a, wrapped[:8])
	r := make([][]byte, n+1) // r[1..n]
	for i := 1; i <= n; i++ {
		r[i] = make([]byte, 8)
		copy(r[i], wrapped[i*8:(i+1)*8])
	}

	buf := make([]byte, 16)
	for j := 5; j >= 0; j-- {
		for i := n; i >= 1; i-- {
			t := uint64(n*j + i)
			copy(buf[:8], a)
			binary.BigEndian.PutUint64(buf[:8], binary.BigEndian.Uint64(a)^t)
			copy(buf[8:], r[i])
			block.Decrypt(buf, buf)
			copy(a, buf[:8])
			copy(r[i], buf[8:])
		}
	}

	iv := []byte{0xA6, 0xA6, 0xA6, 0xA6, 0xA6, 0xA6, 0xA6, 0xA6}
	if !bytes.Equal(a, iv) {
		return nil, errors.New("key unwrap integrity check failed")
	}
	out := make([]byte, 0, n*8)
	for i := 1; i <= n; i++ {
		out = append(out, r[i]...)
	}
	return out, nil
}
