package applebackup

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"

	_ "github.com/mattn/go-sqlite3" // SQLite driver for Manifest.db
)

// Backup is an opened iTunes/Finder-style device backup directory.
type Backup struct {
	dir       string
	encrypted bool
	keybag    *keybag
	db        *sql.DB
	tempDir   string // holds the decrypted Manifest.db for encrypted backups
}

// ErrPasswordRequired is returned by Open for an encrypted backup when no
// password was supplied.
var ErrPasswordRequired = errors.New(
	"backup is encrypted: a backup password is required",
)

// Open opens a backup directory (the folder containing Manifest.plist and
// Manifest.db). For encrypted backups the password is used to unlock the
// keybag and decrypt Manifest.db; only files later read through the Backup
// are decrypted, never the whole backup.
func Open(dir, password string) (*Backup, error) {
	plistPath := filepath.Join(dir, "Manifest.plist")
	plistData, err := os.ReadFile(plistPath)
	if err != nil {
		return nil, fmt.Errorf(
			"read %s: %w (is this a backup directory?)", plistPath, err,
		)
	}
	manifest, err := parseManifestPlist(plistData)
	if err != nil {
		return nil, err
	}

	b := &Backup{dir: dir, encrypted: manifest.IsEncrypted}

	dbPath := filepath.Join(dir, "Manifest.db")
	if !manifest.IsEncrypted {
		if err := b.openManifestDB(dbPath); err != nil {
			return nil, err
		}
		return b, nil
	}

	if password == "" {
		return nil, ErrPasswordRequired
	}
	if len(manifest.BackupKeyBag) == 0 {
		return nil, errors.New("encrypted backup is missing BackupKeyBag")
	}
	kb, err := parseKeybag(manifest.BackupKeyBag)
	if err != nil {
		return nil, fmt.Errorf("parse keybag: %w", err)
	}
	if err := kb.unlock(password); err != nil {
		return nil, err
	}
	b.keybag = kb

	ciphertext, err := os.ReadFile(dbPath)
	if err != nil {
		return nil, fmt.Errorf("read Manifest.db: %w", err)
	}
	if len(manifest.ManifestKey) == 0 {
		return nil, errors.New("encrypted backup is missing ManifestKey")
	}
	plaintext, err := decryptManifestDB(kb, manifest.ManifestKey, ciphertext)
	if err != nil {
		return nil, err
	}

	tempDir, err := os.MkdirTemp("", "msgvault-applebackup-")
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}
	decryptedPath := filepath.Join(tempDir, "Manifest.db")
	if err := os.WriteFile(decryptedPath, plaintext, 0600); err != nil {
		_ = os.RemoveAll(tempDir)
		return nil, fmt.Errorf("write decrypted Manifest.db: %w", err)
	}
	b.tempDir = tempDir
	if err := b.openManifestDB(decryptedPath); err != nil {
		_ = os.RemoveAll(tempDir)
		return nil, err
	}
	return b, nil
}

// IsEncrypted reports whether the backup at dir is password-protected,
// reading only Manifest.plist. Useful for deciding whether to prompt for a
// password before opening the backup.
func IsEncrypted(dir string) (bool, error) {
	data, err := os.ReadFile(filepath.Join(dir, "Manifest.plist"))
	if err != nil {
		return false, fmt.Errorf("read Manifest.plist: %w", err)
	}
	m, err := parseManifestPlist(data)
	if err != nil {
		return false, err
	}
	return m.IsEncrypted, nil
}

func (b *Backup) openManifestDB(path string) error {
	db, err := sql.Open(
		"sqlite3", fmt.Sprintf("file:%s?mode=ro&_busy_timeout=5000", path),
	)
	if err != nil {
		return fmt.Errorf("open Manifest.db: %w", err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return fmt.Errorf("connect to Manifest.db: %w", err)
	}
	b.db = db
	return nil
}

// Encrypted reports whether the backup is password-protected.
func (b *Backup) Encrypted() bool { return b.encrypted }

// Close releases the Manifest.db connection and removes any decrypted
// temporary files.
func (b *Backup) Close() error {
	var err error
	if b.db != nil {
		err = b.db.Close()
		b.db = nil
	}
	if b.tempDir != "" {
		if rmErr := os.RemoveAll(b.tempDir); err == nil {
			err = rmErr
		}
		b.tempDir = ""
	}
	return err
}

// ListFiles returns the backup's file records for one domain whose
// relative paths start with pathPrefix, ordered by relative path. Entries
// that are not regular files (directories, symlinks) are skipped.
func (b *Backup) ListFiles(domain, pathPrefix string) ([]FileRecord, error) {
	rows, err := b.db.Query(
		`SELECT fileID, domain, relativePath, flags, file
		 FROM Files
		 WHERE domain = ? AND relativePath LIKE ? ESCAPE '\'
		 ORDER BY relativePath`,
		domain, escapeLike(pathPrefix)+"%",
	)
	if err != nil {
		return nil, fmt.Errorf("query Manifest.db files: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var records []FileRecord
	for rows.Next() {
		var rec FileRecord
		var blob []byte
		if err := rows.Scan(
			&rec.FileID, &rec.Domain, &rec.RelativePath, &rec.Flags, &blob,
		); err != nil {
			return nil, fmt.Errorf("scan Manifest.db row: %w", err)
		}
		// flags: 1 = file, 2 = directory, 4 = symlink.
		if rec.Flags != 1 {
			continue
		}
		if len(blob) > 0 {
			size, class, wrappedKey, err := parseFileMetadata(blob)
			if err != nil {
				return nil, fmt.Errorf(
					"parse metadata for %s: %w", rec.RelativePath, err,
				)
			}
			rec.Size = size
			rec.protectionClass = class
			rec.wrappedKey = wrappedKey
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate Manifest.db rows: %w", err)
	}
	return records, nil
}

// ReadFile returns the plaintext contents of one backed-up file,
// decrypting it when the backup is encrypted.
func (b *Backup) ReadFile(rec FileRecord) ([]byte, error) {
	path := filepath.Join(b.dir, rec.FileID[:2], rec.FileID)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read backup file %s: %w", rec.RelativePath, err)
	}
	if !b.encrypted {
		return data, nil
	}
	if len(rec.wrappedKey) == 0 {
		return nil, fmt.Errorf(
			"no encryption key recorded for %s", rec.RelativePath,
		)
	}
	key, err := unwrapFileKey(b.keybag, rec.wrappedKey)
	if err != nil {
		return nil, fmt.Errorf("unwrap key for %s: %w", rec.RelativePath, err)
	}
	plaintext, err := decryptCBC(key, data)
	if err != nil {
		return nil, fmt.Errorf("decrypt %s: %w", rec.RelativePath, err)
	}
	if rec.Size > 0 && rec.Size <= int64(len(plaintext)) {
		plaintext = plaintext[:rec.Size]
	}
	return plaintext, nil
}

// DefaultBackupRoot returns the platform's MobileSync backup root
// directory ("" when the platform has no conventional location).
func DefaultBackupRoot() string {
	switch runtime.GOOS {
	case "darwin":
		home, err := os.UserHomeDir()
		if err != nil {
			return ""
		}
		return filepath.Join(
			home, "Library", "Application Support", "MobileSync", "Backup",
		)
	case "windows":
		appData := os.Getenv("APPDATA")
		if appData == "" {
			return ""
		}
		return filepath.Join(appData, "Apple Computer", "MobileSync", "Backup")
	}
	return ""
}

// Discover resolves a path argument to a backup directory. A directory
// containing Manifest.db is returned as-is; otherwise the most recently
// modified child directory containing Manifest.db is chosen (the layout of
// a MobileSync Backup root holding per-device UDID folders).
func Discover(path string) (string, error) {
	if path == "" {
		return "", errors.New("no backup path given")
	}
	if isBackupDir(path) {
		return path, nil
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		return "", fmt.Errorf("read backup root %s: %w", path, err)
	}
	type candidate struct {
		dir     string
		modTime int64
	}
	var candidates []candidate
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(path, e.Name())
		if !isBackupDir(dir) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		candidates = append(candidates, candidate{dir, info.ModTime().UnixNano()})
	}
	if len(candidates) == 0 {
		return "", fmt.Errorf(
			"no device backups found under %s (no subdirectory contains Manifest.db)",
			path,
		)
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].modTime > candidates[j].modTime
	})
	return candidates[0].dir, nil
}

func isBackupDir(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, "Manifest.db"))
	return err == nil
}

func escapeLike(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '%', '_', '\\':
			out = append(out, '\\')
		}
		out = append(out, s[i])
	}
	return string(out)
}
