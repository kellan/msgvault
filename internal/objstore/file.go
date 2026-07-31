package objstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// FileStore serves objects from a directory root. It is the reference
// backend and the test vehicle for object-backed repository reads.
type FileStore struct {
	root string
}

// NewFileStore roots a store at dir.
func NewFileStore(dir string) *FileStore { return &FileStore{root: dir} }

func (s *FileStore) path(key string) (string, error) {
	native := filepath.FromSlash(key)
	if key == "" || !filepath.IsLocal(native) {
		return "", fmt.Errorf("objstore: invalid key %q", key)
	}
	return filepath.Join(s.root, native), nil
}

func (s *FileStore) ReadRange(_ context.Context, key string, off, length int64) (io.ReadCloser, error) {
	path, err := s.path(key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, translateFSErr(key, err)
	}
	return &sectionReadCloser{SectionReader: io.NewSectionReader(f, off, length), f: f}, nil
}

type sectionReadCloser struct {
	*io.SectionReader
	f *os.File
}

func (s *sectionReadCloser) Close() error { return s.f.Close() }

func (s *FileStore) ReadAll(_ context.Context, key string) ([]byte, error) {
	path, err := s.path(key)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, translateFSErr(key, err)
	}
	return data, nil
}

func (s *FileStore) Size(_ context.Context, key string) (int64, error) {
	path, err := s.path(key)
	if err != nil {
		return 0, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0, translateFSErr(key, err)
	}
	return info.Size(), nil
}

func (s *FileStore) List(_ context.Context, prefix string) ([]string, error) {
	dir, err := s.path(strings.TrimSuffix(prefix, "/"))
	if err != nil {
		return nil, err
	}
	var keys []string
	err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(s.root, path)
		if err != nil {
			return err
		}
		keys = append(keys, filepath.ToSlash(rel))
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil // empty prefix is an empty listing, not an error
	}
	if err != nil {
		return nil, fmt.Errorf("objstore: listing %s: %w", prefix, err)
	}
	return keys, nil
}

func translateFSErr(key string, err error) error {
	if errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("objstore: %s: %w", key, ErrNotExist)
	}
	return err
}
