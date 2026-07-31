// Package remoterepo reads individual content-addressed blobs out of a
// msgvault backup repository (docs/internal/remote-blob-tier-design.md).
// It is strictly read-only and takes no repository locks: packs and index
// files are immutable and every read is self-verifying, so the worst a
// concurrent `backup create` can cause is a stale index — handled by one
// bounded reload on miss, the same idiom the local packstore uses for
// pack migration races.
//
// This slice supports repositories reachable as a filesystem path (external
// drive, NAS mount, rclone mount). Object-store backends arrive once kit's
// pack reader accepts an io.ReaderAt.
package remoterepo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"go.kenn.io/kit/backup"
	"go.kenn.io/kit/pack"
)

// packExt is msgvault's frozen pack file extension, matching
// backupapp.App.PackFileExtension.
const packExt = ".mvpack"

// ErrBlobNotFound reports a hash absent from the repository's index even
// after a reload. For a blob the offload catalog claims is in this
// repository, that is an integrity alarm, not a transient condition.
var ErrBlobNotFound = errors.New("blob not present in backup repository")

// Reader resolves content hashes to verified blob streams against one
// backup repository.
type Reader struct {
	repo *backup.Repo

	mu    sync.Mutex
	index map[pack.BlobID]backup.IndexEntry
}

// Open opens the repository rooted at path. Kit validates the format and
// minimum reader version and refuses encrypted repositories.
func Open(root string) (*Reader, error) {
	repo, err := backup.Open(root)
	if err != nil {
		return nil, fmt.Errorf("open offload repository: %w", err)
	}
	return &Reader{repo: repo}, nil
}

// RepoID returns the repository's stable identity from its config.
func (r *Reader) RepoID() string { return r.repo.Config().RepoID }

// LatestSnapshot returns the newest snapshot manifest, or nil when the
// repository has none.
func (r *Reader) LatestSnapshot() (*backup.Manifest, error) {
	m, err := r.repo.LatestSnapshot()
	if err != nil {
		return nil, fmt.Errorf("read latest snapshot: %w", err)
	}
	return m, nil
}

// Has reports whether hash is resolvable through the currently loaded
// index union, loading it on first use. It does not reload on absence;
// callers that need freshness call Refresh first (the offload command) or
// rely on OpenBlob's bounded reload (the serving path).
func (r *Reader) Has(hash string) (bool, error) {
	id, err := pack.ParseBlobID(hash)
	if err != nil {
		return false, fmt.Errorf("parse blob hash %q: %w", hash, err)
	}
	index, err := r.loadedIndex(false)
	if err != nil {
		return false, err
	}
	_, ok := index[id]
	return ok, nil
}

// Refresh forces the next resolution to see index files published since
// the last load.
func (r *Reader) Refresh() error {
	_, err := r.loadedIndex(true)
	return err
}

// OpenBlob returns a verified stream of the blob's raw content and its
// size. The stream must be consumed through EOF; closing earlier returns a
// verification-incomplete error, identical to local packed reads. A hash
// missing from the index is retried after exactly one index reload before
// failing with ErrBlobNotFound.
func (r *Reader) OpenBlob(ctx context.Context, hash string) (io.ReadCloser, int64, error) {
	id, err := pack.ParseBlobID(hash)
	if err != nil {
		return nil, 0, fmt.Errorf("parse blob hash %q: %w", hash, err)
	}

	index, err := r.loadedIndex(false)
	if err != nil {
		return nil, 0, err
	}
	if _, ok := index[id]; !ok {
		if index, err = r.loadedIndex(true); err != nil {
			return nil, 0, err
		}
		if _, ok := index[id]; !ok {
			return nil, 0, fmt.Errorf("%w: %s", ErrBlobNotFound, hash)
		}
	}

	stream, err := r.repo.OpenBlob(ctx, index, id, nil, packExt)
	if err != nil {
		return nil, 0, fmt.Errorf("open repository blob %s: %w", hash, err)
	}
	return stream, stream.Size(), nil
}

// loadedIndex returns the index union, loading it when absent or when
// reload is set. The load reads and SHA-verifies every index file, so
// callers keep reloads rare (once at open, once per miss).
func (r *Reader) loadedIndex(reload bool) (map[pack.BlobID]backup.IndexEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.index != nil && !reload {
		return r.index, nil
	}
	index, err := r.repo.LoadBlobIndex()
	if err != nil {
		return nil, fmt.Errorf("load repository blob index: %w", err)
	}
	r.index = index
	return index, nil
}

// Close releases the reader. The kit repository handle holds no
// descriptors between reads, so this only drops the cached index.
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.index = nil
	return nil
}
