// Package attachmenttier decorates the daemon's attachment blob store with
// a remote fallthrough: on a local miss, a blob the offload catalog marks
// as evicted is served from the configured backup repository instead
// (docs/internal/remote-blob-tier-design.md).
//
// The decorator preserves the two load-bearing error contracts of the read
// path: a genuinely missing blob still satisfies errors.Is(err,
// fs.ErrNotExist), and a remote-tier failure never does — an unmounted or
// unreadable repository must surface as "stored remotely, currently
// unreachable", never as "attachment deleted".
package attachmenttier

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sync"
)

// ErrRemoteUnavailable reports that a blob is recorded as offloaded but the
// remote repository could not serve it. It deliberately never wraps the
// underlying cause with %w: a remote *fs.PathError left in the chain would
// satisfy fs.ErrNotExist and collapse the distinction this package exists
// to make.
var ErrRemoteUnavailable = errors.New("remote blob tier unavailable")

// LocalStore is the daemon's existing attachment read path
// (api.AttachmentBlobStore's method set).
type LocalStore interface {
	OpenStream(ctx context.Context, hash string) (io.ReadCloser, int64, error)
}

// Catalog answers whether a blob's local bytes were deliberately evicted.
type Catalog interface {
	IsBlobOffloaded(ctx context.Context, hash string) (bool, error)
}

// RemoteReader serves verified blob streams from the backup repository.
type RemoteReader interface {
	OpenBlob(ctx context.Context, hash string) (io.ReadCloser, int64, error)
}

// Store is the tiered decorator. It implements both api.AttachmentBlobStore
// and backupapp.BlobStore.
type Store struct {
	local      LocalStore
	catalog    Catalog
	openRemote func() (RemoteReader, error)

	mu     sync.Mutex
	remote RemoteReader
}

// New builds the decorator. openRemote is invoked lazily on the first
// remote read and the result cached; a failed dial is retried on the next
// read so a repository mounted after daemon start becomes reachable
// without a restart.
func New(local LocalStore, catalog Catalog, openRemote func() (RemoteReader, error)) *Store {
	return &Store{local: local, catalog: catalog, openRemote: openRemote}
}

// OpenStream serves hash from local storage, falling through to the remote
// repository only for blobs the catalog records as offloaded.
func (t *Store) OpenStream(ctx context.Context, hash string) (io.ReadCloser, int64, error) {
	rc, size, err := t.local.OpenStream(ctx, hash)
	if err == nil || !errors.Is(err, fs.ErrNotExist) {
		return rc, size, err
	}

	offloaded, catalogErr := t.catalog.IsBlobOffloaded(ctx, hash)
	if catalogErr != nil {
		return nil, 0, fmt.Errorf("check offload catalog for %s: %w", hash, catalogErr)
	}
	if !offloaded {
		return nil, 0, err // preserve the local fs.ErrNotExist verbatim
	}

	remote, dialErr := t.remoteReader()
	if dialErr != nil {
		return nil, 0, fmt.Errorf("%w: opening repository: %v", ErrRemoteUnavailable, dialErr)
	}
	rc, size, remoteErr := remote.OpenBlob(ctx, hash)
	if remoteErr != nil {
		return nil, 0, fmt.Errorf("%w: reading blob %s: %v", ErrRemoteUnavailable, hash, remoteErr)
	}
	return rc, size, nil
}

func (t *Store) remoteReader() (RemoteReader, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.remote != nil {
		return t.remote, nil
	}
	remote, err := t.openRemote()
	if err != nil {
		return nil, err
	}
	t.remote = remote
	return remote, nil
}
