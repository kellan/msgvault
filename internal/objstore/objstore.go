// Package objstore provides read-only ranged access to objects under a
// common interface, backing the remote blob tier's repository reader
// (docs/internal/remote-blob-tier-design.md). Backends: local filesystem
// (reference implementation and test vehicle) and S3-compatible object
// storage. Keys are slash-separated relative paths.
package objstore

import (
	"context"
	"errors"
	"io"
)

// ErrNotExist reports a key with no object behind it. Backends translate
// their native miss (missing file, HTTP 404) to an error satisfying
// errors.Is(err, ErrNotExist).
var ErrNotExist = errors.New("object does not exist")

// Store is a read-only object source. Implementations must be safe for
// concurrent use.
type Store interface {
	// ReadRange streams object bytes [off, off+length). Reading past the
	// end of the object is an error, matching io.ReaderAt semantics for
	// the fixed-range reads pack access performs.
	ReadRange(ctx context.Context, key string, off, length int64) (io.ReadCloser, error)
	// ReadAll buffers a whole small object (repo config, index files,
	// manifests).
	ReadAll(ctx context.Context, key string) ([]byte, error)
	// Size returns the object's total length.
	Size(ctx context.Context, key string) (int64, error)
	// List returns the keys under prefix (recursively), relative to the
	// store root, in unspecified order.
	List(ctx context.Context, prefix string) ([]string, error)
}

// ReaderAt adapts one object to io.ReaderAt for pack.NewReaderFromReaderAt.
// Every ReadAt becomes one ranged read. It intentionally implements no
// Close: the pack reader must not take ownership of anything here.
type ReaderAt struct {
	Ctx   context.Context
	Store Store
	Key   string
	// ObjectSize bounds tail reads: ReadAt(p, off) with off+len(p) past the
	// end returns the available bytes and io.EOF, per the io.ReaderAt
	// contract.
	ObjectSize int64
}

func (r *ReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("objstore: negative offset")
	}
	if off >= r.ObjectSize {
		return 0, io.EOF
	}
	want := int64(len(p))
	eof := false
	if off+want > r.ObjectSize {
		want = r.ObjectSize - off
		eof = true
	}
	rc, err := r.Store.ReadRange(r.Ctx, r.Key, off, want)
	if err != nil {
		return 0, err
	}
	n, err := io.ReadFull(rc, p[:want])
	closeErr := rc.Close()
	if err != nil {
		return n, errors.Join(err, closeErr)
	}
	if closeErr != nil {
		return n, closeErr
	}
	if eof {
		return n, io.EOF
	}
	return n, nil
}
