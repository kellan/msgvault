package attachmenttier_test

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/msgvault/internal/attachmenttier"
)

type fakeLocal struct {
	content map[string]string
	err     error
}

func (f *fakeLocal) OpenStream(_ context.Context, hash string) (io.ReadCloser, int64, error) {
	if f.err != nil {
		return nil, 0, f.err
	}
	c, ok := f.content[hash]
	if !ok {
		return nil, 0, &fs.PathError{Op: "open CAS blob", Path: hash, Err: fs.ErrNotExist}
	}
	return io.NopCloser(strings.NewReader(c)), int64(len(c)), nil
}

type fakeCatalog struct {
	offloaded map[string]bool
	err       error
}

func (f *fakeCatalog) IsBlobOffloaded(_ context.Context, hash string) (bool, error) {
	return f.offloaded[hash], f.err
}

type fakeRemote struct {
	content map[string]string
	opens   int
}

func (f *fakeRemote) OpenBlob(_ context.Context, hash string) (io.ReadCloser, int64, error) {
	f.opens++
	c, ok := f.content[hash]
	if !ok {
		return nil, 0, &fs.PathError{Op: "open pack", Path: hash, Err: fs.ErrNotExist}
	}
	return io.NopCloser(strings.NewReader(c)), int64(len(c)), nil
}

func newTier(local *fakeLocal, catalog *fakeCatalog, remote *fakeRemote, dialErr error) (*attachmenttier.Store, *int) {
	dials := 0
	return attachmenttier.New(local, catalog, func() (attachmenttier.RemoteReader, error) {
		dials++
		if dialErr != nil {
			return nil, dialErr
		}
		return remote, nil
	}), &dials
}

func TestLocalHitPassesThrough(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	local := &fakeLocal{content: map[string]string{"aa": "local bytes"}}
	tier, dials := newTier(local, &fakeCatalog{}, &fakeRemote{}, nil)

	rc, size, err := tier.OpenStream(context.Background(), "aa")
	require.NoError(err)
	got, _ := io.ReadAll(rc)
	assert.Equal("local bytes", string(got))
	assert.Equal(int64(11), size)
	assert.Equal(0, *dials, "remote must not be dialed on a local hit")
}

func TestLocalMissNotOffloadedPreservesNotExist(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	tier, dials := newTier(&fakeLocal{}, &fakeCatalog{}, &fakeRemote{}, nil)

	_, _, err := tier.OpenStream(context.Background(), "bb")
	require.Error(err)
	assert.ErrorIs(err, fs.ErrNotExist,
		"genuinely missing blobs keep the established miss sentinel")
	assert.NotErrorIs(err, attachmenttier.ErrRemoteUnavailable)
	assert.Equal(0, *dials)
}

func TestLocalErrorOtherThanNotExistPassesThrough(t *testing.T) {
	assert := assert.New(t)
	boom := errors.New("disk on fire")
	tier, dials := newTier(&fakeLocal{err: boom}, &fakeCatalog{}, &fakeRemote{}, nil)

	_, _, err := tier.OpenStream(context.Background(), "cc")
	assert.ErrorIs(err, boom)
	assert.Equal(0, *dials)
}

func TestOffloadedServedFromRemote(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	catalog := &fakeCatalog{offloaded: map[string]bool{"dd": true}}
	remote := &fakeRemote{content: map[string]string{"dd": "remote bytes"}}
	tier, dials := newTier(&fakeLocal{}, catalog, remote, nil)

	rc, size, err := tier.OpenStream(context.Background(), "dd")
	require.NoError(err)
	got, _ := io.ReadAll(rc)
	assert.Equal("remote bytes", string(got))
	assert.Equal(int64(12), size)

	// Second read reuses the dialed reader.
	rc2, _, err := tier.OpenStream(context.Background(), "dd")
	require.NoError(err)
	require.NoError(rc2.Close())
	assert.Equal(1, *dials, "remote reader is dialed once and reused")
	assert.Equal(2, remote.opens)
}

func TestRemoteDialFailureIsUnavailableNotMissing(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	catalog := &fakeCatalog{offloaded: map[string]bool{"ee": true}}
	tier, _ := newTier(&fakeLocal{}, catalog, nil,
		&fs.PathError{Op: "open", Path: "/mnt/repo", Err: fs.ErrNotExist})

	_, _, err := tier.OpenStream(context.Background(), "ee")
	require.Error(err)
	assert.ErrorIs(err, attachmenttier.ErrRemoteUnavailable)
	assert.NotErrorIs(err, fs.ErrNotExist,
		"an unmounted repository must never read as a deleted attachment")
}

func TestRemoteOpenFailureIsUnavailableNotMissing(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	catalog := &fakeCatalog{offloaded: map[string]bool{"ff": true}}
	remote := &fakeRemote{} // blob absent remotely: integrity alarm, still 'unavailable'
	tier, _ := newTier(&fakeLocal{}, catalog, remote, nil)

	_, _, err := tier.OpenStream(context.Background(), "ff")
	require.Error(err)
	assert.ErrorIs(err, attachmenttier.ErrRemoteUnavailable)
	assert.NotErrorIs(err, fs.ErrNotExist)
}

func TestDialRetriedAfterFailure(t *testing.T) {
	require := require.New(t)
	catalog := &fakeCatalog{offloaded: map[string]bool{"aa": true}}
	remote := &fakeRemote{content: map[string]string{"aa": "x"}}
	fail := true
	tier := attachmenttier.New(&fakeLocal{}, catalog, func() (attachmenttier.RemoteReader, error) {
		if fail {
			return nil, errors.New("mount not ready")
		}
		return remote, nil
	})

	_, _, err := tier.OpenStream(context.Background(), "aa")
	require.ErrorIs(err, attachmenttier.ErrRemoteUnavailable)

	fail = false
	rc, _, err := tier.OpenStream(context.Background(), "aa")
	require.NoError(err, "a failed dial must not be cached forever")
	require.NoError(rc.Close())
}

func TestCatalogErrorSurfacesAsItself(t *testing.T) {
	assert := assert.New(t)
	boom := errors.New("database locked")
	tier, _ := newTier(&fakeLocal{}, &fakeCatalog{err: boom}, &fakeRemote{}, nil)

	_, _, err := tier.OpenStream(context.Background(), "gg")
	assert.ErrorIs(err, boom)
	assert.NotErrorIs(err, fs.ErrNotExist,
		"a catalog failure must not be mistaken for a missing blob")
}
