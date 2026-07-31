package objstore

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// contractObjects is the fixture every backend must serve identically.
var contractObjects = map[string]string{
	"config.toml":          "repo_id = \"x\"\n",
	"indexes/aa.mvidx":     "index-aa-bytes",
	"indexes/bb.mvidx":     "index-bb-bytes-longer",
	"packs/01/01ab.mvpack": "0123456789abcdefghij",
}

// runStoreContract exercises the Store interface semantics shared by all
// backends.
func runStoreContract(t *testing.T, store Store) {
	t.Helper()
	ctx := context.Background()

	t.Run("read all", func(t *testing.T) {
		require := require.New(t)
		assert := assert.New(t)
		data, err := store.ReadAll(ctx, "config.toml")
		require.NoError(err)
		assert.Equal(contractObjects["config.toml"], string(data))
	})

	t.Run("size", func(t *testing.T) {
		require := require.New(t)
		assert := assert.New(t)
		size, err := store.Size(ctx, "packs/01/01ab.mvpack")
		require.NoError(err)
		assert.Equal(int64(20), size)
	})

	t.Run("read range middle", func(t *testing.T) {
		require := require.New(t)
		assert := assert.New(t)
		rc, err := store.ReadRange(ctx, "packs/01/01ab.mvpack", 4, 6)
		require.NoError(err)
		got, err := io.ReadAll(rc)
		require.NoError(err)
		require.NoError(rc.Close())
		assert.Equal("456789", string(got))
	})

	t.Run("list prefix", func(t *testing.T) {
		require := require.New(t)
		assert := assert.New(t)
		keys, err := store.List(ctx, "indexes/")
		require.NoError(err)
		sort.Strings(keys)
		assert.Equal([]string{"indexes/aa.mvidx", "indexes/bb.mvidx"}, keys)
	})

	t.Run("list empty prefix area", func(t *testing.T) {
		require := require.New(t)
		assert := assert.New(t)
		keys, err := store.List(ctx, "snapshots/")
		require.NoError(err)
		assert.Empty(keys)
	})

	t.Run("missing key maps to ErrNotExist", func(t *testing.T) {
		assert := assert.New(t)
		_, err := store.ReadAll(ctx, "indexes/nope.mvidx")
		assert.ErrorIs(err, ErrNotExist)
		_, err = store.Size(ctx, "indexes/nope.mvidx")
		assert.ErrorIs(err, ErrNotExist)
		rc, err := store.ReadRange(ctx, "indexes/nope.mvidx", 0, 4)
		if err == nil {
			// Backends may defer the miss to the first read.
			_, err = io.ReadAll(rc)
			_ = rc.Close()
		}
		assert.ErrorIs(err, ErrNotExist)
	})
}

func newContractFileStore(t *testing.T) *FileStore {
	t.Helper()
	root := t.TempDir()
	for key, content := range contractObjects {
		path := filepath.Join(root, filepath.FromSlash(key))
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	}
	return NewFileStore(root)
}

func TestFileStoreContract(t *testing.T) {
	runStoreContract(t, newContractFileStore(t))
}

func TestFileStoreRejectsEscapingKeys(t *testing.T) {
	assert := assert.New(t)
	store := newContractFileStore(t)
	for _, key := range []string{"../secrets", "a/../../b", ""} {
		_, err := store.ReadAll(context.Background(), key)
		assert.Error(err, "key %q must be rejected", key)
	}
}

func TestReaderAtAdapter(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	store := newContractFileStore(t)
	content := contractObjects["packs/01/01ab.mvpack"]
	ra := &ReaderAt{
		Ctx: context.Background(), Store: store,
		Key: "packs/01/01ab.mvpack", ObjectSize: int64(len(content)),
	}

	buf := make([]byte, 5)
	n, err := ra.ReadAt(buf, 3)
	require.NoError(err)
	assert.Equal(5, n)
	assert.Equal("34567", string(buf))

	// Tail clamp: shorter read plus io.EOF, per the io.ReaderAt contract.
	n, err = ra.ReadAt(buf, int64(len(content))-2)
	assert.Equal(2, n)
	assert.ErrorIs(err, io.EOF)
	assert.Equal("ij", string(buf[:n]))

	_, err = ra.ReadAt(buf, int64(len(content)))
	assert.ErrorIs(err, io.EOF)

	_, err = ra.ReadAt(buf, -1)
	require.Error(err)
}
