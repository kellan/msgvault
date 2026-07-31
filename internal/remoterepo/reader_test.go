package remoterepo_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/kit/backup"
	"go.kenn.io/kit/pack"

	"go.kenn.io/msgvault/internal/remoterepo"
)

// addFixturePack seals one pack containing the given blobs into an existing
// repository and publishes a matching index file, using kit's real writer so
// every integrity layer (footer hash, CRC, blob SHA) is genuine.
func addFixturePack(t *testing.T, root string, blobs ...[]byte) []pack.Entry {
	t.Helper()
	staging := t.TempDir()
	w, err := pack.NewWriter(staging, pack.WriterOptions{})
	require.NoError(t, err)
	for _, raw := range blobs {
		_, err := w.Append(raw)
		require.NoError(t, err)
	}
	packDir := filepath.Join(root, "packs", w.ID()[:2])
	require.NoError(t, os.MkdirAll(packDir, 0o755))
	entries, err := w.Seal(filepath.Join(packDir, w.ID()+".mvpack"))
	require.NoError(t, err)

	repo, err := backup.Open(root)
	require.NoError(t, err)
	indexEntries := make([]backup.IndexEntry, 0, len(entries))
	for _, e := range entries {
		indexEntries = append(indexEntries, backup.IndexEntry{
			Blob: e.ID, PackID: w.ID(), Offset: e.Offset,
			StoredLen: e.StoredLen, Flags: e.Flags,
		})
	}
	_, err = repo.WriteIndex(indexEntries)
	require.NoError(t, err)
	return entries
}

func newFixtureRepo(t *testing.T, blobs ...[]byte) (string, []pack.Entry) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "repo")
	_, err := backup.Init(root)
	require.NoError(t, err)
	entries := addFixturePack(t, root, blobs...)
	return root, entries
}

func TestHasAndRepoID(t *testing.T) {
	content := []byte("attachment bytes for the tier")
	root, entries := newFixtureRepo(t, content)

	r, err := remoterepo.Open(root)
	require.NoError(t, err)
	defer r.Close() //nolint:errcheck

	assert.NotEmpty(t, r.RepoID())

	has, err := r.Has(entries[0].ID.String())
	require.NoError(t, err)
	assert.True(t, has)

	has, err = r.Has("deadbeef" + entries[0].ID.String()[8:])
	require.NoError(t, err)
	assert.False(t, has)

	_, err = r.Has("not-a-hash")
	require.Error(t, err)
}

func TestOpenBlobRoundTrip(t *testing.T) {
	content := []byte("verified round trip through a real pack")
	root, entries := newFixtureRepo(t, content)

	r, err := remoterepo.Open(root)
	require.NoError(t, err)
	defer r.Close() //nolint:errcheck

	rc, size, err := r.OpenBlob(context.Background(), entries[0].ID.String())
	require.NoError(t, err)
	assert.Equal(t, int64(len(content)), size)

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, content, got)
	// Fully consumed: terminal verification ran, Close must be clean.
	require.NoError(t, rc.Close())
}

func TestOpenBlobEarlyCloseReportsIncompleteVerification(t *testing.T) {
	// Use content large enough that opening cannot have buffered it all.
	content := make([]byte, 1<<20)
	for i := range content {
		content[i] = byte(i)
	}
	root, entries := newFixtureRepo(t, content)

	r, err := remoterepo.Open(root)
	require.NoError(t, err)
	defer r.Close() //nolint:errcheck

	rc, _, err := r.OpenBlob(context.Background(), entries[0].ID.String())
	require.NoError(t, err)
	require.Error(t, rc.Close(), "closing before EOF must not report success")
}

func TestOpenBlobFailsClosedOnCorruption(t *testing.T) {
	content := make([]byte, 64<<10)
	for i := range content {
		content[i] = 0xAB
	}
	root, entries := newFixtureRepo(t, content)

	// Flip one byte in the middle of the pack's data region.
	var packPath string
	require.NoError(t, filepath.Walk(filepath.Join(root, "packs"),
		func(path string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() {
				packPath = path
			}
			return err
		}))
	require.NotEmpty(t, packPath)
	f, err := os.OpenFile(packPath, os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{0xCD}, int64(entries[0].Offset)+100)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	r, err := remoterepo.Open(root)
	require.NoError(t, err)
	defer r.Close() //nolint:errcheck

	rc, _, err := r.OpenBlob(context.Background(), entries[0].ID.String())
	if err != nil {
		return // failed at open: acceptable, corruption detected
	}
	_, readErr := io.ReadAll(rc)
	closeErr := rc.Close()
	assert.Error(t, errors.Join(readErr, closeErr),
		"corrupted content must fail read or close, never verify")
}

func TestOpenBlobReloadsIndexOnMiss(t *testing.T) {
	first := []byte("blob present at open time")
	root, _ := newFixtureRepo(t, first)

	r, err := remoterepo.Open(root)
	require.NoError(t, err)
	defer r.Close() //nolint:errcheck

	// Force the initial index load.
	_, err = r.Has(pack.ComputeBlobID(first).String())
	require.NoError(t, err)

	// A second pack + index published after open (a newer backup run).
	second := []byte("blob published after the reader opened")
	entries := addFixturePack(t, root, second)

	rc, _, err := r.OpenBlob(context.Background(), entries[0].ID.String())
	require.NoError(t, err, "reader must reload the index once on miss")
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, second, got)
	require.NoError(t, rc.Close())
}

func TestOpenBlobUnknownHash(t *testing.T) {
	root, _ := newFixtureRepo(t, []byte("only blob"))

	r, err := remoterepo.Open(root)
	require.NoError(t, err)
	defer r.Close() //nolint:errcheck

	missing := pack.ComputeBlobID([]byte("never stored")).String()
	_, _, err = r.OpenBlob(context.Background(), missing)
	require.Error(t, err)
	assert.ErrorIs(t, err, remoterepo.ErrBlobNotFound)
}

func TestOpenRefusesEncryptedRepository(t *testing.T) {
	root, _ := newFixtureRepo(t, []byte("blob"))
	cfgPath := filepath.Join(root, "config.toml")
	cfg, err := os.ReadFile(cfgPath)
	require.NoError(t, err)
	require.Contains(t, string(cfg), `encryption = "none"`)
	tampered := strings.Replace(string(cfg), `encryption = "none"`, `encryption = "age"`, 1)
	require.NoError(t, os.WriteFile(cfgPath, []byte(tampered), 0o600))

	_, err = remoterepo.Open(root)
	require.Error(t, err)
}
