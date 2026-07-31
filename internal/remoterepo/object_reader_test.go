package remoterepo_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/kit/backup"
	"go.kenn.io/kit/pack"

	"go.kenn.io/msgvault/internal/objstore"
	"go.kenn.io/msgvault/internal/remoterepo"
)

func openObjectFixture(t *testing.T, root string) *remoterepo.ObjectReader {
	t.Helper()
	r, err := remoterepo.OpenObjectStore(context.Background(), objstore.NewFileStore(root))
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func TestObjectReaderRoundTrip(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	content := []byte("object-backed round trip through a real pack")
	root, entries := newFixtureRepo(t, content)
	r := openObjectFixture(t, root)

	assert.NotEmpty(r.RepoID())

	has, err := r.Has(entries[0].ID.String())
	require.NoError(err)
	assert.True(has)

	rc, size, err := r.OpenBlob(context.Background(), entries[0].ID.String())
	require.NoError(err)
	assert.Equal(int64(len(content)), size)
	got, err := io.ReadAll(rc)
	require.NoError(err)
	assert.Equal(content, got)
	require.NoError(rc.Close(), "fully consumed stream closes clean")
}

func TestObjectReaderEarlyCloseReportsIncompleteVerification(t *testing.T) {
	require := require.New(t)
	content := make([]byte, 1<<20)
	for i := range content {
		content[i] = byte(i * 7)
	}
	root, entries := newFixtureRepo(t, content)
	r := openObjectFixture(t, root)

	rc, _, err := r.OpenBlob(context.Background(), entries[0].ID.String())
	require.NoError(err)
	require.Error(rc.Close(), "closing before EOF must not report success")
}

func TestObjectReaderReloadsIndexOnMiss(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	first := []byte("object blob present at open time")
	root, _ := newFixtureRepo(t, first)
	r := openObjectFixture(t, root)

	_, err := r.Has(pack.ComputeBlobID(first).String())
	require.NoError(err)

	second := []byte("object blob published after open")
	entries := addFixturePack(t, root, second)

	rc, _, err := r.OpenBlob(context.Background(), entries[0].ID.String())
	require.NoError(err, "reader must reload the index once on miss")
	got, err := io.ReadAll(rc)
	require.NoError(err)
	assert.Equal(second, got)
	require.NoError(rc.Close())

	missing := pack.ComputeBlobID([]byte("never stored anywhere")).String()
	_, _, err = r.OpenBlob(context.Background(), missing)
	assert.ErrorIs(err, remoterepo.ErrBlobNotFound)
}

func TestObjectReaderLatestSnapshot(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	root, _ := newFixtureRepo(t, []byte("snapshot fixture blob"))
	r := openObjectFixture(t, root)

	latest, err := r.LatestSnapshot()
	require.NoError(err)
	assert.Nil(latest, "no snapshots yet")

	repo, err := backup.Open(root)
	require.NoError(err)
	older := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	for _, created := range []time.Time{older, newer} {
		_, err = repo.WriteManifest(&backup.Manifest{
			FormatVersion: 1, MinReaderVersion: 1,
			CreatedAt: created.Format(time.RFC3339),
		})
		require.NoError(err)
	}

	latest, err = r.LatestSnapshot()
	require.NoError(err)
	require.NotNil(latest)
	assert.Equal(newer.Format(time.RFC3339), latest.CreatedAt, "newest snapshot wins")

	// A tampered manifest fails the snapshot-ID recompute check.
	entries, err := os.ReadDir(filepath.Join(root, "snapshots"))
	require.NoError(err)
	var newest string
	for _, e := range entries {
		if e.Name() > newest {
			newest = e.Name()
		}
	}
	path := filepath.Join(root, "snapshots", newest)
	data, err := os.ReadFile(path)
	require.NoError(err)
	require.NoError(os.WriteFile(path,
		[]byte(strings.Replace(string(data), `"msgvault_version": ""`, `"msgvault_version": "forged"`, 1)), 0o600))
	_, err = r.LatestSnapshot()
	require.Error(err, "tampered manifest must fail the ID recompute")
}

func TestOpenObjectStoreValidatesConfig(t *testing.T) {
	require := require.New(t)
	root, _ := newFixtureRepo(t, []byte("blob"))
	cfgPath := filepath.Join(root, "config.toml")
	original, err := os.ReadFile(cfgPath)
	require.NoError(err)

	tamper := func(old, new string) {
		require.NoError(os.WriteFile(cfgPath,
			[]byte(strings.Replace(string(original), old, new, 1)), 0o600))
	}

	tamper(`encryption = "none"`, `encryption = "age"`)
	_, err = remoterepo.OpenObjectStore(context.Background(), objstore.NewFileStore(root))
	require.ErrorContains(err, "encrypted")

	tamper(`min_reader_version = 1`, `min_reader_version = 99`)
	_, err = remoterepo.OpenObjectStore(context.Background(), objstore.NewFileStore(root))
	require.ErrorContains(err, "reader version")

	require.NoError(os.WriteFile(cfgPath, original, 0o600))
	tampered := strings.Replace(string(original), "repo_id = \"", "repo_id = \"../", 1)
	require.NoError(os.WriteFile(cfgPath, []byte(tampered), 0o600))
	_, err = remoterepo.OpenObjectStore(context.Background(), objstore.NewFileStore(root))
	require.ErrorContains(err, "canonical")
}

func TestOpenLocationDispatch(t *testing.T) {
	root, _ := newFixtureRepo(t, []byte("dispatch blob"))

	repo, err := remoterepo.OpenLocation(context.Background(), remoterepo.Location{Repo: root})
	require.NoError(t, err)
	_, isPath := repo.(*remoterepo.Reader)
	assert.True(t, isPath, "plain paths use the kit-backed reader")
	require.NoError(t, repo.Close())

	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	_, err = remoterepo.OpenLocation(context.Background(),
		remoterepo.Location{Repo: "s3://bucket/prefix"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AWS_ACCESS_KEY_ID",
		"missing credentials fail at open, not first read")
}
