package cmd

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/kit/backup"
	"go.kenn.io/kit/pack"

	"go.kenn.io/msgvault/internal/attachmentstore"
	"go.kenn.io/msgvault/internal/attachmenttier"
	"go.kenn.io/msgvault/internal/remoterepo"
	"go.kenn.io/msgvault/internal/store"
)

// offloadTestArchive is a real temp archive: SQLite store, loose attachment
// files, and a real backup repository holding a chosen subset of the blobs.
type offloadTestArchive struct {
	t              testing.TB
	st             *store.Store
	attachmentsDir string
	repoRoot       string
	seq            int
}

func newOffloadTestArchive(t testing.TB) *offloadTestArchive {
	t.Helper()
	dataDir := t.TempDir()
	st, err := store.OpenForTest(filepath.Join(dataDir, "msgvault.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	require.NoError(t, st.InitSchema())
	repoRoot := filepath.Join(dataDir, "repo")
	_, err = backup.Init(repoRoot)
	require.NoError(t, err)
	return &offloadTestArchive{
		t: t, st: st,
		attachmentsDir: filepath.Join(dataDir, "attachments"),
		repoRoot:       repoRoot,
	}
}

// addBlob creates a message (with the given date) referencing content, and
// writes the loose file. Returns the content hash.
func (a *offloadTestArchive) addBlob(content []byte, sentAt time.Time) string {
	a.t.Helper()
	a.seq++
	hash := pack.ComputeBlobID(content).String()
	src, err := a.st.GetOrCreateSource("gmail", "alice@example.com")
	require.NoError(a.t, err)
	convID, err := a.st.EnsureConversation(src.ID, "offload-e2e", "Offload E2E")
	require.NoError(a.t, err)
	msg := &store.Message{
		ConversationID: convID, SourceID: src.ID,
		SourceMessageID: "off-e2e-" + hash[:8], MessageType: "email",
	}
	if !sentAt.IsZero() {
		msg.SentAt.Time, msg.SentAt.Valid = sentAt, true
	}
	msgID, err := a.st.UpsertMessage(msg)
	require.NoError(a.t, err)
	rel := hash[:2] + "/" + hash
	require.NoError(a.t, a.st.UpsertAttachment(msgID, hash[:8]+".bin",
		"application/octet-stream", rel, hash, len(content)))
	loosePath := filepath.Join(a.attachmentsDir, hash[:2], hash)
	require.NoError(a.t, os.MkdirAll(filepath.Dir(loosePath), 0o755))
	require.NoError(a.t, os.WriteFile(loosePath, content, 0o600))
	return hash
}

func (a *offloadTestArchive) loosePath(hash string) string {
	return filepath.Join(a.attachmentsDir, hash[:2], hash)
}

// storeInRepo seals the given contents into a repository pack + index.
func (a *offloadTestArchive) storeInRepo(contents ...[]byte) {
	a.t.Helper()
	staging := a.t.TempDir()
	w, err := pack.NewWriter(staging, pack.WriterOptions{})
	require.NoError(a.t, err)
	for _, c := range contents {
		_, err := w.Append(c)
		require.NoError(a.t, err)
	}
	packDir := filepath.Join(a.repoRoot, "packs", w.ID()[:2])
	require.NoError(a.t, os.MkdirAll(packDir, 0o755))
	entries, err := w.Seal(filepath.Join(packDir, w.ID()+".mvpack"))
	require.NoError(a.t, err)
	repo, err := backup.Open(a.repoRoot)
	require.NoError(a.t, err)
	idx := make([]backup.IndexEntry, 0, len(entries))
	for _, e := range entries {
		idx = append(idx, backup.IndexEntry{Blob: e.ID, PackID: w.ID(),
			Offset: e.Offset, StoredLen: e.StoredLen, Flags: e.Flags})
	}
	_, err = repo.WriteIndex(idx)
	require.NoError(a.t, err)
}

func (a *offloadTestArchive) writeSnapshot(createdAt time.Time) {
	a.t.Helper()
	repo, err := backup.Open(a.repoRoot)
	require.NoError(a.t, err)
	_, err = repo.WriteManifest(&backup.Manifest{
		FormatVersion: 1, MinReaderVersion: 1,
		CreatedAt: createdAt.UTC().Format(time.RFC3339),
	})
	require.NoError(a.t, err)
}

func (a *offloadTestArchive) openRemote() *remoterepo.Reader {
	a.t.Helper()
	remote, err := remoterepo.Open(a.repoRoot)
	require.NoError(a.t, err)
	a.t.Cleanup(func() { _ = remote.Close() })
	return remote
}

func defaultOffloadOptions(sel store.OffloadSelection) offloadOptions {
	return offloadOptions{Selection: sel, MaxSnapshotAgeDays: 14}
}

func TestOffloadEndToEnd(t *testing.T) {
	ctx := context.Background()
	a := newOffloadTestArchive(t)
	old := time.Date(2010, 3, 1, 0, 0, 0, 0, time.UTC)
	recent := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	cutoff := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	cold := []byte("cold blob: old message, present in repo")
	hot := []byte("hot blob: recent message")
	unbacked := []byte("cold blob missing from the repository")
	hashCold := a.addBlob(cold, old)
	hashHot := a.addBlob(hot, recent)
	hashUnbacked := a.addBlob(unbacked, old)
	a.storeInRepo(cold, hot)
	a.writeSnapshot(time.Now())
	remote := a.openRemote()
	sel := store.OffloadSelection{Before: cutoff}

	t.Run("dry run changes nothing", func(t *testing.T) {
		require := require.New(t)
		assert := assert.New(t)
		var out bytes.Buffer
		opts := defaultOffloadOptions(sel)
		opts.DryRun = true
		result, err := offloadBlobs(ctx, &out, a.st, remote, a.attachmentsDir, opts)
		require.NoError(err)
		assert.Equal(1, result.Offloaded)
		assert.Equal(int64(len(cold)), result.OffloadedBytes)
		assert.Equal(1, result.SkippedMissing, "unbacked blob is skipped, not evicted")
		assert.FileExists(a.loosePath(hashCold))
		offloaded, err := a.st.IsBlobOffloaded(ctx, hashCold)
		require.NoError(err)
		assert.False(offloaded)
	})

	t.Run("offload evicts only verified cold blobs", func(t *testing.T) {
		require := require.New(t)
		assert := assert.New(t)
		var out bytes.Buffer
		result, err := offloadBlobs(ctx, &out, a.st, remote, a.attachmentsDir, defaultOffloadOptions(sel))
		require.NoError(err)
		assert.Equal(1, result.Offloaded)
		assert.Equal(1, result.SkippedMissing)
		assert.Equal(0, result.Failed)

		assert.NoFileExists(a.loosePath(hashCold), "cold blob evicted")
		assert.FileExists(a.loosePath(hashHot), "hot blob untouched")
		assert.FileExists(a.loosePath(hashUnbacked), "unbacked blob untouched")

		offloaded, err := a.st.IsBlobOffloaded(ctx, hashCold)
		require.NoError(err)
		assert.True(offloaded)

		count, bytesFreed, err := a.st.OffloadedBlobStats(ctx)
		require.NoError(err)
		assert.Equal(int64(1), count)
		assert.Equal(int64(len(cold)), bytesFreed)
	})

	t.Run("second run finds nothing new", func(t *testing.T) {
		require := require.New(t)
		assert := assert.New(t)
		var out bytes.Buffer
		result, err := offloadBlobs(ctx, &out, a.st, remote, a.attachmentsDir, defaultOffloadOptions(sel))
		require.NoError(err)
		assert.Equal(0, result.Offloaded)
		assert.Equal(1, result.SkippedMissing)
	})

	t.Run("offloaded blob served through the production tier", func(t *testing.T) {
		require := require.New(t)
		assert := assert.New(t)
		local, err := attachmentstore.New(store.NewPackCatalog(a.st), a.attachmentsDir)
		require.NoError(err)
		t.Cleanup(func() { _ = local.Close() })
		tier := attachmenttier.New(local, a.st, func() (attachmenttier.RemoteReader, error) {
			return remote, nil
		})

		rc, size, err := tier.OpenStream(ctx, hashCold)
		require.NoError(err, "offloaded blob must be served from the repository")
		got, err := io.ReadAll(rc)
		require.NoError(err)
		require.NoError(rc.Close())
		assert.Equal(cold, got)
		assert.Equal(int64(len(cold)), size)

		rc, _, err = tier.OpenStream(ctx, hashHot)
		require.NoError(err, "hot blob still served locally")
		got, err = io.ReadAll(rc)
		require.NoError(err)
		require.NoError(rc.Close())
		assert.Equal(hot, got)
	})

	t.Run("restore re-materializes and clears the record", func(t *testing.T) {
		require := require.New(t)
		assert := assert.New(t)
		require.NoError(restoreOffloadedBlob(ctx, a.st, remote, a.attachmentsDir, hashCold))
		got, err := os.ReadFile(a.loosePath(hashCold))
		require.NoError(err)
		assert.Equal(cold, got)
		offloaded, err := a.st.IsBlobOffloaded(ctx, hashCold)
		require.NoError(err)
		assert.False(offloaded)

		err = restoreOffloadedBlob(ctx, a.st, remote, a.attachmentsDir, hashHot)
		require.Error(err, "restoring a blob that was never offloaded is refused")
	})
}

func TestOffloadRefusesUnsafeRepositories(t *testing.T) {
	ctx := context.Background()
	cutoff := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	sel := store.OffloadSelection{Before: cutoff}

	t.Run("no snapshots", func(t *testing.T) {
		require := require.New(t)
		assert := assert.New(t)
		a := newOffloadTestArchive(t)
		content := []byte("blob")
		a.addBlob(content, cutoff.AddDate(-1, 0, 0))
		a.storeInRepo(content)
		var out bytes.Buffer
		_, err := offloadBlobs(ctx, &out, a.st, a.openRemote(), a.attachmentsDir, defaultOffloadOptions(sel))
		require.Error(err)
		assert.Contains(err.Error(), "no snapshots")
	})

	t.Run("stale snapshot refused unless forced", func(t *testing.T) {
		require := require.New(t)
		assert := assert.New(t)
		a := newOffloadTestArchive(t)
		content := []byte("stale repo blob")
		a.addBlob(content, cutoff.AddDate(-1, 0, 0))
		a.storeInRepo(content)
		a.writeSnapshot(time.Now().AddDate(0, 0, -60))
		remote := a.openRemote()

		var out bytes.Buffer
		_, err := offloadBlobs(ctx, &out, a.st, remote, a.attachmentsDir, defaultOffloadOptions(sel))
		require.Error(err)
		assert.Contains(err.Error(), "force-stale")

		opts := defaultOffloadOptions(sel)
		opts.ForceStale = true
		result, err := offloadBlobs(ctx, &out, a.st, remote, a.attachmentsDir, opts)
		require.NoError(err)
		assert.Equal(1, result.Offloaded)
	})

	t.Run("different repository refused", func(t *testing.T) {
		require := require.New(t)
		assert := assert.New(t)
		a := newOffloadTestArchive(t)
		content := []byte("repo identity blob")
		a.addBlob(content, cutoff.AddDate(-1, 0, 0))
		a.storeInRepo(content)
		a.writeSnapshot(time.Now())
		require.NoError(a.st.RecordBlobOffload(ctx, store.BlobOffloadRecord{
			ContentHash: pack.ComputeBlobID([]byte("elsewhere")).String(),
			RepoID:      "some-other-repo", OffloadedAt: time.Now().UTC(), StoredLen: 1,
		}))
		var out bytes.Buffer
		_, err := offloadBlobs(ctx, &out, a.st, a.openRemote(), a.attachmentsDir, defaultOffloadOptions(sel))
		require.Error(err)
		assert.Contains(err.Error(), "refusing to split")
	})
}

func TestOffloadSelectionFromFlags(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	savedBefore, savedSrc, savedArch := offloadBefore, offloadSourceDeleted, offloadArchiveDeleted
	defer func() {
		offloadBefore, offloadSourceDeleted, offloadArchiveDeleted = savedBefore, savedSrc, savedArch
	}()

	offloadBefore, offloadSourceDeleted, offloadArchiveDeleted = "", false, false
	_, err := offloadSelectionFromFlags()
	require.Error(err, "empty selection is refused")

	offloadBefore = "2020-01-01"
	sel, err := offloadSelectionFromFlags()
	require.NoError(err)
	assert.Equal(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), sel.Before)

	offloadBefore = "not-a-date"
	_, err = offloadSelectionFromFlags()
	require.Error(err)

	offloadBefore, offloadSourceDeleted = "", true
	sel, err = offloadSelectionFromFlags()
	require.NoError(err)
	assert.True(sel.RequireSourceDeleted)
	assert.True(sel.Before.IsZero())
}
