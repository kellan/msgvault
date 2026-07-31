package store_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/testutil"
)

func TestBlobOffloadRoundTrip(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	ctx := context.Background()
	st := testutil.NewTestStore(t)
	hash := packTestHash("0f10")

	offloaded, err := st.IsBlobOffloaded(ctx, hash)
	require.NoError(err)
	assert.False(offloaded)

	require.NoError(st.RecordBlobOffload(ctx, store.BlobOffloadRecord{
		ContentHash: hash,
		RepoID:      "repo-one",
		OffloadedAt: time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC),
		StoredLen:   4096,
	}))

	offloaded, err = st.IsBlobOffloaded(ctx, hash)
	require.NoError(err)
	assert.True(offloaded)

	count, bytes, err := st.OffloadedBlobStats(ctx)
	require.NoError(err)
	assert.Equal(int64(1), count)
	assert.Equal(int64(4096), bytes)

	hashes, err := st.ListOffloadedHashes(ctx)
	require.NoError(err)
	assert.Contains(hashes, hash)

	require.NoError(st.DeleteBlobOffload(ctx, hash))
	offloaded, err = st.IsBlobOffloaded(ctx, hash)
	require.NoError(err)
	assert.False(offloaded)

	count, bytes, err = st.OffloadedBlobStats(ctx)
	require.NoError(err)
	assert.Equal(int64(0), count)
	assert.Equal(int64(0), bytes)
}

func TestBlobOffloadUpsertAndRepoIDs(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	ctx := context.Background()
	st := testutil.NewTestStore(t)
	hashA := packTestHash("1a11")
	hashB := packTestHash("2b22")

	require.NoError(st.RecordBlobOffload(ctx, store.BlobOffloadRecord{
		ContentHash: hashA, RepoID: "repo-one",
		OffloadedAt: time.Now().UTC(), StoredLen: 10,
	}))
	// Re-recording the same hash upserts rather than erroring or duplicating.
	require.NoError(st.RecordBlobOffload(ctx, store.BlobOffloadRecord{
		ContentHash: hashA, RepoID: "repo-one",
		OffloadedAt: time.Now().UTC(), StoredLen: 12,
	}))
	require.NoError(st.RecordBlobOffload(ctx, store.BlobOffloadRecord{
		ContentHash: hashB, RepoID: "repo-two",
		OffloadedAt: time.Now().UTC(), StoredLen: 20,
	}))

	count, bytes, err := st.OffloadedBlobStats(ctx)
	require.NoError(err)
	assert.Equal(int64(2), count)
	assert.Equal(int64(32), bytes)

	repos, err := st.OffloadRepoIDs(ctx)
	require.NoError(err)
	assert.Equal([]string{"repo-one", "repo-two"}, repos)
}

func TestBlobOffloadCanonicalizesHashCase(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	ctx := context.Background()
	st := testutil.NewTestStore(t)
	hash := packTestHash("3c33")

	require.NoError(st.RecordBlobOffload(ctx, store.BlobOffloadRecord{
		ContentHash: strings.ToUpper(hash), RepoID: "repo-one",
		OffloadedAt: time.Now().UTC(), StoredLen: 5,
	}))

	offloaded, err := st.IsBlobOffloaded(ctx, hash)
	require.NoError(err)
	assert.True(offloaded, "uppercase record must be found by lowercase lookup")

	offloaded, err = st.IsBlobOffloaded(ctx, strings.ToUpper(hash))
	require.NoError(err)
	assert.True(offloaded, "uppercase lookup must canonicalize too")
}

func TestListUnpackedBlobsExcludesOffloaded(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	ctx := context.Background()
	st := testutil.NewTestStore(t)
	fx := newPackAttachmentFixture(t, st)

	hashKept := packTestHash("4d44")
	hashGone := packTestHash("5e55")
	fx.addAttachment(hashKept, hashKept[:2]+"/"+hashKept, 100)
	fx.addAttachment(hashGone, hashGone[:2]+"/"+hashGone, 200)

	require.NoError(st.RecordBlobOffload(ctx, store.BlobOffloadRecord{
		ContentHash: hashGone, RepoID: "repo-one",
		OffloadedAt: time.Now().UTC(), StoredLen: 200,
	}))

	blobs, err := st.ListUnpackedBlobs()
	require.NoError(err)
	hashes := make([]string, 0, len(blobs))
	for _, b := range blobs {
		hashes = append(hashes, b.Hash)
	}
	assert.Contains(hashes, hashKept)
	assert.NotContains(hashes, hashGone,
		"offloaded blobs must not be re-pack candidates")

	// Restoring the blob (deleting the record) makes it a candidate again.
	require.NoError(st.DeleteBlobOffload(ctx, hashGone))
	blobs, err = st.ListUnpackedBlobs()
	require.NoError(err)
	hashes = hashes[:0]
	for _, b := range blobs {
		hashes = append(hashes, b.Hash)
	}
	assert.Contains(hashes, hashGone)
}

// offloadMessageFixture creates one message per call with a controllable
// date and deletion state, plus one attachment row referencing hash.
type offloadMessageFixture struct {
	t   *testing.T
	st  *store.Store
	src int64
	seq int
}

func newOffloadMessageFixture(t *testing.T, st *store.Store) *offloadMessageFixture {
	t.Helper()
	src, err := st.GetOrCreateSource("gmail", "alice@example.com")
	require.NoError(t, err)
	return &offloadMessageFixture{t: t, st: st, src: src.ID}
}

func (f *offloadMessageFixture) addMessageWithAttachment(hash string, sentAt time.Time, sourceDeleted, archiveDeleted bool) int64 {
	f.t.Helper()
	f.seq++
	convID, err := f.st.EnsureConversation(f.src, "offload-thread", "Offload Thread")
	require.NoError(f.t, err)
	msg := &store.Message{
		ConversationID: convID, SourceID: f.src,
		SourceMessageID: fmt.Sprintf("offload-msg-%d", f.seq),
		MessageType:     "email",
	}
	if !sentAt.IsZero() {
		msg.SentAt = sql.NullTime{Time: sentAt, Valid: true}
	}
	msgID, err := f.st.UpsertMessage(msg)
	require.NoError(f.t, err)
	require.NoError(f.t, f.st.UpsertAttachment(msgID,
		fmt.Sprintf("off-%d.bin", f.seq), "application/octet-stream",
		hash[:2]+"/"+hash, hash, 100+f.seq))
	if sourceDeleted {
		_, err := f.st.DB().Exec(f.st.Rebind(
			`UPDATE messages SET deleted_from_source_at = CURRENT_TIMESTAMP WHERE id = ?`), msgID)
		require.NoError(f.t, err)
	}
	if archiveDeleted {
		_, err := f.st.DB().Exec(f.st.Rebind(
			`UPDATE messages SET deleted_at = CURRENT_TIMESTAMP WHERE id = ?`), msgID)
		require.NoError(f.t, err)
	}
	return msgID
}

func candidateHashes(t *testing.T, st *store.Store, sel store.OffloadSelection) []string {
	t.Helper()
	cands, err := st.ListOffloadCandidates(context.Background(), sel)
	require.NoError(t, err)
	hashes := make([]string, 0, len(cands))
	for _, c := range cands {
		hashes = append(hashes, c.ContentHash)
	}
	return hashes
}

func TestListOffloadCandidatesByDate(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	st := testutil.NewTestStore(t)
	fx := newOffloadMessageFixture(t, st)
	cutoff := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	hashOld := packTestHash("aa01")
	hashNew := packTestHash("bb02")
	hashShared := packTestHash("cc03")
	hashNoDate := packTestHash("dd04")
	fx.addMessageWithAttachment(hashOld, cutoff.AddDate(-5, 0, 0), false, false)
	fx.addMessageWithAttachment(hashNew, cutoff.AddDate(3, 0, 0), false, false)
	// Shared: one cold reference, one hot — must be excluded.
	fx.addMessageWithAttachment(hashShared, cutoff.AddDate(-2, 0, 0), false, false)
	fx.addMessageWithAttachment(hashShared, cutoff.AddDate(4, 0, 0), false, false)
	// Unknown date: not provably cold — must be excluded.
	fx.addMessageWithAttachment(hashNoDate, time.Time{}, false, false)

	hashes := candidateHashes(t, st, store.OffloadSelection{Before: cutoff})
	assert.Contains(hashes, hashOld)
	assert.NotContains(hashes, hashNew)
	assert.NotContains(hashes, hashShared)
	assert.NotContains(hashes, hashNoDate)

	// Already-offloaded candidates drop out.
	require.NoError(st.RecordBlobOffload(context.Background(), store.BlobOffloadRecord{
		ContentHash: hashOld, RepoID: "r", OffloadedAt: time.Now().UTC(), StoredLen: 1,
	}))
	assert.NotContains(candidateHashes(t, st, store.OffloadSelection{Before: cutoff}), hashOld)
}

func TestListOffloadCandidatesByDeletionState(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	st := testutil.NewTestStore(t)
	fx := newOffloadMessageFixture(t, st)
	date := time.Date(2015, 6, 1, 0, 0, 0, 0, time.UTC)

	hashSourceDeleted := packTestHash("1a10")
	hashArchiveDeleted := packTestHash("2b20")
	hashLive := packTestHash("3c30")
	fx.addMessageWithAttachment(hashSourceDeleted, date, true, false)
	fx.addMessageWithAttachment(hashArchiveDeleted, date, false, true)
	fx.addMessageWithAttachment(hashLive, date, false, false)

	hashes := candidateHashes(t, st, store.OffloadSelection{RequireSourceDeleted: true})
	assert.Contains(hashes, hashSourceDeleted)
	assert.NotContains(hashes, hashArchiveDeleted)
	assert.NotContains(hashes, hashLive)

	hashes = candidateHashes(t, st, store.OffloadSelection{RequireArchiveDeleted: true})
	assert.Contains(hashes, hashArchiveDeleted)
	assert.NotContains(hashes, hashSourceDeleted)

	// Predicates AND: date + source-deleted.
	hashes = candidateHashes(t, st, store.OffloadSelection{
		Before: date.AddDate(1, 0, 0), RequireSourceDeleted: true,
	})
	assert.Equal([]string{hashSourceDeleted}, hashes)

	_, err := st.ListOffloadCandidates(context.Background(), store.OffloadSelection{})
	require.Error(err, "empty selection must be rejected")
}

func TestListBlobLocalPaths(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	st := testutil.NewTestStore(t)
	fx := newPackAttachmentFixture(t, st)

	hash := packTestHash("4d40")
	thumb := packTestHash("5e50")
	fx.addAttachment(hash, hash[:2]+"/"+hash, 100)
	fx.setThumbnail(hash, thumb, "thumbs/"+thumb)
	fx.addAttachment(packTestHash("6f60"), "https://cdn.example.com/x", 5)

	paths, err := st.ListBlobLocalPaths(context.Background(), hash)
	require.NoError(err)
	assert.Equal([]string{hash[:2] + "/" + hash}, paths)

	paths, err = st.ListBlobLocalPaths(context.Background(), thumb)
	require.NoError(err)
	assert.Equal([]string{"thumbs/" + thumb}, paths)

	paths, err = st.ListBlobLocalPaths(context.Background(), packTestHash("6f60"))
	require.NoError(err)
	assert.Empty(paths, "URL-backed rows contribute no local paths")
}

func TestListReferencedBlobHashesKeepsOffloaded(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	// Offloaded hashes stay in the reference inventory on purpose: the
	// orphan sweep deletes loose files that are NOT referenced, and a
	// crash between `offload restore` writing the loose file and deleting
	// the blob_offload row must never make that fresh copy sweepable.
	ctx := context.Background()
	st := testutil.NewTestStore(t)
	fx := newPackAttachmentFixture(t, st)

	hash := packTestHash("6f66")
	fx.addAttachment(hash, hash[:2]+"/"+hash, 100)
	require.NoError(st.RecordBlobOffload(ctx, store.BlobOffloadRecord{
		ContentHash: hash, RepoID: "repo-one",
		OffloadedAt: time.Now().UTC(), StoredLen: 100,
	}))

	refs, err := st.ListReferencedBlobHashes()
	require.NoError(err)
	assert.Contains(refs, hash)
}
