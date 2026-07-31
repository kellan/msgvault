package store_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/testutil"
)

func TestBlobOffloadRoundTrip(t *testing.T) {
	ctx := context.Background()
	st := testutil.NewTestStore(t)
	hash := packTestHash("0f10")

	offloaded, err := st.IsBlobOffloaded(ctx, hash)
	require.NoError(t, err)
	assert.False(t, offloaded)

	require.NoError(t, st.RecordBlobOffload(ctx, store.BlobOffloadRecord{
		ContentHash: hash,
		RepoID:      "repo-one",
		OffloadedAt: time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC),
		StoredLen:   4096,
	}))

	offloaded, err = st.IsBlobOffloaded(ctx, hash)
	require.NoError(t, err)
	assert.True(t, offloaded)

	count, bytes, err := st.OffloadedBlobStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
	assert.Equal(t, int64(4096), bytes)

	hashes, err := st.ListOffloadedHashes(ctx)
	require.NoError(t, err)
	assert.Contains(t, hashes, hash)

	require.NoError(t, st.DeleteBlobOffload(ctx, hash))
	offloaded, err = st.IsBlobOffloaded(ctx, hash)
	require.NoError(t, err)
	assert.False(t, offloaded)

	count, bytes, err = st.OffloadedBlobStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)
	assert.Equal(t, int64(0), bytes)
}

func TestBlobOffloadUpsertAndRepoIDs(t *testing.T) {
	ctx := context.Background()
	st := testutil.NewTestStore(t)
	hashA := packTestHash("1a11")
	hashB := packTestHash("2b22")

	require.NoError(t, st.RecordBlobOffload(ctx, store.BlobOffloadRecord{
		ContentHash: hashA, RepoID: "repo-one",
		OffloadedAt: time.Now().UTC(), StoredLen: 10,
	}))
	// Re-recording the same hash upserts rather than erroring or duplicating.
	require.NoError(t, st.RecordBlobOffload(ctx, store.BlobOffloadRecord{
		ContentHash: hashA, RepoID: "repo-one",
		OffloadedAt: time.Now().UTC(), StoredLen: 12,
	}))
	require.NoError(t, st.RecordBlobOffload(ctx, store.BlobOffloadRecord{
		ContentHash: hashB, RepoID: "repo-two",
		OffloadedAt: time.Now().UTC(), StoredLen: 20,
	}))

	count, bytes, err := st.OffloadedBlobStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)
	assert.Equal(t, int64(32), bytes)

	repos, err := st.OffloadRepoIDs(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"repo-one", "repo-two"}, repos)
}

func TestBlobOffloadCanonicalizesHashCase(t *testing.T) {
	ctx := context.Background()
	st := testutil.NewTestStore(t)
	hash := packTestHash("3c33")

	require.NoError(t, st.RecordBlobOffload(ctx, store.BlobOffloadRecord{
		ContentHash: strings.ToUpper(hash), RepoID: "repo-one",
		OffloadedAt: time.Now().UTC(), StoredLen: 5,
	}))

	offloaded, err := st.IsBlobOffloaded(ctx, hash)
	require.NoError(t, err)
	assert.True(t, offloaded, "uppercase record must be found by lowercase lookup")

	offloaded, err = st.IsBlobOffloaded(ctx, strings.ToUpper(hash))
	require.NoError(t, err)
	assert.True(t, offloaded, "uppercase lookup must canonicalize too")
}

func TestListUnpackedBlobsExcludesOffloaded(t *testing.T) {
	ctx := context.Background()
	st := testutil.NewTestStore(t)
	fx := newPackAttachmentFixture(t, st)

	hashKept := packTestHash("4d44")
	hashGone := packTestHash("5e55")
	fx.addAttachment(hashKept, hashKept[:2]+"/"+hashKept, 100)
	fx.addAttachment(hashGone, hashGone[:2]+"/"+hashGone, 200)

	require.NoError(t, st.RecordBlobOffload(ctx, store.BlobOffloadRecord{
		ContentHash: hashGone, RepoID: "repo-one",
		OffloadedAt: time.Now().UTC(), StoredLen: 200,
	}))

	blobs, err := st.ListUnpackedBlobs()
	require.NoError(t, err)
	hashes := make([]string, 0, len(blobs))
	for _, b := range blobs {
		hashes = append(hashes, b.Hash)
	}
	assert.Contains(t, hashes, hashKept)
	assert.NotContains(t, hashes, hashGone,
		"offloaded blobs must not be re-pack candidates")

	// Restoring the blob (deleting the record) makes it a candidate again.
	require.NoError(t, st.DeleteBlobOffload(ctx, hashGone))
	blobs, err = st.ListUnpackedBlobs()
	require.NoError(t, err)
	hashes = hashes[:0]
	for _, b := range blobs {
		hashes = append(hashes, b.Hash)
	}
	assert.Contains(t, hashes, hashGone)
}

func TestListReferencedBlobHashesKeepsOffloaded(t *testing.T) {
	// Offloaded hashes stay in the reference inventory on purpose: the
	// orphan sweep deletes loose files that are NOT referenced, and a
	// crash between `offload restore` writing the loose file and deleting
	// the blob_offload row must never make that fresh copy sweepable.
	ctx := context.Background()
	st := testutil.NewTestStore(t)
	fx := newPackAttachmentFixture(t, st)

	hash := packTestHash("6f66")
	fx.addAttachment(hash, hash[:2]+"/"+hash, 100)
	require.NoError(t, st.RecordBlobOffload(ctx, store.BlobOffloadRecord{
		ContentHash: hash, RepoID: "repo-one",
		OffloadedAt: time.Now().UTC(), StoredLen: 100,
	}))

	refs, err := st.ListReferencedBlobHashes()
	require.NoError(t, err)
	assert.Contains(t, refs, hash)
}
