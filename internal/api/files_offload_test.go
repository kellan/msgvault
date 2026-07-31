package api

import (
	"context"
	"io"
	"io/fs"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/msgvault/internal/config"
	"go.kenn.io/msgvault/internal/store"
)

// offloadCatalogStore satisfies MessageStore structurally (embedded, never
// called) plus the optional blobOffloadCatalog capability.
type offloadCatalogStore struct {
	MessageStore
	offloaded map[string]bool
}

func (s offloadCatalogStore) IsBlobOffloaded(_ context.Context, hash string) (bool, error) {
	return s.offloaded[hash], nil
}

// probeCountingBlobStore counts availability probes.
type probeCountingBlobStore struct{ probes *int }

func (p probeCountingBlobStore) OpenStream(context.Context, string) (io.ReadCloser, int64, error) {
	*p.probes++
	return nil, 0, fs.ErrNotExist
}

// TestFileContentStateOffloadedSkipsProbe pins the listing-scalability
// contract: classifying an offloaded row consults the catalog only — a
// per-row probe would become one remote round trip per listed file.
func TestFileContentStateOffloadedSkipsProbe(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	const offloadedHash = "aa23456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	const missingHash = "bb23456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	probes := 0
	srv := &Server{
		cfg:       &config.Config{Data: config.DataConfig{DataDir: t.TempDir()}},
		store:     offloadCatalogStore{offloaded: map[string]bool{offloadedHash: true}},
		blobStore: probeCountingBlobStore{probes: &probes},
		logger:    testLogger(),
	}

	state, available := srv.fileContentState(context.Background(),
		store.FileMetadata{ContentHash: offloadedHash})
	assert.Equal(FileContentLocal, state)
	assert.True(available)
	require.Zero(probes, "offloaded rows must classify without opening the blob store")

	// A hash that is neither local nor offloaded still probes and reports
	// missing — the pre-existing contract is unchanged.
	state, available = srv.fileContentState(context.Background(),
		store.FileMetadata{ContentHash: missingHash})
	assert.Equal(FileContentMissingBlob, state)
	assert.False(available)
	assert.Positive(probes, "non-offloaded rows keep the probing behavior")
}
