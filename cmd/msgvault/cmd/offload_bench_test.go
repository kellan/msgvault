package cmd

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.kenn.io/msgvault/internal/attachmentstore"
	"go.kenn.io/msgvault/internal/attachmenttier"
	"go.kenn.io/msgvault/internal/objstore"
	"go.kenn.io/msgvault/internal/remoterepo"
	"go.kenn.io/msgvault/internal/store"
)

// benchBlobSize approximates a typical email attachment. The absolute
// numbers matter less than the deltas between serving flavors.
const benchBlobSize = 256 << 10

// newBenchArchive builds a real archive with n offloaded blobs: rows,
// loose files, repository packs, snapshot, then a real offload run that
// evicts every blob. Returns the archive and the offloaded hashes.
func newBenchArchive(b *testing.B, n int) (*offloadTestArchive, []string) {
	b.Helper()
	a := newOffloadTestArchive(b)
	old := time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC)
	contents := make([][]byte, 0, n)
	hashes := make([]string, 0, n)
	for i := 0; i < n; i++ {
		content := make([]byte, benchBlobSize)
		for j := range content {
			content[j] = byte(i + j*7) // varied, mildly compressible
		}
		contents = append(contents, content)
		hashes = append(hashes, a.addBlob(content, old))
	}
	a.storeInRepo(contents...)
	a.writeSnapshot(time.Now())

	var out bytes.Buffer
	result, err := offloadBlobs(context.Background(), &out, a.st, a.openRemote(),
		a.attachmentsDir, defaultOffloadOptions(store.OffloadSelection{
			Before: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		}))
	require.NoError(b, err)
	require.Equal(b, n, result.Offloaded)
	return a, hashes
}

func benchReadAll(b *testing.B, tier *attachmenttier.Store, hashes []string) {
	b.Helper()
	ctx := context.Background()
	b.SetBytes(benchBlobSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rc, _, err := tier.OpenStream(ctx, hashes[i%len(hashes)])
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, rc); err != nil {
			b.Fatal(err)
		}
		if err := rc.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTierRead measures one 256KiB attachment read through the
// production tier across serving flavors:
//
//   - local-loose: the blob was never offloaded (baseline).
//   - repo-path: offloaded, repository on a local filesystem path
//     (external drive / NAS mount), kit-backed reader.
//   - repo-object: offloaded, repository through the object-store reader
//     with zero added latency — isolates the per-read protocol overhead
//     (size + header + trailer + footer + blob reads, no reader cache).
//   - repo-object-5ms / 25ms: the same with per-operation latency
//     injected, approximating same-region and cross-region S3.
func BenchmarkTierRead(b *testing.B) {
	const blobs = 16
	a, hashes := newBenchArchive(b, blobs)

	local, err := attachmentstore.New(store.NewPackCatalog(a.st), a.attachmentsDir)
	require.NoError(b, err)
	b.Cleanup(func() { _ = local.Close() })

	// Baseline: a separate archive whose blobs stay local.
	baseline, baseHashes := func() (*offloadTestArchive, []string) {
		ba := newOffloadTestArchive(b)
		hs := make([]string, 0, blobs)
		for i := 0; i < blobs; i++ {
			content := make([]byte, benchBlobSize)
			for j := range content {
				content[j] = byte(i + j*13)
			}
			hs = append(hs, ba.addBlob(content, time.Now()))
		}
		return ba, hs
	}()
	baselineLocal, err := attachmentstore.New(store.NewPackCatalog(baseline.st), baseline.attachmentsDir)
	require.NoError(b, err)
	b.Cleanup(func() { _ = baselineLocal.Close() })

	b.Run("local-loose", func(b *testing.B) {
		tier := attachmenttier.New(baselineLocal, baseline.st,
			func() (attachmenttier.RemoteReader, error) { return nil, fmt.Errorf("unused") })
		benchReadAll(b, tier, baseHashes)
	})

	b.Run("repo-path", func(b *testing.B) {
		remote, err := remoterepo.Open(a.repoRoot)
		require.NoError(b, err)
		b.Cleanup(func() { _ = remote.Close() })
		tier := attachmenttier.New(local, a.st,
			func() (attachmenttier.RemoteReader, error) { return remote, nil })
		benchReadAll(b, tier, hashes)
	})

	for _, flavor := range []struct {
		name  string
		delay time.Duration
	}{
		{"repo-object", 0},
		{"repo-object-5ms", 5 * time.Millisecond},
		{"repo-object-25ms", 25 * time.Millisecond},
	} {
		b.Run(flavor.name, func(b *testing.B) {
			var backend objstore.Store = objstore.NewFileStore(a.repoRoot)
			if flavor.delay > 0 {
				backend = objstore.WithLatency(backend, flavor.delay)
			}
			remote, err := remoterepo.OpenObjectStore(context.Background(), backend)
			require.NoError(b, err)
			b.Cleanup(func() { _ = remote.Close() })
			tier := attachmenttier.New(local, a.st,
				func() (attachmenttier.RemoteReader, error) { return remote, nil })
			benchReadAll(b, tier, hashes)
		})
	}
}

// BenchmarkOffloadVerification measures the offload command's per-blob
// verified-read ladder (index lookup + full stream + hash verification),
// the dominant cost of an offload run.
func BenchmarkOffloadVerification(b *testing.B) {
	a, hashes := newBenchArchive(b, 16)
	remote := a.openRemote()
	ctx := context.Background()
	b.SetBytes(benchBlobSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := verifyRemoteBlob(ctx, remote, hashes[i%len(hashes)]); err != nil {
			b.Fatal(err)
		}
	}
}
