package cmd

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/msgvault/internal/attachmentstore"
	"go.kenn.io/msgvault/internal/fakevault"
	"go.kenn.io/msgvault/internal/store"
)

// newFakeArchive generates a deterministic real archive (zlib raw MIME,
// HTML bodies, loose attachments) and opens its store.
func newFakeArchive(t *testing.T) (*store.Store, string) {
	t.Helper()
	dir := t.TempDir()
	_, err := fakevault.Generate(context.Background(), fakevault.Options{
		Dir: dir, Messages: 80, AttachmentBytes: 1 << 18, Seed: 7,
	})
	require.NoError(t, err)
	st, err := store.Open(filepath.Join(dir, "msgvault.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	require.NoError(t, st.InitSchema())
	return st, filepath.Join(dir, "attachments")
}

// sampleInline captures pre-externalization content for equivalence checks.
func sampleInline(t *testing.T, st *store.Store, kind string, limit int) map[int64][]byte {
	t.Helper()
	ctx := context.Background()
	var ids []int64
	var err error
	if kind == "raw" {
		ids, err = st.ListInlineRawBatch(ctx, limit)
	} else {
		ids, err = st.ListInlineHTMLBatch(ctx, limit)
	}
	require.NoError(t, err)
	require.NotEmpty(t, ids, "fixture must generate inline %s rows", kind)
	sample := map[int64][]byte{}
	for _, id := range ids {
		if kind == "raw" {
			raw, err := st.GetMessageRaw(id)
			require.NoError(t, err)
			sample[id] = raw
		} else {
			html, err := st.GetBodyHTMLInline(ctx, id)
			require.NoError(t, err)
			sample[id] = []byte(html)
		}
	}
	return sample
}

func TestExternalizeEndToEnd(t *testing.T) {
	ctx := context.Background()
	st, attachmentsDir := newFakeArchive(t)

	rawBefore := sampleInline(t, st, "raw", 10)
	htmlBefore := sampleInline(t, st, "html", 10)
	rawTotal, htmlTotal, err := st.CountInlineExternalizable(ctx)
	require.NoError(t, err)
	require.Positive(t, rawTotal)
	require.Positive(t, htmlTotal)

	t.Run("dry run changes nothing", func(t *testing.T) {
		require := require.New(t)
		assert := assert.New(t)
		var out bytes.Buffer
		result, err := externalizeArchive(ctx, &out, st, attachmentsDir,
			externalizeOptions{Raw: true, HTML: true, DryRun: true})
		require.NoError(err)
		assert.Equal(rawTotal, result.RawRows)
		assert.Equal(htmlTotal, result.HTMLRows)
		r, h, err := st.CountInlineExternalizable(ctx)
		require.NoError(err)
		assert.Equal(rawTotal, r)
		assert.Equal(htmlTotal, h)
	})

	t.Run("limit stops and rerun resumes idempotently", func(t *testing.T) {
		require := require.New(t)
		assert := assert.New(t)
		var out bytes.Buffer
		result, err := externalizeArchive(ctx, &out, st, attachmentsDir,
			externalizeOptions{Raw: true, HTML: true, Limit: 5})
		require.NoError(err)
		assert.True(result.LimitReached)
		assert.Equal(int64(5), result.RawRows+result.HTMLRows)

		result, err = externalizeArchive(ctx, &out, st, attachmentsDir,
			externalizeOptions{Raw: true, HTML: true})
		require.NoError(err)
		assert.Equal(rawTotal-5, result.RawRows, "resume covers exactly the remainder")
		assert.Equal(htmlTotal, result.HTMLRows)

		r, h, err := st.CountInlineExternalizable(ctx)
		require.NoError(err)
		assert.Zero(r)
		assert.Zero(h)

		// A third run finds nothing.
		result, err = externalizeArchive(ctx, &out, st, attachmentsDir,
			externalizeOptions{Raw: true, HTML: true})
		require.NoError(err)
		assert.Zero(result.RawRows + result.HTMLRows)
	})

	t.Run("content reads back byte-identical through the CAS", func(t *testing.T) {
		require := require.New(t)
		local, err := attachmentstore.New(store.NewPackCatalog(st), attachmentsDir)
		require.NoError(err)
		t.Cleanup(func() { _ = local.Close() })
		st.SetRawBlobOpener(local.OpenStream)

		for id, want := range rawBefore {
			got, err := st.GetMessageRaw(id)
			require.NoError(err)
			require.Equal(want, got, "raw %d must survive externalization byte-for-byte", id)
		}
		for id, want := range htmlBefore {
			m, err := st.GetMessageContext(ctx, id)
			require.NoError(err)
			require.Equal(string(want), m.BodyHTML, "html %d must survive externalization", id)
		}
	})

	t.Run("summary sets the completion marker", func(t *testing.T) {
		require := require.New(t)
		assert := assert.New(t)
		var out bytes.Buffer
		printExternalizeSummary(&out, st, externalizeResult{})
		marked, err := st.ArchiveExternalized(ctx)
		require.NoError(err)
		assert.True(marked)
		assert.Contains(out.String(), "VACUUM")
	})
}
