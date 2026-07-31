package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/msgvault/internal/attachmenttier"
	"go.kenn.io/msgvault/internal/config"
	"go.kenn.io/msgvault/internal/query"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/testutil"
)

// remoteUnavailableBlobStore models the attachment tier when a blob is
// offloaded but the backup repository cannot serve it.
type remoteUnavailableBlobStore struct{}

func (remoteUnavailableBlobStore) OpenStream(context.Context, string) (io.ReadCloser, int64, error) {
	return nil, 0, fmt.Errorf("%w: opening repository: mount not present",
		attachmenttier.ErrRemoteUnavailable)
}

// TestRemoteTierUnavailableMapsTo503 pins the tier's honesty contract at the
// HTTP boundary: an offloaded blob whose repository is unreachable is a 503
// remote_tier_unavailable, never a 404 that reads as "attachment deleted".
func TestRemoteTierUnavailableMapsTo503(t *testing.T) {
	must := require.New(t)
	st := testutil.NewTestStore(t)
	src, err := st.GetOrCreateSource("gmail", "alice@example.com")
	must.NoError(err)
	convID, err := st.EnsureConversation(src.ID, "thread", "Thread")
	must.NoError(err)
	msgID, err := st.UpsertMessage(&store.Message{
		ConversationID: convID, SourceID: src.ID,
		SourceMessageID: "msg-1", MessageType: "email",
	})
	must.NoError(err)
	const hash = "aa23456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	must.NoError(st.UpsertAttachment(msgID, "cold.bin", "application/octet-stream",
		hash[:2]+"/"+hash, hash, 42))

	srv := NewServerWithOptions(ServerOptions{
		Config:    &config.Config{Data: config.DataConfig{DataDir: t.TempDir()}},
		Logger:    testLogger(),
		BlobStore: remoteUnavailableBlobStore{},
		Engine:    query.NewEngine(st.DB(), st.IsPostgreSQL()),
	})

	for name, target := range map[string]string{
		"cli attachment endpoint": "/api/v1/cli/attachment?content_hash=" + hash,
		"public content endpoint": "/api/v1/attachments/" + hash + "/content",
	} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, target, nil)
			w := httptest.NewRecorder()
			srv.Router().ServeHTTP(w, req)

			assert.Equal(t, http.StatusServiceUnavailable, w.Code,
				"body: %s", w.Body.String())
			assert.Contains(t, w.Body.String(), "remote_tier_unavailable")
		})
	}
}
