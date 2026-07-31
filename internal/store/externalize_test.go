package store_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/testutil"
)

// externalizeFixture creates one message with an inline raw row and an
// inline body (text + HTML).
type externalizeFixture struct {
	st    *store.Store
	msgID int64
}

func newExternalizeFixture(t *testing.T) *externalizeFixture {
	t.Helper()
	require := require.New(t)
	st := testutil.NewTestStore(t)
	src, err := st.GetOrCreateSource("gmail", "alice@example.com")
	require.NoError(err)
	convID, err := st.EnsureConversation(src.ID, "ext-thread", "Ext Thread")
	require.NoError(err)
	msgID, err := st.UpsertMessage(&store.Message{
		ConversationID: convID, SourceID: src.ID,
		SourceMessageID: "ext-msg-1", MessageType: "email",
	})
	require.NoError(err)
	require.NoError(st.UpsertMessageRaw(msgID, []byte("raw mime bytes for externalization")))
	_, err = st.DB().Exec(st.Rebind(`
		INSERT INTO message_bodies (message_id, body_text, body_html)
		VALUES (?, ?, ?)`), msgID, "plain text", "<p>rendered html</p>")
	require.NoError(err)
	return &externalizeFixture{st: st, msgID: msgID}
}

func TestMarkMessageRawExternalized(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	ctx := context.Background()
	fx := newExternalizeFixture(t)
	hash := strings.Repeat("ab", 32)

	// Inline before: hash empty, row present, bytes intact.
	got, hasRow, err := fx.st.MessageRawExternalHash(ctx, fx.msgID)
	require.NoError(err)
	assert.True(hasRow)
	assert.Empty(got)
	raw, err := fx.st.GetMessageRaw(fx.msgID)
	require.NoError(err)
	assert.Equal("raw mime bytes for externalization", string(raw))

	require.NoError(fx.st.MarkMessageRawExternalized(ctx, fx.msgID, strings.ToUpper(hash)))

	got, hasRow, err = fx.st.MessageRawExternalHash(ctx, fx.msgID)
	require.NoError(err)
	assert.True(hasRow)
	assert.Equal(hash, got, "hash canonicalizes to lowercase")

	// The inline bytes are gone but the row (and its NOT NULL raw_data)
	// remains, so every presence-only join keeps working.
	var rawLen int
	require.NoError(fx.st.DB().QueryRow(fx.st.Rebind(`
		SELECT LENGTH(raw_data) FROM message_raw WHERE message_id = ?`), fx.msgID).Scan(&rawLen))
	assert.Zero(rawLen)

	// Marking a message with no raw row fails loudly.
	err = fx.st.MarkMessageRawExternalized(ctx, fx.msgID+999, hash)
	require.Error(err)
	assert.Contains(err.Error(), "no message_raw row")

	require.Error(fx.st.MarkMessageRawExternalized(ctx, fx.msgID, ""),
		"empty hash is rejected")
}

func TestMarkBodyHTMLExternalized(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	ctx := context.Background()
	fx := newExternalizeFixture(t)
	hash := strings.Repeat("cd", 32)

	require.NoError(fx.st.MarkBodyHTMLExternalized(ctx, fx.msgID, hash))

	got, hasRow, err := fx.st.BodyHTMLExternalHash(ctx, fx.msgID)
	require.NoError(err)
	assert.True(hasRow)
	assert.Equal(hash, got)

	// body_html is cleared; body_text is untouched (FTS/snippets input).
	var bodyText string
	var bodyHTML any
	require.NoError(fx.st.DB().QueryRow(fx.st.Rebind(`
		SELECT body_text, body_html FROM message_bodies WHERE message_id = ?`),
		fx.msgID).Scan(&bodyText, &bodyHTML))
	assert.Equal("plain text", bodyText)
	assert.Nil(bodyHTML)

	err = fx.st.MarkBodyHTMLExternalized(ctx, fx.msgID+999, hash)
	require.Error(err)
	assert.Contains(err.Error(), "no message_bodies row")
}

// countingOpener serves blobs from a map and counts opens.
type countingOpener struct {
	blobs map[string][]byte
	opens int
}

func (c *countingOpener) open(_ context.Context, hash string) (io.ReadCloser, int64, error) {
	c.opens++
	content, ok := c.blobs[strings.ToLower(hash)]
	if !ok {
		return nil, 0, fmt.Errorf("blob %s not in fake CAS", hash)
	}
	return io.NopCloser(bytes.NewReader(content)), int64(len(content)), nil
}

func TestGetMessageRawExternalizedReadsThroughOpener(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	ctx := context.Background()
	fx := newExternalizeFixture(t)
	raw := []byte("raw mime bytes for externalization")
	hash := strings.Repeat("2a", 32)

	require.NoError(fx.st.MarkMessageRawExternalized(ctx, fx.msgID, hash))

	// No opener: loud, actionable failure — never silent empty content.
	_, err := fx.st.GetMessageRaw(fx.msgID)
	require.Error(err)
	assert.Contains(err.Error(), "no blob opener")

	opener := &countingOpener{blobs: map[string][]byte{hash: raw}}
	fx.st.SetRawBlobOpener(opener.open)
	got, err := fx.st.GetMessageRaw(fx.msgID)
	require.NoError(err)
	assert.Equal(raw, got, "externalized read returns the exact original bytes")
	assert.Equal(1, opener.opens)

	// Inline rows never touch the opener.
	fx2 := newExternalizeFixture(t)
	fx2.st.SetRawBlobOpener(opener.open)
	got, err = fx2.st.GetMessageRaw(fx2.msgID)
	require.NoError(err)
	assert.Equal(raw, got)
	assert.Equal(1, opener.opens, "inline read must not call the opener")
}

func TestGetMessageContextResolvesExternalizedHTML(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	ctx := context.Background()
	fx := newExternalizeFixture(t)
	html := "<p>rendered html</p>"
	hash := strings.Repeat("3b", 32)

	require.NoError(fx.st.MarkBodyHTMLExternalized(ctx, fx.msgID, hash))
	opener := &countingOpener{blobs: map[string][]byte{hash: []byte(html)}}
	fx.st.SetRawBlobOpener(opener.open)

	m, err := fx.st.GetMessageContext(ctx, fx.msgID)
	require.NoError(err)
	assert.Equal(html, m.BodyHTML, "detail view re-materializes externalized HTML")
	assert.Equal("plain text", m.BodyText)
	assert.Equal(1, opener.opens)
}

func TestConversationWindowNeverFetchesExternalizedHTML(t *testing.T) {
	// The batch body path feeds conversation rendering; a per-message blob
	// fetch there would turn one conversation view into N tier round
	// trips. Batch views are text-only by design.
	require := require.New(t)
	assert := assert.New(t)
	ctx := context.Background()
	fx := newExternalizeFixture(t)
	hash := strings.Repeat("4c", 32)
	require.NoError(fx.st.MarkBodyHTMLExternalized(ctx, fx.msgID, hash))

	opener := &countingOpener{blobs: map[string][]byte{hash: []byte("<p>x</p>")}}
	fx.st.SetRawBlobOpener(opener.open)

	var convID int64
	require.NoError(fx.st.DB().QueryRow(fx.st.Rebind(
		`SELECT conversation_id FROM messages WHERE id = ?`), fx.msgID).Scan(&convID))
	window, err := fx.st.GetConversationWindowContext(ctx, convID, fx.msgID, 10, 10, nil, nil)
	require.NoError(err)
	require.NotEmpty(window.Messages)
	assert.Equal("plain text", window.Messages[0].BodyText)
	assert.Zero(opener.opens, "batch body population must never open blobs")
}

func TestExternalHashLookupsWithoutRows(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	ctx := context.Background()
	st := testutil.NewTestStore(t)

	_, hasRow, err := st.MessageRawExternalHash(ctx, 12345)
	require.NoError(err)
	assert.False(hasRow)
	_, hasRow, err = st.BodyHTMLExternalHash(ctx, 12345)
	require.NoError(err)
	assert.False(hasRow)
}

func TestCountInlineExternalizable(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	ctx := context.Background()
	fx := newExternalizeFixture(t)

	rawRows, htmlRows, err := fx.st.CountInlineExternalizable(ctx)
	require.NoError(err)
	assert.Equal(int64(1), rawRows)
	assert.Equal(int64(1), htmlRows)

	require.NoError(fx.st.MarkMessageRawExternalized(ctx, fx.msgID, strings.Repeat("ef", 32)))
	require.NoError(fx.st.MarkBodyHTMLExternalized(ctx, fx.msgID, strings.Repeat("01", 32)))

	rawRows, htmlRows, err = fx.st.CountInlineExternalizable(ctx)
	require.NoError(err)
	assert.Zero(rawRows)
	assert.Zero(htmlRows)

	// An empty-HTML row is not externalizable work.
	src, err := fx.st.GetOrCreateSource("gmail", "alice@example.com")
	require.NoError(err)
	convID, err := fx.st.EnsureConversation(src.ID, "ext-thread", "Ext Thread")
	require.NoError(err)
	msgID2, err := fx.st.UpsertMessage(&store.Message{
		ConversationID: convID, SourceID: src.ID,
		SourceMessageID: "ext-msg-2", MessageType: "email",
	})
	require.NoError(err)
	_, err = fx.st.DB().Exec(fx.st.Rebind(fmt.Sprintf(`
		INSERT INTO message_bodies (message_id, body_text, body_html)
		VALUES (%d, 'text only', NULL)`, msgID2)))
	require.NoError(err)

	_, htmlRows, err = fx.st.CountInlineExternalizable(ctx)
	require.NoError(err)
	assert.Zero(htmlRows)
}
