package ivoicemail

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go.kenn.io/msgvault/internal/export"
	"go.kenn.io/msgvault/internal/mime"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/textimport"
)

// Client imports voicemails from a Reader into the msgvault store.
type Client struct {
	reader         *Reader
	afterDate      time.Time
	beforeDate     time.Time
	limit          int
	ownerHandle    string
	attachmentsDir string
	audio          AudioFunc
	logger         *slog.Logger
}

// Option configures a Client.
type Option func(*Client)

// WithAfterDate filters to voicemails on or after this date.
func WithAfterDate(t time.Time) Option { return func(c *Client) { c.afterDate = t } }

// WithBeforeDate filters to voicemails before this date.
func WithBeforeDate(t time.Time) Option { return func(c *Client) { c.beforeDate = t } }

// WithLimit caps the number of voicemails imported (0 = unlimited).
func WithLimit(n int) Option { return func(c *Client) { c.limit = n } }

// WithOwnerHandle sets the device owner's phone/email used as the "to"
// recipient of every voicemail.
func WithOwnerHandle(h string) Option { return func(c *Client) { c.ownerHandle = h } }

// WithAttachmentsDir enables audio import into the content-addressed
// attachment store rooted at dir.
func WithAttachmentsDir(dir string) Option {
	return func(c *Client) { c.attachmentsDir = dir }
}

// WithAudio provides the audio recording for each voicemail row.
func WithAudio(fn AudioFunc) Option { return func(c *Client) { c.audio = fn } }

// WithLogger sets the logger.
func WithLogger(l *slog.Logger) Option { return func(c *Client) { c.logger = l } }

// NewClient creates an importer over the given voicemail.db reader.
func NewClient(r *Reader, opts ...Option) *Client {
	c := &Client{reader: r, logger: slog.Default()}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// CountFilteredMessages returns an estimate of the rows to import.
func (c *Client) CountFilteredMessages() int {
	n, err := c.reader.Count(c.afterDate, c.beforeDate)
	if err != nil {
		c.logger.Warn("count voicemails failed", "error", err)
		return 0
	}
	if c.limit > 0 && n > c.limit {
		return c.limit
	}
	return n
}

// Import reads matching voicemails and writes them into the store under
// sourceID. The import is idempotent: re-running upserts by a stable
// per-voicemail source_message_id.
func (c *Client) Import(
	ctx context.Context, s *store.Store, sourceID int64,
) (*ImportSummary, error) {
	summary := &ImportSummary{}

	labels, err := c.ensureLabels(s, sourceID)
	if err != nil {
		return summary, err
	}

	phoneCache := map[string]int64{}
	convCache := map[string]int64{}

	ownerID := int64(0)
	if c.ownerHandle != "" {
		ownerID, _ = c.resolveParticipant(s, c.ownerHandle, phoneCache, summary)
	}

	err = c.reader.Iterate(
		c.afterDate, c.beforeDate, c.limit,
		func(vm Voicemail) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if impErr := c.importOne(
				s, sourceID, vm, ownerID, labels, phoneCache, convCache, summary,
			); impErr != nil {
				c.logger.Warn(
					"failed to import voicemail",
					"rowid", vm.RowID, "error", impErr,
				)
				summary.Skipped++
			}
			return nil
		},
	)
	if err != nil {
		return summary, err
	}

	if err := s.RecomputeConversationStats(sourceID); err != nil {
		return summary, fmt.Errorf("recompute stats: %w", err)
	}
	return summary, nil
}

func (c *Client) importOne(
	s *store.Store,
	sourceID int64,
	vm Voicemail,
	ownerID int64,
	labels map[string]int64,
	phoneCache map[string]int64,
	convCache map[string]int64,
	summary *ImportSummary,
) error {
	callerID, err := c.resolveParticipant(s, vm.Sender, phoneCache, summary)
	if err != nil {
		return fmt.Errorf("resolve caller: %w", err)
	}

	threadKey := normalizedThreadKey(vm.Sender)
	convID, err := c.ensureConv(s, sourceID, threadKey, vm.Sender, convCache, summary)
	if err != nil {
		return err
	}
	if callerID > 0 {
		_ = s.EnsureConversationParticipant(convID, callerID, "member")
	}
	if ownerID > 0 {
		_ = s.EnsureConversationParticipant(convID, ownerID, "member")
	}

	body := buildBody(vm)
	sentAt := sql.NullTime{}
	if !vm.Date.IsZero() {
		sentAt = sql.NullTime{Time: vm.Date, Valid: true}
	}
	senderIDNull := sql.NullInt64{}
	if callerID > 0 {
		senderIDNull = sql.NullInt64{Int64: callerID, Valid: true}
	}

	content, filename, hasAudio := c.audioFor(vm)

	msgID, err := s.UpsertMessage(&store.Message{
		SourceID:        sourceID,
		SourceMessageID: sourceMessageID(sourceID, vm),
		ConversationID:  convID,
		Snippet:         nullStr(snippet(body)),
		SentAt:          sentAt,
		MessageType:     MessageType,
		SenderID:        senderIDNull,
		IsFromMe:        false,
		HasAttachments:  hasAudio,
		SizeEstimate:    int64(len(body) + len(content)),
	})
	if err != nil {
		return fmt.Errorf("upsert message: %w", err)
	}

	if err := s.UpsertMessageBody(
		msgID, nullStr(body), sql.NullString{},
	); err != nil {
		return fmt.Errorf("upsert body: %w", err)
	}

	// Voicemail is always inbound: from caller, to owner.
	if callerID > 0 {
		if err := s.ReplaceMessageRecipients(msgID, "from", []int64{callerID}, nil); err != nil {
			return fmt.Errorf("write from recipient: %w", err)
		}
	}
	var toIDs []int64
	if ownerID > 0 {
		toIDs = []int64{ownerID}
	}
	if err := s.ReplaceMessageRecipients(msgID, "to", toIDs, nil); err != nil {
		return fmt.Errorf("write to recipient: %w", err)
	}

	if lid, ok := labels["voicemail"]; ok {
		_ = s.LinkMessageLabel(msgID, lid)
	}
	if vm.Trashed {
		if lid, ok := labels["trashed"]; ok {
			_ = s.LinkMessageLabel(msgID, lid)
		}
	}

	if hasAudio {
		if err := c.storeAudio(s, msgID, filename, content); err != nil {
			return fmt.Errorf("store audio: %w", err)
		}
		summary.AttachmentsImported++
	}

	summary.MessagesImported++
	return nil
}

func (c *Client) audioFor(vm Voicemail) ([]byte, string, bool) {
	if c.audio == nil || c.attachmentsDir == "" {
		return nil, "", false
	}
	content, filename, ok := c.audio(vm.RowID)
	if !ok || len(content) == 0 {
		return nil, "", false
	}
	if filename == "" {
		filename = fmt.Sprintf("voicemail-%d.amr", vm.RowID)
	}
	return content, filename, true
}

func (c *Client) storeAudio(
	s *store.Store, msgID int64, filename string, content []byte,
) error {
	att := &mime.Attachment{
		Filename:    filename,
		ContentType: contentTypeFor(filename),
		Content:     content,
	}
	storagePath, err := export.StoreAttachmentFile(c.attachmentsDir, att)
	if err != nil {
		return fmt.Errorf("store attachment file: %w", err)
	}
	if err := s.UpsertAttachment(
		msgID, filename, att.ContentType, storagePath, att.ContentHash, len(content),
	); err != nil {
		return fmt.Errorf("upsert attachment: %w", err)
	}
	return nil
}

func (c *Client) ensureLabels(
	s *store.Store, sourceID int64,
) (map[string]int64, error) {
	labels := map[string]int64{}
	for _, name := range []string{"voicemail", "trashed"} {
		id, err := s.EnsureLabel(sourceID, name, name, "user")
		if err != nil {
			return nil, fmt.Errorf("ensure label %q: %w", name, err)
		}
		labels[name] = id
	}
	return labels, nil
}

func (c *Client) ensureConv(
	s *store.Store,
	sourceID int64,
	threadKey, title string,
	cache map[string]int64,
	summary *ImportSummary,
) (int64, error) {
	if id, ok := cache[threadKey]; ok {
		return id, nil
	}
	id, err := s.EnsureConversationWithType(
		sourceID, threadKey, "direct_chat", title,
	)
	if err != nil {
		return 0, fmt.Errorf("ensure conversation: %w", err)
	}
	cache[threadKey] = id
	summary.ConversationsImported++
	return id, nil
}

func (c *Client) resolveParticipant(
	s *store.Store,
	handle string,
	cache map[string]int64,
	summary *ImportSummary,
) (int64, error) {
	handle = strings.TrimSpace(handle)
	if handle == "" {
		return 0, nil
	}
	normalized, err := textimport.NormalizePhone(handle)
	if err != nil {
		// Non-phone handles (rare: unknown/private callers) are skipped
		// silently rather than failing the whole voicemail.
		return 0, nil //nolint:nilerr
	}
	if id, ok := cache[normalized]; ok {
		return id, nil
	}
	id, err := s.EnsureParticipantByPhone(normalized, "", SourceType)
	if err != nil {
		return 0, err
	}
	cache[normalized] = id
	summary.ParticipantsResolved++
	return id, nil
}

// normalizedThreadKey groups voicemails from the same caller into one
// conversation, falling back to the raw handle when it cannot be
// normalized so unknown callers still thread together.
func normalizedThreadKey(sender string) string {
	sender = strings.TrimSpace(sender)
	if sender == "" {
		return "voicemail:unknown"
	}
	if n, err := textimport.NormalizePhone(sender); err == nil {
		return "voicemail:" + n
	}
	return "voicemail:" + sender
}

// sourceMessageID is a stable per-voicemail identifier. voicemail.db ROWIDs
// are stable within one device database, and scoping by sourceID keeps two
// devices' row 1 from colliding.
func sourceMessageID(sourceID int64, vm Voicemail) string {
	return fmt.Sprintf("vm-%d-%d", sourceID, vm.RowID)
}

func buildBody(vm Voicemail) string {
	var b strings.Builder
	caller := vm.Sender
	if caller == "" {
		caller = "unknown caller"
	}
	fmt.Fprintf(&b, "Voicemail from %s", caller)
	if vm.Duration > 0 {
		fmt.Fprintf(&b, " (%s)", formatDuration(vm.Duration))
	}
	if vm.Trashed {
		b.WriteString(" [deleted]")
	}
	if t := strings.TrimSpace(vm.Transcription); t != "" {
		b.WriteString("\n\n")
		b.WriteString(t)
	}
	return b.String()
}

func formatDuration(seconds int) string {
	if seconds < 0 {
		seconds = 0
	}
	return fmt.Sprintf("%d:%02d", seconds/60, seconds%60)
}

func contentTypeFor(filename string) string {
	lower := strings.ToLower(filename)
	switch {
	case strings.HasSuffix(lower, ".amr"):
		return "audio/amr"
	case strings.HasSuffix(lower, ".m4a"):
		return "audio/mp4"
	case strings.HasSuffix(lower, ".mp3"):
		return "audio/mpeg"
	case strings.HasSuffix(lower, ".wav"):
		return "audio/wav"
	case strings.HasSuffix(lower, ".caf"):
		return "audio/x-caf"
	default:
		return "application/octet-stream"
	}
}

func nullStr(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

func snippet(s string) string {
	s = strings.TrimSpace(strings.ReplaceAll(s, "\n", " "))
	const max = 200
	if len(s) <= max {
		return s
	}
	return s[:max]
}
