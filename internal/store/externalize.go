package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Externalized content row states
// (docs/internal/message-raw-externalization-design.md): a row is inline
// when its hash column is NULL and externalized when it names a CAS blob.
// message_raw.raw_data keeps its NOT NULL constraint (dropping it would
// force a full-table rebuild in SQLite), so externalized rows hold a
// zero-length blob; message_bodies.body_html is nullable and is set NULL.

// RawBlobOpener resolves an externalized content hash to a verified
// stream. The daemon wires it to the tiered attachment blob store, so
// externalized content that has additionally been offloaded is served from
// the backup repository transparently; local commands that own an archive
// wire a process-local attachment store.
type RawBlobOpener func(ctx context.Context, hash string) (io.ReadCloser, int64, error)

// SetRawBlobOpener installs the opener. Call once at startup before the
// store serves reads; a store holding externalized rows with no opener
// fails those reads loudly rather than returning empty content.
func (s *Store) SetRawBlobOpener(open RawBlobOpener) { s.rawBlobOpener = open }

// openExternalContent buffers one externalized blob through the opener.
// The stream must reach EOF for the CAS verification to complete, which
// io.ReadAll guarantees; Close reports verification failures.
func (s *Store) openExternalContent(ctx context.Context, hash, what string) ([]byte, error) {
	if s.rawBlobOpener == nil {
		return nil, fmt.Errorf(
			"%s is externalized to the content store (hash %s) but no blob opener is configured; "+
				"read through the msgvault daemon, or run this command on the archive host", what, hash)
	}
	rc, _, err := s.rawBlobOpener(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("open externalized %s %s: %w", what, hash, err)
	}
	data, readErr := io.ReadAll(rc)
	if err := errors.Join(readErr, rc.Close()); err != nil {
		return nil, fmt.Errorf("read externalized %s %s: %w", what, hash, err)
	}
	return data, nil
}

// MarkMessageRawExternalized records that messageID's raw content lives in
// the CAS under hash, emptying the inline bytes in the same statement. The
// caller must have made the blob durable (and verified it) first: a crash
// between blob write and this update leaves at worst an unreferenced blob,
// never a row pointing at nothing.
func (s *Store) MarkMessageRawExternalized(ctx context.Context, messageID int64, hash string) error {
	hash = strings.ToLower(hash)
	if hash == "" {
		return errors.New("mark message raw externalized: empty content hash")
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE message_raw
		SET content_hash = ?, raw_data = ?, compression = 'none'
		WHERE message_id = ?`, hash, []byte{}, messageID)
	if err != nil {
		return fmt.Errorf("mark message raw externalized %d: %w", messageID, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("mark message raw externalized %d: %w", messageID, err)
	}
	if affected == 0 {
		return fmt.Errorf("mark message raw externalized %d: no message_raw row", messageID)
	}
	return nil
}

// MarkBodyHTMLExternalized records that messageID's rendered HTML lives in
// the CAS under hash and clears the inline column. body_text is never
// touched: it stays inline as FTS/snippet/embedding input.
func (s *Store) MarkBodyHTMLExternalized(ctx context.Context, messageID int64, hash string) error {
	hash = strings.ToLower(hash)
	if hash == "" {
		return errors.New("mark body html externalized: empty content hash")
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE message_bodies
		SET html_content_hash = ?, body_html = NULL
		WHERE message_id = ?`, hash, messageID)
	if err != nil {
		return fmt.Errorf("mark body html externalized %d: %w", messageID, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("mark body html externalized %d: %w", messageID, err)
	}
	if affected == 0 {
		return fmt.Errorf("mark body html externalized %d: no message_bodies row", messageID)
	}
	return nil
}

// MessageRawExternalHash returns the CAS hash for messageID's raw content,
// "" when the row is inline, and hasRow=false when no raw row exists.
func (s *Store) MessageRawExternalHash(ctx context.Context, messageID int64) (hash string, hasRow bool, err error) {
	var h sql.NullString
	err = s.db.QueryRowContext(ctx, `
		SELECT content_hash FROM message_raw WHERE message_id = ?`, messageID).Scan(&h)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("message raw external hash %d: %w", messageID, err)
	}
	return h.String, true, nil
}

// BodyHTMLExternalHash returns the CAS hash for messageID's rendered HTML,
// "" when inline (or absent), and hasRow=false when no body row exists.
func (s *Store) BodyHTMLExternalHash(ctx context.Context, messageID int64) (hash string, hasRow bool, err error) {
	var h sql.NullString
	err = s.db.QueryRowContext(ctx, `
		SELECT html_content_hash FROM message_bodies WHERE message_id = ?`, messageID).Scan(&h)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("body html external hash %d: %w", messageID, err)
	}
	return h.String, true, nil
}

// CountInlineExternalizable reports how many message_raw and message_bodies
// rows still hold inline content the externalize command could move.
func (s *Store) CountInlineExternalizable(ctx context.Context) (rawRows, htmlRows int64, err error) {
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM message_raw WHERE content_hash IS NULL`).Scan(&rawRows); err != nil {
		return 0, 0, fmt.Errorf("count inline raw rows: %w", err)
	}
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM message_bodies
		WHERE html_content_hash IS NULL AND body_html IS NOT NULL AND body_html != ''`).Scan(&htmlRows); err != nil {
		return 0, 0, fmt.Errorf("count inline html rows: %w", err)
	}
	return rawRows, htmlRows, nil
}
