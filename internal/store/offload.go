package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// BlobOffloadRecord marks one content-addressed blob as evicted locally
// after a verified read from a backup repository (the remote blob tier;
// see docs/internal/remote-blob-tier-design.md).
type BlobOffloadRecord struct {
	ContentHash string
	RepoID      string
	OffloadedAt time.Time
	StoredLen   int64
}

// RecordBlobOffload upserts the offload row for rec.ContentHash. The hash
// is canonicalized to lowercase so lookups match regardless of the case
// recorded on the attachment row.
func (s *Store) RecordBlobOffload(ctx context.Context, rec BlobOffloadRecord) error {
	if rec.ContentHash == "" {
		return errors.New("record blob offload: empty content hash")
	}
	if rec.RepoID == "" {
		return errors.New("record blob offload: empty repo id")
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO blob_offload (content_hash, repo_id, offloaded_at, stored_len)
		VALUES (?, ?, ?, ?)
		ON CONFLICT (content_hash) DO UPDATE SET
			repo_id = excluded.repo_id,
			offloaded_at = excluded.offloaded_at,
			stored_len = excluded.stored_len`,
		strings.ToLower(rec.ContentHash), rec.RepoID,
		rec.OffloadedAt.UTC().Format(time.RFC3339), rec.StoredLen)
	if err != nil {
		return fmt.Errorf("record blob offload %s: %w", rec.ContentHash, err)
	}
	return nil
}

// DeleteBlobOffload removes the offload row for hash, marking the blob as
// locally resident again (the restore path). Deleting a hash that has no
// row is a no-op.
func (s *Store) DeleteBlobOffload(ctx context.Context, hash string) error {
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM blob_offload WHERE content_hash = ?`, strings.ToLower(hash))
	if err != nil {
		return fmt.Errorf("delete blob offload %s: %w", hash, err)
	}
	return nil
}

// IsBlobOffloaded reports whether hash has an offload row.
func (s *Store) IsBlobOffloaded(ctx context.Context, hash string) (bool, error) {
	var one int
	err := s.db.QueryRowContext(ctx, `
		SELECT 1 FROM blob_offload WHERE content_hash = ?`,
		strings.ToLower(hash)).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check blob offload %s: %w", hash, err)
	}
	return true, nil
}

// OffloadedBlobStats returns the number of offloaded blobs and the total
// raw bytes their local eviction freed.
func (s *Store) OffloadedBlobStats(ctx context.Context) (count, bytes int64, err error) {
	err = s.db.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(SUM(stored_len), 0) FROM blob_offload`).Scan(&count, &bytes)
	if err != nil {
		return 0, 0, fmt.Errorf("offloaded blob stats: %w", err)
	}
	return count, bytes, nil
}

// OffloadRepoIDs returns the distinct repository IDs recorded across all
// offload rows, sorted. More than one entry means offloads were recorded
// against different repositories, which the offload command refuses to
// extend.
func (s *Store) OffloadRepoIDs(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT repo_id FROM blob_offload GROUP BY repo_id ORDER BY repo_id`)
	if err != nil {
		return nil, fmt.Errorf("list offload repo ids: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-only cursor
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan offload repo id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate offload repo ids: %w", err)
	}
	return ids, nil
}

// OffloadSelection describes which blobs `msgvault offload` may evict.
// Conditions AND together and every predicate is evaluated per referencing
// message: a blob qualifies only when ALL messages that reference it (via
// content or thumbnail hash) match, so a blob shared with a hot message
// stays local. At least one condition must be set.
type OffloadSelection struct {
	// Before selects messages whose canonical date (sent_at, falling back
	// to internal_date) is known and earlier than this bound. Zero means
	// no date condition.
	Before time.Time
	// RequireSourceDeleted selects messages whose deletion was executed
	// against the source (deleted_from_source_at set).
	RequireSourceDeleted bool
	// RequireArchiveDeleted selects flag-deleted messages (deleted_at set).
	RequireArchiveDeleted bool
}

// Empty reports whether no condition is set.
func (sel OffloadSelection) Empty() bool {
	return sel.Before.IsZero() && !sel.RequireSourceDeleted && !sel.RequireArchiveDeleted
}

// OffloadCandidate is one blob every reference of which matches the
// selection. MaxSize is the largest recorded attachment size for the hash,
// or -1 when the hash is referenced only as a thumbnail (sizes unknown).
type OffloadCandidate struct {
	ContentHash string
	MaxSize     int64
}

// ListOffloadCandidates returns the not-yet-offloaded blobs whose every
// referencing message matches sel, ordered by hash for stable batches.
func (s *Store) ListOffloadCandidates(ctx context.Context, sel OffloadSelection) ([]OffloadCandidate, error) {
	if sel.Empty() {
		return nil, errors.New("list offload candidates: empty selection")
	}
	var conds []string
	var args []any
	if !sel.Before.IsZero() {
		conds = append(conds,
			"COALESCE(m.sent_at, m.internal_date) IS NOT NULL AND COALESCE(m.sent_at, m.internal_date) < ?")
		args = append(args, sel.Before.UTC())
	}
	if sel.RequireSourceDeleted {
		conds = append(conds, "m.deleted_from_source_at IS NOT NULL")
	}
	if sel.RequireArchiveDeleted {
		conds = append(conds, "m.deleted_at IS NOT NULL")
	}
	pred := strings.Join(conds, " AND ")

	query := `
		WITH refs AS (
			SELECT LOWER(content_hash) AS h, message_id, COALESCE(size, -1) AS sz
			FROM attachments
			WHERE content_hash IS NOT NULL AND content_hash != ''
			UNION ALL
			SELECT LOWER(thumbnail_hash) AS h, message_id, -1 AS sz
			FROM attachments
			WHERE thumbnail_hash IS NOT NULL AND thumbnail_hash != ''
		)
		SELECT r.h, MAX(r.sz)
		FROM refs r
		JOIN messages m ON m.id = r.message_id
		WHERE NOT EXISTS (SELECT 1 FROM blob_offload bo WHERE bo.content_hash = r.h)
		GROUP BY r.h
		HAVING COUNT(*) = COUNT(CASE WHEN ` + pred + ` THEN 1 END)
		ORDER BY r.h`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list offload candidates: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-only cursor
	var out []OffloadCandidate
	for rows.Next() {
		var c OffloadCandidate
		if err := rows.Scan(&c.ContentHash, &c.MaxSize); err != nil {
			return nil, fmt.Errorf("scan offload candidate: %w", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate offload candidates: %w", err)
	}
	return out, nil
}

// ListBlobLocalPaths returns the DB-recorded local (non-URL) relative paths
// under the attachments dir where hash may exist as a loose file, from both
// storage_path and thumbnail_path columns.
func (s *Store) ListBlobLocalPaths(ctx context.Context, hash string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT storage_path FROM attachments
		WHERE LOWER(content_hash) = ?
		  AND storage_path IS NOT NULL AND storage_path != ''
		  AND LOWER(storage_path) NOT LIKE 'http://%'
		  AND LOWER(storage_path) NOT LIKE 'https://%'
		UNION
		SELECT thumbnail_path FROM attachments
		WHERE LOWER(thumbnail_hash) = ?
		  AND thumbnail_path IS NOT NULL AND thumbnail_path != ''
		  AND LOWER(thumbnail_path) NOT LIKE 'http://%'
		  AND LOWER(thumbnail_path) NOT LIKE 'https://%'`,
		strings.ToLower(hash), strings.ToLower(hash))
	if err != nil {
		return nil, fmt.Errorf("list blob local paths %s: %w", hash, err)
	}
	defer rows.Close() //nolint:errcheck // read-only cursor
	var paths []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, fmt.Errorf("scan blob local path: %w", err)
		}
		paths = append(paths, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate blob local paths: %w", err)
	}
	return paths, nil
}

// ListOffloadedHashes returns every offloaded content hash (canonical
// lowercase) as a set.
func (s *Store) ListOffloadedHashes(ctx context.Context) (map[string]struct{}, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT content_hash FROM blob_offload`)
	if err != nil {
		return nil, fmt.Errorf("list offloaded hashes: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-only cursor
	hashes := make(map[string]struct{})
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			return nil, fmt.Errorf("scan offloaded hash: %w", err)
		}
		hashes[hash] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate offloaded hashes: %w", err)
	}
	return hashes, nil
}
