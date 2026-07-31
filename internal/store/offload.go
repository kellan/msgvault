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
