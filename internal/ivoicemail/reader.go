package ivoicemail

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3" // SQLite driver for voicemail.db
)

// Reader reads rows from an iPhone voicemail.db.
type Reader struct {
	db               *sql.DB
	hasTranscription bool
}

// OpenReader opens voicemail.db read-only and probes its schema. The
// transcription column is present only on newer iOS releases, so its
// absence is not an error.
func OpenReader(dbPath string) (*Reader, error) {
	db, err := sql.Open(
		"sqlite3",
		fmt.Sprintf("file:%s?mode=ro&_busy_timeout=5000", dbPath),
	)
	if err != nil {
		return nil, fmt.Errorf("open voicemail.db: %w", err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connect to voicemail.db: %w", err)
	}
	r := &Reader{db: db}
	r.hasTranscription, err = columnExists(db, "voicemail", "transcription")
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return r, nil
}

// Close closes the underlying database.
func (r *Reader) Close() error { return r.db.Close() }

// Count returns the number of non-deleted voicemail rows matching the
// optional date window.
func (r *Reader) Count(after, before time.Time) (int, error) {
	where, args := dateWhere(after, before)
	var n int
	err := r.db.QueryRow(
		"SELECT COUNT(*) FROM voicemail WHERE trashed_date = 0"+where, args...,
	).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count voicemails: %w", err)
	}
	return n, nil
}

// Iterate yields each voicemail in chronological order. Trashed rows are
// included (with Trashed set) because carriers purge voicemails and a prior
// import may be the only remaining copy; callers decide how to present
// them. limit <= 0 means no limit.
func (r *Reader) Iterate(
	after, before time.Time, limit int, fn func(Voicemail) error,
) error {
	transcriptCol := "'' AS transcription"
	if r.hasTranscription {
		transcriptCol = "COALESCE(transcription, '')"
	}
	where, args := dateWhere(after, before)
	query := fmt.Sprintf(
		`SELECT ROWID,
		        COALESCE(sender, ''),
		        COALESCE(callback_num, ''),
		        COALESCE(date, 0),
		        COALESCE(duration, 0),
		        COALESCE(trashed_date, 0),
		        %s
		 FROM voicemail
		 WHERE date IS NOT NULL%s
		 ORDER BY date ASC, ROWID ASC`,
		transcriptCol, where,
	)
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return fmt.Errorf("query voicemails: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var (
			vm          Voicemail
			epoch       int64
			trashedDate int64
		)
		if err := rows.Scan(
			&vm.RowID, &vm.Sender, &vm.CallbackNum,
			&epoch, &vm.Duration, &trashedDate, &vm.Transcription,
		); err != nil {
			return fmt.Errorf("scan voicemail row: %w", err)
		}
		if epoch > 0 {
			vm.Date = time.Unix(epoch, 0).UTC()
		}
		vm.Trashed = trashedDate != 0
		if err := fn(vm); err != nil {
			return err
		}
	}
	return rows.Err()
}

// dateWhere builds an AND-prefixed predicate for the voicemail.db date
// column (Unix epoch seconds) plus its bind arguments.
func dateWhere(after, before time.Time) (string, []interface{}) {
	var clause string
	var args []interface{}
	if !after.IsZero() {
		clause += " AND date >= ?"
		args = append(args, after.Unix())
	}
	if !before.IsZero() {
		clause += " AND date < ?"
		args = append(args, before.Unix())
	}
	return clause, args
}

func columnExists(db *sql.DB, table, column string) (bool, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return false, fmt.Errorf("inspect %s schema: %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			cid        int
			name, ctyp string
			notNull    int
			dfltValue  sql.NullString
			pk         int
		)
		if err := rows.Scan(&cid, &name, &ctyp, &notNull, &dfltValue, &pk); err != nil {
			return false, fmt.Errorf("scan %s schema: %w", table, err)
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}
