package ivoicemail

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/testutil"
)

type vmRow struct {
	rowID         int64
	sender        string
	date          int64
	duration      int
	trashedDate   int64
	transcription string
}

// buildVoicemailDB writes a real voicemail.db with iOS's schema. When
// withTranscription is false the transcription column is omitted entirely,
// exercising the reader's schema probing on older-iOS databases.
func buildVoicemailDB(t *testing.T, rows []vmRow, withTranscription bool) string {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "voicemail.db")
	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	schema := `CREATE TABLE voicemail (
		ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
		remote_uid INTEGER,
		date INTEGER,
		token TEXT,
		sender TEXT,
		callback_num TEXT,
		duration INTEGER,
		expiration INTEGER,
		trashed_date INTEGER DEFAULT 0,
		flags INTEGER DEFAULT 0`
	if withTranscription {
		schema += `,
		transcription TEXT`
	}
	schema += "\n)"
	_, err = db.Exec(schema)
	require.NoError(t, err)

	for _, r := range rows {
		if withTranscription {
			_, err = db.Exec(
				`INSERT INTO voicemail (ROWID, date, sender, callback_num, duration, trashed_date, transcription)
				 VALUES (?, ?, ?, ?, ?, ?, ?)`,
				r.rowID, r.date, r.sender, r.sender, r.duration, r.trashedDate, r.transcription,
			)
		} else {
			_, err = db.Exec(
				`INSERT INTO voicemail (ROWID, date, sender, callback_num, duration, trashed_date)
				 VALUES (?, ?, ?, ?, ?, ?)`,
				r.rowID, r.date, r.sender, r.sender, r.duration, r.trashedDate,
			)
		}
		require.NoError(t, err)
	}
	return dbPath
}

func openSource(t *testing.T, s *store.Store) int64 {
	t.Helper()
	src, err := s.GetOrCreateSource(SourceType, "local")
	require.NoError(t, err)
	return src.ID
}

func TestImportVoicemails(t *testing.T) {
	rows := []vmRow{
		{rowID: 1, sender: "+15555550100", date: 1600000000, duration: 42, transcription: "Hey it is alice, call me back"},
		{rowID: 2, sender: "+15555550100", date: 1600100000, duration: 12, transcription: "alice again"},
		{rowID: 3, sender: "+15555550199", date: 1600200000, duration: 75, trashedDate: 1600300000, transcription: "deleted one from bob"},
	}
	dbPath := buildVoicemailDB(t, rows, true)

	audio := map[int64][]byte{
		1: []byte("#!AMR\naudio-one"),
		2: []byte("#!AMR\naudio-two"),
		// row 3 has no recording available
	}
	audioFn := func(rowID int64) ([]byte, string, bool) {
		b, ok := audio[rowID]
		if !ok {
			return nil, "", false
		}
		return b, fmt.Sprintf("%d.amr", rowID), true
	}

	s := testutil.NewTestStore(t)
	sourceID := openSource(t, s)
	attachDir := t.TempDir()

	r, err := OpenReader(dbPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close()) }()

	c := NewClient(r,
		WithOwnerHandle("+15555550000"),
		WithAttachmentsDir(attachDir),
		WithAudio(audioFn),
	)

	summary, err := c.Import(context.Background(), s, sourceID)
	require.NoError(t, err)

	assert.Equal(t, 3, summary.MessagesImported)
	assert.Equal(t, 2, summary.AttachmentsImported)
	// Two distinct callers → two conversations.
	assert.Equal(t, 2, summary.ConversationsImported)

	// Verify rows landed with the right type and body via the store.
	var count int
	require.NoError(t, s.DB().QueryRow(
		s.Rebind(`SELECT COUNT(*) FROM messages WHERE source_id = ? AND message_type = ?`),
		sourceID, MessageType,
	).Scan(&count))
	assert.Equal(t, 3, count)

	var body sql.NullString
	require.NoError(t, s.DB().QueryRow(
		s.Rebind(`SELECT b.body_text FROM messages m
		          JOIN message_bodies b ON b.message_id = m.id
		          WHERE m.source_id = ? AND m.source_message_id = ?`),
		sourceID, "vm-"+fmt.Sprint(sourceID)+"-1",
	).Scan(&body))
	assert.Contains(t, body.String, "Voicemail from +15555550100")
	assert.Contains(t, body.String, "(0:42)")
	assert.Contains(t, body.String, "Hey it is alice")

	// The trashed voicemail carries the marker and the [deleted] note.
	var trashedBody sql.NullString
	require.NoError(t, s.DB().QueryRow(
		s.Rebind(`SELECT b.body_text FROM messages m
		          JOIN message_bodies b ON b.message_id = m.id
		          WHERE m.source_id = ? AND m.source_message_id = ?`),
		sourceID, "vm-"+fmt.Sprint(sourceID)+"-3",
	).Scan(&trashedBody))
	assert.Contains(t, trashedBody.String, "[deleted]")
}

func TestImportIsIdempotent(t *testing.T) {
	rows := []vmRow{
		{rowID: 1, sender: "+15555550100", date: 1600000000, duration: 10, transcription: "one"},
		{rowID: 2, sender: "+15555550101", date: 1600100000, duration: 20, transcription: "two"},
	}
	dbPath := buildVoicemailDB(t, rows, true)
	s := testutil.NewTestStore(t)
	sourceID := openSource(t, s)

	runImport := func() *ImportSummary {
		r, err := OpenReader(dbPath)
		require.NoError(t, err)
		defer func() { require.NoError(t, r.Close()) }()
		summary, err := NewClient(r, WithOwnerHandle("+15555550000")).
			Import(context.Background(), s, sourceID)
		require.NoError(t, err)
		return summary
	}

	runImport()
	runImport()

	var count int
	require.NoError(t, s.DB().QueryRow(
		s.Rebind(`SELECT COUNT(*) FROM messages WHERE source_id = ? AND message_type = ?`),
		sourceID, MessageType,
	).Scan(&count))
	assert.Equal(t, 2, count, "re-import must upsert, not duplicate")
}

func TestReaderWithoutTranscriptionColumn(t *testing.T) {
	rows := []vmRow{
		{rowID: 1, sender: "+15555550100", date: 1600000000, duration: 30},
	}
	dbPath := buildVoicemailDB(t, rows, false)

	r, err := OpenReader(dbPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close()) }()
	assert.False(t, r.hasTranscription)

	var got []Voicemail
	require.NoError(t, r.Iterate(time.Time{}, time.Time{}, 0, func(vm Voicemail) error {
		got = append(got, vm)
		return nil
	}))
	require.Len(t, got, 1)
	assert.Equal(t, "+15555550100", got[0].Sender)
	assert.Empty(t, got[0].Transcription)
	assert.Equal(t, 30, got[0].Duration)
}

func TestDateFilterAndLimit(t *testing.T) {
	rows := []vmRow{
		{rowID: 1, sender: "+15555550100", date: 1000, duration: 5},
		{rowID: 2, sender: "+15555550100", date: 2000, duration: 5},
		{rowID: 3, sender: "+15555550100", date: 3000, duration: 5},
	}
	dbPath := buildVoicemailDB(t, rows, true)

	r, err := OpenReader(dbPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close()) }()

	after := time.Unix(1500, 0)
	before := time.Unix(2500, 0)
	n, err := r.Count(after, before)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	var seen []int64
	require.NoError(t, r.Iterate(after, before, 0, func(vm Voicemail) error {
		seen = append(seen, vm.RowID)
		return nil
	}))
	assert.Equal(t, []int64{2}, seen)

	// Limit caps the scan.
	seen = nil
	require.NoError(t, r.Iterate(time.Time{}, time.Time{}, 2, func(vm Voicemail) error {
		seen = append(seen, vm.RowID)
		return nil
	}))
	assert.Equal(t, []int64{1, 2}, seen)
}
