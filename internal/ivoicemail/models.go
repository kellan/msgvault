// Package ivoicemail imports visual voicemails from an iPhone's
// voicemail.db (as captured in an iTunes/Finder backup) into the msgvault
// store. Each voicemail becomes one message with the caller's transcription
// (when available) as the body and the audio recording as an attachment.
package ivoicemail

import "time"

// MessageType is the message_type value written for imported voicemails.
const MessageType = "apple_voicemail"

// SourceType is the sources.source_type value for iPhone voicemail sources.
const SourceType = "apple_voicemail"

// Voicemail is one row of voicemail.db's voicemail table.
type Voicemail struct {
	RowID       int64
	Sender      string // caller phone number in the carrier's format
	CallbackNum string
	Date        time.Time
	Duration    int // seconds
	Trashed     bool
	// Transcription is the visual-voicemail transcript when the database
	// carries one; empty otherwise.
	Transcription string
}

// ImportSummary reports the results of an import run.
type ImportSummary struct {
	MessagesImported      int
	ConversationsImported int
	ParticipantsResolved  int
	AttachmentsImported   int
	Skipped               int
}

// AudioFunc returns the raw audio bytes and filename for one voicemail row.
// ok is false when no recording is available (e.g. a loose-file import with
// no matching file, or an expired voicemail whose audio was never captured).
type AudioFunc func(rowID int64) (content []byte, filename string, ok bool)
