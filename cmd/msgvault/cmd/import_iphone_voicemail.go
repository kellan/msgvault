package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"

	"go.kenn.io/msgvault/internal/applebackup"
	"go.kenn.io/msgvault/internal/clirun"
	"go.kenn.io/msgvault/internal/ivoicemail"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/vcard"
)

const voicemailDomain = "HomeDomain"
const voicemailPathPrefix = "Library/Voicemail/"

var (
	importVoicemailBackupPath   string
	importVoicemailPasswordFile string
	importVoicemailDBPath       string
	importVoicemailMediaDir     string
	importVoicemailBefore       string
	importVoicemailAfter        string
	importVoicemailLimit        int
	importVoicemailMe           string
	importVoicemailContacts     string
	importVoicemailNoAudio      bool
)

var importIphoneVoicemailCmd = &cobra.Command{
	Use:   "import-iphone-voicemail",
	Short: "Import visual voicemails from an iPhone backup",
	Long: `Import visual voicemails from an iTunes/Finder-style iPhone backup.

Voicemails live only on the phone, but a local device backup captures them
(voicemail.db plus one audio recording per message). Make a backup with
Finder/iTunes (or 'idevicebackup2 backup'), then point this command at it.

Encrypted backups are supported: pass the backup password and only the
voicemail files are decrypted, never the whole backup. The password can be
supplied interactively, via --password-file, or the MSGVAULT_BACKUP_PASSWORD
environment variable.

Backup discovery:
  --backup-path may be a specific backup directory (the folder containing
  Manifest.db) or the MobileSync backup root, in which case the most recent
  device backup is used. With no --backup-path the platform's default backup
  root is searched.

Pre-extracted files:
  If you already extracted the files (e.g. with iMazing), skip the backup and
  pass --db-path /path/to/voicemail.db and --media-dir /path/to/audio-dir.

Contact names:
  voicemail.db stores only phone numbers. Pass --contacts contacts.vcf to
  backfill display names by phone.

Examples:
  msgvault import-iphone-voicemail
  msgvault import-iphone-voicemail --backup-path ~/backup-udid
  msgvault import-iphone-voicemail --after 2024-01-01
  msgvault import-iphone-voicemail --db-path ./voicemail.db --media-dir ./audio
  msgvault import-iphone-voicemail --contacts ~/contacts.vcf`,
	RunE: runImportIphoneVoicemail,
}

func runImportIphoneVoicemail(cmd *cobra.Command, args []string) error {
	if importVoicemailDBPath == "" {
		// Backup mode. Resolve the password on the client (which owns the
		// terminal / password file) before routing to the daemon.
		if !isDaemonCLISubprocess() {
			password, err := resolveBackupPassword(cmd)
			if err != nil {
				return err
			}
			env := map[string]string{}
			if password != "" {
				env[clirun.EnvBackupPassword] = password
			}
			return runDaemonCLICommandHTTPFromCobraWithEnv(cmd, args, env)
		}
	} else if !isDaemonCLISubprocess() {
		return runDaemonCLICommandHTTPFromCobra(cmd, args)
	}

	after, before, err := parseVoicemailDateFilters()
	if err != nil {
		return err
	}

	s, cleanup, err := openWritableStoreAndInitForIngest()
	if err != nil {
		return err
	}
	defer cleanup()

	source, err := resolveVoicemailSource(s, importVoicemailMe)
	if err != nil {
		return fmt.Errorf("get or create source: %w", err)
	}
	if err := runPostSourceCreateMigrations(s); err != nil {
		return fmt.Errorf("post-source-create migrations: %w", err)
	}

	dbPath, audioFn, closeBackup, err := resolveVoicemailInputs(cmd)
	if err != nil {
		return err
	}
	defer closeBackup()

	reader, err := ivoicemail.OpenReader(dbPath)
	if err != nil {
		return fmt.Errorf("open voicemail.db: %w", err)
	}
	defer func() { _ = reader.Close() }()

	opts := []ivoicemail.Option{
		ivoicemail.WithLogger(logger),
		ivoicemail.WithAfterDate(after),
		ivoicemail.WithBeforeDate(before),
		ivoicemail.WithLimit(importVoicemailLimit),
	}
	if importVoicemailMe != "" {
		opts = append(opts, ivoicemail.WithOwnerHandle(importVoicemailMe))
	}
	if !importVoicemailNoAudio && audioFn != nil {
		opts = append(opts,
			ivoicemail.WithAttachmentsDir(cfg.AttachmentsDir()),
			ivoicemail.WithAudio(audioFn),
		)
	}

	client := ivoicemail.NewClient(reader, opts...)

	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Fprintln(cmd.OutOrStdout(), "\nInterrupted.")
		cancel()
	}()

	start := time.Now()
	total := client.CountFilteredMessages()
	fmt.Fprintf(cmd.OutOrStdout(), "Importing iPhone voicemails\n")
	if total > 0 {
		fmt.Fprintf(cmd.OutOrStdout(), "Voicemails to import: ~%d\n", total)
	}
	fmt.Fprintln(cmd.OutOrStdout())

	summary, err := client.Import(ctx, s, source.ID)
	if err != nil && ctx.Err() == nil {
		return fmt.Errorf("import failed: %w", err)
	}

	printVoicemailSummary(cmd, summary, start)
	return finishVoicemailImport(s)
}

// resolveVoicemailInputs returns the voicemail.db path and an audio
// provider. In backup mode it opens the backup, extracts voicemail.db to a
// temp file, and returns an AudioFunc that reads (and decrypts) each
// recording on demand. In pre-extracted mode it reads audio files from
// --media-dir. closeBackup releases any backup/temp resources.
func resolveVoicemailInputs(
	cmd *cobra.Command,
) (dbPath string, audioFn ivoicemail.AudioFunc, closeBackup func(), err error) {
	noop := func() {}

	if importVoicemailDBPath != "" {
		if _, statErr := os.Stat(importVoicemailDBPath); statErr != nil {
			return "", nil, noop, fmt.Errorf("voicemail.db not found: %w", statErr)
		}
		return importVoicemailDBPath, mediaDirAudioFunc(importVoicemailMediaDir), noop, nil
	}

	backupDir, err := resolveBackupDir()
	if err != nil {
		return "", nil, noop, err
	}
	password := os.Getenv(clirun.EnvBackupPassword)
	backup, err := applebackup.Open(backupDir, password)
	if err != nil {
		return "", nil, noop, fmt.Errorf("open backup %s: %w", backupDir, err)
	}
	cleanup := func() { _ = backup.Close() }

	files, err := backup.ListFiles(voicemailDomain, voicemailPathPrefix)
	if err != nil {
		cleanup()
		return "", nil, noop, fmt.Errorf("list voicemail files: %w", err)
	}

	var dbRecord *applebackup.FileRecord
	audioByRow := map[int64]applebackup.FileRecord{}
	for i := range files {
		rec := files[i]
		base := path.Base(rec.RelativePath)
		if base == "voicemail.db" {
			r := rec
			dbRecord = &r
			continue
		}
		if rowID, ok := voicemailRowIDFromName(base); ok {
			audioByRow[rowID] = rec
		}
	}
	if dbRecord == nil {
		cleanup()
		return "", nil, noop, fmt.Errorf(
			"no voicemail.db found in backup %s (is this the right device backup?)",
			backupDir,
		)
	}

	dbBytes, err := backup.ReadFile(*dbRecord)
	if err != nil {
		cleanup()
		return "", nil, noop, fmt.Errorf("read voicemail.db from backup: %w", err)
	}
	tempDir, err := os.MkdirTemp("", "msgvault-voicemail-")
	if err != nil {
		cleanup()
		return "", nil, noop, fmt.Errorf("create temp dir: %w", err)
	}
	tempDB := filepath.Join(tempDir, "voicemail.db")
	if err := os.WriteFile(tempDB, dbBytes, 0600); err != nil {
		cleanup()
		_ = os.RemoveAll(tempDir)
		return "", nil, noop, fmt.Errorf("write voicemail.db: %w", err)
	}

	closeAll := func() {
		cleanup()
		_ = os.RemoveAll(tempDir)
	}
	audioFn = func(rowID int64) ([]byte, string, bool) {
		rec, ok := audioByRow[rowID]
		if !ok {
			return nil, "", false
		}
		content, readErr := backup.ReadFile(rec)
		if readErr != nil {
			logger.Warn("read voicemail audio failed",
				"rowid", rowID, "error", readErr)
			return nil, "", false
		}
		return content, path.Base(rec.RelativePath), true
	}
	return tempDB, audioFn, closeAll, nil
}

// mediaDirAudioFunc reads audio recordings named "<rowid>.<ext>" from dir.
func mediaDirAudioFunc(dir string) ivoicemail.AudioFunc {
	if dir == "" {
		return nil
	}
	index := map[int64]string{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		logger.Warn("read media dir failed", "dir", dir, "error", err)
		return nil
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if rowID, ok := voicemailRowIDFromName(e.Name()); ok {
			index[rowID] = filepath.Join(dir, e.Name())
		}
	}
	if len(index) == 0 {
		return nil
	}
	return func(rowID int64) ([]byte, string, bool) {
		p, ok := index[rowID]
		if !ok {
			return nil, "", false
		}
		content, readErr := os.ReadFile(p)
		if readErr != nil {
			logger.Warn("read voicemail audio failed", "path", p, "error", readErr)
			return nil, "", false
		}
		return content, filepath.Base(p), true
	}
}

// voicemailRowIDFromName parses the ROWID from an audio filename such as
// "17.amr". Returns false for names that are not "<int>.<ext>".
func voicemailRowIDFromName(name string) (int64, bool) {
	ext := path.Ext(name)
	if ext == "" {
		return 0, false
	}
	stem := strings.TrimSuffix(name, ext)
	rowID, err := strconv.ParseInt(stem, 10, 64)
	if err != nil || rowID <= 0 {
		return 0, false
	}
	return rowID, true
}

func resolveBackupDir() (string, error) {
	if importVoicemailBackupPath != "" {
		return applebackup.Discover(importVoicemailBackupPath)
	}
	root := applebackup.DefaultBackupRoot()
	if root == "" {
		return "", fmt.Errorf(
			"no default backup location on this platform; pass --backup-path or --db-path",
		)
	}
	dir, err := applebackup.Discover(root)
	if err != nil {
		return "", fmt.Errorf(
			"%w\n\nPass --backup-path to a specific backup, or make a device backup first",
			err,
		)
	}
	return dir, nil
}

// resolveBackupPassword returns the backup password from --password-file,
// the environment, or an interactive prompt. An empty string means the
// backup is assumed unencrypted; applebackup reports if a password is
// actually required.
func resolveBackupPassword(cmd *cobra.Command) (string, error) {
	if importVoicemailPasswordFile != "" {
		data, err := os.ReadFile(importVoicemailPasswordFile)
		if err != nil {
			return "", fmt.Errorf("read password file: %w", err)
		}
		return strings.TrimRight(string(data), "\r\n"), nil
	}
	if env := os.Getenv(clirun.EnvBackupPassword); env != "" {
		return env, nil
	}
	// Only prompt when the backup is actually encrypted, to avoid asking for
	// a password on unencrypted backups.
	backupDir, err := resolveBackupDir()
	if err != nil {
		// Defer the error to the import path so its messaging (and --db-path
		// handling) stays in one place.
		return "", nil //nolint:nilerr
	}
	encrypted, err := applebackup.IsEncrypted(backupDir)
	if err != nil || !encrypted {
		return "", nil //nolint:nilerr
	}
	if !isatty.IsTerminal(os.Stdin.Fd()) {
		if !isatty.IsCygwinTerminal(os.Stdin.Fd()) {
			return readPasswordFromPipe(os.Stdin)
		}
	}
	out := os.Stderr
	if !isatty.IsTerminal(os.Stderr.Fd()) && isatty.IsTerminal(os.Stdout.Fd()) {
		out = os.Stdout
	}
	return readPasswordInteractive("iPhone backup password:", out)
}

func resolveVoicemailSource(s *store.Store, me string) (*store.Source, error) {
	identifier := "local"
	if me != "" {
		identifier = me
	}
	sources, err := s.ListSources(ivoicemail.SourceType)
	if err == nil && len(sources) > 0 {
		for _, src := range sources {
			if src.Identifier == identifier {
				return src, nil
			}
		}
	}
	return s.GetOrCreateSource(ivoicemail.SourceType, identifier)
}

func parseVoicemailDateFilters() (after, before time.Time, err error) {
	if importVoicemailAfter != "" {
		after, err = time.ParseInLocation("2006-01-02", importVoicemailAfter, time.Local)
		if err != nil {
			return after, before, fmt.Errorf("invalid --after date: %w (use YYYY-MM-DD)", err)
		}
	}
	if importVoicemailBefore != "" {
		before, err = time.ParseInLocation("2006-01-02", importVoicemailBefore, time.Local)
		if err != nil {
			return after, before, fmt.Errorf("invalid --before date: %w (use YYYY-MM-DD)", err)
		}
	}
	return after, before, nil
}

func finishVoicemailImport(s *store.Store) error {
	if importVoicemailContacts != "" {
		applyVoicemailContacts(s, importVoicemailContacts)
	}
	return rebuildCacheAfterWrite(cfg.DatabaseDSN())
}

// applyVoicemailContacts backfills participant display names by phone from a
// vCard file. Only participants with an empty name are updated.
func applyVoicemailContacts(s *store.Store, vcfPath string) {
	contacts, err := vcard.ParseFile(vcfPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: could not read contacts %s: %v\n", vcfPath, err)
		return
	}
	matches := 0
	for _, c := range contacts {
		if c.FullName == "" {
			continue
		}
		for _, phone := range c.Phones {
			updated, err := s.UpdateParticipantDisplayNameByPhone(phone, c.FullName)
			if err != nil {
				continue
			}
			if updated {
				matches++
			}
		}
	}
	fmt.Printf("Contacts applied: %d names backfilled by phone (from %d entries)\n",
		matches, len(contacts))
}

func printVoicemailSummary(
	cmd *cobra.Command, summary *ivoicemail.ImportSummary, start time.Time,
) {
	if summary == nil {
		return
	}
	out := cmd.OutOrStdout()
	fmt.Fprintln(out)
	fmt.Fprintln(out, "iPhone voicemail import complete!")
	fmt.Fprintf(out, "  Duration:      %s\n", time.Since(start).Round(time.Second))
	fmt.Fprintf(out, "  Voicemails:    %d imported\n", summary.MessagesImported)
	fmt.Fprintf(out, "  Recordings:    %d\n", summary.AttachmentsImported)
	fmt.Fprintf(out, "  Conversations: %d\n", summary.ConversationsImported)
	if summary.Skipped > 0 {
		fmt.Fprintf(out, "  Skipped:       %d\n", summary.Skipped)
	}
}

func init() {
	f := importIphoneVoicemailCmd.Flags()
	f.StringVar(&importVoicemailBackupPath, "backup-path", "",
		"path to an iPhone backup directory or the MobileSync backup root")
	f.StringVar(&importVoicemailPasswordFile, "password-file", "",
		"read the encrypted-backup password from this file")
	f.StringVar(&importVoicemailDBPath, "db-path", "",
		"path to a pre-extracted voicemail.db (skips backup handling)")
	f.StringVar(&importVoicemailMediaDir, "media-dir", "",
		"directory of pre-extracted audio files named <rowid>.<ext> (with --db-path)")
	f.StringVar(&importVoicemailAfter, "after", "",
		"only voicemails on or after this date (YYYY-MM-DD)")
	f.StringVar(&importVoicemailBefore, "before", "",
		"only voicemails before this date (YYYY-MM-DD)")
	f.IntVar(&importVoicemailLimit, "limit", 0,
		"limit number of voicemails (for testing)")
	f.StringVar(&importVoicemailMe, "me", "",
		"your phone/email, used as the recipient of each voicemail")
	f.StringVar(&importVoicemailContacts, "contacts", "",
		"path to a .vcf file used to backfill participant names by phone")
	f.BoolVar(&importVoicemailNoAudio, "no-audio", false,
		"import voicemail metadata and transcripts without storing audio recordings")
	rootCmd.AddCommand(importIphoneVoicemailCmd)
}
