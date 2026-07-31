package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"go.kenn.io/msgvault/internal/config"
	msgexport "go.kenn.io/msgvault/internal/export"
	"go.kenn.io/msgvault/internal/remoterepo"
	"go.kenn.io/msgvault/internal/store"
)

var (
	offloadBefore         string
	offloadSourceDeleted  bool
	offloadArchiveDeleted bool
	offloadDryRun         bool
	offloadMaxBytes       int64
	offloadForceStale     bool
	offloadRestoreHashes  []string
)

var offloadCmd = &cobra.Command{
	Use:   "offload",
	Short: "Evict cold attachment content that the backup repository verifiably holds",
	Long: `Evict local attachment content to the remote blob tier.

Selection is by message: a blob qualifies only when EVERY message that
references it matches all given conditions (--before, --deleted-from-source,
--archive-deleted). Each selected blob is streamed back out of the configured
backup repository and fully verified before any local byte is deleted; blobs
the repository cannot prove it holds are skipped.

Offloaded blobs are served transparently by the daemon from the repository
([offload] repo in config.toml): a filesystem path (external drive, NAS
mount, rclone mount) or an s3://bucket/prefix URL for S3-compatible object
storage (AWS, B2, R2, MinIO; credentials from the standard AWS_* environment
variables).

The daemon must be stopped first ('msgvault daemon stop'): offload rewrites
attachment storage state that a running daemon caches and coordinates.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error { return runOffload(cmd) },
}

var offloadStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show offloaded blob counts and bytes",
	Args:  cobra.NoArgs,
	RunE:  func(cmd *cobra.Command, _ []string) error { return runOffloadStatus(cmd) },
}

var offloadRestoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Re-materialize offloaded blobs as local loose files",
	Long: `Fetch offloaded blobs back from the backup repository, verify them, and
write them as loose files in the attachment store. The loose file is written
and durable before the offload record is removed, so an interruption can
never leave a blob unreachable.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error { return runOffloadRestore(cmd) },
}

// offloadLocation maps the [offload] config section to a repository
// location for remoterepo.OpenLocation.
func offloadLocation(c *config.Config) remoterepo.Location {
	return remoterepo.Location{
		Repo:       c.Offload.Repo,
		S3Endpoint: c.Offload.S3Endpoint,
		S3Region:   c.Offload.S3Region,
	}
}

// refuseOffloadWithLiveDaemon mirrors unpack-attachments: offload mutates
// attachment storage state (pack index rows, loose files) that a running
// daemon caches and whose maintenance it schedules.
func refuseOffloadWithLiveDaemon(dataDir string) error {
	if findAnyDaemonRuntime(dataDir) != nil {
		return errors.New(
			"offload: a msgvault daemon is running; stop it with `msgvault daemon stop`, then retry")
	}
	return nil
}

func offloadSelectionFromFlags() (store.OffloadSelection, error) {
	var sel store.OffloadSelection
	if offloadBefore != "" {
		parsed, err := parseOffloadDate(offloadBefore)
		if err != nil {
			return sel, err
		}
		sel.Before = parsed
	}
	sel.RequireSourceDeleted = offloadSourceDeleted
	sel.RequireArchiveDeleted = offloadArchiveDeleted
	if sel.Empty() {
		return sel, errors.New(
			"offload: give at least one selection condition (--before, --deleted-from-source, --archive-deleted)")
	}
	return sel, nil
}

func parseOffloadDate(s string) (time.Time, error) {
	for _, layout := range []string{"2006-01-02", time.RFC3339} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("offload: cannot parse --before %q (want YYYY-MM-DD or RFC3339)", s)
}

func runOffload(cmd *cobra.Command) (runErr error) {
	if IsRemoteMode() {
		return errors.New("offload is local-only; run it on the archive host, " +
			"or pass --local to select this machine's local archive intentionally")
	}
	if !cfg.Offload.Enabled() {
		return errors.New("offload: no [offload] repo configured in config.toml")
	}
	sel, err := offloadSelectionFromFlags()
	if err != nil {
		return err
	}
	daemonLock, err := tryAcquireDaemonOwnerLock(cfg.Data.DataDir)
	if err != nil {
		return fmt.Errorf("offload: %w", err)
	}
	defer func() {
		if err := daemonLock.Close(); err != nil {
			runErr = errors.Join(runErr, err)
		}
	}()
	if err := refuseOffloadWithLiveDaemon(cfg.Data.DataDir); err != nil {
		return err
	}
	s, cleanup, err := openWritableStoreAndInit()
	if err != nil {
		return err
	}
	defer cleanup()
	remote, err := remoterepo.OpenLocation(cmd.Context(), offloadLocation(cfg))
	if err != nil {
		return err
	}
	defer func() { _ = remote.Close() }()

	result, err := offloadBlobs(cmd.Context(), cmd.OutOrStdout(), s, remote,
		cfg.AttachmentsDir(), offloadOptions{
			Selection:          sel,
			DryRun:             offloadDryRun,
			MaxBytes:           offloadMaxBytes,
			ForceStale:         offloadForceStale,
			MaxSnapshotAgeDays: cfg.Offload.MaxSnapshotAgeDays,
		})
	if err != nil {
		return err
	}
	printOffloadSummary(cmd.OutOrStdout(), result)
	return nil
}

type offloadOptions struct {
	Selection          store.OffloadSelection
	DryRun             bool
	MaxBytes           int64
	ForceStale         bool
	MaxSnapshotAgeDays int
}

type offloadResult struct {
	DryRun         bool
	Candidates     int
	Offloaded      int
	OffloadedBytes int64
	SkippedMissing int
	Failed         int
	BudgetStopped  bool
}

// checkOffloadRepository enforces the preconditions that make eviction
// against this repository sane: it has at least one snapshot, the newest
// snapshot is not stale (a stale repo usually means the off-site sync is
// broken), and any previously recorded offloads name this same repository.
func checkOffloadRepository(ctx context.Context, st *store.Store, remote remoterepo.Repo, opts offloadOptions) error {
	latest, err := remote.LatestSnapshot()
	if err != nil {
		return err
	}
	if latest == nil {
		return errors.New("offload: repository has no snapshots; run `msgvault backup create` first")
	}
	created, err := time.Parse(time.RFC3339, latest.CreatedAt)
	if err != nil {
		return fmt.Errorf("offload: latest snapshot has unparseable created_at %q: %w", latest.CreatedAt, err)
	}
	if age := time.Since(created); !opts.ForceStale &&
		age > time.Duration(opts.MaxSnapshotAgeDays)*24*time.Hour {
		return fmt.Errorf("offload: newest repository snapshot is %s old (limit %dd); "+
			"back up first, or pass --force-stale if this is deliberate",
			age.Round(time.Hour), opts.MaxSnapshotAgeDays)
	}
	existing, err := st.OffloadRepoIDs(ctx)
	if err != nil {
		return err
	}
	for _, id := range existing {
		if id != remote.RepoID() {
			return fmt.Errorf("offload: archive already has blobs offloaded to repository %s; "+
				"refusing to split the blob set across repositories (configured: %s)",
				id, remote.RepoID())
		}
	}
	return nil
}

func offloadBlobs(ctx context.Context, out io.Writer, st *store.Store, remote remoterepo.Repo,
	attachmentsDir string, opts offloadOptions) (offloadResult, error) {
	result := offloadResult{DryRun: opts.DryRun}
	if err := checkOffloadRepository(ctx, st, remote, opts); err != nil {
		return result, err
	}
	if err := remote.Refresh(); err != nil {
		return result, err
	}
	candidates, err := st.ListOffloadCandidates(ctx, opts.Selection)
	if err != nil {
		return result, err
	}
	result.Candidates = len(candidates)

	for _, cand := range candidates {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if opts.MaxBytes > 0 && result.OffloadedBytes >= opts.MaxBytes {
			result.BudgetStopped = true
			break
		}
		has, err := remote.Has(cand.ContentHash)
		if err != nil {
			return result, err
		}
		if !has {
			result.SkippedMissing++
			continue
		}
		size, err := verifyRemoteBlob(ctx, remote, cand.ContentHash)
		if err != nil {
			result.Failed++
			fmt.Fprintf(out, "  ! %s: remote verification failed: %v\n", cand.ContentHash, err)
			continue
		}
		if opts.DryRun {
			result.Offloaded++
			result.OffloadedBytes += size
			continue
		}
		// Record before evicting: a crash between the two leaves the blob
		// both cataloged and locally resolvable (safe), never the reverse.
		if err := st.RecordBlobOffload(ctx, store.BlobOffloadRecord{
			ContentHash: cand.ContentHash, RepoID: remote.RepoID(),
			OffloadedAt: time.Now().UTC(), StoredLen: size,
		}); err != nil {
			return result, err
		}
		if err := evictLocalBlob(ctx, st, attachmentsDir, cand.ContentHash); err != nil {
			result.Failed++
			fmt.Fprintf(out, "  ! %s: eviction incomplete: %v\n", cand.ContentHash, err)
			continue
		}
		result.Offloaded++
		result.OffloadedBytes += size
	}
	return result, nil
}

// verifyRemoteBlob streams the blob through kit's full verification stack
// (CRC, stored length, SHA-256 identity) and returns its raw size.
func verifyRemoteBlob(ctx context.Context, remote remoterepo.Repo, hash string) (int64, error) {
	rc, size, err := remote.OpenBlob(ctx, hash)
	if err != nil {
		return 0, err
	}
	n, copyErr := io.Copy(io.Discard, rc)
	if err := errors.Join(copyErr, rc.Close()); err != nil {
		return 0, err
	}
	if n != size {
		return 0, fmt.Errorf("read %d bytes, expected %d", n, size)
	}
	return size, nil
}

// evictLocalBlob deletes the blob's loose files and pack index rows. The
// offload record is already durable, so partial eviction only leaves the
// blob cheaper to serve than intended, never unreachable.
func evictLocalBlob(ctx context.Context, st *store.Store, attachmentsDir, hash string) error {
	relPaths, err := st.ListBlobLocalPaths(ctx, hash)
	if err != nil {
		return err
	}
	casPath, err := msgexport.StoragePath(attachmentsDir, hash)
	if err != nil {
		return err
	}
	paths := map[string]struct{}{casPath: {}}
	for _, rel := range relPaths {
		native := filepath.FromSlash(rel)
		if !filepath.IsLocal(native) {
			continue // never follow a recorded path outside the store
		}
		paths[filepath.Join(attachmentsDir, native)] = struct{}{}
	}
	var errs []error
	for path := range paths {
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	if err := st.DeletePackIndexEntry(strings.ToLower(hash)); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func printOffloadSummary(out io.Writer, r offloadResult) {
	verb := "Offloaded"
	if r.DryRun {
		verb = "Would offload"
	}
	fmt.Fprintf(out, "%s %d of %d candidate blobs (%.1f MB)\n",
		verb, r.Offloaded, r.Candidates, float64(r.OffloadedBytes)/(1024*1024))
	if r.SkippedMissing > 0 {
		fmt.Fprintf(out, "Skipped %d blobs not present in the repository (back up first, then re-run)\n",
			r.SkippedMissing)
	}
	if r.Failed > 0 {
		fmt.Fprintf(out, "Failed %d blobs (see lines above); local copies were kept\n", r.Failed)
	}
	if r.BudgetStopped {
		fmt.Fprintln(out, "Stopped at --max-bytes budget; re-run to continue")
	}
}

func runOffloadStatus(cmd *cobra.Command) error {
	dbPath, err := cfg.DatabasePath()
	if err != nil {
		return err
	}
	st, err := store.OpenReadOnly(dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()
	count, bytes, err := st.OffloadedBlobStats(cmd.Context())
	if err != nil {
		return err
	}
	repos, err := st.OffloadRepoIDs(cmd.Context())
	if err != nil {
		return err
	}
	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "Offloaded blobs: %d\n", count)
	fmt.Fprintf(out, "Bytes freed locally: %.1f MB\n", float64(bytes)/(1024*1024))
	if len(repos) > 0 {
		fmt.Fprintf(out, "Repository: %s\n", strings.Join(repos, ", "))
	}
	if cfg.Offload.Enabled() {
		fmt.Fprintf(out, "Configured repo path: %s\n", cfg.Offload.Repo)
	} else {
		fmt.Fprintln(out, "No [offload] repo configured")
	}
	return nil
}

func runOffloadRestore(cmd *cobra.Command) (runErr error) {
	if IsRemoteMode() {
		return errors.New("offload restore is local-only; run it on the archive host, " +
			"or pass --local to select this machine's local archive intentionally")
	}
	if !cfg.Offload.Enabled() {
		return errors.New("offload restore: no [offload] repo configured in config.toml")
	}
	if len(offloadRestoreHashes) == 0 {
		return errors.New("offload restore: give at least one --hash")
	}
	daemonLock, err := tryAcquireDaemonOwnerLock(cfg.Data.DataDir)
	if err != nil {
		return fmt.Errorf("offload restore: %w", err)
	}
	defer func() {
		if err := daemonLock.Close(); err != nil {
			runErr = errors.Join(runErr, err)
		}
	}()
	if err := refuseOffloadWithLiveDaemon(cfg.Data.DataDir); err != nil {
		return err
	}
	s, cleanup, err := openWritableStoreAndInit()
	if err != nil {
		return err
	}
	defer cleanup()
	remote, err := remoterepo.OpenLocation(cmd.Context(), offloadLocation(cfg))
	if err != nil {
		return err
	}
	defer func() { _ = remote.Close() }()

	for _, hash := range offloadRestoreHashes {
		if err := restoreOffloadedBlob(cmd.Context(), s, remote, cfg.AttachmentsDir(), hash); err != nil {
			return fmt.Errorf("offload restore %s: %w", hash, err)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "Restored %s\n", hash)
	}
	return nil
}

// restoreOffloadedBlob writes the blob back as a loose file, durably, and
// only then deletes the offload record. The reverse order could strand a
// blob with neither a local copy nor a catalog entry.
func restoreOffloadedBlob(ctx context.Context, st *store.Store, remote remoterepo.Repo,
	attachmentsDir, hash string) error {
	offloaded, err := st.IsBlobOffloaded(ctx, hash)
	if err != nil {
		return err
	}
	if !offloaded {
		return errors.New("blob is not recorded as offloaded")
	}
	target, err := msgexport.StoragePath(attachmentsDir, strings.ToLower(hash))
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	rc, _, err := remote.OpenBlob(ctx, hash)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(target), ".restore-*")
	if err != nil {
		_ = rc.Close()
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	_, copyErr := io.Copy(tmp, rc)
	// rc.Close is the verification barrier: it fails unless the stream was
	// consumed to EOF with CRC, length, and SHA-256 all proven.
	if err := errors.Join(copyErr, rc.Close(), tmp.Sync(), tmp.Close()); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpName, target); err != nil {
		cleanup()
		return err
	}
	return st.DeleteBlobOffload(ctx, hash)
}

func init() {
	offloadCmd.Flags().StringVar(&offloadBefore, "before", "",
		"only blobs whose every referencing message predates this date (YYYY-MM-DD)")
	offloadCmd.Flags().BoolVar(&offloadSourceDeleted, "deleted-from-source", false,
		"only blobs whose every referencing message was deleted from its source")
	offloadCmd.Flags().BoolVar(&offloadArchiveDeleted, "archive-deleted", false,
		"only blobs whose every referencing message is flag-deleted in the archive")
	offloadCmd.Flags().BoolVar(&offloadDryRun, "dry-run", false,
		"verify and report, but change nothing")
	offloadCmd.Flags().Int64Var(&offloadMaxBytes, "max-bytes", 0,
		"stop after offloading roughly this many bytes (0 = no limit)")
	offloadCmd.Flags().BoolVar(&offloadForceStale, "force-stale", false,
		"proceed even if the repository's newest snapshot exceeds max_snapshot_age_days")
	offloadRestoreCmd.Flags().StringArrayVar(&offloadRestoreHashes, "hash", nil,
		"content hash to restore (repeatable)")
	offloadCmd.AddCommand(offloadStatusCmd)
	offloadCmd.AddCommand(offloadRestoreCmd)
	rootCmd.AddCommand(offloadCmd)
}
