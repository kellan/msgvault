package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	msgexport "go.kenn.io/msgvault/internal/export"
	"go.kenn.io/msgvault/internal/store"
)

var (
	externalizeRaw    bool
	externalizeHTML   bool
	externalizeLimit  int64
	externalizeDryRun bool
)

// externalizeBatchSize bounds one ID batch; each row is still one blob
// write plus one row update, so the batch size only shapes progress
// output and query overhead.
const externalizeBatchSize = 500

var externalizeCmd = &cobra.Command{
	Use:   "externalize",
	Short: "Move inline raw MIME and rendered HTML into the content store",
	Long: `Move message_raw content and rendered HTML bodies out of the database
and into the content-addressed attachment store, keyed by SHA-256 of the
exact bytes (docs/internal/message-raw-externalization-design.md).

Each value is written as a durable, verified loose blob before its row is
slimmed, so an interruption can never lose content — re-running resumes
where the previous run stopped. Once externalized, the content is captured
by backup as individual blobs, deduplicated across accounts, and eligible
for 'msgvault offload'.

The database file does not shrink until the freed pages are reclaimed:
after externalization completes, run VACUUM (see the command summary).

The daemon must be stopped first ('msgvault daemon stop').`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error { return runExternalize(cmd) },
}

func runExternalize(cmd *cobra.Command) (runErr error) {
	if IsRemoteMode() {
		return errors.New("externalize is local-only; run it on the archive host, " +
			"or pass --local to select this machine's local archive intentionally")
	}
	daemonLock, err := tryAcquireDaemonOwnerLock(cfg.Data.DataDir)
	if err != nil {
		return fmt.Errorf("externalize: %w", err)
	}
	defer func() {
		if err := daemonLock.Close(); err != nil {
			runErr = errors.Join(runErr, err)
		}
	}()
	if findAnyDaemonRuntime(cfg.Data.DataDir) != nil {
		return errors.New(
			"externalize: a msgvault daemon is running; stop it with `msgvault daemon stop`, then retry")
	}
	s, cleanup, err := openWritableStoreAndInit()
	if err != nil {
		return err
	}
	defer cleanup()

	opts := externalizeOptions{
		Raw: externalizeRaw, HTML: externalizeHTML,
		Limit: externalizeLimit, DryRun: externalizeDryRun,
	}
	if !opts.Raw && !opts.HTML {
		opts.Raw, opts.HTML = true, true
	}
	result, err := externalizeArchive(cmd.Context(), cmd.OutOrStdout(), s, cfg.AttachmentsDir(), opts)
	if err != nil {
		return err
	}
	printExternalizeSummary(cmd.OutOrStdout(), s, result)
	return nil
}

type externalizeOptions struct {
	Raw    bool
	HTML   bool
	Limit  int64 // stop after this many rows across both classes (0 = all)
	DryRun bool
}

type externalizeResult struct {
	DryRun       bool
	RawRows      int64
	HTMLRows     int64
	Bytes        int64 // raw bytes now living in the CAS from this run
	Deduped      int64 // rows whose blob already existed (cross-row dedup)
	SkippedEmpty int64 // rows with zero-length content (nothing to move)
	LimitReached bool
}

// externalizeArchive moves inline content into the CAS, blob-before-row:
// every blob is durably written and verified by readback before the row is
// slimmed, so a crash at any point loses nothing and a re-run resumes.
func externalizeArchive(ctx context.Context, out io.Writer, st *store.Store,
	attachmentsDir string, opts externalizeOptions) (externalizeResult, error) {
	result := externalizeResult{DryRun: opts.DryRun}
	if opts.DryRun {
		rawLeft, htmlLeft, err := st.CountInlineExternalizable(ctx)
		if err != nil {
			return result, err
		}
		if opts.Raw {
			result.RawRows = rawLeft
		}
		if opts.HTML {
			result.HTMLRows = htmlLeft
		}
		return result, nil
	}
	budget := func() int {
		if opts.Limit <= 0 {
			return externalizeBatchSize
		}
		remaining := opts.Limit - result.RawRows - result.HTMLRows
		if remaining <= 0 {
			result.LimitReached = true
			return 0
		}
		return int(min(remaining, externalizeBatchSize))
	}

	if opts.Raw {
		for {
			n := budget()
			if n == 0 {
				break
			}
			ids, err := st.ListInlineRawBatch(ctx, n)
			if err != nil {
				return result, err
			}
			if len(ids) == 0 {
				break
			}
			for _, id := range ids {
				if err := ctx.Err(); err != nil {
					return result, err
				}
				raw, err := st.GetMessageRaw(id)
				if err != nil {
					return result, fmt.Errorf("externalize raw %d: %w", id, err)
				}
				if len(raw) == 0 {
					// Nothing to move; hash it anyway so the row leaves
					// the inline predicate instead of re-scanning forever.
					result.SkippedEmpty++
				}
				done, err := externalizeOne(ctx, st, attachmentsDir, id, raw, opts.DryRun,
					st.MarkMessageRawExternalized, &result)
				if err != nil {
					return result, fmt.Errorf("externalize raw %d: %w", id, err)
				}
				if done {
					result.RawRows++
				}
			}
			fmt.Fprintf(out, "  raw: %d rows externalized\n", result.RawRows)
		}
	}

	if opts.HTML {
		for {
			n := budget()
			if n == 0 {
				break
			}
			ids, err := st.ListInlineHTMLBatch(ctx, n)
			if err != nil {
				return result, err
			}
			if len(ids) == 0 {
				break
			}
			for _, id := range ids {
				if err := ctx.Err(); err != nil {
					return result, err
				}
				html, err := st.GetBodyHTMLInline(ctx, id)
				if err != nil {
					return result, err
				}
				done, err := externalizeOne(ctx, st, attachmentsDir, id, []byte(html), opts.DryRun,
					st.MarkBodyHTMLExternalized, &result)
				if err != nil {
					return result, fmt.Errorf("externalize html %d: %w", id, err)
				}
				if done {
					result.HTMLRows++
				}
			}
			fmt.Fprintf(out, "  html: %d rows externalized\n", result.HTMLRows)
		}
	}
	return result, nil
}

// externalizeOne writes one value's blob (durable + verified) and marks the
// row. Returns whether the row was processed (always true unless dry-run
// counting is all that happened — the caller counts either way).
func externalizeOne(ctx context.Context, st *store.Store, attachmentsDir string,
	messageID int64, content []byte, dryRun bool,
	mark func(context.Context, int64, string) error, result *externalizeResult) (bool, error) {
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])
	if dryRun {
		result.Bytes += int64(len(content))
		return true, nil
	}
	deduped, err := writeLooseCASBlob(attachmentsDir, hash, content)
	if err != nil {
		return false, err
	}
	if deduped {
		result.Deduped++
	} else {
		result.Bytes += int64(len(content))
	}
	if err := mark(ctx, messageID, hash); err != nil {
		return false, err
	}
	return true, nil
}

// writeLooseCASBlob durably writes content at the canonical CAS path,
// verifying by readback before reporting success. An existing file is
// verified against the expected hash instead of rewritten (content-addressed
// dedup); a mismatch is corruption and fails loudly.
func writeLooseCASBlob(attachmentsDir, hash string, content []byte) (deduped bool, err error) {
	target, err := msgexport.StoragePath(attachmentsDir, hash)
	if err != nil {
		return false, err
	}
	if _, statErr := os.Stat(target); statErr == nil {
		existing, readErr := os.ReadFile(target)
		if readErr != nil {
			return false, fmt.Errorf("verify existing blob %s: %w", hash, readErr)
		}
		if sum := sha256.Sum256(existing); hex.EncodeToString(sum[:]) != hash {
			return false, fmt.Errorf("existing blob %s does not match its hash; refusing to reuse", hash)
		}
		return true, nil
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return false, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(target), ".externalize-*")
	if err != nil {
		return false, err
	}
	tmpName := tmp.Name()
	_, writeErr := tmp.Write(content)
	if err := errors.Join(writeErr, tmp.Sync(), tmp.Close()); err != nil {
		_ = os.Remove(tmpName)
		return false, err
	}
	// Readback verification: the row is only slimmed once the durable
	// bytes provably reproduce the hash.
	written, err := os.ReadFile(tmpName)
	if err != nil {
		_ = os.Remove(tmpName)
		return false, err
	}
	if sum := sha256.Sum256(written); hex.EncodeToString(sum[:]) != hash {
		_ = os.Remove(tmpName)
		return false, fmt.Errorf("blob %s readback mismatch; not marking row", hash)
	}
	if err := os.Rename(tmpName, target); err != nil {
		_ = os.Remove(tmpName)
		return false, err
	}
	return false, nil
}

func printExternalizeSummary(out io.Writer, st *store.Store, r externalizeResult) {
	if r.DryRun {
		fmt.Fprintf(out, "Would externalize %d raw rows and %d html rows\n", r.RawRows, r.HTMLRows)
		return
	}
	fmt.Fprintf(out, "Externalized %d raw rows and %d html rows (%.1f MB into the content store, %d deduplicated)\n",
		r.RawRows, r.HTMLRows, float64(r.Bytes)/(1024*1024), r.Deduped)
	if r.SkippedEmpty > 0 {
		fmt.Fprintf(out, "%d rows had zero-length content\n", r.SkippedEmpty)
	}
	if r.LimitReached {
		fmt.Fprintln(out, "Stopped at --limit; re-run to continue")
		return
	}
	if r.DryRun {
		return
	}
	rawLeft, htmlLeft, err := st.CountInlineExternalizable(context.Background())
	if err == nil && rawLeft == 0 && htmlLeft == 0 {
		if err := st.SetArchiveExternalized(context.Background()); err == nil {
			fmt.Fprintln(out, "Archive fully externalized; new syncs will write content CAS-native")
		}
		fmt.Fprintln(out, "Reclaim the freed database space with VACUUM, e.g.:")
		fmt.Fprintln(out, "  sqlite3 <data-dir>/msgvault.db 'VACUUM;'")
		fmt.Fprintln(out, "Then run `msgvault backup create`. Note: earlier snapshots still hold this")
		fmt.Fprintln(out, "content as database pages, so the repository stores it twice until")
		fmt.Fprintln(out, "retention ships; seed a fresh repository if remote space is tight.")
	}
}

func init() {
	externalizeCmd.Flags().BoolVar(&externalizeRaw, "raw", false,
		"externalize raw MIME (message_raw); default: both classes")
	externalizeCmd.Flags().BoolVar(&externalizeHTML, "html", false,
		"externalize rendered HTML bodies; default: both classes")
	externalizeCmd.Flags().Int64Var(&externalizeLimit, "limit", 0,
		"stop after this many rows across both classes (0 = all)")
	externalizeCmd.Flags().BoolVar(&externalizeDryRun, "dry-run", false,
		"count and report, but change nothing")
	rootCmd.AddCommand(externalizeCmd)
}
