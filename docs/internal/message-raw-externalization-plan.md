# Message Raw Externalization Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development
> if subagents are available, or superpowers:executing-plans otherwise, to
> implement this plan. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Move `message_raw.raw_data` (~9.3 GiB) and `message_bodies.body_html`
(~10.3 GiB) into the content-addressed attachment store per
`message-raw-externalization-design.md`, making both classes
backup-capturable as blobs and offloadable through the shipped remote blob
tier — the change that takes the measured archive from 25.6 GB of SQLite to
a few GB.

**Architecture:** Rows gain a content-hash column and become slim pointers
into the CAS; a `RawBlobOpener` seam injected into `internal/store` resolves
externalized content through the daemon's tiered blob store (or a
process-local attachmentstore outside the daemon); every blob-reference
enumeration (resolve membership, maintenance inventory, backup capture)
gains arms for the two new columns; a batched, resumable, idempotent
`msgvault externalize` command migrates existing rows blob-before-row with
verify-readback; `offload` gains `--only` class selection. Existing inline
rows keep working forever (mixed-mode reads); new syncs write CAS-native
once the archive is marked migrated.

**Tech Stack:** Go 1.26, Cobra, SQLite, PostgreSQL, Testify,
go.kenn.io/kit packstore.

---

## Working rules

- Branch: continue `claude/msgvault-backup-storage-3h579k` if PR #1 is
  unmerged, else a fresh branch cut from the merged main.
- **Kit pin**: go.mod replaces `go.kenn.io/kit` with
  `github.com/kellan/kit` @ the `pack-reader-readerat-v0.9.1` branch
  (exactly v0.9.1 + `pack.NewReaderFromReaderAt`). Do not ride the fork's
  `main` — its unreleased packstore changes break
  `TestRemoveAccountCmd_DeletesUniquePackedMappings`. Drop the replace only
  when the widening lands upstream in a tagged kenn-io/kit release.
- Use Testify (`require`/`assert` via `New(t)` for tests with 4+ calls —
  `make testify-helper-check` enforces this). Tags: `fts5 sqlite_vec`.
- `go fmt ./...` and `go vet ./...` before each commit; stage everything.
- One focused commit per task; never amend or squash unless asked.
- Known container noise (not yours): root ignores read-only fixtures in
  `internal/api/settings_test.go` and `internal/microsoft/oauth_test.go`;
  `golangci-lint` binary may be Go-version-mismatched.

## Load-bearing facts (verified in this repo, cite before re-deriving)

- `message_raw`: `internal/store/schema.sql:367` — `raw_data BLOB NOT NULL`,
  per-row `compression` (zlib applied at `internal/store/messages.go:806-826`).
  Byte-returning reader: `GetMessageRaw` (`messages.go:829`), two production
  callers (`internal/meetingimport/importer.go:129`,
  `internal/slack/importer.go:1324`) plus the query engines
  (`internal/query/shared.go:404`, `internal/query/sqlite.go:1086`).
  Writers: `UpsertMessageRaw` (`messages.go:798`),
  `UpsertMessageRawWithFormat` (`messages.go:3083`).
- SQL-native byte movement (must become hash copies):
  `internal/store/dedup.go:292` (copy between messages), `dedup.go:428`
  (bulk IN(...) comparison read), `internal/store/subset.go:457`
  (cross-database copy), `internal/store/migrations.go:107` (legacy
  re-encode scan — gets a `WHERE content_hash IS NULL` guard only).
- `message_bodies`: two sanctioned read paths only —
  `internal/store/api.go:263` (`GetMessageContext`, single PK) and
  `internal/store/conversations.go:309` (`batchPopulateBodies`, PK IN).
  FTS triggers fire on body rows (`schema.sql:354-361`): never drop rows or
  touch `body_text`.
- Blob liveness authority: `ResolveAttachmentBlob`
  (`internal/store/packs.go:486`), reference inventory
  `ListReferencedBlobHashes` (`packs.go:544`), pack candidates
  `ListUnpackedBlobs` (`packs.go:630`) — all currently enumerate only the
  two `attachments` hash columns.
- Backup capture enumerates content blobs in `frozenView.ContentInfo`
  (`internal/backupapp/app.go:135-190`) from `attachments` only.
- One-time migrations: `applied_migrations` table +
  `IsMigrationApplied`/`MarkMigrationApplied`
  (`internal/store/migrations.go:198-221`).
- Offload machinery to reuse unchanged: `blob_offload` catalog
  (`internal/store/offload.go`), tier decorator
  (`internal/attachmenttier`), verified-eviction ladder
  (`cmd/msgvault/cmd/offload.go`), benchmarks
  (`cmd/msgvault/cmd/offload_bench_test.go`).
- Measured targets (2026-07-31, real archive): `message_raw` 403,958 rows /
  ~9.3 GiB zlib; `body_html` ~10.3 GiB; zero `body_html` rows lack a raw
  row; `body_text` (~1.8 GiB) and FTS (~0.9 GiB) stay local.

## Implementation choices the design leaves open (decide as written here)

- **Do not drop NOT NULL on `raw_data` in SQLite.** SQLite cannot ALTER a
  NOT NULL away; a 12-step rebuild of a 9 GiB table is the migration risk
  this plan avoids. Externalized rows keep `raw_data` as a zero-length
  blob; `content_hash IS NOT NULL` is the sole discriminator (design
  amendment note required — one paragraph).
- **One command, two classes**: `msgvault externalize --raw --html`
  (default: both), sharing batch/resume/verify machinery.
- **Batch views stay text-only.** `batchPopulateBodies` feeds conversation
  rendering; it must never fetch HTML blobs (N messages × tier fetch).
  Only `GetMessageContext` (single-message detail) resolves
  `html_content_hash`. Audit both consumers in Task 5 and pin with a test
  that counts opener calls during a batch populate.

## Task 1: schema columns and store row-state plumbing

- [x] Failing tests: `message_raw` round trip in all three states (inline,
      externalized via empty-blob + hash, transient both-set reads as
      externalized); `message_bodies.html_content_hash` round trip;
      migration marker applied once.
- [x] Add `content_hash TEXT` to `message_raw` and `html_content_hash TEXT`
      to `message_bodies` in both schemas; one-time ALTER migrations via
      `applied_migrations` for existing archives (both dialects).
- [x] `go test ./internal/store/ -run 'Externaliz|MessageRaw' -tags "fts5 sqlite_vec"`
- [x] Commit: `Add externalization hash columns to message_raw and message_bodies`

## Task 2: RawBlobOpener seam and mixed-mode reads

- [x] Failing tests: `GetMessageRaw` inline unchanged; externalized row
      streams via injected opener and returns identical bytes; nil opener +
      externalized row = loud error; `GetMessageContext` resolves
      `html_content_hash` through the opener; `batchPopulateBodies` never
      calls the opener (call-count fake).
- [x] `Store.SetRawBlobOpener(func(ctx, hash) (io.ReadCloser, int64, error))`;
      wire in serve.go (tier-backed) and in direct-CLI open paths
      (attachmentstore-backed); thread through the query engines' raw/HTML
      accessors.
- [x] Commit: `Resolve externalized raw and HTML content through the blob opener`

## Task 3: reference authority and backup capture arms

- [x] Failing tests: hashes from both new columns appear in
      `ListReferencedBlobHashes` and resolve as members in
      `ResolveAttachmentBlob`; excluded from `ListUnpackedBlobs` when
      offloaded; `backupapp` ContentInfo counts include them (extend
      `internal/store/backup_test.go` fixtures).
- [x] UNION arms for `message_raw.content_hash` and
      `message_bodies.html_content_hash` in the resolve/membership,
      reference-inventory, and pack-candidate SQL (content blobs written by
      the externalize command land loose in the CAS, so candidates need
      recorded paths — write them CAS-canonical `hash[:2]/hash`); extend
      `frozenView.ContentInfo`.
- [x] Commit: `Treat externalized raw and HTML blobs as first-class content references`

## Task 4: `msgvault externalize` migration command

- [x] Failing e2e (fakevault archive — `internal/fakevault` generates rows
      with real zlib raw_data): batch externalizes N rows; interrupt (limit)
      + re-run resumes idempotently; verify-readback failure leaves the row
      inline; `export-eml` output byte-identical before/after; summary
      prints the VACUUM/compact follow-up and repository re-baseline note;
      `--dry-run` counts only; daemon-stopped guard (mirror offload).
- [x] Implement: select `WHERE content_hash IS NULL` batches; inflate,
      SHA-256, write blob through the packstore mutation path
      (coordinator lease, as `attachment_maintenance.go:239`), verify
      readback, then one transaction setting `content_hash` and emptying
      `raw_data` (same shape for `body_html`/`html_content_hash`).
      Progress = remaining NULL-hash count. Archive marker in
      `archive_metadata` flips new writes (Task 6) to CAS-native.
- [x] Commit: `Add msgvault externalize for raw MIME and body HTML`

## Task 5: byte-moving call-site conversions

- [x] Failing tests per site, covering all three row-state pairings:
      dedup copy (`dedup.go:292`) copies hashes for externalized rows;
      dedup compare (`dedup.go:428`) short-circuits on hash equality and
      fetches bytes only for mixed pairs; subset (`subset.go:457`) carries
      slim rows plus referenced CAS blobs through its existing blob-copy
      pass; legacy migration (`migrations.go:107`) skips
      `content_hash IS NOT NULL` rows.
- [x] Commit: `Convert raw-data movers to hash-aware paths`

## Task 6: CAS-native writes for new syncs

- [x] Failing tests: with the archive marker set, `UpsertMessageRaw*`
      writes blob + slim row (blob durable before row); without it,
      inline as today; Gmail/Slack/Teams importer paths compile against
      the unchanged signatures.
- [x] Commit: `Write raw content CAS-native on migrated archives`

## Task 7: offload class selection and end-to-end tier proof

- [x] Failing e2e (extend `offload_test.go` fixture with raw/HTML blobs):
      `offload --only raw` / `--only html` / `--only attachments` select
      the right classes (raw selection joins `message_raw.message_id`
      directly); offloaded raw served back through
      `show-message --raw`-equivalent engine call via the tier; restore
      works; add a raw-blob flavor to `BenchmarkTierRead`.
- [x] Commit: `Extend offload to raw and HTML blob classes`

## Task 8: docs

- [x] `docs/usage/offload.md` (new: full lifecycle — externalize, backup,
      offload, restore, compact), `docs/configuration.md` touch-ups,
      design-doc status updates, plan checkboxes.
- [x] Commit: `Document externalization and offload lifecycle`

## Verification gates (run on the operator's real archive, not in CI)

- `msgvault externalize --dry-run` count/bytes sanity vs. the measured
  9.3 GiB / 10.3 GiB.
- `export-eml` spot-equivalence on a sample before/after.
- `backup create` after externalization: manifest attachment-blob count
  jumps by ~404k + HTML rows; nightly delta shrinks.
- VACUUM, then `msgvault stats` shows the database near
  metadata+text+FTS size.

## Execution notes (2026-07-31)

All eight tasks landed on the branch, one focused commit each. Deviations
and discoveries worth knowing:

- **Schema probes must never run mid-transaction.** The first
  implementation probed for the externalization columns lazily inside
  maintenance transactions and deadlocked single-connection stores; the
  probe now runs once at store open (all four constructors) and
  `InitSchema` sets the flag after migrating. Same discipline for the
  CAS-native marker probe.
- **Schema-adaptive reference SQL was a hard requirement, not polish.**
  Restore targets are byte-exact unmigrated databases and `OpenReadOnly`
  never migrates, so every reference/membership/stats query carries a
  base variant for pre-externalization schemas
  (`attachmentReferencedHashesBaseSQL` et al.), and
  `backupapp`/`pack_restore` probe the frozen/restored schema directly.
- **Expression indexes live in Go, not schema.sql** (schema executes
  before column migrations on legacy databases):
  `idx_message_raw_content_hash_lower`,
  `idx_message_bodies_html_hash_lower`, created in `InitSchema` and
  asserted by the pack-liveness EXPLAIN test.
- Dedup's normalized-MIME hashing cannot use `content_hash` (it hashes
  *normalized* content), so `StreamMessageRaw` resolves externalized
  rows through the opener instead of short-circuiting on hash equality.
- Dry-run reports from `CountInlineExternalizable` rather than batch
  iteration (nothing leaves the predicate on a dry run).
- The `BenchmarkTierRead` raw-blob flavor was not added: offloaded raw
  content rides the identical `OpenStream` path the attachment flavors
  already measure.

## Deferred coordinated follow-up

- Verified drop-and-derive for `body_html` (own design; zero-orphan
  measurement means full coverage is possible).
- Pack-reader/footer caching in `remoterepo` (budget 5 → ~2 round trips).
- Upstream the kit widening; drop the go.mod replace.
- `remote_content` API/web availability state; `store.Stats` byte
  accounting; daemon-proxied offload/externalize.
