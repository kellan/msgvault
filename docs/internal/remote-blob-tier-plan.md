# Remote Blob Tier Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development
> if subagents are available, or superpowers:executing-plans otherwise, to
> implement this plan. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Ship the first usable slice of the remote blob tier
(`docs/internal/remote-blob-tier-design.md`): offload attachment content to a
backup repository reachable as a filesystem path (external drive, NAS mount,
rclone mount), serve offloaded blobs back through the daemon transparently,
and keep every safety property of the design's verified-eviction ladder.

**Architecture:** A `[offload]` config section names a backup repository; a
read-only `internal/remoterepo` client (kit-backed, local paths only in this
slice) resolves content hashes to verified blob streams; a `blob_offload`
catalog table records which hashes have been evicted locally;
`internal/attachmenttier` decorates the daemon's `AttachmentBlobStore` to fall
through to the repository on local miss for cataloged hashes, surfacing a
distinct `ErrRemoteUnavailable` when the repository cannot serve; the
`msgvault offload` command selects cold attachment blobs by date and deletion
state, proves each one readable from the repository byte-for-byte, then evicts
the local copy (loose file delete + pack-index row delete, existing repack
maintenance reclaims pack space); maintenance inventory queries exclude
offloaded hashes so nightly pack/orphan sweeps neither count them missing nor
resurrect them.

**Tech Stack:** Go 1.26, Cobra, SQLite, PostgreSQL, Testify,
go.kenn.io/kit (backup, pack, packstore).

---

## Working rules

- Branch: `claude/msgvault-backup-storage-3h579k`.
- Use Testify (`require` for setup, `assert` for independent checks; expected
  value first). Run Go tests with `-tags "fts5 sqlite_vec"`.
- After Go changes: `go fmt ./...` and `go vet ./...`; stage everything.
- One focused commit per task; never amend or squash unless asked.
- Never name private downstream projects in public artifacts; scrub before
  publishing.
- Superpowers skills are not installed in this environment; execute the same
  loop manually: write failing tests, confirm red, implement, confirm green,
  commit.

## Scope of this slice (and what is deliberately out)

In scope: local-path repositories only (`[offload] repo` is a directory).
This is a complete, testable vertical: config → catalog → remote reader →
decorator → serve wiring → offload/status/restore commands.

Out of scope, tracked in "Deferred coordinated follow-up" below: the kit
`pack.Reader` `io.ReaderAt` widening and native `https://`/`s3://` backends
(blocked on the upstream kit module, not in this repository); the bounded
fetch cache (meaningful only for network backends); the `remote_content`
API/web availability state; `store.Stats` byte accounting; daemon-proxied
offload (this slice requires the daemon stopped, mirroring
`unpack-attachments`); `message_raw` externalization (own design doc, the
critical-path follow-up per the amended delivery order).

## Store model

New table (both `schema.sql` and `schema_pg.sql`, created idempotently):

```sql
CREATE TABLE IF NOT EXISTS blob_offload (
    content_hash TEXT PRIMARY KEY,
    repo_id      TEXT NOT NULL,
    offloaded_at TIMESTAMP NOT NULL,
    stored_len   INTEGER NOT NULL
);
```

Invariants: a row exists iff the blob's local bytes were deliberately evicted
after a verified repository read; `repo_id` is the repository's
`config.toml` `repo_id`; offloading against a different repository than an
existing row's `repo_id` is refused.

## File map

- `internal/config/config.go` — `OffloadConfig` (`[offload]`), defaults,
  validation, path expansion; `internal/config/config_test.go`.
- `internal/store/schema.sql`, `internal/store/schema_pg.sql` — `blob_offload`.
- `internal/store/offload.go` (+ `offload_test.go`) — record/delete/lookup/
  stats accessors; hash-set loader for eviction checks.
- `internal/store/packs.go` — exclusion of offloaded hashes from
  `ListReferencedBlobHashes` and `ListUnpackedBlobs` (+ tests).
- `internal/remoterepo/reader.go` (+ tests) — kit-backed read-only repository
  client: open/validate, index load with single reload-on-miss, `Has`,
  `OpenBlob`, `RepoID`, `LatestSnapshotAge`.
- `internal/attachmenttier/tier.go` (+ tests) — `ErrRemoteUnavailable`,
  tiered `OpenStream` decorator, lazy remote open.
- `cmd/msgvault/cmd/serve.go` — decorator wiring when `[offload]` configured.
- `internal/api/handlers.go`, `internal/api/files.go`,
  `internal/api/cli_handlers.go` — map `ErrRemoteUnavailable` to HTTP 503
  `remote_tier_unavailable` (+ handler test).
- `cmd/msgvault/cmd/offload.go` (+ test) — `offload`, `offload status`,
  `offload restore`.
- `docs/configuration.md` — `[offload]` section.

## Task 1: `[offload]` config section

- [x] Failing tests in `internal/config/config_test.go`: defaults applied
      (`max_snapshot_age_days` = 14), `repo` path `~` expansion and
      config-relative resolution, validation rejects `s3://`/`https://`
      schemes with a "not yet supported" error, `Enabled()` false when empty.
- [x] Implement `OffloadConfig { Repo string; MaxSnapshotAgeDays int }` with
      `ApplyDefaults`/`Validate`/`Enabled`, register in `NewDefaultConfig`,
      `decodeConfig`, `expandPath` and `resolveRelative` lists (mirror
      `cfg.Backup.Repo`).
- [x] `go test ./internal/config/ -tags "fts5 sqlite_vec"`
- [x] Commit: `Add [offload] config section`

## Task 2: blob_offload table and store accessors

- [x] Failing tests in `internal/store/offload_test.go` (via
      `testutil.NewTestStore`): record → lookup true; delete → false; stats
      sum count/bytes; re-record same hash upserts; record with different
      repo_id for existing rows surfaces in `OffloadRepoIDs`.
- [x] Add table to both schemas; implement `internal/store/offload.go`:
      `RecordBlobOffload`, `DeleteBlobOffload`, `IsBlobOffloaded`,
      `OffloadedBlobStats`, `OffloadRepoIDs`, `ListOffloadedHashes`.
- [x] `go test ./internal/store/ -run Offload -tags "fts5 sqlite_vec"`
- [x] Commit: `Add blob_offload catalog table and store accessors`

## Task 3: maintenance inventory excludes offloaded blobs

- [x] Failing test: seed an attachment row + `blob_offload` row; assert the
      hash is absent from `ListReferencedBlobHashes` and `ListUnpackedBlobs`,
      and still present after `DeleteBlobOffload`.
- [x] Add `NOT EXISTS (SELECT 1 FROM blob_offload bo WHERE bo.content_hash =
      ...)` predicates to both queries (content and thumbnail hash arms).
- [x] `go test ./internal/store/ -tags "fts5 sqlite_vec"`
- [x] Commit: `Exclude offloaded blobs from attachment maintenance inventory`

## Task 4: remoterepo read-only client

- [x] Failing tests using a fixture repository built with kit's real
      primitives (`backup.Init`, `pack.NewWriter`, `Repo.WriteIndex`):
      `Has` true/false; `OpenBlob` streams bytes equal to the original and
      verifies on EOF; corrupted pack byte fails closed; blob added by a
      second index file is found after reload-on-miss; encrypted repo config
      refused; `RepoID` round-trips.
- [x] Implement `internal/remoterepo`: `Open(root)` (kit `backup.Open`,
      refuse `Encryption != ""`), lazy `LoadBlobIndex` union with one forced
      reload on miss, `Has(hash)`, `OpenBlob(ctx, hash)` returning
      `io.ReadCloser` + raw size via kit `Repo.OpenBlob` (ext `.mvpack`,
      nil crypter), `RepoID()`, `LatestSnapshot()`.
- [x] `go test ./internal/remoterepo/ -tags "fts5 sqlite_vec"`
- [x] Commit: `Add remoterepo read-only backup repository client`

## Task 5: attachmenttier decorator

- [x] Failing tests with fakes: local hit passes through; local
      `fs.ErrNotExist` + not offloaded → `fs.ErrNotExist` preserved; local
      miss + offloaded → remote stream served; remote open failure →
      `ErrRemoteUnavailable` (and NOT `fs.ErrNotExist`); remote lazily opened
      once and reused; nil remote config → decorator not constructed.
- [x] Implement `internal/attachmenttier`: `ErrRemoteUnavailable`, `Store`
      with `OpenStream`, lazy remote dial via constructor-injected opener.
- [x] `go test ./internal/attachmenttier/ -tags "fts5 sqlite_vec"`
- [x] Commit: `Add attachment tier decorator with remote fallthrough`

## Task 6: serve wiring and 503 mapping

- [x] Failing handler test: request for an offloaded hash whose repository is
      unreachable returns 503 `remote_tier_unavailable` (not 404) on
      `/attachments/{hash}/content` and `/api/v1/cli/attachment`.
- [x] Wire decorator in `serve.go` when `cfg.Offload.Enabled()`; add
      `errors.Is(err, attachmenttier.ErrRemoteUnavailable)` → 503 mapping in
      the three read handlers.
- [x] `go test ./internal/api/ -tags "fts5 sqlite_vec"`
- [x] Commit: `Serve offloaded attachments through the remote tier`

## Task 7: offload command family

- [x] Failing e2e test (temp archive + fixture repo): `offload --before`
      selects only attachments whose every referencing message predates the
      cutoff; `--deleted-from-source`/`--archive-deleted` predicates AND in;
      shared blob with a live reference is skipped; eviction removes loose
      file and pack-index rows and records `blob_offload`; `--dry-run`
      changes nothing; repository missing the blob → no eviction, error
      counted; stale repository (latest snapshot older than
      `max_snapshot_age_days`) refuses; empty repository refuses;
      `offload status` reports counts/bytes; `offload restore` re-materializes
      the loose file, verifies hash, deletes the record.
- [x] Implement `cmd/msgvault/cmd/offload.go`: local-only + daemon-stopped
      guard (mirror `unpack-attachments`), selection SQL in
      `internal/store/offload.go` (`ListOffloadCandidates(cutoff, requireSourceDeleted,
      requireArchiveDeleted)` returning hash, stored size, loose paths),
      verified read-through ladder (stream to EOF, compare raw length),
      transactional record+evict per blob, summary output.
- [x] `go test ./cmd/msgvault/cmd/ -run Offload -tags "fts5 sqlite_vec"`
- [x] Commit: `Add msgvault offload, offload status, offload restore`

## Task 8: docs

- [x] `docs/configuration.md`: `[offload]` section (local-path repositories,
      the s3/https roadmap note, plaintext caveat pointer).
- [x] Update design doc status line to reference this plan.
- [x] Commit: `Document [offload] configuration`

## Execution notes (2026-07-31)

All eight tasks landed on the branch. Two deliberate deviations from the
task text as written:

- **Task 3 narrowed to `ListUnpackedBlobs` only.** Offloaded hashes stay in
  `ListReferencedBlobHashes` on purpose: the loose-orphan sweep deletes
  files that are NOT referenced, and a crash between `offload restore`
  writing the loose file and deleting the `blob_offload` row must never
  make that fresh copy sweepable. The test
  `TestListReferencedBlobHashesKeepsOffloaded` pins the rationale; Tasks 2
  and 3 shipped as one store commit.
- **Offload requires the daemon stopped** (daemon-owner lock plus
  live-daemon probe, the `unpack-attachments` pattern) rather than running
  through the daemon-backed CLI; daemon-proxied offload stays deferred.

Environment note: `TestPatchSettingsClassifiesFilesystemFailureAsServerError`
fails in this container with or without these changes (root ignores the
read-only-directory fixture); unrelated to this work.

## Delivered after the first slice (2026-07-31, same branch)

- Kit widening landed on the `github.com/kellan/kit` fork
  (`pack.NewReaderFromReaderAt`); msgvault consumes it via a go.mod
  `replace` until it is upstreamed.
- `internal/objstore` (Store interface, filesystem backend, SigV4 S3
  backend with signature-verifying fake-server tests).
- `internal/remoterepo.ObjectReader` + `OpenLocation` dispatch;
  `[offload] repo` now accepts `s3://bucket/prefix` with `s3_endpoint` /
  `s3_region`.

## Deferred coordinated follow-up

- Upstream the kit widening to `kenn-io/kit` and drop the go.mod replace.
- `https://` static-host backend (needs a listing story; S3 covers the
  cloud case) + bounded local fetch cache for network repositories.
- `remote_content` availability state end-to-end (OpenAPI, web UI) and
  `store.Stats` local/offloaded byte accounting.
- Daemon-proxied `offload` (mutation lease instead of daemon-stopped).
- `message_raw` externalization into the attachment CAS — companion design
  doc, then the same offload machinery applies; the dominant-size follow-up
  per the design doc's 2026-07-31 sizing amendment.
