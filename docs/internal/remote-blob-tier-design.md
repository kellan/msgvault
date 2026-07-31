# Remote Blob Tier

Design for offloading cold attachment content to an object store holding the
backup repository, with on-demand ranged reads through the daemon, so a
laptop keeps full search/browse/analytics locally while the bulk of archive
bytes live on cheap remote storage.

Written 2026-07-31. Status: draft for review.

## Motivation

A mature archive is dominated by immutable blobs with a lookup-by-key access
pattern — attachment content and raw MIME — while the hot data (metadata,
FTS index, snippets, Parquet analytics) is a small fraction of total size.
Today the only ways to get the big blobs off the primary machine are to move
the whole archive to a NAS/daemon host, or to hold only a backup repository
remotely and lose interactive access. There is no middle tier.

Goals, in priority order:

1. **Shrink the local footprint** of an archive by evicting attachment
   content that is durably held in a backup repository, with an explicit,
   date-drivable policy (`offload --before 2020-01-01`).
2. **Keep every existing surface working**: web UI, TUI, `export-attachment`,
   and backup capture must transparently read offloaded blobs from the
   remote repository, with integrity verification identical to local reads.
3. **Invent no new remote format.** The backup repository is already a
   content-addressed, immutable, append-only, self-verifying store whose
   blob IDs are byte-for-byte the attachment store's content hashes; the
   remote tier reads *it*, so one uploaded artifact serves both disaster
   recovery and tiering.
4. **Degrade honestly offline**: an unreachable remote must surface as
   "remote unavailable", never as "attachment deleted", and must never block
   metadata/search/browse paths.

Non-goals for this design: sharding or remoting the SQLite database itself
(SQLite requires local page I/O; that problem is solved by daemon mode);
externalizing `message_bodies` (small, trigger-coupled, and disciplined to
two sanctioned PK read paths — `internal/store/api.go:263`,
`internal/store/conversations.go:309`); DuckDB/Parquet-on-S3 (the Parquet
cache excludes bodies and is not a size problem); repository encryption
(tracked by the backup engine's own roadmap; interaction is covered under
Security); and offloading raw MIME in the first delivery (see "Raw MIME
follow-up" — it requires a storage move that is its own project).

## Baseline used for the design (verified 2026-07-31)

Attachment read path:

- All daemon attachment bytes flow through one seam:
  `api.AttachmentBlobStore` (`internal/api/server.go:153-158`), a single
  method `OpenStream(ctx, hash) (io.ReadCloser, int64, error)` whose
  documented miss contract is `errors.Is(err, fs.ErrNotExist)`. It is wired
  once, at `cmd/msgvault/cmd/serve.go:221` (`blobStore :=
  attachmentMaint.blob`).
- `internal/backupapp/content_source.go:15-18` declares a structurally
  identical `BlobStore` interface used by backup capture, so one decorator
  satisfies both.
- Three HTTP handlers serve bytes (`internal/api/files.go:502`
  `openFileContent`; `internal/api/handlers.go:2760`
  `handleGetAttachmentContent`; `internal/api/cli_handlers.go:2354`
  `handleCLIAttachment`), each with its own copy of the packed → loose →
  recorded-path fallback. TUI and export tunnel through
  `/api/v1/cli/attachment` (`internal/tui/actions.go:295`,
  `cmd/msgvault/cmd/tui.go:126`); they never open the store in-process.
- `internal/attachmentstore` is a 117-line adapter over Kit's
  `packstore.Store`. The layered resolve lives in Kit
  (`packstore/store.go:122-166`): catalog resolve → loose read → packed
  read, with exactly one re-resolve retry on `fs.ErrNotExist` to tolerate
  concurrent pack migration. Attachment *rows* are the liveness authority
  (`internal/store/packs.go:486-540`, `ResolveAttachmentBlob`).
- Availability classification opens and closes a stream per file row
  (`internal/api/files.go:537` `fileContentResolvable`), feeding the
  `FileContentState` enum (`files.go:22-28`) that the web UI renders
  ("missing_blob" → "Archived bytes are missing.").

Backup repository (engine `go.kenn.io/kit v0.9.1`):

- Layout (`backup/repo.go:76-83`): `config.toml`,
  `snapshots/<id>.mvmanifest`, `packs/<aa>/<ulid>.mvpack`,
  `indexes/<ulid>.mvidx`, `locks/`, `staging/`. Packs are sealed once and
  never modified; publication refuses to replace an existing pack; indexes
  are immutable and only added.
- Blob resolution: readers union all `.mvidx` files
  (`backup/index.go:160-191`) into `map[BlobID]IndexEntry` where
  `IndexEntry` already carries `(PackID, Offset, StoredLen, Flags)` — 65
  bytes per blob. Opening a pack costs exactly three `ReadAt`s (16-byte
  header, fixed 40-byte trailer, footer region —
  `pack/reader.go:99-239`); reading a blob is one `ReadAt` of `StoredLen`
  at `Offset` (`pack/reader.go:250-267`), or an
  `io.NewSectionReader` for streaming (`pack/blob_reader.go:101`). Every
  layer is self-verifying: footer SHA-256, per-blob CRC32C over stored
  bytes, stored-length accounting, and raw SHA-256 == blob ID.
- **Repo blob IDs are the attachment store's content hashes.** `BlobID =
  SHA-256(plaintext)` computed before compression;
  `attachments.content_hash`/`thumbnail_hash` are hex-decoded verbatim into
  the repository (`backup/attachments.go:26-56`,
  `internal/backupapp/app.go:135-190`). A hash from the live DB is directly
  a repository index key; no manifest walk is needed to fetch a blob.
- Raw MIME is **not** a repository blob: `message_raw.raw_data` is a zlib
  BLOB inside SQLite (`internal/store/schema.sql:367`), captured as
  database pages reachable only via the snapshot page map.
- The engine has no storage abstraction — `Repo` and `pack.Reader` are
  hardcoded to `os.*`/`*os.File` — but the reader's entire I/O surface is
  `ReadAt` + `Size` + `Close` (`pack/reader.go`, `pack/blob_reader.go`),
  and `pack.NewReaderFromFileWithOptions` (`pack/reader.go:88-97`) is the
  single constructor to widen.
- Locks are advisory JSON files that no read path consults; lock-free
  reads are sound because packs/indexes are immutable and reads are
  self-verifying. A long-lived shared lock would break `create`
  (`backup/lock.go:79-93` waits 60s for shared holders, then fails), so
  lock-free is also the *correct* reader posture.
- Retention (`forget`/`prune`) is designed but unimplemented; when it
  ships, repack will retire pack ULIDs and merge indexes, so cached
  mappings can go permanently stale.

Environment: there is no object-storage SDK or client anywhere in the
module today; the only outbound content fetch on a request path is the
SSRF-guarded remote-image proxy (`internal/api/remote_image.go:224-330`).
`msgvault stats` reports database size only — no attachment-store bytes
(`cmd/msgvault/cmd/stats.go:96-109`).

## Design

### Shape

Three new pieces, all msgvault-side except one small upstream widening:

```
internal/objstore      ObjectStore: ranged reads over file/https/s3 backends
internal/remoterepo    Read-only backup-repo client over an ObjectStore:
                       index cache, pack footer cache, verified blob streams
internal/attachmenttier Decorator around the daemon blob store:
                       local OpenStream → on fs.ErrNotExist → remoterepo,
                       plus the local fetch cache and offload catalog
```

Plus one Kit change: `pack.NewReaderFromReaderAt(ra io.ReaderAt, size int64,
id string, crypter, opts)` alongside the existing file constructor, with
`pack.Reader` holding `io.ReaderAt` instead of `*os.File`. Nothing
downstream changes — CRC, zstd streaming, SHA verification, and the
`BlobReader` early-close contract are already expressed against
`ReadAt`/`SectionReader`. The pack ID must be passed explicitly (today it is
derived from the filename, and it participates in the encrypted-footer AAD).
This widening independently benefits `packstore`, which has the same
`*os.File` constraint.

### Object store abstraction

```go
// internal/objstore
type ObjectStore interface {
    // ReadRange returns [off, off+len) of the named object.
    ReadRange(ctx context.Context, key string, off, length int64) (io.ReadCloser, error)
    // Size returns the object's total length.
    Size(ctx context.Context, key string) (int64, error)
    // List returns keys under a prefix (used only for indexes/ reconciliation).
    List(ctx context.Context, prefix string) ([]string, error)
}
```

Three backends, selected by the URL scheme of `[offload] remote`:

- `file://` (or a bare path) — the repository on a local/USB/NAS-mounted
  path. Zero new dependencies; also the test backend.
- `https://` — plain ranged GETs against any static host (rclone `serve
  http`, a public/presigned bucket, Caddy on a NAS). Auth via optional
  bearer-token config. Reuses the outbound-client hygiene of the
  remote-image proxy (explicit timeouts; but no SSRF allowlist — the URL is
  operator-configured, like `[remote] url`).
- `s3://bucket/prefix` — SigV4 ranged GET/HEAD/LIST against S3-compatible
  endpoints (AWS, B2, R2, MinIO). Credentials come from the standard
  `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_REGION` environment or an
  optional `credentials_file`; never from `config.toml`, which `Save()`
  rewrites and round-trips. Implementation preference: a minimal internal
  SigV4 signer (~200 lines, GET/HEAD/LIST only) rather than adopting
  aws-sdk-go-v2 for three verbs; revisit if requirements grow.

The remote is treated as **read-only** by this feature. Uploading the
repository remains the operator's `rclone sync` (or the repo simply *is* the
mounted path). A future `backup push` is out of scope here.

### Repository reader (`internal/remoterepo`)

A read-only client that resolves `hash → verified stream` against a backup
repository accessed through an `ObjectStore`:

1. **Open**: fetch `config.toml` once; enforce `min_reader_version` exactly
   as the local engine does; refuse encrypted repositories until streaming
   decryption exists (plain-format packs only, matching Kit's
   `UnsupportedStreamError` posture).
2. **Index**: list `indexes/`, fetch and SHA-verify each `.mvidx` not
   already cached, and maintain the union map. Index files are cached
   *per file name* on local disk under `~/.msgvault/backup-cache/<repo-id>/`
   (beside the existing page-hash cache) because prune will one day merge
   and delete them — the cache must reconcile against `List`, not assume
   append-only forever. Steady-state cost: ~65 bytes/blob, refreshed only
   when new index files appear.
3. **Pack open**: on first use of a pack, two ranged reads (40-byte tail,
   then the footer region) via the widened `pack.Reader` over an
   `ObjectStore`-backed `io.ReaderAt`; footers are LRU-cached (bounded
   count, same spirit as `packstore`'s 16 reader slots).
4. **Blob read**: one ranged read of `(Offset, StoredLen)`, streamed through
   Kit's `BlobReader` so CRC32C, stored-length, and raw-SHA-256
   verification — and the `ErrVerificationIncomplete` early-close
   contract — are bit-identical to local reads. The zstd frame-header peek
   (`pack/blob_reader.go:135-152`) is satisfied by over-reading the first
   `zstd.HeaderMaxSize` bytes of the same range rather than issuing a
   second remote read.
5. **Staleness**: on a pack `NotFound` (future prune retiring a ULID), drop
   cached state for that pack, force an index reconcile, and re-resolve
   **once** — the same bounded single-retry idiom `packstore` uses for
   local pack migration. A second miss is a hard error.
6. **Locks**: none taken, deliberately (see Baseline). Reads are
   self-verifying and packs immutable; a shared lock would break nightly
   `create`.

No manifest is read on the blob path. Manifests are only consulted by the
offload command's safety check (below) to confirm the repository is one
that snapshots this archive.

### Read-path integration

A decorator implementing both `api.AttachmentBlobStore` and
`backupapp.BlobStore` (identical signatures):

```go
func (t *TieredStore) OpenStream(ctx, hash) (io.ReadCloser, int64, error) {
    rc, n, err := t.local.OpenStream(ctx, hash)
    if err == nil || !errors.Is(err, fs.ErrNotExist) {
        return rc, n, err
    }
    if !t.catalog.Offloaded(hash) {      // genuinely missing, not tiered
        return nil, 0, err               // preserve fs.ErrNotExist
    }
    return t.fetchRemote(ctx, hash)      // cache-through, verified
}
```

Wired at `cmd/msgvault/cmd/serve.go:221` when `[offload]` is configured —
a one-line change covering all three HTTP handlers, and therefore TUI and
export for free. `backup create` (`cmd/msgvault/cmd/backup.go:472`)
constructs the same decorator so capture can read offloaded blobs, though
the offload invariant below means it should never need to.

Error contract:

- Local miss with no offload record → unchanged `fs.ErrNotExist` (existing
  loose-candidate fallbacks in handlers keep working).
- Offloaded but remote unreachable/failed → a new sentinel
  `attachmenttier.ErrRemoteUnavailable` that does **not** satisfy
  `fs.ErrNotExist`. Handlers map it to HTTP 503 with a distinct error code
  (`remote_tier_unavailable`) so the UI can say "stored remotely,
  currently unreachable" instead of "missing". This distinction is the
  core honesty requirement of goal 4.

### Offload catalog and availability classification

A new table (SQLite and PG schemas):

```sql
CREATE TABLE IF NOT EXISTS blob_offload (
    content_hash TEXT PRIMARY KEY,      -- lowercase hex SHA-256
    repo_id      TEXT NOT NULL,         -- repository that holds it
    offloaded_at TIMESTAMP NOT NULL,
    stored_len   INTEGER NOT NULL       -- bytes freed locally (reporting)
);
```

Consumers:

- The decorator's `Offloaded(hash)` check (PK lookup, cached in-process).
- `PackCatalog` excludes offloaded hashes from the packing/maintenance
  reference inventory so nightly maintenance does not perpetually count
  them as `BlobsMissing` and never re-packs or orphan-sweeps around them.
- `fileContentState` (`internal/api/files.go:524`) gains a
  `remote_content` state resolved from the catalog — a table lookup, **not**
  a network probe. This also removes the existing per-row open/close in
  `fileContentResolvable` for offloaded rows; against a remote tier a
  per-row probe would be a request-path network round trip and is
  explicitly forbidden by this design. The web UI maps the new state
  ("Stored in backup repository") alongside the existing `missing_blob`
  mapping; the OpenAPI enum gains the value.
- `msgvault stats` gains `Local attachment bytes / Offloaded bytes / Blobs
  offloaded`, carried through `store.Stats` and the daemon CLI-stats
  endpoint (today no attachment byte accounting exists at all).

### `msgvault offload` command

```text
msgvault offload --before DATE [--account EMAIL] [--dry-run]
                 [--max-bytes N] [--jobs N]
msgvault offload status
msgvault offload restore --before DATE | --hash H   # re-materialize locally
```

Selection: attachments whose newest referencing message predates `--before`
(date drives *eviction policy*, not storage layout — a blob referenced by
both a 2009 and a 2025 message is hot and stays). Selection reuses the
attachment-rows-as-authority model; thumbnails follow their parent.

Safety ladder, per blob, mirroring the existing verified loose sweep
(`packstore/pack.go:550-570` — "never delete local until a verified second
copy is proven"):

1. Hash present in the remote repository index.
2. **Verified remote read**: stream the blob from the remote through full
   verification (CRC + SHA). This is the expensive step and is the default;
   it is exactly the read path production will later depend on, so it
   doubles as a rehearsal. `--max-bytes` bounds a run; repeat runs resume.
3. Record in `blob_offload`, then evict: delete the loose file, and/or
   delete the blob's `attachment_pack_index` rows so local pack space
   becomes dead and the existing repack maintenance reclaims it. (Packed
   blobs are not individually deletable; this reuses the established
   `PackUsage`/repack machinery unchanged.)
4. Refuse to run at all if the repository's latest manifest does not
   identify this archive (repo captures a different `msgvault.db`), or if
   the newest snapshot is older than a configurable staleness bound
   (default 14 days) — a stale repo is a red flag that the off-site sync
   is broken, and offloading against it would be betting the only copy.

The command runs through the daemon-backed CLI path like other mutating
maintenance (`docker exec` / proxied), taking the attachment mutation lease
(`runWithAttachmentMutation`, `cmd/msgvault/cmd/attachment_maintenance.go:239`)
so it cannot race pack/repack/backup capture. It takes **no repository
lock** (reads only).

`offload restore` is the inverse: verified fetch → write loose → remove the
`blob_offload` row (the blob becomes ordinary loose content; nightly
maintenance may re-pack it).

### Local fetch cache

Remote fetches are cached under `~/.msgvault/offload-cache/` (derived
accessor beside `AttachmentsDir()` in `internal/config`), content-addressed
like the loose store but **separate from it** — cached copies must not be
re-packed, re-offloaded, or counted by maintenance, and a bounded LRU
(default `cache_max_bytes = 2 GiB`, evict-least-recently-opened) needs
different lifecycle rules than archive content. Cache writes are
verify-then-rename; a cache hit re-verifies SHA on read like any loose
read. `Offloaded` remains true for cached blobs — the cache is an
accelerator, not a state change.

### Backup capture interplay

Invariant: **offload-eligible ⇒ already present in the remote repository**,
and capture only reads content it has not previously stored (unchanged
attachments are skipped by hash). Therefore nightly `backup create` never
needs to fetch an offloaded blob back over the network. The decorator on
seam C exists as a belt-and-braces fallback (e.g. a fresh repository seeded
while blobs are offloaded to an *older* repository), and capture through it
remains correct, just slow — `create` logs a prominent warning the first
time it reads a blob remotely.

Multiple repositories: `blob_offload.repo_id` records which repository
holds each blob, but v1 supports a single configured remote; offloading
against a different repo than previously recorded is refused rather than
silently splitting the blob set.

### Configuration

```toml
[offload]
# Backup repository the remote tier reads. Schemes: file://, https://, s3://
remote = "s3://my-bucket/msgvault-backup"
# S3-compatible endpoint override (B2, R2, MinIO). Credentials come from the
# standard AWS_* environment or credentials_file — never from this file.
# endpoint = "https://s3.us-west-000.backblazeb2.com"
# credentials_file = "~/.msgvault/offload-credentials"
# Local cache for remotely fetched blobs.
# cache_max_bytes = "2GiB"
# Refuse to offload if the newest snapshot is older than this.
# max_snapshot_age_days = 14
```

Follows the section conventions in `internal/config/config.go`:
`OffloadConfig` struct with `ApplyDefaults()`/`Validate()`, registered in
both `NewDefaultConfig()` and `decodeConfig()`; `credentials_file` joins
the `expandPath`/`resolveRelative` lists exactly as `cfg.Backup.Repo` does
(`config.go:621,636`). User-facing docs land in `docs/configuration.md` and
a new `docs/usage/offload.md`.

### Security and privacy

- **Plaintext exposure is the headline tradeoff.** Until repository
  encryption ships in the backup engine, offloaded blobs sit in the bucket
  protected only by transport TLS, bucket ACLs, and provider-side
  encryption (SSE). `rclone crypt` remotes are *incompatible* with the
  direct-read tier (the reader speaks the repository format, not rclone's
  envelope) — the docs must say so explicitly, since current backup docs
  recommend rclone. `offload` prints a one-time warning when the remote is
  not `file://`. When engine-level encryption lands, `remoterepo` inherits
  it by construction (same pack reader), minus streaming until Kit
  supports it.
- Credentials never enter `config.toml` (rewritten by `Save()`); env or a
  0600 credentials file only, mirroring the tokens-directory posture.
- All remote reads terminate in Kit's verification stack; a tampered or
  torn object fails closed (`ErrBlobMismatch`/`ErrChecksum`), never serves
  wrong bytes.
- The daemon's outbound HTTP client uses explicit dial/TLS/header timeouts
  (remote-image proxy precedent); fetches are request-scoped and
  context-cancelled. Remote fetch handlers are GETs and already bypass the
  operation gate, so a slow remote cannot wedge mutating operations; the
  offload command itself holds the mutation lease, not the gate.

### Observability

- `offload status`: blobs/bytes offloaded, cache size/hit-rate, last
  successful remote read, index cache age.
- Structured logs for every remote fetch (hash prefix, pack, latency,
  bytes) at debug; misses and re-resolutions at info; verification
  failures at error.
- `stats` additions as above.

## Raw MIME follow-up (separate design)

`message_raw` is the other large blob class, but it is stored as SQLite
pages, so it is invisible to the repository's content-addressed index —
offloading it requires first externalizing `raw_data` into the attachment
CAS, then this design applies unchanged. The store-side survey says that is
tractable but real work: `GetMessageRaw` is the only byte-returning
accessor (two production callers), a `compression`/location discriminator
column already exists, and most `message_raw` joins need presence only —
but three sites do SQL-native blob movement (`internal/store/dedup.go:292`,
`internal/store/subset.go:457`, `internal/store/migrations.go:107`), the
column is `NOT NULL` in both schemas, and `dedup.go:428` bulk-reads by ID
set. That migration (nullable column, stub rows, `RawStore` seam mirroring
`AttachmentBlobStore`) is deliberately excluded from this delivery and
should be written up once the attachment tier has proven the read path.

## Testing

- `objstore`: contract test suite run against all three backends (file
  always; https against `httptest`; s3 against the signer with a canned
  request-verifying fake — no live-bucket dependency in CI).
- `remoterepo`: golden repositories built with the real `backup create`
  engine in a temp dir, served through the `file://` and `httptest`
  backends; assert byte-identical streams vs. local reads, index
  reconciliation after adding/deleting index files, bounded single retry on
  retired packs, refusal of encrypted/newer-format repos.
- `attachmenttier`: decorator semantics — miss passthrough preserves
  `fs.ErrNotExist`; offloaded+unreachable yields `ErrRemoteUnavailable`;
  cache fill/evict/verify; early-close propagates
  `ErrVerificationIncomplete`.
- Offload command: end-to-end against a real temp archive + real
  repository — offload, assert local eviction and repack reclamation,
  read back through the daemon handlers, `offload restore`, and the
  stale-repo / wrong-archive refusals.
- API: handler mapping tests for the new 503 code and `remote_content`
  state; OpenAPI schema update; web UI state mapping unit test beside the
  existing `missing_blob` one.
- Kit: `NewReaderFromReaderAt` round-trip tests live upstream with the
  existing pack reader suite.

## Alternatives considered

### s3fs / FUSE under the archive

Rejected. SQLite over network FUSE breaks locking and WAL and serializes
B-tree page walks over 50–200 ms round trips; even the attachment store
would pay per-syscall latency with no verification story. The daemon model
already solves "archive elsewhere" for the whole-archive case.

### A bespoke remote blob format (mirror the loose/pack store to S3)

Rejected. It would duplicate what the backup repository already is —
content-addressed, immutable, verified — while doubling upload bytes and
creating a second thing to operate, sync, and prune. Reusing the repo means
the off-site artifact users already maintain becomes the tier.

### Extending Kit's `packstore.Location` with a remote variant (seam A)

Deferred, not rejected. Architecturally the resolver is where
authority+location belong, but it forces an upstream API change on the hot
local path and couples Kit to object storage. The msgvault-side decorator
achieves the same behavior at the established `fs.ErrNotExist` boundary;
if the tier later needs richer states (e.g. per-blob remote location
overrides), revisit seam A.

### DuckDB `httpfs` over Parquet on S3

Orthogonal, not competing: it remotes the *analytics cache*, which excludes
bodies and is not a meaningful share of archive size. May be worth doing
someday for daemonless remote analytics, but it does not address the 100 GB
problem this design targets.

### rclone mount instead of a native reader

Rejected as the supported path (fine as an unsupported stopgap): it
reintroduces FUSE latency and failure modes, cannot express the
"offloaded vs. missing" distinction, defeats crypt anyway for ranged
reads in practice, and gives no hook for the verified-eviction safety
ladder.

## Delivery order

1. **Kit**: widen `pack.Reader` to `io.ReaderAt` +
   `NewReaderFromReaderAt`; release and bump msgvault's dependency.
2. `internal/objstore` with `file://` + `https://` backends and the
   contract suite (s3 backend follows once the read path is proven).
3. `internal/remoterepo` read-only client (index cache, footer cache,
   verified streams, bounded retry).
4. `blob_offload` table + `PackCatalog` inventory exclusion + decorator +
   serve wiring; new API error code and `remote_content` state end-to-end
   (OpenAPI, web UI).
5. `msgvault offload` / `offload status` / `offload restore` with the
   verified-eviction ladder and mutation-lease integration.
6. `s3://` backend (SigV4 signer) + `docs/usage/offload.md` +
   `docs/configuration.md` section + stats/observability additions.
7. Follow-up design: `message_raw` externalization into the CAS.
