# Message Raw Externalization

Design for moving raw message content (`message_raw.raw_data`) out of the
SQLite database and into the content-addressed attachment store, so it is
capturable by backup as individual blobs and offloadable through the
remote blob tier (`remote-blob-tier-design.md`). This is step 2 of that
design's amended delivery order and the change that attacks the dominant
share of archive size.

Written 2026-07-31. Status: draft for review; sizing confirmation on the
target archive pending (per-table dbstat run in progress).

## Motivation

On the measured real archive, `msgvault.db` is 25.6 GB of a 29.5 GB total
while the attachment store is 3.9 GB. The expected dominant share of the
database is `message_raw`: zlib-compressed raw MIME, which embeds a
base64 copy of every attachment inline. While that content lives inside
SQLite it is invisible to every blob-level capability msgvault has built:
backup captures it only as opaque database pages, the remote tier cannot
serve it, and `offload` cannot evict it.

Goals, in priority order:

1. **Make raw message content offloadable.** Once raw MIME is a
   content-addressed blob, the existing `blob_offload` catalog, tier
   decorator, verified-eviction ladder, and `offload` command apply with
   only additive changes.
2. **Shrink the database** to metadata + bodies + FTS, restoring fast
   whole-file operations (backup page scans, integrity checks, copies).
3. **Shrink nightly backup deltas.** Raw MIME today rides along as page
   churn; as CAS blobs it is written once and deduplicated forever —
   including across accounts that hold copies of the same message.
4. **Change no consumer semantics.** `GetMessageRaw`, dedup, subset
   export, `export-eml`, and re-parse flows keep their contracts.

Non-goals: externalizing `message_bodies` (small, FTS-trigger-coupled,
disciplined to two sanctioned PK lookups); changing the MIME bytes in any
way (the blob is the exact raw content, byte-for-byte); PostgreSQL bulk
migration tooling in the first delivery (the schema change lands on both
dialects, the migration command targets SQLite archives first).

## Baseline used for the design (verified 2026-07-31)

- Schema: `message_raw` (`internal/store/schema.sql:367`) —
  `message_id INTEGER PRIMARY KEY`, `raw_data BLOB NOT NULL`,
  `raw_format TEXT NOT NULL`, `compression` per-row (zlib today, applied
  in-process at `internal/store/messages.go:806-826`). PG mirror uses
  BYTEA (`schema_pg.sql:284`).
- Byte-returning read path is a single method with two production
  callers: `Store.GetMessageRaw` (`internal/store/messages.go:829`,
  PK lookup + zlib inflate), called from
  `internal/meetingimport/importer.go:129` and
  `internal/slack/importer.go:1324`; the query engines expose it to the
  API/CLI (`internal/query/shared.go:404-434`,
  `internal/query/sqlite.go:1086`), serving `show-message --raw`,
  `export-eml`, and the daemon raw endpoint
  (`internal/api/cli_handlers.go:2334`).
- Writers: `UpsertMessageRaw` (`messages.go:798`),
  `UpsertMessageRawWithFormat` (`messages.go:3083`), called by Gmail sync,
  the generic importer, and the Slack/Teams importers.
- Most other `message_raw` SQL needs **presence only** (existence joins in
  `dedup.go:153,245,368,698,732,748`, `inspect.go:76,190`,
  `messages.go:575,1780`) and keeps working against a stub row.
- Three sites move blob bytes **inside SQL** and are the real migration
  obstacles: `dedup.go:292` (INSERT..SELECT copy between message IDs),
  `subset.go:457` (cross-attached-database `INSERT..SELECT *`),
  `migrations.go:107` (legacy full-table re-encode scan).
- One bulk byte read: `dedup.go:428` (chunked `IN (...)` read of
  `raw_data` for content comparison).
- Backup captures only `attachments.content_hash`/`thumbnail_hash` as
  content blobs (`internal/backupapp/app.go:135-190`); everything in
  `message_raw` is DB pages via the snapshot page map.
- The attachment CAS (packstore + catalog) treats attachment **rows** as
  liveness authority (`internal/store/packs.go:486`); the reference
  inventory and pack-candidate queries enumerate the two attachment hash
  columns only (`packs.go:544`, `packs.go:630`).

## Design

### Storage model

Raw message content becomes a blob in the existing attachment CAS,
identified by `content_hash = SHA-256(raw bytes)` — the uncompressed,
exact wire bytes. Pack-level zstd replaces row-level zlib (equivalent
compression, and pack storage detects incompressible content). The blob
space is already shared and content-addressed, so two accounts archiving
the same message store its raw MIME once; today they store it twice as
unrelated zlib rows.

Schema change (both dialects):

```sql
ALTER TABLE message_raw ADD COLUMN content_hash TEXT;  -- lowercase hex SHA-256
-- raw_data becomes nullable (schema rewrite on SQLite; ALTER on PG)
```

Row states, discriminated by `content_hash`:

- `content_hash IS NULL` — legacy inline row: bytes in `raw_data`,
  decoded per `compression`. Read path unchanged.
- `content_hash IS NOT NULL AND raw_data IS NULL` — externalized: bytes
  live in the CAS under that hash.
- `content_hash IS NOT NULL AND raw_data IS NOT NULL` is transient
  (mid-migration, hash computed and blob durable but row not yet
  slimmed) and reads as externalized — the hash is already authoritative.

The row itself never disappears: it keeps `raw_format`, and its presence
is what all the existence joins test, so the ~8 presence-only call sites
need no change at all.

### Read seam

`internal/store` gains a narrow injected opener, mirroring the tier
interfaces:

```go
// RawBlobOpener resolves an externalized raw-content hash to a verified
// stream. Wired to the daemon's tiered blob store in serve mode and to a
// process-local attachmentstore elsewhere.
type RawBlobOpener func(ctx context.Context, hash string) (io.ReadCloser, int64, error)

func (s *Store) SetRawBlobOpener(open RawBlobOpener)
```

`GetMessageRaw` becomes: PK lookup → inline row decodes as today;
externalized row streams from the opener and buffers (its existing
contract returns `[]byte`; both production callers re-parse whole
messages, so buffering is unchanged behavior). A nil opener on an
archive containing externalized rows is a loud configuration error, not
a silent nil.

Because the daemon wires the opener to the **tier** store, an offloaded
raw blob is transparently served from the backup repository, and an
unreachable repository surfaces the existing
`remote_tier_unavailable` distinction end to end.

### Write path

`UpsertMessageRaw*` gains a CAS-backed mode once the archive opts in
(see Migration): hash the raw bytes, write the blob through the
packstore mutation path (same coordinator/lease discipline as attachment
ingest), then upsert the slim row (`content_hash`, `raw_format`,
`raw_data = NULL`). Blob-before-row ordering means a crash leaves at
worst an unreferenced CAS blob for maintenance to sweep — never a row
pointing at nothing.

### Reference authority and maintenance

Every query that enumerates blob references gains a `message_raw` arm:

- `ResolveAttachmentBlob` membership: a hash referenced by
  `message_raw.content_hash` is live.
- `ListReferencedBlobHashes` (orphan-sweep safety) and
  `ListUnpackedBlobs` (pack candidates): UNION in
  `message_raw.content_hash` (the offload exclusion applies to the new
  arm identically).
- Backup capture: `frozenView.ContentInfo` adds the
  `message_raw.content_hash` rows to the content-blob enumeration, so
  the repository holds raw content as first-class blobs and the offload
  invariant ("offload-eligible ⇒ present in repo") extends to them.

### Offload integration

`blob_offload`, the tier decorator, and the eviction ladder work
unchanged — the blob is just another hash. `offload` selection extends
naturally: for raw blobs the "every referencing message" rule collapses
to the single owning message (`message_raw.message_id`), joined through
the same predicate SQL. A `--only attachments|raw` flag gates classes;
the default covers both, which is why the command is named `offload`
rather than `offload-attachments` — the command is the policy surface
for all evictable blob classes.

### The four byte-moving call sites

- `dedup.go:292` (copy raw between duplicate messages): post-migration
  this is a one-row copy of `content_hash` — *cheaper* than today's
  server-side blob copy. The SQL keeps a `raw_data` arm for legacy rows.
- `dedup.go:428` (bulk read for content comparison): for externalized
  rows the comparison short-circuits on `content_hash` equality without
  touching bytes at all — the hash *is* the content identity. Bytes are
  fetched only for legacy-vs-externalized mixed pairs.
- `subset.go:457` (cross-database copy): copies the slim rows as today;
  the subset target additionally needs the referenced CAS blobs copied,
  which subset already does for attachments — the raw arm joins that
  existing blob-copy pass.
- `migrations.go:107` (legacy re-encode scan): predates externalization
  and only touches inline rows; it gains a `WHERE content_hash IS NULL`
  guard and is otherwise unaffected.

### Migration: `msgvault externalize-raw`

A batched, resumable, idempotent local command (daemon stopped, same
guard as `offload`/`unpack-attachments`):

1. Select a batch of rows `WHERE content_hash IS NULL` (ordered by
   `message_id`).
2. For each: inflate `raw_data`, SHA-256, write the blob through the
   packstore mutation path, verify readback, set `content_hash`.
3. Null `raw_data` in the same transaction as step 2's row update.
4. Repeat; progress is `COUNT(*) WHERE content_hash IS NULL`. Interrupt
   at any point and re-run — done rows are skipped by the predicate.

Flags: `--limit`, `--dry-run` (count + projected bytes), `--jobs` for
hashing parallelism (writes stay serialized through the store).

**Reclaiming the space is a separate, explicit step**: nulling
`raw_data` leaves free pages, so the command finishes by printing the
exact follow-up (`VACUUM` via a `msgvault compact` invocation), with the
warning the backup docs already carry — earlier snapshots still hold the
content as pages, the repository temporarily stores raw content twice
(old page-form snapshots plus new blobs) until retention ships, and a
fresh repository seed after vacuum is the remedy for tight remote space.
New syncs write externalized rows from the moment the migration has been
run to completion (a `settings` marker records the archive as
CAS-native for raw content, keeping mixed-mode reads but single-mode
writes).

### Expected effect (to be confirmed by dbstat)

If `message_raw` is ~N GB of the database: the database shrinks by ~N GB
after migration + vacuum; the attachment CAS grows by roughly the same
(pack zstd ≈ row zlib); `offload --before`/`--deleted-from-source` can
then evict nearly all of it, leaving the local archive at metadata +
bodies + FTS + hot blobs. If dbstat shows a different whale (embeddings,
FTS), this design still stands but drops in priority — decide after the
numbers land.

## Testing

- Store: round-trip inline → externalize → read equality; nil-opener
  error; mixed-mode `GetMessageRaw`; dedup copy/compare across all three
  row-state pairings; subset carries blobs; presence joins against stub
  rows.
- Migration command: interrupt/resume idempotency on a real temp
  archive; byte-identical `export-eml` before vs after; verify-readback
  failure aborts the row untouched.
- Reference authority: raw hashes appear in reference inventory and
  pack candidates; orphan sweep never removes a blob referenced only by
  `message_raw`; backup `ContentInfo` counts match.
- End to end: sync → externalize → backup → offload raw blobs → serve
  `show-message --raw` through the tier from the repository → restore.

## Alternatives considered

### Keep zlib rows, offload database pages

Not possible: SQLite pages are not individually addressable content, and
the backup page map is a whole-file representation. Any page-level
tiering amounts to remoting SQLite I/O, rejected in the parent design.

### A separate raw-files directory (non-CAS)

Loses deduplication (the measured archive syncs multiple accounts),
loses the shared reference/maintenance/offload machinery, and adds a
second storage layout to back up and verify. The CAS already provides
identity, integrity, packing, and the tier.

### Compress harder instead of externalizing (re-zlib → zstd in-row)

Buys a one-time constant factor, leaves the bytes inside SQLite where
backup, tiering, and dedup-by-hash cannot reach them, and re-dirties
every page it touches. Orthogonal at best; not pursued.

## Delivery order

1. Schema change (nullable `raw_data`, `content_hash` column) + row-state
   read/write plumbing behind the `RawBlobOpener` seam, mixed-mode
   `GetMessageRaw`, and the reference-authority arms. No behavior change
   for archives with no externalized rows.
2. `externalize-raw` migration command with resume/verify semantics and
   the compact follow-up messaging.
3. Backup `ContentInfo` enumeration + capture tests (raw blobs land in
   the repository).
4. `offload --only raw|attachments` selection arm + end-to-end tier
   tests.
5. Dedup/subset call-site conversions with equivalence tests.
6. New-sync CAS-native write mode behind the archive marker.
