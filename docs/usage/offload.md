---
title: Offload & Externalize
description: Shrink the local archive by moving cold content into your backup repository, served back transparently on demand.
---

A mature archive is dominated by three kinds of immutable content: attachment
files, raw message MIME, and rendered HTML bodies. `msgvault externalize` and
`msgvault offload` together let the hot data — metadata, searchable text, the
FTS index — stay small and local while those bulk bytes live in your
[backup repository](/usage/backup/), on an external drive, NAS, or
S3-compatible object storage. Offloaded content is served back transparently
when you open a message or attachment.

Nothing in this workflow deletes information: every step moves bytes only
after verifying an identical copy exists at the destination, and every step
is reversible.

## The lifecycle

```bash
# 1. Move raw MIME and HTML bodies out of the database into the
#    content-addressed store (one-time migration; resumable).
msgvault externalize --dry-run
msgvault externalize

# 2. Reclaim the freed database pages.
sqlite3 ~/.msgvault/msgvault.db 'VACUUM;'

# 3. Capture everything into the backup repository.
msgvault backup create

# 4. Evict local copies of cold content the repository verifiably holds.
msgvault offload --before 2020-01-01 --dry-run
msgvault offload --before 2020-01-01

# 5. Keep using msgvault normally — offloaded content streams back from
#    the repository when a message or attachment is opened.
```

Both commands are local-only and require the daemon stopped
(`msgvault daemon stop`).

## `msgvault externalize`

Moves `message_raw` content (raw MIME) and rendered HTML bodies into the
attachment content store, keyed by SHA-256 of the exact bytes. Each value is
written as a durable, verified blob **before** its database row is slimmed,
so an interruption never loses content and re-running resumes where the
previous run stopped. Searchable text (`body_text`) and the FTS index always
stay in the database.

| Flag | Effect |
|------|--------|
| `--raw` / `--html` | Externalize only one class (default: both) |
| `--limit N` | Stop after N rows (resume by re-running) |
| `--dry-run` | Count and report, change nothing |

Why this matters beyond offload: once externalized, identical content
deduplicates across accounts, backups capture it as individual blobs
instead of database page churn (smaller nightly deltas), and the database
shrinks to a fraction of its size after `VACUUM`.

Note for tight remote storage: snapshots taken *before* externalization
still hold this content as database pages, so the repository stores it
twice until retention ships. Seeding a fresh repository after the
migration avoids that.

## `msgvault offload`

Evicts local copies of blobs the configured repository verifiably holds.
Selection is by message, and a blob qualifies only when **every** message
referencing it — across all classes — matches every given condition:

| Flag | Effect |
|------|--------|
| `--before DATE` | Every referencing message predates DATE |
| `--deleted-from-source` | Every referencing message was deleted from its source |
| `--archive-deleted` | Every referencing message is flag-deleted locally |
| `--only attachments,raw,html` | Restrict which blob classes are candidates |
| `--dry-run` | Verify and report, change nothing |
| `--max-bytes N` | Stop after roughly N bytes (re-run to continue) |
| `--force-stale` | Proceed despite a stale newest snapshot |

The safety ladder, per blob: present in the repository index → streamed
back out of the repository through full verification (CRC and SHA-256) →
recorded in the offload catalog → only then is the local copy removed.
Offload refuses empty repositories, repositories whose newest snapshot is
older than `max_snapshot_age_days` (a stale repo usually means your
off-site sync is broken), and repositories other than the one previous
offloads used.

`msgvault offload status` shows counts and bytes freed;
`msgvault offload restore --hash H` re-materializes a blob locally
(verified, written durably before the catalog entry clears).

## Configuration

```toml
[offload]
# Filesystem path (external drive, NAS mount, rclone mount)…
repo = "~/Backups/msgvault"
# …or S3-compatible object storage (AWS, B2, R2, MinIO).
# repo = "s3://my-bucket/msgvault-backup"
# s3_endpoint = "https://s3.us-west-000.backblazeb2.com"
# s3_region = "us-east-1"
max_snapshot_age_days = 14
```

S3 credentials come from the standard `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY` environment variables — never from config.toml.
The repository contents are not encrypted (repository encryption is a
planned backup-engine feature), so protect the bucket or path accordingly.

## What to expect when content is remote

- **Opening one message or attachment**: one repository round trip —
  roughly 25–100 ms against object storage, imperceptible for reading an
  old email. Against a local path or NAS mount, effectively free.
- **Search, browsing, conversation views, file listings**: always local,
  zero repository traffic. Batch views never fetch remote content.
- **Repository unreachable** (drive unplugged, network down): affected
  content reports "stored remotely, currently unreachable" — never
  "missing" — and everything local keeps working. Reads recover as soon
  as the repository is reachable again; no restart needed.
- **Backups and nightly maintenance** never re-download offloaded
  content.
