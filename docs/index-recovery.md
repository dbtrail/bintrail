# Index loss and recovery — `bintrail restore-index`

> This runbook assumes the S3 archive tier survived the incident. To make
> that assumption hold against ransomware or a stolen credential, put the
> archives on an S3 Object Lock bucket — see [object-lock.md](object-lock.md).

The index database is the system of record, but it is also just a MySQL
database — disks die, volumes get deleted. The archive tier is the answer to
"who backs up the backup": `bintrail restore-index` turns the Parquet
archives back into a working index.

## Partial availability first

Mid-incident, before the rebuild finishes, two things still work:

- **DuckDB directly on the Parquet files** — no bintrail index needed at
  all; see [parquet-debugging.md](parquet-debugging.md) for ready-made
  queries over the archive layout.
- **`bintrail query --archive-s3 ... --bintrail-id ...` against the fresh
  index as soon as step 2 below has created its schema** — the explicit
  archive flags don't depend on `archive_state`, so "what changed?" is
  answerable while the bulk load is still running. (`query`, `recover` and
  `reconstruct` all need a reachable index MySQL to run — the flags
  supplement an index, they don't replace it. `reconstruct --baseline-only`
  is the one truly index-less read: baseline state, no deltas.)

## Rebuilding the index

```bash
bintrail restore-index \
  --index-dsn "root:pw@tcp(127.0.0.1:3306)/bintrail_index" \
  --archive-s3 s3://backups/bintrail --region us-east-1
```

What it does, in order:

1. **Refuses an index that already holds state** (events, a stream
   position, or schema snapshots). restore-index rebuilds a *fresh* index —
   point it at a new, empty database. A surviving `stream_state` row is the
   dangerous case: the restarted stream would resume a stale position and
   fake continuity across the hole.
2. Creates the index schema (the same table set as `bintrail init`; pass
   `--encrypt` if the lost index was encrypted — parity is **not**
   inferred).
3. Scans the Hive layout, re-partitions `binlog_events` to cover exactly
   the archived hours plus a forward horizon (a single `ALTER` on the
   empty table, instant), then bulk-loads every archived partition back,
   preserving `event_id` identity, and rebuilds `archive_state` from the
   scan (it is a rebuildable cache by design).
4. Restores `schema_snapshots` and server identity from the **index-meta
   sidecar** (`bintrail_id=<id>/index-meta.json`) that rotation writes
   alongside the archives — for archives produced before this feature, the
   report says so and the next step is a fresh `bintrail snapshot`.
5. Prints an honest inventory: what was recovered, what was **not**, and
   the next steps. `--format json` for automation; exit non-zero if any
   file failed to load.

**If a file fails mid-load, the index is PARTIAL**: batches already flushed
stay loaded (the report counts them), and a re-run is refused by the
fresh-index guard. To retry, drop and recreate the database, then run
restore-index again.

## What does NOT come back — by design

- **`stream_state`** (the replication position). A position that survived
  an index loss is stale; resuming from it would *fake* continuity across
  a hole. Restart the stream cleanly (`bintrail stream` /
  `bintrail-console watch`): the hole then shows up honestly as **missing
  restore coverage** (the continuity verdict describes the new capture
  range — it does not stamp a `gap_lost` for the pre-restore window).
- **`index_state`** (the per-file indexing ledger) — historical bookkeeping
  for binlog files that may no longer exist.
- Events that were **never archived**: anything in live partitions that had
  not been rotated to Parquet yet is gone with the index. The window
  between the newest archive and the stream restart is a real hole — it
  appears as missing restore coverage, not papered over.
- **Archives older than the current schema restore with NULLs** in the
  later columns (`connection_id`, `query_text`/`query_hash`,
  `commit_ts_us`) — same tolerance queries already apply when reading
  them.

## After the rebuild

```bash
bintrail archive reconcile --index-dsn ... --archive-s3 s3://...   # cross-check archive_state
bintrail snapshot --source-dsn ... --index-dsn ...   # if no sidecar was found
bintrail status --index-dsn ...              # coverage + continuity sanity
```

Then restart the stream. Consider a `bintrail drill` ([drill.md](drill.md))
against the rebuilt index — a rebuild you haven't rehearsed a restore from
is still a hope.
