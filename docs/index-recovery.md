# Index loss and recovery — `bintrail restore-index`

The index database is the system of record, but it is also just a MySQL
database — disks die, volumes get deleted. The archive tier is the answer to
"who backs up the backup": `bintrail restore-index` turns the Parquet
archives back into a working index.

## Partial availability first

Before rebuilding anything: **`query` and `reconstruct` can read archives
directly without a live index** (`--archive-dir` / `--archive-s3` on
`query`; baselines plus archives for `reconstruct`). If you are
mid-incident, you can answer "what changed" and produce recovery SQL from
the archive tier alone while the index rebuild runs.

## Rebuilding the index

```bash
bintrail restore-index \
  --index-dsn "root:pw@tcp(127.0.0.1:3306)/bintrail_index" \
  --archive-s3 s3://backups/bintrail --region us-east-1
```

What it does, in order:

1. **Refuses a non-empty index.** restore-index rebuilds a *fresh* index —
   point it at a new, empty database. Mixing a partial restore into a live
   index creates states nothing can reason about.
2. Creates the index schema (same DDL as `bintrail init`) and re-partitions
   `binlog_events` to cover exactly the archived hours plus a forward
   horizon — a single `ALTER` on the empty table, instant.
3. Scans the Hive layout and bulk-loads every archived partition back,
   preserving `event_id` identity, and rebuilds `archive_state` from the
   scan (it is a rebuildable cache by design).
4. Restores `schema_snapshots` and server identity from the **index-meta
   sidecar** (`bintrail_id=<id>/index-meta.json`) that rotation writes
   alongside the archives — for archives produced before this feature, the
   report says so and the next step is a fresh `bintrail snapshot`.
5. Prints an honest inventory: what was recovered, what was **not**, and
   the next steps. `--format json` for automation; exit non-zero if any
   file failed to load.

## What does NOT come back — by design

- **`stream_state`** (the replication position). A position that survived
  an index loss is stale; resuming from it would *fake* continuity across
  a hole. Restart the stream cleanly (`bintrail stream` /
  `bintrail-console watch`): the continuity verdict then reports the
  capture seam honestly.
- **`index_state`** (the per-file indexing ledger) — historical bookkeeping
  for binlog files that may no longer exist.
- Events that were **never archived**: anything in live partitions that had
  not been rotated to Parquet yet is gone with the index. The gap between
  the newest archive and the stream restart is a real capture gap —
  `status` and the coverage card will show it, not paper over it.

## After the rebuild

```bash
bintrail archive reconcile --index-dsn ...   # cross-check archive_state
bintrail snapshot --source-dsn ... --index-dsn ...   # if no sidecar was found
bintrail status --index-dsn ...              # coverage + continuity sanity
```

Then restart the stream. Consider a `bintrail drill` ([drill.md](drill.md))
against the rebuilt index — a rebuild you haven't rehearsed a restore from
is still a hope.
