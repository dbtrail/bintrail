# Indexing binlog files

`bintrail index` reads MySQL binary log files from disk and writes every row
event into the queryable index, with full before/after images. Use it to
**backfill** history from existing binlog files; for continuous real-time
capture, use [`bintrail stream`](streaming.md) instead.

---

## First it needs a schema snapshot

MySQL's ROW-format binlog records rows as positional arrays (`[1, "Alice", 42]`)
with no column names — those live in `information_schema` on the source server.
`bintrail snapshot` captures that column metadata (and foreign-key
relationships) into the index so events can be decoded into named columns.

- **No extra grants.** The privileges that let dbtrail read
  `information_schema.COLUMNS` also cover the FK metadata
  (`KEY_COLUMN_USAGE`, `REFERENTIAL_CONSTRAINTS`) — MySQL's metadata visibility
  is row-level, so if you can see a table's columns you can see its constraints.
- **Re-snapshot after a schema change.** If an `ALTER TABLE` runs after the
  snapshot, the binlog column count no longer matches; dbtrail warns and
  **skips that table's events** rather than corrupting data. The fix is to
  re-run `bintrail snapshot`. To automate this, see
  [DDL tracking](ddl-tracking.md).
- **Same-count changes are the dangerous ones.** A column rename (or a
  `DROP COLUMN` + `ADD COLUMN` in one `ALTER`) keeps the count equal, so the
  count check can't see it — values would silently index under the wrong
  column names. If the source sets `binlog_row_metadata=FULL` (MySQL 8.0+,
  MariaDB 10.5+ — `bintrail doctor` reports it), every row event's TABLE_MAP
  carries the table's real column names (a handful of extra bytes per column
  per TABLE_MAP event) and dbtrail verifies the snapshot against them,
  **stopping with a loud error** instead of indexing corrupt data:

  ```sql
  -- MySQL 8.0+ (optional):
  SET PERSIST binlog_row_metadata = 'FULL';
  -- MariaDB 10.5+ (no SET PERSIST; persist it in my.cnf under [mysqld]):
  SET GLOBAL binlog_row_metadata = 'FULL';
  ```

  Only events **at or after the snapshot's creation time** stop indexing —
  that is the stale case a fresh `bintrail snapshot` genuinely fixes. Events
  *older* than the snapshot (re-indexing history after a rename, a stream
  catching up through a backlog) index under the snapshot's current names
  with a loud warning, exactly as they did before drift detection existed.

---

## Running it

```sh
bintrail index \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --source-dsn "user:pass@tcp(source-db:3306)/" \
  --binlog-dir /var/lib/mysql \
  --all
```

`--source-dsn` lets `index` preflight the source (`binlog_format=ROW` and
`binlog_row_image=FULL`, both required; `ON DELETE/UPDATE CASCADE` foreign keys
are allowed but warned — InnoDB executes cascades below the binlog, so the
cascaded child row changes are not captured and plain `recover` cannot reverse
them. `bintrail recover-cascade` reconstructs **`ON DELETE CASCADE`/`SET NULL`**
only — an `ON UPDATE CASCADE`/`SET NULL` is not synthesized (see
[`recover-cascade` limitations](query-and-recovery.md#recover-cascade-limitations)
in [Query & Recovery](query-and-recovery.md))
and auto-snapshot if no snapshot exists yet. It is **required**
unless you pass `--skip-source-validation` to index offline binlogs against an
already-captured snapshot — the silent skip is gone so a non-`FULL` source can't
be indexed by accident.

Either way, `index` also guards **every row event**: if a binlog event carries a
partial row image (the signature of a session-level
`binlog_row_image=MINIMAL`/`NOBLOB` that the one-shot server-wide check can't
catch), `index` aborts loudly rather than store the absent columns as `NULL` and
corrupt the before/after images `recover` relies on.

`--all` discovers and processes every binlog file in `--binlog-dir` in order;
`--files` takes an explicit comma-separated list. Tune write throughput with
`--batch-size` (default 1000) and scope with `--schemas` / `--tables`.

---

## Safe to re-run

Each file's progress is tracked in `index_state` (`in_progress` →
`completed` / `failed`), so `bintrail index --all` is idempotent: completed
files are skipped, and failed or interrupted files (e.g. after a crash) are
retried on the next run. When several source servers share one index database,
each file is tagged with its `bintrail_id` so [`bintrail status`](rotation-and-status.md#status-command)
can group indexed files by origin server — see
[Server Identity](server-identity.md).

---

## Where events land: `binlog_events`

Indexed events go into `binlog_events`, range-partitioned by hour on
`event_timestamp`. PK lookups are fast because `pk_hash`
(`SHA2(pk_values, 256)`) is a stored, indexed column; queries match on
`pk_hash` **and** the exact `pk_values` (the second check guards against the
astronomically rare hash collision). Partition lifecycle, retention, and
archiving to Parquet are covered in [Rotation and status](rotation-and-status.md).
