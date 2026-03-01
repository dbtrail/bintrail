# How Rotation and Status Work

This page explains how `bintrail rotate` manages the partition lifecycle of the `binlog_events` table, and how `bintrail status` reports the state of the index.

---

## The Partition Management Problem

The `binlog_events` table grows continuously. On a busy database, it can accumulate millions of rows per day. You need a way to reclaim space without slow, lock-heavy `DELETE` operations.

MySQL's solution is table partitioning.

---

## Why Partitioning?

`binlog_events` is partitioned by `RANGE (TO_SECONDS(event_timestamp))`. Each partition holds one hour's worth of events. This gives you two powerful properties:

**Instant deletes**: Dropping a partition is a metadata operation — MySQL removes the partition's data files directly, without scanning or logging individual row deletions. Dropping 30 days of events takes milliseconds instead of minutes.

**Partition pruning**: When you query with a time range (`--since`/`--until`), MySQL's optimizer sees that only certain partitions can contain the matching rows and skips the rest entirely. A query for "events in the last hour" touches one or two partitions out of potentially hundreds.

---

## Why `TO_SECONDS`, Not `UNIX_TIMESTAMP`?

MySQL 8.0 rejects `UNIX_TIMESTAMP()` in partition expressions when `time_zone=SYSTEM`:

```
Error 1486: Constant, random or timezone-dependent expressions in (sub)partitioning function are not allowed
```

`TO_SECONDS()` is timezone-independent — it returns the number of seconds since year 0, which is a pure calendar computation with no timezone involvement. This is why every partition boundary in bintrail is expressed as a `TO_SECONDS` value.

The DDL looks like:

```sql
PARTITION p_2026021914 VALUES LESS THAN (TO_SECONDS('2026-02-19 15:00:00'))
```

The `VALUES LESS THAN` value is the *next* hour boundary — a row with `event_timestamp = '2026-02-19 14:59:59'` goes into `p_2026021914` because `TO_SECONDS('2026-02-19 14:59:59') < TO_SECONDS('2026-02-19 15:00:00')`.

---

## The `p_future` Catch-All

There is always a special partition:

```sql
PARTITION p_future VALUES LESS THAN MAXVALUE
```

`p_future` catches any event whose timestamp is beyond all named partition boundaries. This is MySQL's safety net — without it, inserting an event with a timestamp in the future would fail with an error.

**The invariant**: `p_future` must always exist. You can add or drop any other partition, but never drop `p_future`. The `addFuturePartitions` function in `rotate.go` always appends it at the end of every `REORGANIZE PARTITION` operation.

---

## Dropping Old Partitions

```sh
bintrail rotate --index-dsn "..." --retain 7d
```

The `--retain` flag accepts a duration: `7d` (days) or `24h` (hours). The command:

1. Computes `cutoff = now - retain_duration` (truncated to the current hour UTC).
2. Lists all partitions from `information_schema.PARTITIONS`.
3. Parses the hour from each `p_YYYYMMDDHH` name (`p_future` is skipped automatically because `partitionDate` returns `false` for it).
4. Collects all partitions whose date is before the cutoff.
5. Issues a single `ALTER TABLE binlog_events DROP PARTITION p1, p2, p3` statement.

A single `ALTER TABLE DROP PARTITION` statement for multiple partitions is more efficient than separate statements — MySQL does it in one pass.

After dropping, the command automatically adds the same number of future hourly partitions to keep the rolling window size constant (e.g. dropping 168 partitions adds 168 new ones). Use `--no-replace` to suppress this auto-replacement when you genuinely want to reclaim space:

```sh
bintrail rotate --index-dsn "..." --retain 7d --no-replace
```

After dropping, the command warns if `p_future` contains data. If events are landing in `p_future`, it means events are arriving with timestamps beyond all named partition boundaries — you need to add more future partitions.

---

## Adding Future Partitions

```sh
bintrail rotate --index-dsn "..." --add-future 14
```

Adding future partitions converts the `p_future` catch-all into specific hourly partitions, then appends a new `p_future` at the end. This is done with `REORGANIZE PARTITION`:

```sql
ALTER TABLE `binlog_index`.`binlog_events`
REORGANIZE PARTITION p_future INTO (
    PARTITION p_2026021900 VALUES LESS THAN (TO_SECONDS('2026-02-19 01:00:00')),
    PARTITION p_2026021901 VALUES LESS THAN (TO_SECONDS('2026-02-19 02:00:00')),
    ...
    PARTITION p_future VALUES LESS THAN MAXVALUE
)
```

`REORGANIZE PARTITION` moves data from `p_future` into the appropriate new named partitions and creates a fresh `p_future`. Any data that was already in `p_future` goes to the right named partition — nothing is lost.

`nextPartitionStart` determines where to start adding partitions: it finds the latest existing `p_YYYYMMDDHH` partition and starts the hour after. If no named partitions exist yet, it starts from the current hour (UTC).

---

## Partition Naming and Parsing

Two functions handle the hour ↔ name mapping:

```go
// cmd/bintrail/rotate.go
func partitionName(d time.Time) string {
    return d.UTC().Format("p_2006010215")  // Go reference time for YYYYMMDDHH
}

func partitionDate(name string) (time.Time, bool) {
    if len(name) != 12 || !strings.HasPrefix(name, "p_") {
        return time.Time{}, false  // rejects p_future and anything malformed
    }
    t, err := time.ParseInLocation("p_2006010215", name, time.UTC)
    ...
}
```

These two functions round-trip correctly: `partitionName(partitionDate("p_2026021914"))` → `"p_2026021914"`. Tests in `rotate_test.go` verify this.

---

## Archiving Partitions to Parquet

Before dropping old partitions, bintrail can serialize each partition's events to a Parquet file. This gives you a long-term queryable record outside the index database — without requiring the original binlog files.

### Archiving to a local directory

Pass `--archive-dir` to write Parquet files locally before each drop:

```sh
bintrail rotate \
  --index-dsn           "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --retain              7d \
  --archive-dir         /mnt/archives \
  --archive-compression zstd
```

Each archived partition becomes a single Parquet file: `<archive-dir>/p_YYYYMMDDHH.parquet`. If any archive write fails, no partitions are dropped — the command aborts before touching the table.

`--archive-compression` accepts `zstd` (default), `snappy`, `gzip`, or `none`.

### Archiving directly to S3

Pass `--archive-s3` alongside `--archive-dir` to upload each Parquet file to S3 after writing it locally:

```sh
bintrail rotate \
  --index-dsn         "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --retain            7d \
  --archive-dir       /tmp/rotate-staging \
  --archive-s3        s3://my-bintrail-archives/events/ \
  --archive-s3-region us-east-1
```

`--archive-dir` is still required — files are written locally first, then uploaded. You can use a temporary directory if you don't need local copies after upload.

**Hive-partitioned layout**: S3 objects are stored with a Hive-compatible directory structure for compatibility with Athena, Glue, and DuckDB:

```
s3://my-bintrail-archives/events/
  bintrail_id=abc123de-0000-0000-0000-000000000001/
    event_date=2026-02-13/
      events_00.parquet   ← p_2026021300
      events_01.parquet   ← p_2026021301
      ...
    event_date=2026-02-14/
      events_00.parquet
      ...
```

The `bintrail_id` partition key is the stable UUID of the bintrail server instance that indexed the data (see [Server Identity](server-identity.md)). Multiple bintrail instances indexing different MySQL sources can share the same S3 prefix without collision.

**AWS credentials**: bintrail uses the standard credential chain — environment variables (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`), `~/.aws/credentials`, or EC2/ECS instance metadata. `--archive-s3-region` is optional if `AWS_REGION` is already set.

### Querying archived events

Once partitions are archived to S3, query them alongside live index data with `--archive-s3` on the `query` command. Provide `--bintrail-id` to scope the archive path to a specific server:

```sh
bintrail query \
  --index-dsn   "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --schema      mydb \
  --table       orders \
  --since       "2026-01-01 00:00:00" \
  --until       "2026-01-31 23:59:59" \
  --archive-s3  s3://my-bintrail-archives/events/ \
  --bintrail-id abc123de-0000-0000-0000-000000000001
```

Results from the live MySQL index and from Parquet archives are merged, deduplicated by `event_id`, and sorted by timestamp before being returned. Archive query failures are non-fatal — the command logs a warning and continues with live results only.

---

## Status Command: Three Sections

```sh
bintrail status --index-dsn "..."
```

The status command produces a three-section report, implemented in `internal/status/status.go`:

**Section 1 — Indexed Files**: Shows every row in `index_state`. The `BINTRAIL_ID` column identifies which bintrail server instance indexed each file:

```
=== Indexed Files ===
FILE              STATUS     EVENTS  STARTED_AT           COMPLETED_AT         ERROR  BINTRAIL_ID
────              ──────     ──────  ──────────           ────────────         ─────  ───────────
binlog.000042     completed  12345   2026-02-19 10:00:00  2026-02-19 10:00:42  -      abc123de-0000-0000-0000-000000000001
binlog.000043     completed  8901    2026-02-19 10:00:43  2026-02-19 10:01:12  -      abc123de-0000-0000-0000-000000000001
binlog.000001     completed  999     2026-02-01 00:00:00  2026-02-01 00:05:00  -      -
```

Rows with `-` in `BINTRAIL_ID` were indexed before the server identity feature was introduced; their server of origin is unknown.

**Section 2 — Partitions**: Shows each partition with its boundary and estimated row count:

```
=== Partitions ===
PARTITION     LESS_THAN           ROWS (est.)
─────────     ─────────           ───────────
p_2026021300    2026-02-13 01:00 UTC   142389
p_2026021301    2026-02-13 02:00 UTC   198234
...
p_future      MAXVALUE            0
Total events (est.): 987654
```

**Section 3 — Summary**: Groups files by server identity and aggregates counts:

```
=== Summary ===
Server abc123de-0000-0000-0000-000000000001
  Files:  12 completed, 0 in_progress, 0 failed
  Events: 986655 indexed

Server (unknown)
  Files:  1 completed, 0 in_progress, 0 failed
  Events: 999 indexed
```

Files with a NULL `bintrail_id` are grouped under `Server (unknown)`. This is common when a shared index database receives files from multiple bintrail instances (e.g. one per replica), or when upgrading from a version predating the server identity feature.

The row counts in the partitions section are **estimates** from `information_schema.PARTITIONS.TABLE_ROWS`. InnoDB doesn't maintain exact row counts, so these are good approximations for capacity planning but not for exact totals.

---

## `DescriptionToHuman`: Converting Partition Boundaries

MySQL stores partition boundary values as evaluated integers in `information_schema.PARTITIONS.PARTITION_DESCRIPTION`. For `TO_SECONDS`-based partitions, this is the numeric `TO_SECONDS` result (e.g. `63786820800`). For `p_future`, it's the literal string `MAXVALUE`.

`DescriptionToHuman` converts the integer back to a human-readable datetime:

```go
// internal/status/status.go
func DescriptionToHuman(desc string) string {
    if desc == "" || strings.EqualFold(desc, "MAXVALUE") {
        return "MAXVALUE"
    }
    secs, err := strconv.ParseInt(desc, 10, 64)
    if err != nil {
        return desc  // not an integer — return raw
    }
    // TO_SECONDS('1970-01-01 00:00:00') = 62167219200 in MySQL 8.0
    return time.Unix(secs-62167219200, 0).UTC().Format("2006-01-02 15:00 UTC")
}
```

The constant `62167219200` is MySQL 8.0's value for `TO_SECONDS('1970-01-01 00:00:00')` (= 719528 days × 86400 seconds). Subtracting it converts the MySQL second count to a Unix timestamp; `time.Unix` converts to Go's `time.Time`.

---

## Full Lifecycle Diagram

```
bintrail init
    └── creates binlog_events (48 hourly partitions + p_future)
        creates schema_snapshots, index_state, stream_state

bintrail snapshot
    └── reads information_schema on source
        writes to schema_snapshots (snapshot_id N)

bintrail index / bintrail stream
    └── parses events → inserts into binlog_events partitions
        tracks progress in index_state / stream_state (with bintrail_id)

bintrail rotate --retain 7d [--archive-s3 s3://...]
    └── (optional) archives each partition to Parquet → uploads to S3
        drops old partitions (instant metadata operation)
        auto-adds replacement future partitions (reorganize p_future)

bintrail status
    └── reads index_state, information_schema.PARTITIONS
        prints three-section report (with per-server Summary)

bintrail query [--archive-s3 s3://...]
    └── partition pruning: only reads relevant partitions
        pk_hash index: finds rows in microseconds
        merges with Parquet archives when --archive-s3 is given

bintrail recover
    └── generates reversal SQL from row_before / row_after
        → apply manually to source database
```

---

## Automating Rotation

In production, run `bintrail rotate` from an hourly cron job or systemd timer. A typical setup:

```sh
# Maintain a 30-day rolling window: drop old partitions and auto-add the same count back
bintrail rotate \
  --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --retain 720h
```

`--retain` automatically replaces every dropped partition with a new future hourly partition, keeping the total partition count constant. If you also want extra future partitions beyond the replacements (e.g. 48 hours of extra headroom), use `--add-future`:

```sh
bintrail rotate \
  --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --retain 720h \
  --add-future 48   # adds 720 replacements + 48 extras = 768 new partitions total
```

To archive partitions to S3 before dropping:

```sh
bintrail rotate \
  --index-dsn         "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --retain            720h \
  --archive-dir       /tmp/rotate-staging \
  --archive-s3        s3://my-bintrail-archives/events/ \
  --archive-s3-region us-east-1
```

To drop without adding anything back — useful when disk is critically full — use `--no-replace`:

```sh
bintrail rotate \
  --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --retain 720h \
  --no-replace
```

Schedule the timer to run once per hour. The drop operation is instant, but `REORGANIZE PARTITION` on a partition containing data does a full table scan of `p_future` to redistribute rows — if your `p_future` is empty (because you add future partitions frequently), the reorganize is also instant.
