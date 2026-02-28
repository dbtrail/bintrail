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

## Status Command: Three Sections

```sh
bintrail status --index-dsn "..."
```

The status command produces a three-section report, implemented in `internal/status/status.go`:

**Section 1 — Indexed Files**: Shows every row in `index_state`. For each binlog file that has ever been indexed (or attempted):

```
=== Indexed Files ===
FILE              STATUS     EVENTS  STARTED_AT           COMPLETED_AT         ERROR
────              ──────     ──────  ──────────           ────────────         ─────
binlog.000042     completed  12345   2026-02-19 10:00:00  2026-02-19 10:00:42  -
binlog.000043     completed  8901    2026-02-19 10:00:43  2026-02-19 10:01:12  -
```

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

**Section 3 — Summary**: Aggregates the index_state counts:

```
=== Summary ===
Files:  12 completed, 0 in_progress, 0 failed
Events: 987654 indexed
```

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
        tracks progress in index_state / stream_state

bintrail rotate --retain 7d --add-future 14
    └── drops old partitions (instant metadata operation)
        adds future partitions (reorganize p_future)

bintrail status
    └── reads index_state, information_schema.PARTITIONS
        prints three-section report

bintrail query
    └── partition pruning: only reads relevant partitions
        pk_hash index: finds rows in microseconds

bintrail recover
    └── generates reversal SQL from row_before / row_after
        → apply manually to source database
```

---

## Automating Rotation

In production, run `bintrail rotate` from an hourly cron job or systemd timer. A typical setup:

```sh
# Drop data older than 30 days (720 hours); ensure 48 hours of future partitions exist
bintrail rotate \
  --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --retain 720h \
  --add-future 48
```

`--retain` and `--add-future` can be combined in one invocation. The command drops old partitions first, then adds new ones.

Schedule the timer to run once per hour. The drop operation is instant, but `REORGANIZE PARTITION` on a partition containing data does a full table scan of `p_future` to redistribute rows — if your `p_future` is empty (because you add future partitions frequently), the reorganize is also instant.
