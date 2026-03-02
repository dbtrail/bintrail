# DDL Tracking and Auto-Snapshot

This page explains how bintrail detects DDL statements (schema changes) in the binlog stream, automatically takes new schema snapshots in stream mode, and tracks restore coverage so you know what can be recovered and how far back.

---

## The Problem

Bintrail maps binlog row events to column names using a schema snapshot — a point-in-time copy of `information_schema.COLUMNS` stored in the index database (see [indexing.md](indexing.md) for details). When someone runs `ALTER TABLE` on the source, the snapshot becomes stale: new columns don't have names, removed columns cause mismatches, and the parser starts skipping events.

Before DDL tracking, the only solution was to notice the "column count mismatch" warnings in the logs, manually run `bintrail snapshot`, and hope you didn't miss too many events in between. In stream mode (continuous replication), an unattended schema change could silently break indexing for hours.

DDL tracking solves three problems:

1. **Detection**: The parser identifies DDL statements (`ALTER TABLE`, `CREATE TABLE`, `DROP TABLE`, `RENAME TABLE`) and emits them as events instead of just logging warnings.
2. **Auto-snapshot** (stream mode only): When a DDL is detected during live replication, bintrail automatically takes a new snapshot and hot-swaps the resolver — no manual intervention needed.
3. **Restore coverage**: The `status` command shows the time range of indexed events and warns about DDLs that weren't followed by a snapshot (file mode), so you know where recovery gaps might exist.

---

## How DDL Detection Works

### The parseDDL function

DDL statements arrive as `QueryEvent`s in the binlog — the same event type used for `BEGIN`, `COMMIT`, and other SQL statements. The `parseDDL` function in `internal/parser/parser.go` identifies DDL statements by checking if the query starts with one of four prefixes:

```go
type DDLKind string

const (
    DDLAlterTable  DDLKind = "ALTER TABLE"
    DDLCreateTable DDLKind = "CREATE TABLE"
    DDLDropTable   DDLKind = "DROP TABLE"
    DDLRenameTable DDLKind = "RENAME TABLE"
)
```

When a DDL is detected, `parseDDL` extracts the schema and table name from the query using a regex:

```
(?:(?:` "`" + "`" + `)?(\\w+)(?:` "`" + "`" + `)?\\.)?(` "`" + "`" + `?(\\w+)` "`" + "`" + `?)
```

This handles both `ALTER TABLE mydb.users` and `ALTER TABLE users` (no schema prefix). When the schema isn't in the query, it falls back to the default schema from the `QueryEvent` header.

The function returns an `Event` with `EventType = EventDDL` (type 4) and the `DDLType` field set to the appropriate `DDLKind` constant.

### Event pipeline

DDL events flow through the same channel as row events:

```
binlog file / replication stream
       │
       │  QueryEvent
       │
   parseDDL()
       │
       ├── not DDL? → ignore
       │
       └── DDL? → Event{EventType: EventDDL, DDLType: "ALTER TABLE", ...}
                     │
                     ▼
              events channel
                     │
                     ▼
              indexer.Run()
                     │
                     ├── flush current batch (ensure all prior events are written)
                     ├── call onDDL callback (if set)
                     └── skip insertion (DDL events are not stored in binlog_events)
```

DDL events are never inserted into `binlog_events`. They exist in the pipeline only to trigger callbacks — the indexer flushes its current batch first (so all events before the DDL are safely written), then invokes the callback.

---

## Stream Mode: Auto-Snapshot

In stream mode (`bintrail stream`), a DDL means the source schema just changed. Since we're connected to the live server, `information_schema` already reflects the new schema — the perfect moment to take a snapshot.

### The ddlHandler

When `runStream` sets up the indexer, it registers a DDL callback that:

1. Takes a new snapshot via `metadata.TakeSnapshot(sourceDB, indexDB, schemas)`
2. Builds a new resolver from the fresh snapshot
3. Atomically swaps the parser's resolver using `streamParser.SwapResolver(newResolver)`
4. Records the DDL in the `schema_changes` table with the new `snapshot_id`

```
DDL detected in replication stream
       │
       ▼
   onDDL callback
       │
       ├── TakeSnapshot(sourceDB, indexDB, schemas)
       │         └── reads information_schema → writes schema_snapshots
       │
       ├── NewResolver(indexDB, newSnapshotID)
       │         └── loads new column mappings into memory
       │
       ├── streamParser.SwapResolver(newResolver)
       │         └── atomic.Pointer.Store() — parser goroutine picks it up
       │
       └── INSERT INTO schema_changes (... snapshot_id = N)
```

### Atomic resolver swap

The parser runs in a separate goroutine from the DDL handler. Swapping the resolver must be safe for concurrent access. Both `Parser` and `StreamParser` store their resolver as an `atomic.Pointer[metadata.Resolver]`:

```go
type StreamParser struct {
    resolver atomic.Pointer[metadata.Resolver]
    // ...
}

func (sp *StreamParser) SwapResolver(r *metadata.Resolver) {
    sp.resolver.Store(r)
}
```

When `handleRows` needs the resolver, it calls `sp.resolver.Load()`. This is a classic RCU (Read-Copy-Update) pattern: the old resolver remains valid for any in-flight `handleRows` call, while new calls pick up the updated one. No locks needed.

### Error handling

The DDL handler uses best-effort error handling — it always returns nil to avoid stopping the stream. If the snapshot fails (e.g., source DB is temporarily unreachable), the DDL is still recorded in `schema_changes` but without a `snapshot_id`. The parser continues with the old resolver, which may produce column count mismatches until the next successful snapshot.

Failures are logged at `slog.Error` level so they're visible in production monitoring:

```
ERROR DDL snapshot failed schema=mydb table=users error="connection refused"
```

---

## File Mode: Record Only

In file mode (`bintrail index`), a DDL in a binlog file means the schema changed at some point in the past. The current `information_schema` may have changed again since then — taking a snapshot now would capture the wrong schema. So file mode only records the DDL in `schema_changes` and logs a warning:

```
WARN DDL detected in file mode — auto-snapshot not available. Run 'bintrail snapshot' if schema changed.
```

The `snapshot_id` column is NULL for file-mode DDLs, which the status command uses to flag potential restore coverage gaps.

---

## The schema_changes Table

```sql
CREATE TABLE IF NOT EXISTS schema_changes (
    id              INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    detected_at     DATETIME NOT NULL,
    binlog_file     VARCHAR(255) NOT NULL,
    binlog_pos      BIGINT UNSIGNED NOT NULL,
    gtid            VARCHAR(255) DEFAULT NULL,
    schema_name     VARCHAR(64) NOT NULL,
    table_name      VARCHAR(64) NOT NULL,
    ddl_type        VARCHAR(50) NOT NULL,
    ddl_query       TEXT NOT NULL,
    snapshot_id     INT UNSIGNED DEFAULT NULL,
    INDEX idx_detected_at (detected_at)
)
```

Key fields:

| Field | Description |
|---|---|
| `ddl_type` | One of `ALTER TABLE`, `CREATE TABLE`, `DROP TABLE`, `RENAME TABLE` |
| `ddl_query` | The full DDL statement from the binlog |
| `snapshot_id` | The snapshot taken after this DDL (stream mode), or NULL (file mode) |

This table is created by `bintrail init` and must exist in the index database. Older index databases (created before this feature) won't have it — the status command handles this gracefully by treating a missing table as zero schema changes.

---

## Restore Coverage

The `status` command includes a "Restore Coverage" section that answers the question: **what can be restored and how far back?**

### Text output

```
=== Restore Coverage ===
  Earliest event:     2026-02-28 14:00:00 UTC
  Latest event:       2026-03-02 09:45:00 UTC
  Total events:       1,284,567
  Schema changes:     3 detected
  Warning: 1 DDL(s) detected without snapshot — recovery across these DDLs may be incomplete
```

The warning appears when any `schema_changes` rows have `snapshot_id = NULL` — meaning a DDL was detected in file mode without a subsequent snapshot. Recovery SQL generated for events spanning such a DDL boundary may use incorrect column names.

### JSON output

```json
{
  "coverage": {
    "earliest_event": "2026-02-28T14:00:00Z",
    "latest_event": "2026-03-02T09:45:00Z",
    "total_events": 1284567,
    "schema_changes": 3,
    "uncovered_ddls": 1
  }
}
```

### How it works

`status.LoadCoverage` runs two queries:

1. `SELECT MIN(event_timestamp), MAX(event_timestamp), COUNT(*) FROM binlog_events` — the time range and volume of indexed events.
2. `SELECT COUNT(*) FROM schema_changes WHERE snapshot_id IS NULL` — DDLs without a corresponding snapshot.

Both queries use best-effort error handling: if `schema_changes` doesn't exist (older index database), the count defaults to zero.

---

## Comparison: Stream vs File Mode

| Behavior | Stream Mode | File Mode |
|---|---|---|
| DDL detection | Yes — from replication events | Yes — from binlog file events |
| Auto-snapshot | Yes — immediate, from live `information_schema` | No — binlog may be stale |
| Resolver swap | Atomic (`atomic.Pointer`) | N/A (single-threaded per file) |
| schema_changes record | Yes, with `snapshot_id` | Yes, with `snapshot_id = NULL` |
| User action needed | None | Run `bintrail snapshot` after DDL |
| Restore coverage warning | No (snapshot covers the DDL) | Yes (warns about uncovered DDLs) |
