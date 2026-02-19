# Bintrail

A CLI tool that parses MySQL ROW-format binary logs, indexes every row event into MySQL with full before/after images, and generates reversal SQL for point-in-time recovery — without needing the original binlog files.

## Requirements

- Go 1.22+
- MySQL 8.0+ (index database)
- Source MySQL server with `binlog_format = ROW` and `binlog_row_image = FULL`

## Install

```sh
go install github.com/bintrail/bintrail/cmd/bintrail@latest
```

Or build from source:

```sh
git clone https://github.com/bintrail/bintrail
cd bintrail
go build ./cmd/bintrail
```

## Quick start

```sh
# 1. Create index tables (run once)
bintrail init --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index"

# 2. Snapshot schema metadata from the source server
bintrail snapshot \
  --source-dsn "user:pass@tcp(source:3306)/" \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index"

# 3. Index binlog files
bintrail index \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --source-dsn "user:pass@tcp(source:3306)/" \
  --binlog-dir /var/lib/mysql \
  --all

# 4. Query the index
bintrail query \
  --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --schema mydb --table orders --pk 12345

# 5. Generate recovery SQL
bintrail recover \
  --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --schema mydb --table orders --event-type DELETE \
  --since "2026-02-19 14:00:00" --until "2026-02-19 14:05:00" \
  --output recovery.sql
```

## Commands

### `bintrail init`

Creates the three index tables in the target MySQL database. The database is created if it does not exist. Generates daily time-range partitions on `binlog_events` for efficient rotation.

```
Flags:
  --index-dsn    DSN for the index database (required)
  --partitions   Number of daily partitions to create from today (default: 7)
```

### `bintrail snapshot`

Captures table and column metadata from the source server's `information_schema` and stores it in the index database. The indexer uses this to map binlog column ordinals to column names and identify primary key columns.

Re-run after schema changes (ALTER TABLE, etc.) to keep the snapshot current.

```
Flags:
  --source-dsn   DSN for the source MySQL server (required)
  --index-dsn    DSN for the index database (required)
  --schemas      Comma-separated list of schemas to snapshot (default: all user schemas)
```

### `bintrail index`

Parses binlog files and writes every INSERT, UPDATE, and DELETE row event into `binlog_events` with full before/after images. Files already marked `completed` are skipped.

If no schema snapshot exists, one is taken automatically using `--source-dsn`.

```
Flags:
  --index-dsn    DSN for the index database (required)
  --source-dsn   DSN for the source MySQL server (for validation and auto-snapshot)
  --binlog-dir   Directory containing binlog files (required)
  --files        Comma-separated specific filenames to index
  --all          Index all binlog files found in --binlog-dir
  --batch-size   Events per batch INSERT (default: 1000)
  --schemas      Only index events from these schemas (comma-separated)
  --tables       Only index these tables (e.g. mydb.orders,mydb.items)
```

### `bintrail query`

Search the index with flexible filters. Output defaults to a human-readable table; JSON and CSV are also supported.

```
Flags:
  --index-dsn       DSN for the index database (required)
  --schema          Filter by schema name
  --table           Filter by table name
  --pk              Filter by primary key value(s), pipe-delimited for composite PKs
  --event-type      INSERT, UPDATE, or DELETE
  --gtid            Filter by GTID (e.g. uuid:42)
  --since           Events at or after this time (2006-01-02 15:04:05)
  --until           Events at or before this time
  --changed-column  UPDATEs that modified this column
  --format          Output format: table (default), json, csv
  --limit           Max rows returned (default: 100)
```

Examples:

```sh
# All events for a single row
bintrail query --index-dsn "..." --schema mydb --table orders --pk 12345

# Composite PK (pipe-delimited, ordinal order)
bintrail query --index-dsn "..." --schema mydb --table order_items --pk '12345|2'

# DELETEs in a time window
bintrail query --index-dsn "..." --schema mydb --table orders \
  --event-type DELETE --since "2026-02-19 14:00:00" --until "2026-02-19 15:00:00"

# What did a transaction touch?
bintrail query --index-dsn "..." --gtid "3e11fa47-71ca-11e1-9e33-c80aa9429562:42"

# Rows where 'status' changed
bintrail query --index-dsn "..." --schema mydb --table orders --changed-column status

# JSON output (includes full row_before / row_after)
bintrail query --index-dsn "..." --schema mydb --table orders --pk 12345 --format json
```

### `bintrail recover`

Generates a `BEGIN`/`COMMIT`-wrapped SQL script that reverses matching events. Events are applied in reverse chronological order.

Reversal logic:
- `DELETE` → `INSERT INTO … (row_before values)`
- `UPDATE` → `UPDATE … SET (row_before) WHERE (row_after)`
- `INSERT` → `DELETE FROM … WHERE (row_after)`

```
Flags:
  --index-dsn    DSN for the index database (required)
  --schema       Filter by schema name
  --table        Filter by table name
  --pk           Filter by primary key value(s)
  --event-type   INSERT, UPDATE, or DELETE
  --gtid         Filter by GTID
  --since        Events at or after this time
  --until        Events at or before this time
  --output       Write SQL to this file (required unless --dry-run)
  --dry-run      Print SQL to stdout instead of writing a file
  --limit        Max events to reverse (default: 1000)
```

Examples:

```sh
# Preview what would be generated (dry run)
bintrail recover --index-dsn "..." \
  --schema mydb --table orders --event-type DELETE \
  --since "2026-02-19 14:00:00" --until "2026-02-19 14:05:00" \
  --dry-run

# Write to file, then review before applying
bintrail recover --index-dsn "..." --schema mydb --table orders --pk 12345 \
  --output recovery.sql
mysql -u root -p mydb < recovery.sql

# Reverse an entire transaction
bintrail recover --index-dsn "..." \
  --gtid "3e11fa47-71ca-11e1-9e33-c80aa9429562:42" \
  --output recovery.sql
```

> **Always review the generated SQL before applying to production.**

### `bintrail rotate`

Manages the time-range partitions on `binlog_events`. Drop old partitions to reclaim space; add new daily partitions so future events land in named partitions rather than the catch-all `p_future`.

```
Flags:
  --index-dsn    DSN for the index database (required)
  --retain       Drop partitions older than this duration (e.g. 7d, 24h)
  --add-future   Number of new daily partitions to add
```

```sh
# Drop partitions older than 7 days and add 3 new ones
bintrail rotate --index-dsn "..." --retain 7d --add-future 3

# Just add more future partitions without dropping anything
bintrail rotate --index-dsn "..." --add-future 14
```

### `bintrail status`

Shows the current state of the index: which binlog files have been processed, partition sizes, and aggregate counts.

```
Flags:
  --index-dsn   DSN for the index database (required)
```

```sh
bintrail status --index-dsn "..."
```

Example output:

```
=== Indexed Files ===
FILE              STATUS     EVENTS  STARTED_AT           COMPLETED_AT         ERROR
────              ──────     ──────  ──────────           ────────────         ─────
binlog.000042     completed  12345   2026-02-19 14:00:00  2026-02-19 14:05:00  -
binlog.000043     completed  8901    2026-02-19 14:06:00  2026-02-19 14:09:00  -

=== Partitions ===
PARTITION    LESS_THAN                    ROWS (est.)
─────────    ─────────                    ───────────
p_20260218   2026-02-19 00:00:00 UTC      9823
p_20260219   2026-02-20 00:00:00 UTC      11423
p_future     MAXVALUE                     0
Total events (est.): 21246

=== Summary ===
Files:  2 completed, 0 in_progress, 0 failed
Events: 21246 indexed
```

## How it works

```
Source MySQL            Index MySQL
(information_schema) ──snapshot──► schema_snapshots
                                        │
Binlog files on disk ──index──►   binlog_events (partitioned)
                                  index_state
                                        │
                          query / recover ──► stdout / .sql file
```

The index stores the complete before and after row images for every event, so recovery never requires the original binlog files.

**Primary key lookup** uses a generated `pk_hash = SHA2(pk_values, 256)` column for the index scan, with `pk_values` as an exact-match collision guard. Composite PKs are stored as pipe-delimited strings in column ordinal order (e.g. `12345|2`).

**Schema snapshots** map binlog column ordinals to names and identify PK columns. Re-snapshot after DDL changes; the indexer warns on detected DDL events.

## Index database tables

| Table | Purpose |
|---|---|
| `binlog_events` | All indexed row events, range-partitioned by `event_timestamp` |
| `schema_snapshots` | Table/column metadata from `information_schema` at snapshot time |
| `index_state` | Per-file indexing progress and status |

## License

MIT
