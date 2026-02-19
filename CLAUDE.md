# Bintrail — Claude Code Guide

## Project overview

Bintrail is a Go CLI that parses MySQL ROW-format binary logs, indexes every row event into MySQL with full before/after images, and generates reversal SQL for recovery. The index is self-contained — recovery never requires the original binlog files.

Module: `github.com/bintrail/bintrail`
Go version: 1.25.7 (all modern APIs available — see below)

## Project structure

```
cmd/bintrail/          # One file per command + shared helpers in query.go
  main.go              # Root cobra command
  init.go              # bintrail init
  snapshot.go          # bintrail snapshot
  index.go             # bintrail index (core — parser + indexer pipeline)
  query.go             # bintrail query + shared helpers: parseEventType, parseQueryTime, isValidFormat
  recover.go           # bintrail recover
  rotate.go            # bintrail rotate + partition helpers
  status.go            # bintrail status
  rotate_test.go       # Unit tests for rotate helpers
  status_test.go       # Unit tests for status helpers

internal/
  config/config.go     # config.Connect(dsn) — opens and pings *sql.DB
  metadata/            # Schema snapshot loader and resolver
  parser/              # Binlog file parser (go-mysql-org/go-mysql)
  indexer/             # Batch writer to binlog_events
  query/               # Query engine + result formatters (table/json/csv)
  recovery/            # Reversal SQL generator

migrations/
  001_create_tables.sql  # Reference DDL (tables are created by `bintrail init`, not this file)
```

## Commands

| Command | File | Key flags |
|---|---|---|
| `init` | `init.go` | `--index-dsn` (req), `--partitions` (default 7) |
| `snapshot` | `snapshot.go` | `--source-dsn` (req), `--index-dsn` (req), `--schemas` |
| `index` | `index.go` | `--index-dsn` (req), `--source-dsn`, `--binlog-dir` (req), `--files`, `--all`, `--batch-size`, `--schemas`, `--tables` |
| `query` | `query.go` | `--index-dsn` (req), `--schema`, `--table`, `--pk`, `--event-type`, `--gtid`, `--since`, `--until`, `--changed-column`, `--format`, `--limit` |
| `recover` | `recover.go` | same filters as query + `--output`, `--dry-run`, `--limit` (default 1000) |
| `rotate` | `rotate.go` | `--index-dsn` (req), `--retain` (e.g. `7d`, `24h`), `--add-future` |
| `status` | `status.go` | `--index-dsn` (req) |

Flag variable naming convention: prefixed by command abbreviation (e.g. `idxIndexDSN`, `qSchema`, `rDryRun`, `rotRetain`, `stIndexDSN`).

`parseEventType` and `parseQueryTime` are defined in `query.go` and reused by `recover.go` — same `main` package, no duplication needed.

## Database tables

### `binlog_events` (range-partitioned)
- `pk_values` is a plain `VARCHAR(512)` — pipe-delimited PK values in ordinal order (e.g. `12345` or `12345|2`). NOT JSON.
- `pk_hash = SHA2(pk_values, 256)` is a **generated stored column** — never insert it explicitly.
- `row_before`, `row_after`, `changed_columns` are JSON columns.
- Partitioned by `RANGE (TO_DAYS(event_timestamp))` — timezone-independent; `UNIX_TIMESTAMP()` is rejected by MySQL 8.0 when `time_zone=SYSTEM` (Error 1486).
- Daily partitions named `p_YYYYMMDD`; catch-all is always `p_future VALUES LESS THAN MAXVALUE`.
- **PK lookup pattern**: always use both `pk_hash = SHA2(?, 256)` (index scan) AND `pk_values = ?` (collision guard).

### `schema_snapshots`
- Has TWO ID columns: `id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY` (row ID) and `snapshot_id INT UNSIGNED` (group identifier shared by all rows of one snapshot). The spec originally had snapshot_id as the PK — that was a bug, fixed during implementation.
- `NewResolver(db, 0)` loads the **latest** snapshot; `NewResolver(db, N)` loads snapshot N.

### `index_state`
- Tracks per-file indexing progress. Status: `in_progress`, `completed`, `failed`.
- `INSERT … ON DUPLICATE KEY UPDATE` pattern for upserts (see `upsertFileState` in `index.go`).

## Architecture: indexer pipeline

Parser and indexer run concurrently to avoid buffering entire binlog files in memory:

```
ParseFile goroutine ──► events chan ──► idx.Run (main goroutine)
         │                                      │
         └──► parseErrCh (buffered, size 1)    │
                                                │ on error: cancel()
                                                │ ctx.Done() unblocks parser
```

- `parseErrCh` is buffered (size 1) so the parser never blocks trying to send its error.
- If the indexer fails, it calls `cancel()` so the parser's `ctx.Done()` fires and it stops sending. This prevents deadlock.
- After `idx.Run` returns, the main goroutine reads from `parseErrCh`.
- `errors.Is(parseErr, context.Canceled)` distinguishes real parse errors from cancellation.

## Key implementation details

### JSON round-trip and float64
After `json.Unmarshal` into `map[string]any`, **all numbers are `float64`**. `formatValue` in `recovery.go` handles this:
```go
if val == math.Trunc(val) && math.Abs(val) < 1e15 {
    return strconv.FormatInt(int64(val), 10) // format as integer
}
```

### MySQL JSON columns and base64
`marshalRow` in `indexer.go` promotes valid-JSON `[]byte` values to `json.RawMessage` before inserting. Without this, MySQL JSON column values (which go-mysql returns as raw JSON bytes) get base64-encoded when marshalled to Go's `json.Marshal`.

### PK values encoding
Pipe-delimited, with `|` → `\|` and `\` → `\\` escaping. See `BuildPKValues` in `parser/parser.go`. In practice PKs are almost always integers or UUIDs so escaping is rarely triggered.

### Partition management
- `partitionName(d time.Time) string` → `"p_YYYYMMDD"` (uses Go reference time `20060102`)
- `partitionDate(name string) (time.Time, bool)` → parses `p_YYYYMMDD`, returns `false` for `p_future` or malformed names
- These two round-trip correctly and are tested in `rotate_test.go`.
- To add partitions: `REORGANIZE PARTITION p_future INTO (... new partitions ..., PARTITION p_future VALUES LESS THAN MAXVALUE)`. Never leave out the new `p_future`.
- To drop partitions: `ALTER TABLE … DROP PARTITION p1, p2` — single statement for multiple partitions.
- `PARTITION_DESCRIPTION` in `information_schema.PARTITIONS` stores the evaluated integer TO_DAYS value for named partitions and `MAXVALUE` for the catch-all. `descriptionToHuman` in `status.go` converts it back to a date via `time.Unix((days-719528)*86400, 0)` (since `TO_DAYS('1970-01-01') = 719528`).
- `TABLE_ROWS` in `information_schema.PARTITIONS` is an **estimate** for InnoDB — good enough for status display, not for exact counts.

### Getting DB name from DSN
Use `mysql.ParseDSN(dsn)` from `github.com/go-sql-driver/mysql` — consistent with `init.go`, `rotate.go`, `status.go`. Do not use `SELECT DATABASE()`.

### parseTime=true in connections
`config.Connect` always injects `parseTime=true` via `mysql.ParseDSN` → `cfg.ParseTime = true` → `cfg.FormatDSN()`. Without this, go-sql-driver returns DATETIME columns as `[]uint8` (raw bytes) instead of `time.Time`, causing scan errors. Do not use raw `sql.Open("mysql", dsn)` in commands that read DATETIME columns — always go through `config.Connect`.

### Recovery SQL generation
- Events are reversed with `slices.Reverse(rows)` before generating SQL, so the most-recent event is undone first.
- `pkWhereClause` uses resolver PK columns when available; falls back to `allColsWhere` (all columns) when resolver is nil or table not found. This is always correct for tables with no duplicate rows.
- Resolver is loaded best-effort in the `recover` command — a failure logs a warning and proceeds with the all-columns fallback.

## Go version features in use

This codebase uses Go 1.22–1.24 APIs freely:
- `range N` (integer range, Go 1.22)
- `min()` built-in (Go 1.21)
- `slices.Reverse` (Go 1.21)
- `strings.SplitSeq` (Go 1.24)
- `sql.NullTime` (Go 1.15)

Use `any` instead of `interface{}` everywhere.

## Testing conventions

- All tests are unit tests — no live DB required.
- Recovery tests use `newGen()` helper: `func newGen() *Generator { return New(nil, nil) }` — nil DB and nil resolver triggers the all-columns WHERE fallback.
- `assertSQL(t, stmt, want)` helper checks `strings.Contains` for SQL fragments.
- Do not hardcode UNIX timestamps for specific dates in tests — compute them with `time.Date(...).Unix()` to avoid year-sensitive failures.
- `assertContains(t, s, want)` is the equivalent helper in `status_test.go`.
- Test files in `cmd/bintrail/` are `package main` and can access all unexported helpers directly.

## Dependencies

| Package | Purpose |
|---|---|
| `github.com/go-mysql-org/go-mysql` | Binlog file parsing (`replication` package) |
| `github.com/go-sql-driver/mysql` | MySQL driver + `mysql.ParseDSN` |
| `github.com/spf13/cobra` | CLI framework |

Transitive deps pulled in by go-mysql: shopspring/decimal, pingcap/errors, pingcap/tidb, google/uuid, klauspost/compress, zap, etc. These are indirect — don't import them directly.

## Common gotchas

- **`go mod tidy` removes go-mysql**: if nothing in the codebase imports it yet, tidy will drop it. Re-add with `go get github.com/go-mysql-org/go-mysql@v1.13.0`.
- **`binlog_row_image=FULL` required**: the `index` command validates this via `SHOW VARIABLES LIKE 'binlog_row_image'` and refuses to proceed if not FULL.
- **DDL changes break indexing**: if the source schema changes (ALTER TABLE etc.), the snapshot must be re-taken. The parser warns on DDL detection but does not automatically re-snapshot.
- **Column count mismatch**: when a TABLE_MAP_EVENT's column count differs from the snapshot, the indexer logs a warning and skips that table's events — it does not fail.
- **`p_future` must always exist**: never drop it. `REORGANIZE PARTITION` always recreates it at the end.
- **`schema_snapshots` snapshot_id ≠ row id**: a common source of confusion. `snapshot_id` groups all rows of one snapshot; `id` is the auto-increment row primary key.
