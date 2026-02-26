# Bintrail — Claude Code Guide

## Project overview

Bintrail is a Go CLI that parses MySQL ROW-format binary logs, indexes every row event into MySQL with full before/after images, and generates reversal SQL for recovery. The index is self-contained — recovery never requires the original binlog files.

Module: `github.com/bintrail/bintrail`
Go version: 1.25.7 (all modern APIs available — see below)

## Project structure

```
cmd/bintrail/          # One file per command
  main.go              # Root cobra command
  init.go              # bintrail init
  snapshot.go          # bintrail snapshot
  index.go             # bintrail index (core — parser + indexer pipeline)
  query.go             # bintrail query
  recover.go           # bintrail recover
  rotate.go            # bintrail rotate + partition helpers
  status.go            # bintrail status
  stream.go            # bintrail stream (live replication indexing)
  index_test.go        # Unit tests for binlogFileRe, buildIndexFilters, resolveFiles, findBinlogFiles
  query_test.go        # Unit tests for queryCmd cobra wiring + runQuery validation logic
  recover_test.go      # Unit tests for recoverCmd cobra wiring + runRecover validation logic
  snapshot_test.go     # Unit tests for parseSchemaList
  rotate_test.go       # Unit tests for parseRetain, partitionDate, partitionName, nextPartitionStart + cobra wiring
  stream_test.go       # Unit tests for parseSourceDSN, resolveStart, GTID accumulation, cobra wiring
  cmd_integration_test.go             # Integration tests (//go:build integration) for all DB helpers
  stream_integration_test.go          # Integration tests for stream_state persistence and streamLoop behaviour

cmd/bintrail-mcp/      # MCP server (query, recover, status as read-only tools)
  main.go              # Server entry point + newServer() + tool handlers + buildQueryOptions
  main_test.go         # Unit tests for buildQueryOptions, resolveDSN, errorResult
  integration_test.go  # Integration tests (//go:build integration) — in-memory MCP transport + live MySQL
  e2e_test.go          # E2E test (//go:build integration) — subprocess JSON-RPC stdio protocol

internal/
  cliutil/cliutil.go   # Shared filter parsers: ParseEventType, ParseTime, IsValidFormat
  config/config.go     # config.Connect(dsn) — opens and pings *sql.DB
  metadata/            # Schema snapshot loader and resolver
  observe/             # Observability: Setup(), ParseLevel(), Nop() (slog) + Prometheus metrics for stream
  parser/              # Binlog file parser + StreamParser (go-mysql-org/go-mysql)
  indexer/             # Batch writer to binlog_events
  query/               # Query engine + result formatters (table/json/csv)
  recovery/            # Reversal SQL generator
  status/status.go     # Shared status types and display: LoadIndexState, LoadPartitionStats, WriteStatus
  testutil/testutil.go # Shared test helpers: CreateTestDB, InitIndexTables, SkipIfNoMySQL, etc.

e2e_test.go            # E2E integration test (//go:build integration) — exercises full CLI pipeline
                       # init → snapshot → index → query (json/csv/table/pk/changed-column) →
                       # recover (dry-run + file output) → status → rotate
                       # Built with `go build -cover` for binary coverage instrumentation

.mcp.json              # Project-level MCP server registration (bintrail server via go run)

migrations/
  001_create_tables.sql  # Reference DDL (tables are created by `bintrail init`, not this file)

docs/
  guide.md             # Practical DBA guide — scenario walkthroughs + troubleshooting FAQ
```

## Commands

Global persistent flags (all commands via `rootCmd.PersistentFlags`):
- `--log-level` (default `info`) — slog level: debug, info, warn, error
- `--log-format` (default `text`) — log format: text or json (JSON for log aggregators)

| Command | File | Key flags |
|---|---|---|
| `init` | `init.go` | `--index-dsn` (req), `--partitions` (default 7) |
| `snapshot` | `snapshot.go` | `--source-dsn` (req), `--index-dsn` (req), `--schemas` |
| `index` | `index.go` | `--index-dsn` (req), `--source-dsn`, `--binlog-dir` (req), `--files`, `--all`, `--batch-size`, `--schemas`, `--tables` |
| `query` | `query.go` | `--index-dsn` (req), `--schema`, `--table`, `--pk`, `--event-type`, `--gtid`, `--since`, `--until`, `--changed-column`, `--format`, `--limit` |
| `recover` | `recover.go` | same filters as query + `--output`, `--dry-run`, `--limit` (default 1000) |
| `rotate` | `rotate.go` | `--index-dsn` (req), `--retain` (e.g. `7d`, `24h`), `--add-future` |
| `status` | `status.go` | `--index-dsn` (req) |
| `stream` | `stream.go` | … + `--metrics-addr` (e.g. `:9090`; empty = disabled) |
| `stream` | `stream.go` | `--index-dsn` (req), `--source-dsn` (req), `--server-id` (req), `--start-file`, `--start-pos`, `--start-gtid`, `--batch-size`, `--schemas`, `--tables`, `--checkpoint` |

Flag variable naming convention: prefixed by command abbreviation (e.g. `idxIndexDSN`, `qSchema`, `rDryRun`, `rotRetain`, `stIndexDSN`, `strmIndexDSN`).

Filter helpers (`ParseEventType`, `ParseTime`, `IsValidFormat`) live in `internal/cliutil` and are shared by both `cmd/bintrail/` commands and `cmd/bintrail-mcp/`.

## MCP server

`cmd/bintrail-mcp/` is a stdio MCP server exposing three read-only tools:

| Tool | Handler | Description |
|---|---|---|
| `query` | `queryTool` | Search binlog events (same filters as CLI `query`) |
| `recover` | `recoverTool` | Generate reversal SQL (dry-run only) |
| `status` | `statusTool` | Show indexed files, partitions, summary |

All tools are annotated with `ReadOnlyHint: true` and `IdempotentHint: true`. DSN resolution: `index_dsn` parameter overrides `BINTRAIL_INDEX_DSN` env var.

`newServer() *mcp.Server` constructs and returns the configured server (extracted from `main()` so tests can call it). `buildQueryOptions` is the shared filter builder used by both `queryTool` and `recoverTool`. Both are tested in `cmd/bintrail-mcp/main_test.go`.

**`jsonschema` tag format**: use plain description strings (`jsonschema:"My description"`) — NOT the old `key=value` format (`jsonschema:"description=..."`) which is rejected by jsonschema-go v0.3+.

Integration tests (`integration_test.go`) use `mcp.NewInMemoryTransports()` to connect a test client to the server in-process — no subprocess or stdio framing needed. The E2E test (`e2e_test.go`) builds the binary with `go build -cover` and speaks raw newline-delimited JSON-RPC over stdin/stdout (protocol version `"2025-06-18"`).

Project-level registration via `.mcp.json` uses `go run ./cmd/bintrail-mcp` — no pre-build needed.

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

### `stream_state`
- Single-row table (id=1, enforced by `CHECK (id = 1)`) tracking live replication position.
- `mode`: `"position"` (file+pos) or `"gtid"` (GTID set string).
- `INSERT … ON DUPLICATE KEY UPDATE` via `saveCheckpoint` in `stream.go` for atomic upserts.
- Saved on a ticker (default 10s) and on graceful shutdown (SIGINT/SIGTERM).
- In GTID mode, `gtid_set` is the full **accumulated** executed set (not just the latest single GTID), so resuming passes the full set to `syncer.StartSyncGTID`.
- `loadStreamState` returns `nil` for an empty table — callers treat nil as "no checkpoint yet".

## Architecture: indexer pipeline

### File-based (`bintrail index`)

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

### Replication-based (`bintrail stream`)

`StreamParser` and `streamLoop` run in separate goroutines, connected via the same `chan parser.Event`:

```
StreamParser goroutine ──► events chan ──► streamLoop (main goroutine)
     │                                           │
     └──► parseErrCh (buffered, size 1)         │
                                                 │ ticker → checkpoint
                                                 │ SIGINT/SIGTERM → cancel()
```

- `StreamParser.Run` calls `streamer.GetEvent(ctx)` which blocks until an event arrives.
- `streamLoop` uses a `time.Ticker` for periodic checkpoints rather than relying on `idx.Run`; this requires the exported `idx.InsertBatch` and `idx.BatchSize` methods.
- Both `Parser` (file-based) and `StreamParser` share the package-level `handleRows`, `emitInserts`, `emitDeletes`, `emitUpdates` functions in `internal/parser/parser.go`.

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

### Dual MySQL imports in stream.go
`stream.go` needs both `github.com/go-mysql-org/go-mysql/mysql` (for `Position`, `GTIDSet`, `MysqlGTIDSet`) and `github.com/go-sql-driver/mysql` (for `ParseDSN`). They are aliased:
- `gomysql "github.com/go-mysql-org/go-mysql/mysql"` (used more heavily)
- `drivermysql "github.com/go-sql-driver/mysql"` (only for `drivermysql.ParseDSN`)

### parseTime=true in connections
`config.Connect` always injects `parseTime=true` via `mysql.ParseDSN` → `cfg.ParseTime = true` → `cfg.FormatDSN()`. Without this, go-sql-driver returns DATETIME columns as `[]uint8` (raw bytes) instead of `time.Time`, causing scan errors. Do not use raw `sql.Open("mysql", dsn)` in commands that read DATETIME columns — always go through `config.Connect`.

### Recovery SQL generation
- `recover` only generates SQL (`--dry-run` to stdout, `--output` to file) — it never executes against the source database. Application is always a manual step.
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

### Test tiers

There are three tiers of tests:

1. **Unit tests** (no build tag) — run with `go test ./...`. No live DB required. Cover pure functions, formatting, SQL shape, regex matching, etc.
2. **Integration tests** (`//go:build integration`) — run with `go test -tags integration ./...`. Require a Docker MySQL container named `bintrail-test-mysql` on port 13306 (user `root`, password `testroot`). Each test creates and drops its own database via `testutil.CreateTestDB`.
3. **E2E test** (`e2e_test.go`, `//go:build integration`) — builds the `bintrail` binary with `go build -cover` and exercises the full CLI pipeline (init → snapshot → index → query → recover → status → rotate) as subprocesses. Coverage data is captured via `GOCOVERDIR` and converted with `go tool covdata textfmt`.

### Running tests

```bash
# Unit tests only (no Docker needed)
go test ./... -count=1

# Full suite including integration tests (requires Docker MySQL)
go test -tags integration ./... -count=1

# E2E test only (shows binary coverage summary)
go test -tags integration -run TestEndToEnd -v .

# Full suite with coverage report
go test -tags integration -coverprofile=cover.out ./... -count=1
go tool cover -func=cover.out
```

### Coverage baseline (as of ParseTime UTC fix + E2E stdout/stderr fix)

Full suite (`go test -tags integration -coverprofile=cover.out ./... -count=1`):

| Package | Coverage |
|---|---|
| `internal/cliutil` | 100% |
| `internal/observe` | 100% |
| `internal/recovery` | 92% |
| `internal/config` | 91% |
| `internal/query` | 91% |
| `internal/indexer` | 86% |
| `cmd/bintrail-mcp` | 89% |
| `internal/metadata` | 83% |
| `internal/parser` | 82% |
| `internal/status` | 68% |
| `cmd/bintrail` | 51% |
| **total** | **66%** |

**Known gaps and why:**
- `cmd/bintrail` `run*` handlers (51%): cobra entry points are only exercised by the root `e2e_test.go` subprocess test, whose coverage lands in `GOCOVERDIR` (not `cover.out`). `runStream`, `runInit`, `runSnapshot`, `runStatus` are included in this gap. Validation logic in `runQuery`/`runRecover`/`runRotate` is covered by unit tests in `query_test.go`, `recover_test.go`, `rotate_test.go`.
- `internal/status` `LoadIndexState`/`LoadPartitionStats` (0% in cover.out): called through the MCP/CLI handlers which run as subprocesses; `WriteStatus`/`DescriptionToHuman` are 100%.
- `cmd/bintrail-mcp` `main()` (0%): the stdio entry point is intentionally excluded — exercised by `TestMCPE2E`.

### Test infrastructure

- **`internal/testutil`** — shared helpers for integration tests:
  - `SkipIfNoMySQL(t)` — gracefully skips if Docker MySQL is unreachable
  - `CreateTestDB(t)` — creates a uniquely-named database with automatic cleanup via `t.Cleanup`
  - `InitIndexTables(t, db)` — creates `binlog_events` (with only `p_future`), `schema_snapshots`, and `index_state`
  - `InsertEvent(t, db, ...)` / `InsertSnapshot(t, db, ...)` — insert test data directly
  - `MustExec(t, db, query)` — exec or fatal
  - `SnapshotDSN(dbName)` / `IntegrationDSN(dbName)` — DSN builders for test databases

### Conventions

- Recovery unit tests use `newGen()` helper: `func newGen() *Generator { return New(nil, nil) }` — nil DB and nil resolver triggers the all-columns WHERE fallback.
- `assertSQL(t, stmt, want)` helper checks `strings.Contains` for SQL fragments.
- Do not hardcode UNIX timestamps for specific dates in tests — compute them with `time.Date(...).Unix()` to avoid year-sensitive failures.
- `assertContains(t, s, want)` is the equivalent helper in `internal/status/status_test.go`.
- Test files in `cmd/bintrail/` are `package main` and can access all unexported helpers directly.
- Integration test files use the `_integration_test.go` suffix and have `//go:build integration` at the top.
- Every integration test calls `testutil.SkipIfNoMySQL(t)` or `testutil.CreateTestDB(t)` (which calls `SkipIfNoMySQL` internally) as its first action.

## Dependencies

| Package | Purpose |
|---|---|
| `github.com/go-mysql-org/go-mysql` | Binlog file parsing (`replication` package) |
| `github.com/go-sql-driver/mysql` | MySQL driver + `mysql.ParseDSN` |
| `github.com/spf13/cobra` | CLI framework |
| `github.com/modelcontextprotocol/go-sdk` | MCP server SDK (`cmd/bintrail-mcp` only) |
| `github.com/prometheus/client_golang` | Prometheus metrics (`internal/observe`, `cmd/bintrail/stream.go`) |

Transitive deps pulled in by go-mysql: shopspring/decimal, pingcap/errors, pingcap/tidb, google/uuid, klauspost/compress, zap, etc. These are indirect — don't import them directly.

## Observability

### Structured logging (`internal/observe`)

All logging uses `log/slog` (stdlib, Go 1.21+). The root command configures the global logger via `PersistentPreRunE`:

```
bintrail --log-level debug --log-format json query ...
```

- `observe.Setup(w, format, level)` — creates and sets `slog.Default()`; called once in `PersistentPreRunE`
- `observe.ParseLevel(s)` — string → `slog.Level`; unrecognised strings default to `info`
- `observe.Nop()` — discards all output (use in tests)
- `internal/parser`: `Parser` and `StreamParser` accept `*slog.Logger` (nil → `slog.Default()`)

All commands emit structured completion log lines at `slog.Info` level:
- `index`: `"indexing complete"` with `files_processed`, `events_indexed`
- `query`: `"query complete"` with `results`, `format`, `duration_ms`
- `recover`: `"recovery SQL generated"` with `statements`, `dry_run`, `duration_ms`
- `rotate`: `"rotation complete"` with `partitions_dropped`, `partitions_added`, `duration_ms`

### Prometheus metrics (`internal/observe/metrics.go`)

The `stream` command exposes Prometheus metrics on an optional HTTP endpoint:

```
bintrail stream --metrics-addr :9090 ...
curl localhost:9090/metrics | grep bintrail_stream
```

All metrics are in the `bintrail_stream_` namespace:

| Metric | Type | Description |
|---|---|---|
| `bintrail_stream_events_received_total` | Counter | Binlog row events received |
| `bintrail_stream_events_indexed_total` | Counter | Events written to binlog_events |
| `bintrail_stream_batch_flushes_total` | Counter | Batch INSERT operations |
| `bintrail_stream_checkpoint_saves_total` | Counter | Successful checkpoint writes |
| `bintrail_stream_last_event_timestamp_seconds` | Gauge | Unix timestamp of last event |
| `bintrail_stream_replication_lag_seconds` | Gauge | Age of last event in seconds |
| `bintrail_stream_errors_total{type}` | Counter | Errors by type: batch_flush, checkpoint, gtid_update |
| `bintrail_stream_batch_size` | Histogram | Events per batch flush |

## Documentation

- `README.md` — project overview + Quick Start (line 56 links to `docs/guide.md` for the full DBA walkthrough)
- `docs/guide.md` — scenario-driven guide: initial setup, daily rotation, point-in-time recovery, multi-table recovery, troubleshooting FAQ

## Common gotchas

- **`go mod tidy` removes go-mysql**: if nothing in the codebase imports it yet, tidy will drop it. Re-add with `go get github.com/go-mysql-org/go-mysql@v1.13.0`.
- **`binlog_row_image=FULL` required**: the `index` command validates this via `SHOW VARIABLES LIKE 'binlog_row_image'` and refuses to proceed if not FULL.
- **DDL changes break indexing**: if the source schema changes (ALTER TABLE etc.), the snapshot must be re-taken. The parser warns on DDL detection but does not automatically re-snapshot.
- **Column count mismatch**: when a TABLE_MAP_EVENT's column count differs from the snapshot, the indexer logs a warning and skips that table's events — it does not fail.
- **`p_future` must always exist**: never drop it. `REORGANIZE PARTITION` always recreates it at the end.
- **`schema_snapshots` snapshot_id ≠ row id**: a common source of confusion. `snapshot_id` groups all rows of one snapshot; `id` is the auto-increment row primary key.
- **go-mysql lowercases GTID UUIDs**: `MysqlGTIDSet.String()` always returns lowercase UUIDs (e.g. `3e11fa47-...`). Tests comparing GTID strings must use lowercase or `strings.ToLower` — never uppercase UUID literals.
