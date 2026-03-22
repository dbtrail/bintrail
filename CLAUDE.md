# Bintrail — Claude Code Guide

## Tool preferences

- **GitHub**: Always use the `mcp__github__*` MCP tools (e.g. `mcp__github__get_issue`, `mcp__github__create_pull_request`) instead of the `gh` CLI. The GitHub MCP server is always available in this project.

## Project overview

Bintrail is a Go CLI that parses MySQL ROW-format binary logs, indexes every row event into MySQL with full before/after images, and generates reversal SQL for recovery. The index is self-contained — recovery never requires the original binlog files.

Module: `github.com/dbtrail/bintrail`
Go version: 1.25.7 — uses `range N`, `min()`, `slices.Reverse`, `strings.SplitSeq`, `any` (not `interface{}`).

## Project structure

```
cmd/bintrail/          # One file per command (init, snapshot, index, query, recover, rotate, status, stream, dump, baseline, upload, generate-key, config)
                       # envload.go: env file loading (.bintrail.env / ~/.config/bintrail/config.env) + bindCommandEnv
                       # Test files: *_test.go (unit), *_integration_test.go (//go:build integration)
cmd/bintrail-mcp/      # MCP server: main.go, proxy.py, tests
internal/
  cliutil/             # Shared filter parsers: ParseEventType, ParseTime, IsValidFormat, IsValidOutputFormat
  config/              # config.Connect(dsn) — opens *sql.DB with parseTime=true + 10s timeout
  metadata/            # Schema snapshot loader and resolver
  observe/             # slog setup + Prometheus metrics for stream
  parser/              # Binlog file parser + StreamParser (go-mysql-org/go-mysql)
  indexer/             # Batch writer to binlog_events
  query/               # Query engine + result formatters (table/json/csv) + archive auto-discovery + merge + planner
  recovery/            # Reversal SQL generator
  status/              # Shared status types and display
  baseline/            # mydumper → Parquet converter
  archive/             # Partition archiver → Parquet via baseline.Writer
  parquetquery/        # DuckDB-backed Parquet query engine
  testutil/            # Shared test helpers: CreateTestDB, InitIndexTables, SkipIfNoMySQL, etc.
e2e_test.go            # Full CLI pipeline E2E test (//go:build integration), built with go build -cover
.mcp.json              # MCP server registration (go run ./cmd/bintrail-mcp)
migrations/            # Reference DDL (tables created by `bintrail init`, not this file)
docs/                  # guide.md, indexing.md, query-and-recovery.md, streaming.md, rotation-and-status.md, mcp-server.md, upload.md, parquet-debugging.md, deployment.md, quickstart.md, dump-and-baseline.md, docker.md, server-identity.md, mcp-gateway.md
```

## Commands

Global flags: `--log-level` (default `info`), `--log-format` (default `text`).

Per-command `--format`: most commands accept `text`/`json` (`IsValidOutputFormat`). Query accepts `table`/`json`/`csv` (`IsValidFormat`). When `--format json`, errors emit as `{"error":"message"}` on stderr via `wantsJSON(root)` + `outputJSON(v any)`. `SilenceErrors: true` on rootCmd.

| Command | File | Key flags |
|---|---|---|
| `init` | `init.go` | `--index-dsn` (req), `--partitions` (default 48), `--encrypt`, `--s3-bucket`, `--s3-region`, `--s3-arn` |
| `snapshot` | `snapshot.go` | `--source-dsn` (req), `--index-dsn` (req), `--schemas` |
| `index` | `index.go` | `--index-dsn` (req), `--source-dsn`, `--binlog-dir` (req), `--files`, `--all`, `--batch-size`, `--schemas`, `--tables` |
| `query` | `query.go` | `--index-dsn` (req), `--schema`, `--table`, `--pk`, `--event-type`, `--gtid`, `--since`, `--until`, `--changed-column`, `--flag`, `--format` (table/json/csv), `--limit`, `--archive-dir`, `--archive-s3`, `--bintrail-id`, `--profile`, `--no-archive` |
| `recover` | `recover.go` | same filters as query + `--output`, `--dry-run`, `--limit` (default 1000), `--profile`, `--no-archive` |
| `rotate` | `rotate.go` | `--index-dsn` (req), `--retain` (e.g. `7d`, `24h`), `--add-future`, `--archive-dir`, `--archive-compression` (default `zstd`) |
| `status` | `status.go` | `--index-dsn` (req) |
| `stream` | `stream.go` | `--index-dsn` (req), `--source-dsn` (req), `--server-id` (req), `--start-file`, `--start-pos`, `--start-gtid`, `--batch-size`, `--schemas`, `--tables`, `--checkpoint`, `--metrics-addr` |
| `dump` | `dump.go` | `--source-dsn` (req), `--output-dir` (req), `--schemas`, `--tables`, `--mydumper-path`, `--mydumper-image`, `--threads`, `--encrypt`, `--encrypt-key` |
| `baseline` | `baseline.go` | `--input` (req), `--output` (req), `--timestamp`, `--tables`, `--compression`, `--row-group-size`, `--upload`, `--upload-region`, `--encrypt`, `--encrypt-key` |
| `generate-key` | `generate_key.go` | `--output` (default `~/.config/bintrail/dump.key`) |
| `upload` | `upload.go` | `--source` (req), `--destination` (req), `--region`, `--retry`, `--index-dsn` |
| `config init` | `config.go` | `--global` |

Flag variable naming: prefixed by command abbreviation (e.g. `idxIndexDSN`, `qSchema`, `rDryRun`, `rotRetain`, `strmIndexDSN`, `dmpSourceDSN`, `bslInput`, `uplSource`, `cfgGlobal`).

### Environment file loading

All commands load a `.bintrail.env` file (local) or `~/.config/bintrail/config.env` (global) on startup via `loadEnvFile()` in `envload.go`. Each command calls `bindCommandEnv(cmd)` in its `init()` to map `BINTRAIL_*` env vars to CLI flags. Precedence: CLI flag > environment variable > default value. The env file is loaded once (via `sync.Once`). `bintrail config init` generates a template env file with all available variables.

## MCP server

`cmd/bintrail-mcp/` exposes three read-only tools (`query`, `recover`, `status`) via stdio (default, `.mcp.json`) or HTTP (`--http :8080`). DSN: `index_dsn` parameter overrides `BINTRAIL_INDEX_DSN` env var. All tools annotated `ReadOnlyHint: true`, `IdempotentHint: true`.

Key patterns:
- `newServer() *mcp.Server` — extracted from `main()` for testability
- `buildQueryOptions` — shared filter builder for `queryTool` and `recoverTool`
- **`jsonschema` tag**: use `jsonschema:"My description"` — NOT `jsonschema:"description=..."` (rejected by jsonschema-go v0.3+)
- Integration tests use `mcp.NewInMemoryTransports()` (protocol version `"2025-06-18"`)
- proxy.py: uses `# type: str` comments (not `str | None` — requires Python 3.10+, macOS ships 3.9)
- **Archive auto-discovery**: `resolveArchiveSources(ctx, db)` checks `BINTRAIL_ARCHIVE_S3` + `BINTRAIL_ID` env vars first, then falls back to `query.ResolveArchiveSources(ctx, db)` (from `archive_state` table). Both `query` and `recover` tools merge results from live MySQL + Parquet archives.
- **`no_archive` parameter**: Both `query` and `recover` MCP tools accept `no_archive` (bool) to disable archive auto-routing.
- `recoverTool` calls `recovery.GenerateSQLFromRows(rows, w)` with pre-fetched+merged rows (not `GenerateSQL` which fetches internally).

## Database tables

### `binlog_events` (range-partitioned)
- `pk_values`: `VARCHAR(512)` — pipe-delimited PK values. NOT JSON.
- `pk_hash = SHA2(pk_values, 256)`: **generated stored column** — never insert explicitly.
- `row_before`, `row_after`, `changed_columns`: JSON columns.
- Partitioned by `RANGE (TO_SECONDS(event_timestamp))` — timezone-independent; `UNIX_TIMESTAMP()` rejected by MySQL 8.0 when `time_zone=SYSTEM` (Error 1486).
- Hourly partitions: `p_YYYYMMDDHH` (12 chars); catch-all: `p_future VALUES LESS THAN MAXVALUE`.
- **PK lookup**: always use BOTH `pk_hash = SHA2(?, 256)` (index scan) AND `pk_values = ?` (collision guard).

### `schema_snapshots`
- TWO ID columns: `id` (auto-increment PK) and `snapshot_id` (group identifier for all rows of one snapshot). These are NOT the same.
- `NewResolver(db, 0)` loads latest snapshot; `NewResolver(db, N)` loads snapshot N.

### `index_state`
- Per-file indexing progress. Status: `in_progress`, `completed`, `failed`.
- `INSERT … ON DUPLICATE KEY UPDATE` for upserts.

### `stream_state`
- Single-row (id=1, `CHECK (id = 1)`) tracking replication position.
- `mode`: `"position"` or `"gtid"`. In GTID mode, `gtid_set` is the full **accumulated** executed set.
- `loadStreamState` returns `nil` for empty table — callers treat nil as "no checkpoint yet".

### `archive_state`
- Tracks which partitions have been archived to Parquet (local path + S3 location).
- Used by `query.ResolveArchiveSources` for auto-discovery of archive sources.
- Columns: `bintrail_id`, `partition_name`, `local_path`, `s3_bucket`, `s3_key`.

## Architecture: indexer pipeline

### File-based (`bintrail index`)

```
ParseFile goroutine ──► events chan ──► idx.Run (main goroutine)
         │                                      │
         └──► parseErrCh (buffered, size 1)    │ on error: cancel()
```

- `parseErrCh` buffered (size 1) so parser never blocks sending its error.
- Indexer calls `cancel()` on failure → parser's `ctx.Done()` fires → prevents deadlock.
- `errors.Is(parseErr, context.Canceled)` distinguishes real parse errors from cancellation.

### Replication-based (`bintrail stream`)

```
StreamParser goroutine ──► events chan ──► streamLoop (main goroutine)
     │                                           │
     └──► parseErrCh (buffered, size 1)         │ ticker → checkpoint
```

- `streamLoop` uses `time.Ticker` for checkpoints; uses exported `idx.InsertBatch` and `idx.BatchSize`.
- Both parsers share `handleRows`, `emitInserts`, `emitDeletes`, `emitUpdates` in `internal/parser/parser.go`.

## Key implementation patterns

### JSON round-trip and float64
After `json.Unmarshal` into `map[string]any`, **all numbers are `float64`**. `formatValue` in `recovery.go` handles this with `math.Trunc` check.

### MySQL JSON columns and base64
`marshalRow` in `indexer.go` promotes valid-JSON `[]byte` to `json.RawMessage` before inserting. Without this, go-mysql's raw JSON bytes get base64-encoded by `json.Marshal`.

### PK values encoding
Pipe-delimited with `|` → `\|` and `\` → `\\` escaping. See `BuildPKValues` in `parser/parser.go`.

### Partition management
- `partitionName(d)` → `"p_YYYYMMDDHH"` (Go format `"p_2006010215"`); `partitionDate(name)` parses back.
- Add: `REORGANIZE PARTITION p_future INTO (... new ..., PARTITION p_future VALUES LESS THAN MAXVALUE)`. **Never leave out p_future.**
- Drop: `ALTER TABLE … DROP PARTITION p1, p2` — single statement.
- `DescriptionToHuman`: `time.Unix(secs-62167219200, 0)` (since `TO_SECONDS('1970-01-01') = 62167219200`).
- `TABLE_ROWS` in `information_schema.PARTITIONS` is an InnoDB **estimate**.

### Getting DB name from DSN
Use `mysql.ParseDSN(dsn)` from `github.com/go-sql-driver/mysql`. Do not use `SELECT DATABASE()`.

### Dual MySQL imports (stream.go)
- `gomysql "github.com/go-mysql-org/go-mysql/mysql"` — Position, GTIDSet, MysqlGTIDSet
- `drivermysql "github.com/go-sql-driver/mysql"` — ParseDSN only

### parseTime=true and connection timeout
`config.Connect` injects `parseTime=true` (without it, DATETIME → `[]uint8` → scan errors) and 10s TCP timeout. Always use `config.Connect`, not raw `sql.Open`.

### Recovery SQL
- Never executes — only generates SQL (`--dry-run` to stdout, `--output` to file).
- Events reversed with `slices.Reverse(rows)` — most-recent undone first.
- `pkWhereClause` uses resolver PK columns; falls back to all-columns WHERE when resolver is nil.
- Two entry points: `GenerateSQL(ctx, opts, w)` fetches events internally; `GenerateSQLFromRows(rows, w)` takes pre-fetched rows (used by CLI/MCP when merging live MySQL + archive results).

### Archive auto-discovery and merge
- `query.ResolveArchiveSources(ctx, db)` in `internal/query/archive.go` queries `archive_state` for distinct `bintrail_id` paths. Prefers local paths over S3 when the directory exists on disk.
- `query.MergeResults(rows, limit)` in `internal/query/merge.go` deduplicates by `event_id` (MySQL wins), sorts by `(event_timestamp, event_id)`, applies limit.
- `extractBasePath(path)` extracts up to and including `bintrail_id=<uuid>` from an archive file path.

### Query planner
- `query.Plan(ctx, db, dbName, since, until)` in `internal/query/planner.go` inspects live partition boundaries and `archive_state` to build a `QueryPlan`.
- `QueryPlan.SkipMySQL()` returns true when the entire time range is covered by archives (no gaps).
- `QueryPlan.GapHours` lists hours with no data (rotated but not archived); emitted as `slog.Warn` via `RunPlanAndWarn`.
- `ParsePartitionName(name)` converts `"p_2026021914"` to UTC hour; returns false for `"p_future"` or malformed names.
- `--no-archive` flag on `query`/`recover` skips archive auto-discovery entirely (MySQL-only results).

## Testing conventions

### Test tiers

1. **Unit tests** (no build tag): `go test ./... -count=1`
2. **Integration tests** (`//go:build integration`): `go test -tags integration ./... -count=1` — requires Docker MySQL on port 13306 (user `root`, password `testroot`)
3. **E2E test** (`e2e_test.go`, `//go:build integration`): `go test -tags integration -run TestEndToEnd -v .`

### Key conventions
- Test files in `cmd/bintrail/` are `package main` — access unexported helpers directly.
- Integration tests use `_integration_test.go` suffix with `//go:build integration`.
- Every integration test calls `testutil.SkipIfNoMySQL(t)` or `testutil.CreateTestDB(t)` first.
- Recovery tests: `newGen()` → `New(nil, nil)` triggers all-columns WHERE fallback.
- `assertSQL(t, stmt, want)` checks `strings.Contains` for SQL fragments.
- Do not hardcode UNIX timestamps — compute with `time.Date(...).Unix()`.

## Dependencies

| Package | Purpose |
|---|---|
| `github.com/go-mysql-org/go-mysql` | Binlog parsing |
| `github.com/go-sql-driver/mysql` | MySQL driver + `ParseDSN` |
| `github.com/spf13/cobra` | CLI framework |
| `github.com/modelcontextprotocol/go-sdk` | MCP server SDK |
| `github.com/prometheus/client_golang` | Prometheus metrics (stream) |
| `github.com/duckdb/duckdb-go/v2` | DuckDB for Parquet archive queries |

Do not import transitive deps (shopspring/decimal, pingcap/*, etc.) directly.

## Build and release

Version injection via `-ldflags -X`: `main.Version`, `main.CommitSHA`, `main.BuildDate` (bintrail); `main.mcpVersion` (bintrail-mcp). `make build` builds both. CGO_ENABLED=1 required (DuckDB).

Release: update CHANGELOG.md, commit, create annotated tag `vX.Y.Z`, push. GitHub Actions runs GoReleaser automatically. Use `/release` skill.

## Common gotchas

- **`go mod tidy` removes go-mysql**: re-add with `go get github.com/go-mysql-org/go-mysql@v1.13.0`.
- **`binlog_row_image=FULL` required**: `index` validates via `SHOW VARIABLES` and refuses if not FULL.
- **DDL changes break indexing**: snapshot must be re-taken after ALTER TABLE etc.
- **Column count mismatch**: indexer logs warning and skips table's events — does not fail.
- **`p_future` must always exist**: never drop it.
- **`schema_snapshots` snapshot_id ≠ row id**: snapshot_id groups rows; id is auto-increment PK.
- **go-mysql lowercases GTID UUIDs**: tests must use lowercase — never uppercase UUID literals.
