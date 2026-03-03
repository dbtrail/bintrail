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
  dump.go              # bintrail dump (invoke mydumper; lockfile, schema/table filters)
  baseline.go          # bintrail baseline (convert mydumper output → Parquet; no DB connection)
  index_test.go        # Unit tests for binlogFileRe, buildIndexFilters, resolveFiles, findBinlogFiles
  query_test.go        # Unit tests for queryCmd cobra wiring + runQuery validation logic
  recover_test.go      # Unit tests for recoverCmd cobra wiring + runRecover validation logic
  snapshot_test.go     # Unit tests for parseSchemaList
  rotate_test.go       # Unit tests for parseRetain, partitionDate, partitionName, nextPartitionStart + cobra wiring
  stream_test.go       # Unit tests for parseSourceDSN, resolveStart, GTID accumulation, cobra wiring
  dump_test.go         # Unit tests for dumpCmd cobra wiring, buildMydumperArgs, lock mechanism, extractSchemasFromTables
  baseline_test.go     # Unit tests for parseTableFilter, runBaseline timestamp parsing
  init_test.go         # Unit tests for initCmd cobra wiring, buildBinlogEventsDDL, buildPartitionDefs, parseS3ARN, s3Instructions, runInit validation
  status_test.go       # Unit tests for statusCmd cobra wiring + runStatus DSN validation
  cmd_integration_test.go             # Integration tests (//go:build integration) for all DB helpers
  stream_integration_test.go          # Integration tests for stream_state persistence and streamLoop behaviour

cmd/bintrail-mcp/      # MCP server (query, recover, status as read-only tools)
  main.go              # Server entry point + newServer() + tool handlers + buildQueryOptions
  main_test.go         # Unit tests for buildQueryOptions, resolveDSN, errorResult
  integration_test.go  # Integration tests (//go:build integration) — in-memory MCP transport + live MySQL
  e2e_test.go          # E2E test (//go:build integration) — subprocess JSON-RPC stdio protocol
  proxy.py             # Python stdio↔HTTP proxy for Claude Desktop on remote machines (stdlib only)

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
  baseline/            # mydumper → Parquet converter: ParseMetadata, ParseSchema, DiscoverTables, Writer
  archive/             # Partition archiver: ArchivePartition writes a binlog_events partition to Parquet
  parquetquery/        # DuckDB-backed Parquet query engine: Fetch, buildGlob, buildQuery, scanRows
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
  indexing.md          # Developer doc: parser/indexer pipeline, Resolver, concurrent goroutine design
  query-and-recovery.md # Developer doc: dynamic SQL builder, PK lookup, reversal logic, float64 gotcha
  streaming.md         # Developer doc: replication protocol, StreamParser, checkpointing, Prometheus metrics
  rotation-and-status.md # Developer doc: partition management, TO_DAYS rationale, DescriptionToHuman
  mcp-server.md        # Developer doc: MCP tools, transport modes, proxy.py, newServer() pattern
```

## Commands

Global persistent flags (all commands via `rootCmd.PersistentFlags`):
- `--log-level` (default `info`) — slog level: debug, info, warn, error
- `--log-format` (default `text`) — log format: text or json (JSON for log aggregators)

Per-command `--format` flag (default `text`): every command accepts `--format text` (human-readable) or `--format json` (structured JSON to stdout). Query is the exception — its `--format` accepts `table`, `json`, or `csv` (validated by `IsValidFormat`). All other commands use `IsValidOutputFormat` (text/json). When `--format json` is active, errors are emitted as `{"error":"message"}` on stderr. The `outputJSON(v any)` helper in `main.go` handles consistent JSON encoding; `wantsJSON(root)` inspects the invoked command's `--format` flag to decide error formatting. `SilenceErrors: true` on rootCmd lets `main()` control error output.

| Command | File | Key flags |
|---|---|---|
| `init` | `init.go` | `--index-dsn` (req), `--partitions` (default 48), `--encrypt`, `--s3-bucket`, `--s3-region` (default `us-east-1`), `--s3-arn`, `--format` |
| `snapshot` | `snapshot.go` | `--source-dsn` (req), `--index-dsn` (req), `--schemas`, `--format` |
| `index` | `index.go` | `--index-dsn` (req), `--source-dsn`, `--binlog-dir` (req), `--files`, `--all`, `--batch-size`, `--schemas`, `--tables`, `--format` |
| `query` | `query.go` | `--index-dsn` (req), `--schema`, `--table`, `--pk`, `--event-type`, `--gtid`, `--since`, `--until`, `--changed-column`, `--format` (table/json/csv), `--limit`, `--archive-dir`, `--archive-s3` |
| `recover` | `recover.go` | same filters as query + `--output`, `--dry-run`, `--limit` (default 1000), `--format` |
| `rotate` | `rotate.go` | `--index-dsn` (req), `--retain` (e.g. `7d`, `24h`), `--add-future`, `--archive-dir`, `--archive-compression` (default `zstd`), `--format` |
| `status` | `status.go` | `--index-dsn` (req), `--format` |
| `stream` | `stream.go` | … + `--metrics-addr` (e.g. `:9090`; empty = disabled), `--format` |
| `stream` | `stream.go` | `--index-dsn` (req), `--source-dsn` (req), `--server-id` (req), `--start-file`, `--start-pos`, `--start-gtid`, `--batch-size`, `--schemas`, `--tables`, `--checkpoint`, `--format` |
| `dump` | `dump.go` | `--source-dsn` (req), `--output-dir` (req), `--schemas`, `--tables`, `--mydumper-path` (default `mydumper`), `--threads` (default 4), `--format` |
| `baseline` | `baseline.go` | `--input` (req), `--output` (req), `--timestamp`, `--tables`, `--compression` (default `zstd`), `--row-group-size` (default 500000), `--upload` (S3 URL), `--upload-region`, `--format` |

Flag variable naming convention: prefixed by command abbreviation (e.g. `idxIndexDSN`, `qSchema`, `rDryRun`, `rotRetain`, `rotArchiveDir`, `rotArchiveCompression`, `stIndexDSN`, `strmIndexDSN`, `dmpSourceDSN`, `bslInput`).

Filter helpers (`ParseEventType`, `ParseTime`, `IsValidFormat`, `IsValidOutputFormat`) live in `internal/cliutil` and are shared by both `cmd/bintrail/` commands and `cmd/bintrail-mcp/`.

## MCP server

`cmd/bintrail-mcp/` exposes three read-only tools via two transport modes:

| Tool | Handler | Description |
|---|---|---|
| `query` | `queryTool` | Search binlog events (same filters as CLI `query`) |
| `recover` | `recoverTool` | Generate reversal SQL (dry-run only) |
| `status` | `statusTool` | Show indexed files, partitions, summary |

All tools are annotated with `ReadOnlyHint: true` and `IdempotentHint: true`. DSN resolution: `index_dsn` parameter overrides `BINTRAIL_INDEX_DSN` env var.

### Transport modes

| Mode | Command | When to use |
|---|---|---|
| **stdio** (default) | `go run ./cmd/bintrail-mcp` | Claude Code on the same machine — `.mcp.json` handles this automatically |
| **HTTP** | `bintrail-mcp --http :8080` | Serve over the network for Claude Desktop on other machines |

**stdio mode**: `.mcp.json` at the project root registers `go run ./cmd/bintrail-mcp` so Claude Code auto-starts it. No pre-build needed.

**HTTP mode**: Starts a persistent `net/http` server using `mcp.NewStreamableHTTPHandler` (MCP Streamable HTTP spec 2025-03-26), serving at `/mcp`. Each incoming connection gets a fresh `newServer()` instance; the SDK manages session state via `Mcp-Session-Id` response header. The server handles SIGINT/SIGTERM for graceful shutdown, draining in-flight requests before exiting.

```bash
BINTRAIL_INDEX_DSN='root:pass@tcp(127.0.0.1:3306)/binlog_index' bintrail-mcp --http :8080
```

**Important**: Always start `bintrail-mcp --http` with `BINTRAIL_INDEX_DSN` set in the environment so all tools work without callers passing `index_dsn` on every call.

### Remote access via proxy.py

`cmd/bintrail-mcp/proxy.py` is a self-contained Python 3.7+ script (zero dependencies — stdlib only, logging is inlined) that bridges Claude Desktop's MCP stdio protocol to the remote HTTP server:

```
Claude Desktop  →  proxy.py (stdin/stdout)  →  bintrail-mcp --http :8080  →  Index MySQL
```

**Setup on the remote machine** (the one running Claude Desktop):

1. Copy `proxy.py` to the remote machine (e.g. via `scp`).
2. Edit `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "bintrail": {
      "command": "python3",
      "args": ["/Users/you/proxy.py"],
      "env": { "BINTRAIL_SERVER": "http://192.168.1.37:8080/mcp" }
    }
  }
}
```

3. Restart Claude Desktop.

**Testing connectivity** from the remote machine before touching Claude Desktop config:

```bash
BINTRAIL_SERVER=http://192.168.1.37:8080/mcp python3 ~/proxy.py <<'EOF'
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}
{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}
EOF
```

You should get two JSON responses back. If you do, Claude Desktop will work.

**Stale session gotcha**: When `bintrail-mcp --http` is restarted, all sessions are invalidated but the proxy process (started by Claude Desktop) still holds the old `Mcp-Session-Id` in memory. Result: tool calls fail with validation errors. Fix: **restart Claude Desktop** — this kills and restarts the proxy process, clearing the stale session ID.

### Implementation notes

`newServer() *mcp.Server` constructs and returns the configured server (extracted from `main()` so tests can call it). `buildQueryOptions` is the shared filter builder used by both `queryTool` and `recoverTool`. Both are tested in `cmd/bintrail-mcp/main_test.go`.

**`jsonschema` tag format**: use plain description strings (`jsonschema:"My description"`) — NOT the old `key=value` format (`jsonschema:"description=..."`) which is rejected by jsonschema-go v0.3+.

Integration tests (`integration_test.go`) use `mcp.NewInMemoryTransports()` to connect a test client to the server in-process — no subprocess or stdio framing needed. The E2E test (`e2e_test.go`) builds the binary with `go build -cover` and speaks raw newline-delimited JSON-RPC over stdin/stdout (protocol version `"2025-06-18"`).

**proxy.py Python compatibility**: uses comment-style type annotations (`# type: str`) instead of `str | None` syntax — `str | None` requires Python 3.10+, but macOS ships Python 3.9 or older. Notifications (no `id` field) never get error responses — Claude Desktop rejects JSON-RPC errors with `null` id.

## Database tables

### `binlog_events` (range-partitioned)
- `pk_values` is a plain `VARCHAR(512)` — pipe-delimited PK values in ordinal order (e.g. `12345` or `12345|2`). NOT JSON.
- `pk_hash = SHA2(pk_values, 256)` is a **generated stored column** — never insert it explicitly.
- `row_before`, `row_after`, `changed_columns` are JSON columns.
- Partitioned by `RANGE (TO_SECONDS(event_timestamp))` — timezone-independent; `UNIX_TIMESTAMP()` is rejected by MySQL 8.0 when `time_zone=SYSTEM` (Error 1486).
- Hourly partitions named `p_YYYYMMDDHH` (12 chars, e.g. `p_2026022814`); catch-all is always `p_future VALUES LESS THAN MAXVALUE`.
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
- `partitionName(d time.Time) string` → `"p_YYYYMMDDHH"` (uses Go reference time `"p_2006010215"`)
- `partitionDate(name string) (time.Time, bool)` → parses `p_YYYYMMDDHH` (12 chars), returns `false` for `p_future` or malformed names
- These two round-trip correctly and are tested in `rotate_test.go`.
- To add partitions: `REORGANIZE PARTITION p_future INTO (... new partitions ..., PARTITION p_future VALUES LESS THAN MAXVALUE)`. Never leave out the new `p_future`.
- To drop partitions: `ALTER TABLE … DROP PARTITION p1, p2` — single statement for multiple partitions.
- `PARTITION_DESCRIPTION` in `information_schema.PARTITIONS` stores the evaluated integer TO_SECONDS value for named partitions and `MAXVALUE` for the catch-all. `DescriptionToHuman` in `status.go` converts it back via `time.Unix(secs-62167219200, 0)` (since `TO_SECONDS('1970-01-01 00:00:00') = 62167219200`).
- `TABLE_ROWS` in `information_schema.PARTITIONS` is an **estimate** for InnoDB — good enough for status display, not for exact counts.

### Getting DB name from DSN
Use `mysql.ParseDSN(dsn)` from `github.com/go-sql-driver/mysql` — consistent with `init.go`, `rotate.go`, `status.go`. Do not use `SELECT DATABASE()`.

### Dual MySQL imports in stream.go
`stream.go` needs both `github.com/go-mysql-org/go-mysql/mysql` (for `Position`, `GTIDSet`, `MysqlGTIDSet`) and `github.com/go-sql-driver/mysql` (for `ParseDSN`). They are aliased:
- `gomysql "github.com/go-mysql-org/go-mysql/mysql"` (used more heavily)
- `drivermysql "github.com/go-sql-driver/mysql"` (only for `drivermysql.ParseDSN`)

### parseTime=true and connection timeout
`config.Connect` always injects `parseTime=true` via `mysql.ParseDSN` → `cfg.ParseTime = true` → `cfg.FormatDSN()`. Without this, go-sql-driver returns DATETIME columns as `[]uint8` (raw bytes) instead of `time.Time`, causing scan errors. Do not use raw `sql.Open("mysql", dsn)` in commands that read DATETIME columns — always go through `config.Connect`.

`config.Connect` also injects a 10-second TCP connect timeout (`cfg.Timeout`) when the DSN does not specify one. This prevents indefinite hangs when MySQL is unreachable. Users can override this with the `timeout=` DSN parameter (e.g. `user:pass@tcp(host:3306)/db?timeout=30s`).

### dump command: lockfile and mydumper args

`dump.go` enforces single-concurrency via a lockfile at `os.TempDir()/bintrail-dump.lock`:
- `acquireDumpLock()` uses `O_CREATE|O_EXCL|O_WRONLY` for atomic creation and writes the current PID. On `ErrExist`, reads the PID, probes liveness with `syscall.Signal(0)`, removes stale locks, and retries once.
- `releaseDumpLock(f)` closes and removes the file.
- `var dumpLockDir = os.TempDir` stores the function (not its result) so tests can override it with a `t.TempDir()` closure.

`buildMydumperArgs` maps Go args to mydumper CLI flags:
- Always: `--host`, `--port`, `--user`, `--outputdir`, `--threads`, `--compress-protocol`, `--complete-insert`
- Single schema → `--database schema`; multiple schemas → `--regex ^(s1|s2)\.`
- Tables → `--tables-list t1,t2`

`extractSchemasFromTables(tables []string) []string` derives unique schema names from `db.table` entries (used when `--tables` is given without `--schemas`).

### baseline command: mydumper → Parquet pipeline

`baseline.go` (cmd) + `internal/baseline/` implement a file-only conversion requiring no DB connection:

**Parsing pipeline** (`internal/baseline/`):
- `ParseMetadata(inputDir)` reads the mydumper `metadata` file: `Started dump at:` → `DumpMetadata.StartedAt`, `\tLog:` → `BinlogFile`, `\tPos:` → `BinlogPos` (int64), `\tGTID:` → `GTIDSet`.
- `DiscoverTables(inputDir)` scans for `db.table-schema.sql` files and pairs them with data files (`*.sql` chunks → format `"sql"`, `*.dat` chunks → format `"tab"`). Returns `[]TableFiles` sorted alphabetically.
- `ParseSchema(schemaFile)` extracts column names and MySQL types from a mydumper `CREATE TABLE` statement. Stops at the first line matching `PRIMARY KEY`, `UNIQUE KEY`, `KEY`, or `)` to avoid false column matches.
- `filterTables` applies the `--tables` filter (case-insensitive `db.table` match).

**Parquet writer** (`writer.go`):
- Columns are written in **alphabetical order** (parquet.Group sorts fields alphabetically). `sortColumnsForParquet` builds the MySQL→Parquet index mapping `mysqlOrder[parquetIdx] = mysqlIdx` so rows are remapped correctly.
- `ValidateCodec(name)` validates the compression codec name: `"zstd"`, `""`, `"snappy"`, `"gzip"`, `"none"` are accepted; unknown codecs return an error. CLI layers (`baseline.go`, `rotate.go`) call this early for fast feedback; `NewWriter` also validates.
- `resolveCodec(name)` returns the codec: `"zstd"`/`""` → `&zstd.Codec{}`, `"snappy"` → `&snappy.Codec{}`, `"gzip"` → `&gzip.Codec{}`, `"none"` → nil (no compression).
- `convertValue(col, raw)` maps MySQL types to parquet.Value: integers → INT32/INT64, float/double → FLOAT/DOUBLE, datetime/timestamp → INT64 (microseconds since Unix epoch, UTC), date → INT32 (days since Unix epoch), all others (decimal, varchar, blob, etc.) → BYTE_ARRAY.
- Key-value metadata embedded: `bintrail.snapshot_timestamp`, `bintrail.source_database`, `bintrail.source_table`, `bintrail.mydumper_format`, `bintrail.bintrail_version`.

**Output structure**: `<output>/<timestamp>/<database>/<table>.parquet` where timestamp uses RFC3339 with colons replaced by dashes for filesystem compatibility (e.g. `2025-02-28T00-00-00Z`).

**`parseTableFilter(s string) []string`** in `baseline.go` (cmd) is the shared helper — `runBaseline` calls it instead of inlining the split loop.

Flag variable prefix: `bsl` (e.g. `bslInput`, `bslOutput`, `bslCompression`).

### baseline/archive package design

**`WriterConfig.Metadata map[string]string`** — `NewWriter` loops over this map to emit `parquet.KeyValueMetadata` entries. Callers (baseline's `processTable` and archive's `ArchivePartition`) supply the keys themselves.

**`baseline.MysqlToParquetNode(typeToken string) parquet.Node`** (exported) — maps a MySQL type token to a parquet-go node. Used by `ParseSchema` internally and by `internal/archive` to build the `binlogEventColumns` slice.

### archive command: partition archiving pipeline

`internal/archive/ArchivePartition(ctx, db, dbName, partition, outputPath, compression)` archives a single partition:
- Defines 14 `binlog_events` columns (all non-generated — `pk_hash` is skipped)
- SELECTs from `binlog_events PARTITION (<name>) ORDER BY event_id`
- Scans typed Go vars (`uint64`, `time.Time`, `sql.NullString`, `[]byte` for JSON)
- Converts to `[]string` + `[]bool` nulls, calls `baseline.NewWriter` / `WriteRow` / `Close`
- On error: removes partial file before returning

**Parquet metadata keys**: `bintrail.archive.partition`, `bintrail.archive.timestamp`, `bintrail.archive.version`.

**Output path convention**: `<archive-dir>/bintrail_id=<uuid>/event_date=<YYYY-MM-DD>/event_hour=<HH>/events.parquet` (set by `hiveArchivePath` in `rotate.go`; archive package is path-agnostic).

**`rotate --archive-dir`** triggers archiving before each `dropPartitions` call. If any archive fails, no partitions are dropped.

### internal/parquetquery: DuckDB-backed archive query

`internal/parquetquery` provides a `Fetch(ctx, opts, source)` function that queries Parquet archive files (written by `rotate --archive-dir`) using an in-process DuckDB instance.

- **`source`**: a local directory path or an S3 URL prefix (`s3://bucket/prefix/`)
- **`buildGlob(source)`**: converts source to a glob pattern (`source/*.parquet`); if source already ends in `.parquet`, returns as-is
- **`buildQuery(glob, opts)`**: constructs a DuckDB SQL query using `parquet_scan('...')`. The glob path is embedded directly in the SQL (DuckDB table functions don't support bind parameters for file paths); single quotes in the path are escaped as `''`.
  - PK lookup uses plain `pk_values = ?` — no SHA2 index available in Parquet
  - `changed_columns` filter uses DuckDB's `json_contains(changed_columns, ?)` with the JSON-encoded column name as the needle (e.g. `"status"`)
  - `Limit: 0` means no LIMIT clause (used during merge fetches)
- **S3 sources**: installs and loads the `httpfs` extension first (`INSTALL httpfs; LOAD httpfs;`)
- Returns `[]query.ResultRow` — same type as `internal/query`, so results can be merged with live index results

### query command: archive merge

When `--archive-dir` or `--archive-s3` is given, `runQuery` fetches from both the live MySQL index and each archive source, then merges:
- **`archiveSources()`**: returns non-empty archive source flags as a slice
- **`mergeResults(rows, limit)`**: deduplicates by `event_id` (MySQL rows first → index version kept on collision), sorts by `(event_timestamp, event_id)`, applies `limit`
- During fetch, `opts.Limit` is set to 0 so older archive events aren't truncated before the merge sort; the user's `--limit` is applied once after sorting
- Archive query failures are logged as warnings and skipped (non-fatal), so a bad S3 connection doesn't block live results

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

### Coverage baseline (as of test coverage session / 2026-02-28)

Full suite (`go test -tags integration -coverprofile=cover.out ./... -count=1`):

| Package | Coverage |
|---|---|
| `internal/cliutil` | 100% |
| `internal/observe` | 100% |
| `internal/recovery` | 92% |
| `internal/config` | 92% |
| `internal/query` | 91% |
| `internal/indexer` | 86% |
| `internal/metadata` | 85% |
| `internal/baseline` | 84% |
| `internal/parser` | 82% |
| `cmd/bintrail-mcp` | 79% |
| `internal/status` | 68% |
| `cmd/bintrail` | 53% |
| `internal/parquetquery` | ~80% (unit tests cover buildGlob, buildQuery; Fetch/scanRows require DuckDB) |
| `internal/archive` | 0% |
| **total** | **68%** |

**Known gaps and why:**
- `cmd/bintrail` `run*` handlers (53%): cobra entry points are only exercised by the root `e2e_test.go` subprocess test, whose coverage lands in `GOCOVERDIR` (not `cover.out`). `runStream`, `runInit`, `runSnapshot`, `runStatus` are included in this gap. Validation logic in `runQuery`/`runRecover`/`runRotate` is covered by unit tests in `query_test.go`, `recover_test.go`, `rotate_test.go`. The `mergeResults`/`archiveSources` helpers in `query.go` and the `--archive-dir`/`--archive-s3` flags are not yet unit-tested.
- `internal/status` `LoadIndexState`/`LoadPartitionStats` (0% in cover.out): called through the MCP/CLI handlers which run as subprocesses; `WriteStatus`/`DescriptionToHuman` are 100%.
- `cmd/bintrail-mcp` (79%): `main()` stdio entry point is intentionally excluded — exercised by `TestMCPE2E` whose coverage lands in `GOCOVERDIR`.
- `internal/parquetquery` (~80%): `buildGlob` and `buildQuery` are fully unit-tested; `Fetch` and `scanRows` require a DuckDB connection with actual Parquet files — not yet integration-tested.
- `internal/archive` (0% in cover.out): `ArchivePartition` requires a live DB; it is exercised by `TestArchivePartition` in `cmd/bintrail/cmd_integration_test.go` (package main), so coverage lands in `cmd/bintrail`'s profile, not `internal/archive`. Unit tests in `archive_test.go` cover column definitions and write/read round-trips via `baseline.NewWriter` directly.

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
| `github.com/duckdb/duckdb-go/v2` | In-process DuckDB engine for querying Parquet archives (`internal/parquetquery`) |

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

## Build and release

### Version injection

Both binaries have build-time variables injected via `-ldflags -X main.VarName=value`:

| Binary | Variables | Source |
|---|---|---|
| `bintrail` | `Version`, `CommitSHA`, `BuildDate` | `cmd/bintrail/main.go` |
| `bintrail-mcp` | `mcpVersion` | `cmd/bintrail-mcp/main.go` |

For `package main`, ldflags uses `main.VarName` (not the full import path). The Makefile reads the version from `git describe --tags`.

`rootCmd.Version` is set in `init()` using `fmt.Sprintf` to compose all three values:
```
bintrail version v0.1.0 (commit abc1234, built 2026-03-01T00:00:00Z)
```

### Makefile

`make build` builds both binaries with version injection. `make build-all` cross-compiles for linux/darwin × amd64/arm64. CGO_ENABLED=1 is required (DuckDB dependency). linux/arm64 needs `aarch64-linux-gnu-gcc`; darwin targets must be built on macOS.

### GoReleaser

`.goreleaser.yaml` defines two builds (`bintrail` + `bintrail-mcp`) for linux/amd64 and linux/arm64 only (macOS excluded — CGO cross-compilation to darwin is not supported on Linux CI runners). The arm64 override sets `CC=aarch64-linux-gnu-gcc` and `CXX=aarch64-linux-gnu-g++`. Both `g++` and `gcc` cross-compiler packages are required because DuckDB's pre-compiled static libs link against `libstdc++`.

### Release workflow

`.github/workflows/release.yaml` triggers on `v*` tags. Single `ubuntu-latest` job:
1. Install `gcc-aarch64-linux-gnu` + `g++-aarch64-linux-gnu`
2. Run `go test ./...`
3. Run GoReleaser → creates GitHub Release with linux archives + checksums

### Release process

Use the `/release` skill or follow manually:
1. Ensure `CHANGELOG.md` has entries under `[Unreleased]`
2. Rename `[Unreleased]` to `[X.Y.Z] - YYYY-MM-DD`, add new empty `[Unreleased]`
3. Run `go vet ./...` and `go test ./...`
4. Commit changelog, create annotated tag `v X.Y.Z`, push both
5. GitHub Actions runs GoReleaser automatically

## Common gotchas

- **`go mod tidy` removes go-mysql**: if nothing in the codebase imports it yet, tidy will drop it. Re-add with `go get github.com/go-mysql-org/go-mysql@v1.13.0`.
- **`binlog_row_image=FULL` required**: the `index` command validates this via `SHOW VARIABLES LIKE 'binlog_row_image'` and refuses to proceed if not FULL.
- **DDL changes break indexing**: if the source schema changes (ALTER TABLE etc.), the snapshot must be re-taken. The parser warns on DDL detection but does not automatically re-snapshot.
- **Column count mismatch**: when a TABLE_MAP_EVENT's column count differs from the snapshot, the indexer logs a warning and skips that table's events — it does not fail.
- **`p_future` must always exist**: never drop it. `REORGANIZE PARTITION` always recreates it at the end.
- **`schema_snapshots` snapshot_id ≠ row id**: a common source of confusion. `snapshot_id` groups all rows of one snapshot; `id` is the auto-increment row primary key.
- **go-mysql lowercases GTID UUIDs**: `MysqlGTIDSet.String()` always returns lowercase UUIDs (e.g. `3e11fa47-...`). Tests comparing GTID strings must use lowercase or `strings.ToLower` — never uppercase UUID literals.
