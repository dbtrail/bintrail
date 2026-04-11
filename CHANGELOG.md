# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `bintrail reconstruct --output-format mydumper` reconstructs entire tables at a target point in time and emits a mydumper-compatible dump directory (schema file, chunked INSERT files, and a `metadata` file with baseline binlog position). The output is restorable with plain `mysql < *.sql` or `myloader` with no further binlog replay needed — closes the gap between single-row investigation and full point-in-time recovery. The algorithm is merge-on-read: stream the baseline Parquet row-by-row via DuckDB, look up each row's PK in an in-memory change map built from the merged MySQL + archive event set, and emit SQL directly. No MySQL restore cycle, no InnoDB buffer warmup. Requires baselines written by this version (embeds the raw `CREATE TABLE` text as a new `bintrail.create_table_sql` Parquet metadata key); older baselines are rejected with a clear re-baseline message. New flags: `--output-format mydumper`, `--output-dir`, `--tables schema.table,...`, `--chunk-size` (default `256MB`), `--parallelism` (default `runtime.NumCPU()`). Strict `--allow-gaps=false` semantics inherited from single-row reconstruct — a coverage gap between baseline and target aborts unless the user explicitly accepts incomplete data (#187).
- `bintrail baseline` now embeds the raw mydumper `<db>.<table>-schema.sql` contents in the Parquet file as `bintrail.create_table_sql`. Full-table reconstruct (#187) reads this back to emit a faithful `CREATE TABLE` in its schema file without re-synthesising from Parquet column types (which would lose indexes, foreign keys, charsets, engine, etc.).
- `recovery.FormatSQLValue`, `recovery.EscapeString`, `recovery.QuoteName` exported from `internal/recovery` so the full-table mydumper writer reuses the exact MySQL literal formatting used by reversal SQL. `FormatSQLValue` extended to handle `int64`/`int32`/`int`/`uint64`/`uint32`/`float32`/`time.Time`/`[]byte` (DuckDB scan types) in addition to the JSON-round-tripped types it already handled.
- `baseline.ReadParquetMetadataAny` reads Parquet metadata from either local paths or `s3://` URLs (using DuckDB's `parquet_kv_metadata` via the `httpfs` extension), so full-table reconstruct works against S3-resident baselines without adding a direct AWS SDK dependency.

### Fixed
- `bintrail reconstruct --pk ...` now fetches events from both live MySQL partitions **and** Parquet archives, closing a latent correctness bug where single-row reconstruction silently missed events that had been rotated out of MySQL and archived. The previous code called `engine.Fetch` directly — bypassing the query planner, archive auto-discovery, and `MergeResults` pipeline already used by `bintrail recover`. Factored out a shared `query.FetchMerged` helper so `recover` and `reconstruct` share one pipeline, and the upcoming full-table reconstruct (#187) can reuse it. New `--no-archive` and `--allow-gaps` flags on `reconstruct` mirror `recover`'s surface area; `--allow-gaps` defaults to `false` because a silently incomplete row state is worse than a clear error for a recovery tool. Strict mode (`AllowGaps=false`) now aborts when the query planner fails, when no DBName is available for gap detection, or when every archive source fails — previously these conditions silently degraded to partial data. The query planner now runs regardless of `--no-archive` so users retain gap visibility even when opting out of archive queries (#209).

## [0.4.9] - 2026-04-10

### Added
- BYOS buffer now supports size-based eviction via `--buffer-max-events` and `--buffer-max-bytes` flags (e.g. `--buffer-max-bytes 256MB`). Previously the in-memory buffer only evicted by age (`--buffer-retain`), so a write burst within the retention window could grow RAM unbounded. When a cap is exceeded, the oldest events are evicted FIFO with a `slog.Warn` for operator visibility. New heartbeat fields `buffer_bytes` and `size_evictions` report buffer pressure to dbtrail. Both caps default to 0 (unlimited) for backward compatibility (#194).

## [0.4.8] - 2026-04-10

### Fixed
- Archive queries against pre-v0.4.4 parquet files no longer silently return 0 events. Older parquets lack the `connection_id` column added in 0.4.4; DuckDB threw a `Binder Error` when the per-file query SELECTed that column from a single file (where `union_by_name=true` is a no-op). The error was swallowed by the caller and the query returned empty. Fix: probe the parquet schema before building the SELECT and substitute `NULL::INT32 AS connection_id` when the column is absent. Applied to both the S3 per-file download path and the local glob path (#203).

## [0.4.7] - 2026-04-09

### Added
- `bintrail agent` now exits with distinct process codes when the dbtrail backend rejects the WebSocket with a permanent close: **64** for auth/config failures (`missing_credentials`, `invalid_key`, `wrong_tenant_mode`) and **65** for `rate_limited`. systemd units should add `RestartPreventExitStatus=64 65` to stop respawning on permanent failures — previously every fatal close exited 1 and the supervisor kept respawning a doomed agent. Unknown reason strings on a fatal close code fall through as a transient exit (safe to respawn) so backend contract drift never silently pins the agent into a fatal loop. The permanent-error log line now includes `close_code` / `close_reason` structured fields for grep/alerting. Recognizes both canonical short forms (`invalid_key`) and legacy human strings (`Invalid API key`) for back-compat with older dbtrail versions (#201, #202).

## [0.4.6] - 2026-04-08

### Fixed
- `bintrail rotate --add-future N` is now declarative: it maintains *at least* N future hourly partitions beyond the current hour (top-up only) instead of adding N new partitions per invocation. In daemon mode the old behaviour leaked `+N` partitions per cycle — a demo tenant running `--add-future=1 --interval=1h` accumulated 413 partitions over 17 days and blew up `SELECT DISTINCT` on `binlog_events` from sub-second to 23s. Semantics now match the documentation in `docs/deployment.md` (#199).

## [0.4.5] - 2026-04-07

### Removed
- BYOS agent no longer requires `--index-dsn` when `--s3-bucket` is configured (#197). The requirement was added in 0.4.1 to guarantee stable S3 partition keys via a locally-persisted `bintrail_id`, but it forced customers to provision a dedicated MySQL (a footgun the customer-facing setup docs never mentioned). With the source identity propagation shipped in 0.4.4 (#195), the dbtrail SaaS side now resolves a stable `bintrail_id` server-side from the `@@server_uuid` + host/port/user fields carried on every metadata record (architecture §22.11, nethalo/dbtrail#1179). The local customer agent falls back to `--server-id` for the S3 partition key and WebSocket heartbeat label, which are customer-local identifiers intentionally decoupled from the SaaS-resolved `bintrail_id`.

## [0.4.4] - 2026-04-07

### Added
- BYOS agent now propagates source server identity (`server_uuid`, `source_host`, `source_port`, `source_user`) on every metadata record sent to dbtrail — lets the SaaS side register the source server and resolve a stable `bintrail_id`, closing the identity-model gap between hosted and BYOS modes (#195)

### Changed
- BYOS agent fails loud at startup if source identity capture (`@@server_uuid` query or `--source-dsn` parse) fails, instead of silently emitting metadata with an empty `server_uuid` for the process lifetime (#195)

## [0.4.3] - 2026-04-06

### Added
- `bintrail stream --gap-timeout` flag (default 30s) configures the timeout for gap-detection queries (`SHOW BINARY LOGS`, `@@gtid_purged`, `@@gtid_executed`); raise this on managed MySQL instances with many binlog files where the default 10s was too tight (#190)
- `bintrail agent --max-reconnect-attempts` flag (default 10) bounds the WebSocket reconnect loop so the agent exits non-zero after consecutive failures, letting a process supervisor (e.g. systemd `Restart=on-failure`) respawn it (#191)

### Changed
- `bintrail stream` gap-detection query timeout default raised from 10s to 30s — the query only runs once per resume so a higher ceiling has no ongoing cost (#190)

### Fixed
- `bintrail agent` no longer stays "active" in systemd when its WebSocket connection dies but cannot reconnect — the new retry budget surfaces the failure as a process exit so systemd can respawn the agent and the dashboard sees a fresh, healthy connection (#191)

## [0.4.2] - 2026-04-05

### Fixed
- Release binaries now target glibc 2.17 (via Zig linker) — fixes `GLIBC_2.38 not found` on Amazon Linux 2023, RHEL 9, and other distros with glibc < 2.39

## [0.4.1] - 2026-04-03

### Fixed
- BYOS MetadataClient now sends `bintrail_id` (stable UUID) instead of numeric `@@server_id` in metadata records and WebSocket heartbeats — prevents misidentification when multiple MySQL servers share the default `server_id = 1`
- BYOS+S3 mode now requires `--index-dsn` to ensure stable `bintrail_id` resolution for S3 partitioning — prevents orphaned partitions that cannot be correlated with future runs

## [0.4.0] - 2026-04-01

### Added
- `bintrail agent` command — opens an outbound WebSocket to dbtrail for remote query, recovery, and forensics commands; no inbound ports required
- BYOS (Bring Your Own Storage) mode — parsed binlog events are split into metadata (sent to dbtrail API, zero row data) and payload (written as Parquet to customer S3, never leaves customer infrastructure)
- Abstract storage backend interface (`internal/storage`) with S3 implementation for BYOS payload writes
- In-memory event buffer for BYOS mode — keeps recent events in memory for fast local access while S3 remains authoritative
- BYOS flush pipeline with configurable interval (`--flush-interval`, default 5s) and retry with exponential backoff; flush health reported in agent heartbeat
- Agent pre-flight validation (`--validate` flag) — checks MySQL connectivity, replication privileges, S3 access, schema snapshot, dbtrail API auth, and WebSocket channel in one pass
- `connection_id` column in `binlog_events` — captures MySQL `pseudo_thread_id` from binlog `QueryEvent.SlaveProxyID`; included in all query output formats, archives, buffer, and BYOS metadata
- Auto-migration via `indexer.EnsureSchema()` for `connection_id` column on existing installations (instant DDL in MySQL 8.0+)
- systemd service unit (`deploy/bintrail-agent.service`) for bare metal agent installs

### Changed
- Module path moved from `github.com/nethalo/bintrail` to `github.com/dbtrail/bintrail`
- License changed from Apache 2.0 to Business Source License 1.1

## [0.3.2] - 2026-03-15

### Added
- Binlog gap detection on `bintrail stream` restart — position mode checks `SHOW BINARY LOGS`, GTID mode compares checkpoint against `@@gtid_purged` and `@@gtid_executed`
- Automatic gap filling for fillable gaps; unfillable gaps auto-advance to the earliest available position with a warning logged and checkpoint updated to prevent crash loops
- `--no-gap-fill` flag for `bintrail stream` to refuse starting when a gap is detected

## [0.3.1] - 2026-03-13

### Added
- Capture foreign key constraints in schema snapshots — `bintrail snapshot` now queries `INFORMATION_SCHEMA.KEY_COLUMN_USAGE` joined with `REFERENTIAL_CONSTRAINTS` and stores FK relationships in a new `fk_constraints` table, using the same `snapshot_id` as `schema_snapshots`
- New `fk_constraints` table created by `bintrail init` — no additional MySQL grants required
- Graceful upgrade path: existing installations that upgrade without re-running `bintrail init` get a warning instead of a failed snapshot

## [0.3.0] - 2026-03-12

### Added
- `list_schema_changes` MCP tool — queries the `schema_changes` table with filters (`schema`, `table`, `ddl_type`, `since`, `until`, `limit`), making DDL audit data queryable via MCP
- TRUNCATE DDL support — `parseDDL()` now detects `TRUNCATE [TABLE]` statements and records them in `schema_changes`
- Composite index `idx_schema_table` on `schema_changes` for efficient per-table lookups

### Changed
- Rotate now archives and drops one partition at a time instead of archiving all partitions first and then dropping in a single bulk `ALTER TABLE` — reduces disk space pressure during large rotations and ensures each partition is freed immediately after archiving

## [0.2.16] - 2026-03-10

### Added
- Truncation warning when query or recover results hit the limit — CLI prints to stderr, MCP tools append to the response text so the LLM knows to narrow the time range or increase the limit

## [0.2.15] - 2026-03-09

### Changed
- Bump DuckDB threads from 1 to 2 — archive Parquet files have 3-4 row groups (500K rows each) and DuckDB can only parallelize across row groups; with 6GB container memory there is enough headroom for two threads (125MB each)

## [0.2.14] - 2026-03-09

### Changed
- Raise DuckDB memory limit from 1GB to 4GB — 190MB compressed Parquet files decompress to well over 1GB; 2GB containers still OOM-killed during scans

### Fixed
- Apply ORDER BY + LIMIT per archive file instead of scanning all rows — DuckDB's top-N optimization keeps only the LIMIT rows in memory during sort, then MergeResults merge-sorts the per-file results for the correct global top-K. Previously a single hour with 1.75M events was fully materialized in Go before applying the limit, causing both excessive memory usage and 2+ minute query times

## [0.2.13] - 2026-03-09

### Changed
- Raise DuckDB memory limit from 1GB to 4GB — 190MB compressed Parquet files decompress to well over 1GB; 2GB containers still OOM-killed during scans

## [0.2.12] - 2026-03-09

### Changed
- Raise DuckDB memory limit from 256MB to 1GB for 2GB container environments — 190MB compressed Parquet files need more than 256MB to decompress and scan; the previous limit caused OOM kills even with single-file sequential processing

## [0.2.11] - 2026-03-09

### Fixed
- Remove ORDER BY from per-file S3 archive queries — forces DuckDB to buffer the entire result set for sorting, spiking memory. Sorting now happens in Go via `MergeResults` after all files are collected, letting DuckDB stream rows with minimal memory

## [0.2.10] - 2026-03-09

### Changed
- Download S3 Parquet files to local temp via AWS SDK before querying with DuckDB — eliminates httpfs extension which held entire S3 files in memory (outside `memory_limit` tracking), causing OOM kills even with conservative limits. Local reads use OS page cache (mmap), keeping memory usage predictable and low

## [0.2.9] - 2026-03-09

### Fixed
- Query S3 Parquet files one at a time instead of all at once — each file can require hundreds of MB when decompressed; sequential processing lets DuckDB release memory between files, fitting comfortably in container memory limits
- Reduce DuckDB to 1 thread (125MB baseline per thread per DuckDB docs) to maximize memory available for data

## [0.2.8] - 2026-03-09

### Fixed
- Pre-filter S3 archive files by Hive partition time range (`event_date`/`event_hour`) before passing to DuckDB — a 1-hour query now reads 1-2 files instead of all 10, dramatically reducing memory usage and S3 transfer
- Bump DuckDB `memory_limit` to 512MB (was 256MB) as a safety net for larger archive scans

## [0.2.7] - 2026-03-09

### Fixed
- Tune DuckDB for container environments — limit to 2 threads and disable `preserve_insertion_order` to reduce peak memory when querying S3 Parquet archives; prevents OOM kills (`exit=137`) and DuckDB out-of-memory errors on memory-constrained containers

## [0.2.6] - 2026-03-09

### Fixed
- Cap DuckDB memory at 256MB to prevent OOM kills in memory-constrained containers — DuckDB defaults to 80% of system RAM, which exhausts memory when reading S3 Parquet files; with the limit it spills to disk instead

## [0.2.5] - 2026-03-09

### Fixed
- Auto-detect S3 bucket region via `GetBucketLocation` — when `AWS_DEFAULT_REGION` differs from the bucket's actual region, `ListObjectsV2` and DuckDB `parquet_scan` both failed with 301 PermanentRedirect
- Set DuckDB `temp_directory` to OS temp dir — DuckDB creates a `.tmp` scratch directory in the CWD, which fails in containers where the working directory is read-only

## [0.2.4] - 2026-03-09

### Fixed
- Bypass DuckDB S3 glob expansion entirely — use AWS SDK `ListObjectsV2` to enumerate `.parquet` files, then pass explicit paths to `parquet_scan()`. DuckDB's glob fails on S3 paths containing `=` signs (Hive partition keys like `event_date=2026-03-09/`), silently returning zero results even with valid credentials and correct single-level globs

## [0.2.3] - 2026-03-09

### Fixed
- Load DuckDB `aws` extension for S3 credential resolution — without it, DuckDB attempts anonymous S3 access which silently returns zero results instead of using `AWS_ACCESS_KEY_ID` / `AWS_SESSION_TOKEN` from the environment

## [0.2.2] - 2026-03-09

### Fixed
- Use explicit single-level S3 globs (`/*/*/*.parquet`) instead of unsupported `**` recursive glob — DuckDB's httpfs extension does not support recursive globs on S3, causing "No files found" errors on archive queries

## [0.2.1] - 2026-03-09

### Fixed
- Enable `hive_partitioning` in `parquet_scan` for S3 archive queries — DuckDB's glob resolution failed on S3 paths containing `=` signs (Hive-partitioned directories like `event_date=2026-03-09/`)

## [0.2.0] - 2026-03-06

### Added
- MCP gateway with OAuth 2.1 for Claude Connector support
- Tenant provisioning admin API and backend health monitoring for MCP gateway
- Rate limiting and request logging for MCP gateway
- Auto-snapshot on DDL detection with restore coverage tracking
- `--reset` flag for `bintrail stream` to force new start position
- Seamless mode switching between position and GTID in `bintrail stream`
- Idempotent stream startup by preferring saved checkpoint over flags
- S3 upload retry with `--retry` flag for `baseline` and `rotate` commands
- Standalone `bintrail upload` command for S3 uploads
- At-rest encryption for mydumper dump files (`dump --encrypt`)
- Docker support: Dockerfile, docker-compose template, and mydumper Docker fallback
- `event_hour` Hive partition level in archive path
- `archive_state` table to track archived Parquet files
- Archive and S3 stats in `bintrail status` output
- `--format json` for all commands
- `--sync-thread-lock-mode` and `--trx-tables` flags for mydumper
- Stream state and `bintrail-id` in status output
- `/health` endpoint for `bintrail-mcp` HTTP server
- Debug logging for status command

### Fixed
- Always emit `TO_SECONDS` partition pruning hints for `since`/`until` queries
- Include `archive_state` data in status restore coverage
- Add missing Archives section to MCP status tool
- Emit GTID tracking events to prevent gaps in accumulated GTID set
- Place `--outputdir` last in mydumper args for Docker wrapper compatibility
- Skip shell script wrappers when resolving mydumper on PATH
- Remove `Truncate(time.Hour)` from rotate cutoff so hourly partitions drop correctly
- Parse mydumper 0.16+ metadata format
- Replace `UTC_TIMESTAMP()` with `CURRENT_TIMESTAMP` in `archive_state` DDL
- Show S3 upload progress in rotate output
- Create partitions from current hour forward
- Suppress usage output on command errors
- Normalize shortened UUIDs in GTID sets from RDS
- Allow gateway's own issuer origin in origin middleware

## [0.1.1] - 2026-03-01

### Fixed
- Nil dereference in `parser.New()` when resolver is nil (crash risk)
- MCP HTTP server now shuts down gracefully on SIGINT/SIGTERM
- `proxy.py` SSE stream parsing crash on non-UTF-8 bytes
- Unchecked `os.MkdirAll` error in E2E test

### Changed
- Unknown compression codecs now return an error instead of silently falling back to no compression; new `ValidateCodec()` function validates early in CLI layers
- `config.Connect()` injects a 10-second default TCP connect timeout when the DSN does not specify one
- `proxy.py` is now fully self-contained (inlined `log.py`); `log.py` removed
- Partial file cleanup errors in baseline are now logged

## [0.1.0] - 2026-03-01

### Added
- MySQL ROW-format binlog parser and indexer (`bintrail index`)
- Live replication indexing (`bintrail stream`) with Prometheus metrics
- Query engine with table/json/csv output (`bintrail query`)
- Reversal SQL generator (`bintrail recover`)
- Schema snapshot management (`bintrail snapshot`)
- Hourly partition rotation with retention policy (`bintrail rotate`)
- Partition archiving to Parquet (`rotate --archive-dir`, `--archive-s3`)
- DuckDB-backed Parquet archive querying (`query --archive-dir`, `--archive-s3`)
- mydumper integration (`bintrail dump`) and Parquet baseline converter (`bintrail baseline`)
- MCP server with stdio and HTTP transport modes (`bintrail-mcp`)
- RBAC foundation with table flags, profiles, and access rules (`bintrail flag`, `bintrail profile`, `bintrail access`)
- TLS/SSL support for stream mode
- Server identity system (`--bintrail-id`)
- GoReleaser-based release process with version injection
