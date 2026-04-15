# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.8] - 2026-04-15

### Added
- `bintrail query` accepts `--include-snapshot` + `--baseline <path-or-s3-url>` to merge a mydumper baseline Parquet as a third source alongside the live MySQL index and S3 archive. Baseline rows are emitted as synthetic `SNAPSHOT` events (new `parser.EventSnapshot = 6`) with the baseline's `snapshot_timestamp` metadata as their `event_timestamp`, so they flow through the existing `MergeAndTrim` pipeline and slot into sorted output before any subsequent binlog event for the same PK. Filter semantics: `--column-eq` hits the typed Parquet column via DuckDB `CAST(… AS VARCHAR) = ?` (index-friendly equality, no `JSON_EXTRACT`); `--event-type ≠ SNAPSHOT`, `--gtid`, `--changed-column`, `--flag` all exclude the snapshot source with a visible `slog.Info` reason; `--since`/`--until` compare against the baseline's recorded creation timestamp. `--include-snapshot` rejects combinations that would silently produce wrong data (`--pk`/`--pks`: snapshot rows have no `pk_values` in this release; `--profile`: RBAC rules are not applied to snapshot rows) and requires `--baseline` + `--schema` + `--table`. `--event-type SNAPSHOT` without `--include-snapshot` is rejected (previously silently returned zero rows). Unblocks dbtrail SaaS Phase 2 FK-aware cascade victim reconstruction for rows that existed before streaming began (#234).

## [0.5.7] - 2026-04-15

### Added
- `bintrail query` and `bintrail recover` now accept `--column-eq column=value`, a repeatable filter that matches events where a column inside `row_after` **or** `row_before` equals the given value. The OR across both sides covers DELETEs (value in `row_before`) and INSERTs (value in `row_after`) symmetrically. Repeating the flag composes AND. A column-name allowlist (`[A-Za-z0-9_]`) keeps the interpolated JSON path safe. The literal unquoted `NULL` sentinel matches rows where the column is explicitly JSON null (via `JSON_TYPE = 'NULL'`). The same filter is mirrored into the DuckDB archive path so merged live + archive queries stay consistent. MCP `query` and `recover` tools accept a matching `column_eq: [string]` parameter (#229).
- `bintrail query` and `bintrail recover` now accept `--pks` (comma-separated or repeatable) and `--limit-per-pk N` for batched multi-PK lookups. On the archive path this collapses N sequential DuckDB scans into one pass with `WHERE pk_values IN (...)` and `QUALIFY ROW_NUMBER() OVER (PARTITION BY pk_values ORDER BY event_timestamp DESC, event_id DESC) <= N`; on the MySQL side a ROW_NUMBER subquery enforces the per-PK cap server-side. The primary workload is dbtrail SaaS's "find latest DELETE per PK" auto-detection pass, which now completes in a single invocation instead of N shell-outs re-opening MySQL and re-scanning the same parquet files each time. `bintrail query --pks=a,b,c --format=json` emits a grouped shape `{"results": [{"pk": "X", "events": [...]}, ...]}` preserving input order, with empty groups for PKs that had no matches so callers can correlate inputs to outputs without a second lookup. `--pk` keeps its existing SHA2-indexed fast path. `--pk` and `--pks` are mutually exclusive; `--limit-per-pk` requires one of them; empty or whitespace-only PKs in `--pks` are rejected with a clear error, and duplicates are deduplicated with first-occurrence-wins ordering. Cross-source merge applies the per-PK cap before the global `--limit` trim via a new shared `query.MergeAndTrim` helper, so a large `--limit-per-pk` cannot starve later PKs under the ASC-sorted global cutoff (#231).

## [0.5.6] - 2026-04-14

### Fixed
- BYOS agent now detects two classes of silent source-identity misattribution. (1) The source `@@server_uuid` is re-read every 60 seconds and compared against the UUID captured at startup; if the source MySQL restarts with a regenerated `auto.cnf`, fails over behind a VIP, or resolves to a different instance, the agent exits with an actionable error instead of continuing to stamp the stale UUID on every `MetadataRecord`. Transient DB errors on the identity tick are tolerated with a warning. (2) `byos.EnsurePartitionKey` writes a `.bintrail-partition-key` marker to the customer S3 prefix on first run and validates it on every subsequent run; a mismatch (e.g. upgrading across the `--index-dsn` UUID-vs-numeric partition-key cutover) hard-fails with a message explaining the cutover and the operator's remediation options. An additional startup warning fires in the BYOS+S3-without-index-dsn path so the chosen partition key appears in the banner (#196, #198, #228).

### Changed
- S3 archive downloads for `query`/`recover` are now a single-pass prefetch pipeline instead of a per-batch download-then-query loop. Up to 2 files are prefetched in parallel while DuckDB queries the current one; queries remain strictly sequential (one DuckDB query at a time), so peak per-query RAM is unchanged, and peak temp files on disk drops from 4 to 3. Early termination is now checked after every file rather than at batch boundaries, so wide time-range queries with `--limit` stop as soon as the collected results cannot be displaced by any later file. S3 download errors that race with consumer cancellation are now logged at Debug for context errors and Warn for real failures (403, DNS, throttling) so production problems surface (#225, #227).

## [0.5.5] - 2026-04-13

### Fixed
- `bintrail baseline` now emits valid 0-row Parquet files for empty tables (tables with a schema DDL but no data rows in the mydumper dump). Previously, empty tables were silently skipped, causing `bintrail reconstruct` to fail with "no baseline snapshot found" for any table that was legitimately empty at dump time. Views are still correctly skipped via a `CREATE TABLE` vs `CREATE VIEW` heuristic on the schema SQL file (#226).
- `bintrail reconstruct` now handles missing baselines gracefully as defense-in-depth: when no baseline Parquet exists for a table, it emits a warning and treats the table as empty instead of aborting the entire reconstruct run. This covers pre-fix baselines and tables created after the last baseline snapshot (#226).

## [0.5.4] - 2026-04-13

### Changed
- S3 archive queries (`query --archive-s3`, `recover --archive-s3`) now use prefix-scoped S3 listing, batched concurrent downloads (4 at a time), chronological file ordering, and early termination when `--limit` is satisfied. A 24-hour query that previously took 18s+ now completes in under 5s; queries with small limits terminate after downloading only the first few files. The SaaS backend can remove its 144-chunk splitting logic entirely (#225).

## [0.5.3] - 2026-04-13

### Fixed
- `bintrail reconstruct` S3 baseline discovery now works with DuckDB v1.4.4, which changed `glob()` from a scalar function to a table function. The previous `SELECT unnest(glob('s3://...'))` syntax fails with a Binder Error; replaced with `SELECT * FROM glob('s3://...')` which works across DuckDB versions (#223).

## [0.5.2] - 2026-04-12

### Fixed
- `bintrail baseline` now recognizes mydumper 0.10.0's unchunked data-file naming (`<db>.<table>.sql`) in addition to the chunked format (`<db>.<table>.<chunk>.sql`) used by mydumper >= 0.11.0. Ubuntu 24.04's apt package ships mydumper 0.10.0 which produces single data files without a numeric chunk suffix; `DiscoverTables` previously required the chunk number and silently skipped these files, producing zero Parquet output. Together with #219 (mydumper version probing in `bintrail dump`), this unblocks the entire `dump → baseline → reconstruct --output-format mydumper` pipeline on Ubuntu 24.04 (#221).

## [0.5.1] - 2026-04-12

### Fixed
- `bintrail dump` no longer passes `--sync-thread-lock-mode` and `--trx-tables` to mydumper versions older than 0.11.0. These flags were introduced in mydumper 0.11.0 but were hardcoded in `buildMydumperArgs`, so `bintrail dump` failed immediately on Ubuntu 24.04's apt-installed mydumper 0.10.0 with `Unknown option --sync-thread-lock-mode`. The fix probes the local mydumper binary's version via `mydumper --version` and conditionally includes the flags only when the version is >= 0.11.0. Docker-mode invocations (`--mydumper-image`) always include the flags since the official Docker image ships a recent version. When the version cannot be determined, the flags are omitted conservatively with a `slog.Warn`. This unblocks the entire `dump → baseline → reconstruct --output-format mydumper` pipeline on Ubuntu 24.04 (#219).

## [0.5.0] - 2026-04-11

### Added
- `bintrail reconstruct --output-format mydumper` reconstructs entire tables at a target point in time and emits a mydumper-compatible dump directory (schema file, chunked INSERT files, and a `metadata` file with baseline binlog position). The output is restorable with plain `mysql < *.sql` or `myloader` with no further binlog replay needed — closes the gap between single-row investigation and full point-in-time recovery. The algorithm is merge-on-read: stream the baseline Parquet row-by-row via DuckDB, look up each row's PK in an in-memory change map built from the merged MySQL + archive event set, and emit SQL directly. No MySQL restore cycle, no InnoDB buffer warmup. Requires baselines written by this version (embeds the raw `CREATE TABLE` text as a new `bintrail.create_table_sql` Parquet metadata key); older baselines are rejected with a clear re-baseline message. New flags: `--output-format mydumper`, `--output-dir`, `--tables schema.table,...`, `--chunk-size` (default `256MB`), `--parallelism` (default `runtime.NumCPU()`). Strict `--allow-gaps=false` semantics inherited from single-row reconstruct — a coverage gap between baseline and target aborts unless the user explicitly accepts incomplete data. Supported PK types: integer (int/smallint/tinyint/mediumint/bigint), string (char/varchar/text/enum/set), DATETIME, TIMESTAMP, DATE, YEAR. Any PK column outside the allow-list (DECIMAL, BINARY, BLOB, BIT, JSON, spatial types, and any unknown type) causes reconstruct to hard-fail at start with a clear error message naming the column; #214 tracks expanding the supported set. UPDATE events that mutate the primary key itself are not handled — the change map is keyed by the before-image PK, so the after-image row may be dropped. Re-snapshot the baseline after PK-reshaping schema changes (#187).
- Full-table reconstruct now supports **DECIMAL and NUMERIC primary key columns**. Previously these were rejected at `ReconstructTable` entry because the #212 canonicalizer allow-list only covered integer, string, datetime, date, and year types. Investigation for #214 showed DECIMAL is the one type in the originally-listed set that can be added as a pure canonicalizer change without touching on-disk formats: go-mysql v1.13.0's `decodeDecimal` returns a pre-formatted Go string when `useDecimal` is false (the bintrail default — never set in `cmd/bintrail/stream.go` or `agent.go`), and the baseline writer stores DECIMAL as `parquet.String()`, which DuckDB reads back as a Go string. Both sides land on byte-identical strings for every representable value including zeros (`"0.00"` not `".00"` — `decodeDecimal` at `replication/row_event.go:1565-1567` explicitly writes `"0"` for zero-leading integer parts), so the canonicalizer branch is a type-check + pass-through. BINARY, VARBINARY, BLOB variants, BIT, and JSON remain deferred with explicit hard errors: each has a real representation mismatch between the indexer's `%v`-of-`[]byte` format and `baseline.parseSQLValue`, which does not decode MySQL hex literals (`0x...`), bit-string literals (`b'...'`), or convert JSON between raw-bytes and string forms. Fixing any of those requires a non-additive change to either `parser.BuildPKValues` or `internal/baseline/reader_sql.go`, tracked as separate follow-up issues. New integration test `TestRunReconstruct_fullTableRoundTrip_decimalPK` covers the full round-trip end-to-end (zero, positive, negative, INSERT-via-event); new regression test `TestRunReconstruct_rejectsRemainingUnsupportedPKTypes` is a table-driven pin against accidental allow-list expansion without a matching representation fix (#214).
- `bintrail baseline` now embeds the raw mydumper `<db>.<table>-schema.sql` contents in the Parquet file as `bintrail.create_table_sql`. Full-table reconstruct (#187) reads this back to emit a faithful `CREATE TABLE` in its schema file without re-synthesising from Parquet column types (which would lose indexes, foreign keys, charsets, engine, etc.).
- `recovery.FormatSQLValue`, `recovery.EscapeString`, `recovery.QuoteName` exported from `internal/recovery` so the full-table mydumper writer reuses the exact MySQL literal formatting used by reversal SQL. `FormatSQLValue` extended to handle `int64`/`int32`/`int`/`uint64`/`uint32`/`float32`/`time.Time`/`[]byte` (DuckDB scan types) in addition to the JSON-round-tripped types it already handled.
- `baseline.ReadParquetMetadataAny` reads Parquet metadata from either local paths or `s3://` URLs (using DuckDB's `parquet_kv_metadata` via the `httpfs` extension), so full-table reconstruct works against S3-resident baselines without adding a direct AWS SDK dependency.
- Benchmark harness for the full-table reconstruct merge loop at `internal/reconstruct/fulltable_bench_test.go`. Parameterised on baseline size × change rate (100k/1k, 1M/10k, 10M/100k at 1% change rate), reports `rows/sec` and `hit-ratio` as custom metrics via `b.ReportMetric`, and asserts the end-state counters on the first iteration so a regression in the merge loop surfaces as a benchmark failure instead of a silent throughput drop. This is the preparatory measurement work for #207 (row-group-level PK-range pruning). Run via `go test -bench=BenchmarkMergeBaselineIntoWriter -benchmem -run=^$ -count=3 ./internal/reconstruct/`. The 10M tier is gated behind `-short` because the synthetic fixture build alone is ~30s. **Initial numbers on Apple M1** (zstd-compressed 500k-row-group Parquet, single-column INT PK, evenly-spaced change events): 100k/1k ≈ 98ms / 1.02M rows/sec, 1M/10k ≈ 785ms / 1.27M rows/sec, 10M/100k ≈ 8.56s / 1.17M rows/sec. Throughput is linear across three orders of magnitude — the merge loop is not allocation-bound, so the speedup headroom #207 would unlock is bounded by Amdahl on a workload that already sustains >1M rows/sec. Extrapolated to a 50GB / ~500M-row baseline: roughly 7 minutes wall clock. This number goes in the #207 thread for the pruning decision (#207).

### Fixed
- `bintrail query` now **always surfaces archive fetch failures on stderr** and aborts immediately on context cancellation. The archive loop previously wrapped every `parquetquery.Fetch` error in a `slog.Warn` and continued, so expired AWS credentials, S3 `AccessDenied`, DuckDB `memory_limit` OOM, corrupted Parquet files, and `Ctrl-C` all produced partial/empty results with exit 0 and no visible signal at the default text log level. 0.4.8's #203 fix addressed the specific Binder Error trigger (pre-0.4.4 parquets missing the `connection_id` column) at the `parquetquery` layer, but left the surrounding silent-swallow anti-pattern untouched — any future archive failure mode would have reproduced the same six-days-of-empty-results outage. The fix extracts the entire archive fetch loop into `queryArchiveSources(ctx, sources, opts, fetch, stderr)` which (a) prints `Warning: archive query failed for <src>: <err>` to stderr regardless of log level via a `lineBreakReplacer` that collapses every line-terminator character (`\r\n`, `\r`, `\n`, `\v`, `\f`) to ` | ` so one failure = one stderr line, (b) still emits the structured `slog.Warn` with the raw (unsanitized) error for log-format=json consumers and full-fidelity debugging, and (c) runs a dual cancellation check — `ctx.Err()` AND `errors.Is(err, context.Canceled/DeadlineExceeded)` — so Ctrl-C short-circuits the query immediately instead of iterating every remaining source printing warnings. Non-cancellation errors keep the per-source "log and continue" semantics operators rely on — one broken archive still does not kill the whole query, only ones that are visibly broken. The helper takes `query.ArchiveFetcher` (the same named type `FetchMerged` uses) so signature drift with the shared pipeline becomes a compile error. Scoped to `cmd/bintrail/query.go` to preserve the existing (already-tested) `recover`, `reconstruct`, agent, and MCP-server paths untouched; the same silent-swallow still lives in `internal/query/fetchmerged.go`, `cmd/bintrail-mcp/main.go`, and `internal/agent/handler.go` as tracked follow-ups (#203).
- Full-table reconstruct now correctly handles **DATETIME/TIMESTAMP primary keys at every declared fractional precision** (0 through 6). The initial #187 implementation used `fmt.Sprintf("%v", ...)` to hash PK values on both sides of the merge, but the bintrail indexer stores DATETIME pk_values as go-mysql-formatted strings with the column's declared precision (`"14:30:45"` for DATETIME(0), `"14:30:45.123456"` for DATETIME(6)) while DuckDB's `parquet_scan` returns the same column as a `time.Time`. The resulting `%v` strings diverged and every event for a DATETIME-PK table silently missed the baseline, producing a dump with duplicate PK rows. **Schema change**: `schema_snapshots` gains a `column_type` column (e.g. `"datetime(6)"`) populated by `TakeSnapshot` from `information_schema.COLUMNS.COLUMN_TYPE`. `indexer.EnsureSchema` adds it idempotently on startup so existing installations upgrade transparently; `ReconstructTables` and the MCP server's `recoverTool` also run the migration eagerly because those code paths previously didn't. The new `canonicalizePKValue` helper in `internal/reconstruct` reads the declared precision from `ColumnType` and formats DuckDB values to match go-mysql's `formatDatetime` output exactly. Pre-#212 snapshots (empty ColumnType) fall back to a best-effort heuristic that works for DATETIME(0); full-table reconstruct emits an `slog.Warn` at the start of the run whenever a DATETIME/TIMESTAMP PK column has an empty ColumnType, instructing the operator to re-run `bintrail snapshot`. The canonicalizer **hard-fails** on nil PK values, missing PK columns, unknown DuckDB scan types, and any PK column type outside the supported allow-list (integers, strings, enum/set, datetime/timestamp/date, year) — all conditions that previously could silently corrupt the output. Any PK column outside the allow-list (including DECIMAL, BINARY, VARBINARY, BLOB, BIT, JSON, and spatial types) is rejected at `ReconstructTable` entry; #214 tracks extending support to those types. `MissingPKColumnError` is now a typed error exposing the offending column name via `errors.As`. `ReconstructTables` aggregates multi-table failures via `errors.Join` so every per-table error surfaces in the CLI exit wrap. 4 new integration tests pin the contract end-to-end: `TestRunReconstruct_fullTableRoundTrip_datetimePK` (DATETIME(0)), `TestRunReconstruct_fullTableRoundTrip_datetime6PK` (DATETIME(6) with mixed whole-second and microsecond values — the empirical validation of the precision-aware fix), `TestRunReconstruct_fullTableRoundTrip_varcharPK`, plus the pre-existing #187 INT PK round trip. A dedicated canonicalizer unit test suite covers every supported type, precision 0-6, fallback heuristic, and every hard-fail path (#212).
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
