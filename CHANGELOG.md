# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
