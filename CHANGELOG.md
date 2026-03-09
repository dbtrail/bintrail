# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
