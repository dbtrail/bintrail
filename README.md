# Bintrail

A CLI tool that parses MySQL ROW-format binary logs, indexes every row event into MySQL with full before/after images, and generates reversal SQL for point-in-time recovery — without needing the original binlog files.

## Requirements

- Go 1.24+
- MySQL 8.0+ (index database)
- Source MySQL server with `binlog_format = ROW` and `binlog_row_image = FULL`

## Install

```sh
go install github.com/dbtrail/bintrail/cmd/bintrail@latest
```

Or build from source:

```sh
git clone https://github.com/dbtrail/bintrail
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

# 3. Index binlog files (requires access to /var/lib/mysql on the source host)
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

> **Managed MySQL (RDS, Aurora, Cloud SQL)?** Use `bintrail stream` instead of `bintrail index` — it connects over the replication protocol and requires no access to binlog files on disk. See [Streaming](docs/streaming.md).

> **New to bintrail?** See the [Practical Guide for DBAs](docs/guide.md) for scenario-based walkthroughs and troubleshooting.

## Commands

| Command | Description |
|---|---|
| `init` | Create index tables in the target MySQL database |
| `snapshot` | Capture table and column metadata from the source server |
| `index` | Parse binlog files from disk and write row events to the index |
| `stream` | Connect as a replica and index row events in real-time |
| `query` | Search the index with flexible filters (schema, table, PK, time range, GTID) |
| `recover` | Generate reversal SQL for matching events |
| `reconstruct` | Rebuild row state at a point in time from baselines + binlog events |
| `rotate` | Drop old partitions, add new ones, optionally archive to Parquet |
| `status` | Show indexed files, partition sizes, and event counts |
| `dump` | Invoke mydumper to create a logical dump of the source server |
| `baseline` | Convert mydumper output to Parquet snapshots |
| `upload` | Upload local Parquet files to S3 |
| `config init` | Generate a `.bintrail.env` configuration file |
| `profile` | Manage RBAC access profiles for query and recover |
| `flag` | Label tables and columns (e.g. `pii`, `sensitive`) for access rules |
| `access` | Link flags to profiles with allow/deny permissions |
| `generate-key` | Generate an AES-256 encryption key for dump encryption |

All commands accept `--log-level` (default `info`) and `--log-format` (default `text`). See each command's `--help` for flags and usage.

## MCP Server

Bintrail ships an [MCP](https://modelcontextprotocol.io) server that exposes query, recover, and status as read-only tools — letting Claude (or any MCP client) explore your binlog index conversationally.

### Claude Connector

The easiest way to connect — works from claude.ai, Claude Desktop, and Claude mobile:

1. Deploy the [MCP Gateway](docs/mcp-gateway.md) (handles OAuth + tenant routing)
2. In Claude, go to **Settings > Integrations > Add custom integration**
3. Enter your gateway URL (e.g. `https://mcp.dbtrail.com/mcp`)
4. Authorize with your tenant ID — done

### Claude Code (local)

The project ships `.mcp.json` which pre-registers the server using `go run`:

```json
{
  "mcpServers": {
    "bintrail": {
      "command": "go",
      "args": ["run", "./cmd/bintrail-mcp"],
      "env": { "BINTRAIL_INDEX_DSN": "user:pass@tcp(127.0.0.1:3306)/binlog_index" }
    }
  }
}
```

Set `BINTRAIL_INDEX_DSN` to your index database DSN, then enable with `claude mcp enable bintrail`.

See [MCP Server docs](docs/mcp-server.md) for HTTP mode, proxy setup, and tool details.

## How it works

```
Source MySQL            Index MySQL
(information_schema) ──snapshot──► schema_snapshots
                                        │
Binlog files on disk ──index──►   binlog_events (partitioned)
                                  index_state
                                        │
Replication stream   ──stream──►  binlog_events (partitioned)
                                  stream_state (checkpoint)
                                        │
                          query / recover ──► stdout / .sql file
```

The index stores complete before and after row images for every event, so recovery never requires the original binlog files.

**`bintrail index`** reads binlog files directly from disk — best for self-managed MySQL where the binlog directory is accessible.

**`bintrail stream`** connects as a replica over the replication protocol — best for managed MySQL (RDS, Aurora, Cloud SQL) where binlog files are not directly accessible.

## Documentation

| Guide | Description |
|---|---|
| [Quickstart](docs/quickstart.md) | Zero to recovery in 10 minutes |
| [Practical Guide for DBAs](docs/guide.md) | Scenario-based walkthroughs and troubleshooting |
| [Indexing](docs/indexing.md) | File-based indexing in depth |
| [Streaming](docs/streaming.md) | Real-time replication indexing |
| [Streaming 101](docs/streaming-101.md) | Getting started with stream |
| [Query and Recovery](docs/query-and-recovery.md) | Filters, output formats, and recovery workflows |
| [Rotation and Status](docs/rotation-and-status.md) | Partition management and monitoring |
| [Dump and Baseline](docs/dump-and-baseline.md) | mydumper workflow and Parquet baselines |
| [DDL Tracking](docs/ddl-tracking.md) | Schema change detection and handling |
| [Server Identity](docs/server-identity.md) | Multi-server identity management |
| [Upload](docs/upload.md) | Parquet archive uploads to S3 |
| [MCP Server](docs/mcp-server.md) | MCP server setup, HTTP mode, and proxy |
| [MCP Gateway](docs/mcp-gateway.md) | OAuth gateway for Claude Connector |
| [Deployment](docs/deployment.md) | cron, systemd, Ansible, and production setup |
| [Docker](docs/docker.md) | Container images and Docker Compose |
| [Parquet Debugging](docs/parquet-debugging.md) | Inspecting and troubleshooting Parquet archives |

## License

This project is licensed under the [Business Source License 1.1](LICENSE). You may use bintrail for any purpose, including production use, except offering it as part of a competing commercial hosted service or managed consulting service. Each version converts to Apache License 2.0 four years after its release.

For alternative licensing arrangements, contact daniel@dbtrail.com.

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request. All contributors must agree to the [Contributor License Agreement](CLA.md) — first-time contributors will be prompted automatically via CLA Assistant.
