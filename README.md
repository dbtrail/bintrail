# Bintrail

**Point-in-time recovery for MySQL, without locking tables or changing your schema.** Bintrail tails the binary log, keeps every row change with full before/after images in a searchable index, and generates exact reversal SQL when something goes wrong.

> `SELECT * FROM orders WHERE id = 123 AS OF '2026-05-20 14:00:00'` against production MySQL — that's the experience bintrail makes possible.

## Requirements

- Go 1.24+ (only when building from source)
- MySQL 8.0+ for the index database
- Source MySQL with `binlog_format = ROW` and `binlog_row_image = FULL` — `bintrail doctor` will tell you exactly what to fix if your source isn't ready

## Install

**Docker (no Go toolchain required):**

```sh
docker pull ghcr.io/dbtrail/bintrail:latest
docker run --rm ghcr.io/dbtrail/bintrail:latest --version
```

Multi-arch (`linux/amd64` + `linux/arm64`), signed with cosign. The image bundles both `bintrail` and `bintrail-mcp`. See [docs/docker.md](docs/docker.md) for signature verification and `docker run` usage.

**Linux packages:** `.deb` and `.rpm` for both architectures are attached to each [release](https://github.com/dbtrail/bintrail/releases).

**Go install:**

```sh
go install github.com/dbtrail/bintrail/cmd/bintrail@latest
```

**Build from source:**

```sh
git clone https://github.com/dbtrail/bintrail
cd bintrail
go build ./cmd/bintrail
```

## 30-second evaluation

Want to *feel* time-travel SQL before wiring anything up? The appliance image
bundles MySQL, bintrail, ProxySQL, and a traffic generator in one
evaluation-only container:

```sh
docker run --rm -p 6033:6033 ghcr.io/dbtrail/bintrail-appliance
```

Wait for the banner, give the traffic a minute to build history, then:

```sh
mysql -h 127.0.0.1 -P 6033 -u demo -pdemo demo \
  -e "SELECT * FROM _flashback.orders AS OF '1 minute ago' WHERE id = 1"
```

…returns the row as it was a minute ago — compare with
`SELECT * FROM orders WHERE id = 1` on the same connection. Stateless and for
evaluation only; see [docs/appliance.md](docs/appliance.md) for what's inside
and more queries to try.

## Quick start (2 commands)

```sh
# 1. Verify prerequisites and get copy-pasteable remediation for anything missing
bintrail doctor \
  --source-dsn "user:pass@tcp(source:3306)/" \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index"

# 2. Initialize and start streaming (idempotent — safe to re-run)
bintrail up \
  --source-dsn "user:pass@tcp(source:3306)/" \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index"
```

`bintrail up` runs preflight + creates index tables + auto-snapshots + starts streaming, all in one. It resumes from the last checkpoint on restart and auto-derives a unique `server-id` from your source DSN so you don't have to think about replica collisions.

Once it's running, query and recover:

```sh
# Search the index
bintrail query \
  --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --schema mydb --table orders --pk 12345

# Generate reversal SQL
bintrail recover \
  --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --schema mydb --table orders --event-type DELETE \
  --since "2026-02-19 14:00:00" --until "2026-02-19 14:05:00" \
  --output recovery.sql
```

> **Prefer a browser?** `bintrail console --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index"` opens a read-only web UI to browse changes with before/after diffs and generate undo SQL — no extra infrastructure. See [Web console](#web-console).

> **Managed MySQL (RDS, Aurora, Cloud SQL)?** `bintrail up` connects over the replication protocol — no disk access to binlogs required. See [Streaming](docs/streaming.md).

> **Want manual control of each step?** See [Step-by-step setup](#step-by-step-setup) below for the underlying `init` / `snapshot` / `stream` commands.

> **New to bintrail?** See the [Practical Guide for DBAs](docs/guide.md) for scenario-based walkthroughs and troubleshooting.

### Step-by-step setup

If you prefer running each phase explicitly (e.g. to deploy init and stream on separate hosts), the underlying commands remain available:

```sh
# 1. Verify prereqs (same as `bintrail up` Phase 1)
bintrail doctor --source-dsn "$SRC" --index-dsn "$IDX"

# 2. Create index tables
bintrail init --index-dsn "$IDX"

# 3. Snapshot schema metadata
bintrail snapshot --source-dsn "$SRC" --index-dsn "$IDX"

# 4. Either: stream live (recommended)
bintrail stream --source-dsn "$SRC" --index-dsn "$IDX" --server-id 12345

# Or: index from binlog files on disk
bintrail index --binlog-dir /var/lib/mysql --source-dsn "$SRC" --index-dsn "$IDX" --all
```

## Web console

Prefer a browser to the CLI? `bintrail console` serves a **read-only, single-operator web UI** over your index — same single binary, no extra infrastructure. Think of it as the MCP server with a web face: browse every change, see before/after diffs, and generate undo SQL from a browser.

```sh
bintrail console --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index"
```

It prints a tokenized loopback URL to open:

```
Bintrail console (read-only) is running. Open:

    http://127.0.0.1:8090/?token=ab12cd34ef56ab12cd34ef56ab12cd34
```

Or serve it alongside a live stream in one process: `bintrail up --source-dsn "$SRC" --index-dsn "$IDX" --console` — add `--baseline-dir`/`--baseline-s3` to enable Time-travel there too.

Four screens (Time-travel appears only when a baseline is configured):

- **Recover** — filter by schema / table / PK / time, preview the affected rows with before→after diffs, then generate undo SQL to copy or download.
- **Events** — browse every indexed change with full before/after images; download results as JSON or CSV.
- **Status** — index health: partitions, coverage, stream lag, archives.
- **Time-travel** — single-row point-in-time reconstruct, gated on a baseline source (`--baseline-dir`/`--baseline-s3`).

It **never executes SQL** — recover produces a script you review and apply yourself, exactly like `bintrail recover --dry-run`. It binds to loopback with an auto-generated access token by default; binding to a non-loopback address requires an explicit `--token`. See [Web console](docs/console.md) for the security model and HTTP API.

| Command | Description |
|---|---|
| `doctor` | Diagnose source MySQL prerequisites and emit copy-pasteable remediation |
| `up` | One command: preflight + init + stream (the friction-free quickstart) |
| `init` | Create index tables in the target MySQL database |
| `snapshot` | Capture table and column metadata from the source server |
| `index` | Parse binlog files from disk and write row events to the index |
| `stream` | Connect as a replica and index row events in real-time |
| `query` | Search the index with flexible filters (schema, table, PK, time range, GTID) |
| `recover` | Generate reversal SQL for matching events |
| `console` | Serve a read-only web UI to browse changes and generate undo SQL from a browser |
| `reconstruct` | Rebuild row state at a point in time from baselines + binlog events |
| `rotate` | Drop old partitions, add new ones, optionally archive to Parquet |
| `status` | Show indexed files, partition sizes, and event counts |
| `dump` | Invoke mydumper to create a logical dump of the source server |
| `baseline` | Convert mydumper output to Parquet snapshots |
| `upload` | Upload local Parquet files to S3 |
| `config init` | Generate a `.bintrail.env` configuration file |
| `init-shim` | Generate a `shim.yaml` for the time-travel SQL shim |
| `proxysql-config` | Generate ProxySQL setup SQL for time-travel SQL routing |
| `shim` | Run the in-process MySQL-protocol server for `_flashback`/`_diff`/`_snapshot` queries |
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
| [Appliance](docs/appliance.md) | 30-second evaluation: single-container demo with MySQL + bintrail + ProxySQL preconfigured |
| [Quickstart](docs/quickstart.md) | Zero to recovery in 10 minutes |
| [Practical Guide for DBAs](docs/guide.md) | Scenario-based walkthroughs and troubleshooting |
| [Indexing](docs/indexing.md) | File-based indexing in depth |
| [Streaming](docs/streaming.md) | Real-time replication indexing |
| [Streaming 101](docs/streaming-101.md) | Getting started with stream |
| [Query and Recovery](docs/query-and-recovery.md) | Filters, output formats, and recovery workflows |
| [Web console](docs/console.md) | Read-only browser UI: browse changes, diffs, and generate undo SQL |
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
| [Time-Travel SQL](docs/time-travel-sql.md) | End-to-end setup for `_flashback` / `_diff` / `_snapshot` virtual schemas via ProxySQL + `bintrail shim` |

## Agent exit codes

`bintrail agent` uses distinct process exit codes so a supervisor (e.g. systemd) can distinguish permanent failures from transient ones:

| Code | Meaning | Supervisor action |
|---|---|---|
| 0 | Clean shutdown (SIGTERM/SIGINT) | — |
| 64 | Fatal auth/config error (missing, invalid, or revoked API key; wrong tenant mode) | Fix credentials, restart manually |
| 65 | Rate-limited by the server | Contact support before restarting |
| 1 | Transient/unknown error | Safe to respawn (default systemd behavior) |

For systemd, add `RestartPreventExitStatus=64 65` to the service unit so the agent is not respawned on permanent failures.

## License

This project is licensed under the [Apache License, Version 2.0](LICENSE). You may use, modify, and redistribute bintrail for any purpose, including commercial and production use, subject to the terms of that license. See the [NOTICE](NOTICE) file for attribution requirements.

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request. All contributors must agree to the [Contributor License Agreement](CLA.md) — first-time contributors will be prompted automatically via CLA Assistant.
