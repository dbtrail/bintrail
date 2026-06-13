<div align="center">

<img src="docs/img/header.png" alt="dbtrail — the open-source time-travel flashback for MySQL. Every change leaves a trail: follow it back." width="100%">

**Point-in-time recovery for MySQL — no locks, no schema changes, no waiting for a restore.**

[![Release](https://img.shields.io/github/v/release/dbtrail/dbtrail)](https://github.com/dbtrail/dbtrail/releases/latest)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![CI](https://github.com/dbtrail/dbtrail/actions/workflows/ci.yml/badge.svg)](https://github.com/dbtrail/dbtrail/actions)

```sql
SELECT * FROM orders WHERE id = 123 AS OF '2026-05-20 14:00:00'
```

*— against production MySQL. That's the experience dbtrail makes possible.*

<img src="docs/img/console-overview.png" alt="dbtrail console: what changed recently and where — every row change indexed, deletes surfaced first" width="850">

</div>

---

## What you get

dbtrail tails the MySQL binary log and keeps every row change with full
before/after images in a searchable index:

- **See every change** — what changed and when, for every row, with before → after diffs
- **Undo precisely** — generate exact reversal SQL for just the damaged rows
- **Time-travel** — query any row (or table) as it was at any moment (requires ProxySQL — see [Time-Travel SQL](docs/time-travel-sql.md))
- **Web console** — browse, recover, and add servers to monitor, all in the UI
- **[MCP server](docs/mcp-server.md)** — Claude or any MCP client can search history and draft recoveries

Works with **MySQL**, **Percona Server for MySQL**, **Amazon RDS for MySQL**,
**Amazon Aurora MySQL**, and **Google Cloud SQL for MySQL** — dbtrail connects
over the replication protocol, so it never needs the binlog files on disk
(that's what makes managed cloud databases work). Requires MySQL 8.0+ with
`binlog_format=ROW` and `binlog_row_image=FULL`; `bintrail doctor` checks both
and prints the exact fix.

## Install

```sh
curl -fsSL https://raw.githubusercontent.com/dbtrail/dbtrail/main/install.sh | sh
```

This downloads the Compose stack, brings it up, waits for the console, and prints
what to do next. Then:

1. Open **http://127.0.0.1:8090** — on first run, create a username and password (that's your login from now on).
2. Click **+ Add server** and paste the MySQL you want to watch — host, user, password. dbtrail runs preflight checks, provisions an index, and starts streaming within the minute.

**Just curious?** One container, zero setup, time-travel SQL in 30 seconds:

```sh
docker run --rm -p 6033:6033 ghcr.io/dbtrail/bintrail-demo
```

See [the demo image](docs/demo.md).

## Documentation

| Start here | Reference | Operations |
|---|---|---|
| [Install](docs/install.md) | [Query & Recovery](docs/query-and-recovery.md) | [Deployment](docs/deployment.md) · [Capacity](docs/capacity.md) |
| [Quickstart](docs/quickstart.md) | [Web console](docs/console.md) | [Rotation & Status](docs/rotation-and-status.md) |
| [DBA guide](docs/guide.md) | [Time-Travel SQL](docs/time-travel-sql.md) | [Docker](docs/docker.md) |
| [30-second demo](docs/demo.md) | [Streaming](docs/streaming.md) · [Indexing](docs/indexing.md) | [Upload to S3](docs/upload.md) |
| | [MCP server](docs/mcp-server.md) · [MCP gateway](docs/mcp-gateway.md) | [Server identity](docs/server-identity.md) |
| | [Dump & Baseline](docs/dump-and-baseline.md) · [DDL tracking](docs/ddl-tracking.md) | [Parquet debugging](docs/parquet-debugging.md) |

## License

[Apache-2.0](LICENSE) — free for any use, including commercial and production.
Contributions welcome: see [CONTRIBUTING.md](CONTRIBUTING.md) (CLA required,
prompted automatically on your first PR).
