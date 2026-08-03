<div align="center">

<img src="docs/img/dbtrail _ header.png" alt="dbtrail — the open-source time-travel flashback for MySQL. Every change leaves a trail: follow it back." width="100%">

**Point-in-time recovery for MySQL and PostgreSQL --- no locks, no schema changes, no waiting for a restore.**

[![Release](https://img.shields.io/github/v/release/dbtrail/dbtrail)](https://github.com/dbtrail/dbtrail/releases/latest)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![CI](https://github.com/dbtrail/dbtrail/actions/workflows/ci.yml/badge.svg)](https://github.com/dbtrail/dbtrail/actions)

```sql
SELECT * FROM orders WHERE id = 123 AS OF '2026-05-20 14:00:00'
```

*— against production MySQL/Postgres. That's the experience dbtrail makes possible.*

<img src="docs/img/console-overview.png" alt="dbtrail console: what changed recently and where — every row change indexed, deletes surfaced first" width="850">

</div>

---

## What you get

dbtrail tails the MySQL binary log or the Postgres WAL and keeps every row change with full
before/after images in a searchable index:

- **See every change** — what changed and when, for every row, with before → after diffs
- **Undo precisely** — generate exact reversal SQL for just the damaged rows
- **Undo foreign-key cascades** — reconstruct child rows an `ON DELETE CASCADE` wiped out, and restore the foreign keys an `ON DELETE SET NULL` cleared or an `ON UPDATE CASCADE`/`SET NULL` re-pointed when a parent's key changed — all of which InnoDB applies *below* the binlog, where most tools can't see them — see [Query & Recovery](docs/query-and-recovery.md)
- **Time-travel** — query any row (or table) as it was at any moment, from the web console or the `reconstruct` CLI; the live SQL `AS OF` interface additionally needs ProxySQL — see [Time-Travel SQL](docs/time-travel-sql.md)
- **Who changed this?** — session attribution (database user, host, and client program behind a change) is available in the commercial distribution (dbtrail EE)
- **Prove the safety net holds** — `bintrail verify` checks (off-line, drift-free) that a recovery would actually reproduce the source, and `bintrail status` flags any gap in the captured stream — so you find out *before* you need it — see [Verify](docs/verify.md)
- **Web console** — browse, recover, and add servers to monitor, all in the UI
- **Ask it in plain English** — connect Claude Desktop to your console in one click (an `.mcpb` bundle and a copy-paste URL: [5-minute guide](docs/connect-ai.md)); any MCP client works — see the [MCP server](docs/mcp-server.md) reference

Works with **MySQL**, **Percona Server for MySQL**, **Amazon RDS for MySQL**,
and **Amazon Aurora MySQL** (verified); **Google Cloud SQL for MySQL** is
expected to work — please report issues. dbtrail connects
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
2. Click **+ Add server**, pick the **source type** (MySQL, MariaDB, or PostgreSQL), and paste the database you want to watch — host, user, password. dbtrail runs preflight checks, provisions an index, and starts streaming within the minute.

### PostgreSQL & MariaDB (alpha) sources

The same one-line install — the console captures **PostgreSQL** and **MariaDB**
sources too, not just MySQL. In **+ Add server**, choose the source type;
PostgreSQL reveals fields for the database, replication slot, and publication.
PostgreSQL needs a one-time source-side setup first (`wal_level = logical`,
`REPLICA IDENTITY FULL`, and a publication — dbtrail validates these and never
runs DDL on your source). Full walkthrough, incl. managed RDS/Aurora/Cloud SQL:
**[PostgreSQL source](docs/postgres.md)** · **[MariaDB source
(alpha)](docs/mariadb.md)**.

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
| [DBA guide](docs/guide.md) | [Time-Travel SQL](docs/time-travel-sql.md) · [Verify recoveries](docs/verify.md) | [Docker](docs/docker.md) |
| [30-second demo](docs/demo.md) | [Streaming](docs/streaming.md) · [Indexing](docs/indexing.md) | [Upload to S3](docs/upload.md) · [S3 IAM policy](docs/s3-iam-policy.md) · [Upgrading](docs/upgrade.md) |
| | [MariaDB source (alpha)](docs/mariadb.md) · [PostgreSQL source](docs/postgres.md) | [Server identity](docs/server-identity.md) |
| | [Connect an AI assistant](docs/connect-ai.md) · [MCP server](docs/mcp-server.md) | [Parquet debugging](docs/parquet-debugging.md) |
| | [Dump & Baseline](docs/dump-and-baseline.md) · [DDL tracking](docs/ddl-tracking.md) | |

## Privacy

bintrail runs entirely in your infrastructure. **Your database data never
leaves it** — not your rows, schemas, table names, queries, hostnames, DSNs, or
file paths.

Official release builds do report **metadata-only usage statistics**: which
command ran, whether it succeeded, a coarse error class, and your version and
platform. It is on by default and off in one line:

```sh
bintrail telemetry off      # or: DO_NOT_TRACK=1, BINTRAIL_TELEMETRY=off
bintrail telemetry show     # see exactly what would be sent — sends nothing
```

No identifier of any kind is stored or transmitted, so there is nothing tying
those statistics to you or your machine. A binary you build yourself has no
reporting address compiled in and is incapable of sending anything.
[TELEMETRY.md](TELEMETRY.md) documents every field, every control, and the CI
tests that enforce both.

The [privacy policy](PRIVACY.md) covers the Claude Desktop extension (`.mcpb`)
— which reports nothing at all — and how data moves when an AI client queries
your deployment.

## License

[Apache-2.0](LICENSE) — free for any use, including commercial and production.
Contributions welcome: see [CONTRIBUTING.md](CONTRIBUTING.md) (CLA required,
prompted automatically on your first PR).

Want the index server **operated** for you — sized, backed up, upgraded, kept
alive on-call — instead of running it yourself? That is the managed service at
[dbtrail.com](https://dbtrail.com). See [SUPPORT.md](SUPPORT.md) for the
ship-vs-operate boundary.
