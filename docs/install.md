# Installing bintrail

Every way to install and first-run bintrail, from the zero-friction Docker
Compose stack to building from source. If you just want the fastest path,
it's the first section — the same four lines as the README.

## Requirements

- A **source MySQL 8.0+** with `binlog_format = ROW` and
  `binlog_row_image = FULL`. Don't guess: `bintrail doctor` checks everything
  and prints copy-pasteable remediation for whatever is missing.
- A MySQL user on the source with `REPLICATION SLAVE`, `REPLICATION CLIENT`,
  and `SELECT`.
- An **index MySQL 8.0+** database for bintrail's data (the Compose stack
  bundles one).
- Go 1.24+ — only when building from source.

## Docker Compose (the bundled default)

One file, zero config — an index MySQL (persisted in a volume) and
`bintrail-console watch` in source-less daemon mode: the console plus the
control plane, waiting for you to add servers from the UI:

```sh
curl -fsSLO https://raw.githubusercontent.com/dbtrail/bintrail/main/docker-compose.yml
docker compose up -d
docker compose logs -f bintrail
```

The logs print the console URL with its access token:

```
Console is running — open it and add the MySQL servers to watch:

    http://127.0.0.1:8090/?token=ab12cd34…
```

Open it and use **+ Add server** (the Servers screen opens itself on a
fresh install): paste the MySQL to watch — host, user, password, optional
schema filter (`host.docker.internal` reaches a MySQL on this same machine
from inside Docker). Bintrail runs the preflight (failures come back as
remediation cards), provisions a dedicated index for that source, and starts
streaming. Repeat per server; everything you add resumes automatically when
the container restarts.

Prefer to start streaming one source immediately at boot? Set `SOURCE_DSN`
in a `.env` next to the compose file — that's optional now, not required.

**The bundled index is a pinned MySQL 8.4** with a generated password — it
holds the forensic record, so **it is your system of record, not a throwaway:
back up its volumes** (`bintrail-index-data` + `bintrail-index-secret`
together; volume loss means re-indexing). bintrail **ships** that MySQL but
does not **operate** it — disk, backups, and upgrades are yours, as is sizing
(see [Capacity Planning](./capacity.md)). The ship-vs-operate boundary triage
cites is [SUPPORT.md](../SUPPORT.md). See [docker.md](./docker.md) for the
credential mechanism and the `8.0→8.4` upgrade note.

**Bring your own index MySQL** (co-equal path, not an afterthought): set
`INDEX_DSN` in `.env` to a MySQL 8.0+ you operate, and remove the bundled
`index-init` + `index-mysql` services. Same split — bintrail installs and
migrates only its schema on whatever server you point it at; the contract
floor stays MySQL 8.0+ (only the *bundled* index is 8.4). Want it operated for
you? That is [dbtrail](https://dbtrail.com).

All the optional knobs (pinned console token, schema filter, image tag)
live in
[`.env.example`](https://github.com/dbtrail/bintrail/blob/main/.env.example);
the full walkthrough is in [docker.md](./docker.md).

## Try it without installing anything (30 seconds)

Want to *feel* time-travel SQL before wiring anything up? The demo image
bundles MySQL, bintrail, ProxySQL, and a traffic generator in one
evaluation-only container:

```sh
docker run --rm -p 6033:6033 ghcr.io/dbtrail/bintrail-demo
```

Wait for the banner, give the traffic a minute to build history, then:

```sh
mysql -h 127.0.0.1 -P 6033 -u demo -pdemo demo \
  -e "SELECT * FROM orders WHERE id = 1 AS OF '1 minute ago'"
```

…returns the row as it was a minute ago. Stateless and for evaluation only;
amd64-only (it runs under emulation on Apple Silicon — the main bintrail
image is multi-arch); see [demo.md](./demo.md) for what's inside
and more queries to try.

## Docker image (without Compose)

```sh
docker pull ghcr.io/dbtrail/bintrail:latest
docker run --rm ghcr.io/dbtrail/bintrail:latest --version
```

Multi-arch (`linux/amd64` + `linux/arm64`), signed with cosign, SBOM attached
to every release. The image bundles both `bintrail` and `bintrail-mcp`; the
web console ships as its own image, `ghcr.io/dbtrail/bintrail-console`
(`serve` = read-only console, `watch` = stream + console daemon — what the
Compose stack runs). See [docker.md](./docker.md) for signature verification,
`docker run` recipes, and the long-running stream container.

## Linux packages

`.deb` and `.rpm` for amd64 and arm64 are attached to every
[release](https://github.com/dbtrail/bintrail/releases):

```sh
# Debian/Ubuntu
curl -fsSLO https://github.com/dbtrail/bintrail/releases/latest/download/bintrail_VERSION_linux_amd64.deb
sudo dpkg -i bintrail_*_linux_amd64.deb

# RHEL/Fedora
sudo rpm -i bintrail_VERSION_linux_amd64.rpm
```

(Replace `VERSION` with the release version; `checksums.txt` is cosign-signed.)

The `bintrail` package carries the core CLI + `bintrail-mcp`; the web console
is a separate `bintrail-console` package — install it only where an operator
wants the UI.

## Go install

```sh
go install github.com/dbtrail/bintrail/cmd/bintrail@latest
```

Requires CGO (bintrail embeds DuckDB for Parquet archive queries).

## Build from source

```sh
git clone https://github.com/dbtrail/bintrail
cd bintrail
go build ./cmd/bintrail
```

`make build` builds both `bintrail` and `bintrail-mcp` with version metadata;
`make build-console` builds the `bintrail-console` web-console binary.

> macOS binaries and a Homebrew tap are tracked in
> [#349](https://github.com/dbtrail/bintrail/issues/349) — today the
> supported paths on macOS are Docker (works great on Apple Silicon) and
> `go install`/source builds.

## First run with the binary

Two commands from zero to streaming:

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

`bintrail up` runs preflight + creates index tables + auto-snapshots + starts
streaming, all in one. It resumes from the last checkpoint on restart and
auto-derives a unique `server-id` from your source DSN. Want the web UI in
the same process? Run `bintrail-console watch` (same flags) instead — it is
`up` plus the console and the multi-server control plane.

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

> **Managed MySQL (RDS, Aurora, Cloud SQL)?** `bintrail up` connects over the
> replication protocol — no disk access to binlogs required. See
> [streaming.md](./streaming.md).

### Step-by-step setup

Prefer running each phase explicitly (e.g. to deploy init and stream on
separate hosts)? The underlying commands remain available:

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

For cron, systemd units, and Ansible recipes, see [deployment.md](./deployment.md).

## Every command

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

All commands accept `--log-level` (default `info`) and `--log-format`
(default `text`). See each command's `--help` for flags and usage.

The web console lives in the separate `bintrail-console` binary —
`serve` (read-only UI over an index) and `watch` (stream + console +
control plane in one daemon). See [console.md](./console.md).

## Appendix: agent exit codes

`bintrail agent` uses distinct process exit codes so a supervisor (e.g.
systemd) can distinguish permanent failures from transient ones:

| Code | Meaning | Supervisor action |
|---|---|---|
| 0 | Clean shutdown (SIGTERM/SIGINT) | — |
| 64 | Fatal auth/config error (missing, invalid, or revoked API key; wrong tenant mode) | Fix credentials, restart manually |
| 65 | Rate-limited by the server | Contact support before restarting |
| 1 | Transient/unknown error | Safe to respawn (default systemd behavior) |

For systemd, add `RestartPreventExitStatus=64 65` to the service unit so the
agent is not respawned on permanent failures.
