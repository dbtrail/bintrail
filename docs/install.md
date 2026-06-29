# Installing dbtrail

Every way to install and first-run dbtrail, from the zero-friction Docker
Compose stack to building from source. If you just want the fastest path,
it's the first section — the same four lines as the README.

> **Naming note:** the project is **dbtrail**; the binaries, packages, and
> images keep the original engine name **`bintrail`** (`bintrail`,
> `bintrail-console`, `ghcr.io/dbtrail/bintrail`, `BINTRAIL_*` env vars).
> Existing installs, scripts, and services stay valid as-is.

## Requirements

- A **source MySQL 8.0+** with `binlog_format = ROW` and
  `binlog_row_image = FULL`. Don't guess: `bintrail doctor` checks everything
  and prints copy-pasteable remediation for whatever is missing.
- A MySQL user on the source with `REPLICATION SLAVE`, `REPLICATION CLIENT`,
  and `SELECT`.
- An **index MySQL 8.0+** database for dbtrail's data (the Compose stack
  bundles one).
- Go 1.25+ (the module targets `go 1.25.11`) — only when building from source. The default `GOTOOLCHAIN=auto` fetches the right toolchain for you.

## Docker Compose (the bundled default)

One file, zero config — an index MySQL (persisted in a volume) and
`bintrail-console watch` in source-less daemon mode: the console plus the
control plane, waiting for you to add servers from the UI.

The shortest path is the install script — it does the two commands below
*and* waits for the console to actually answer before telling you where to
go next (and opens it in your browser when it can):

```sh
curl -fsSL https://raw.githubusercontent.com/dbtrail/dbtrail/main/install.sh | sh
```

It drops the stack in `./dbtrail` (override with `DBTRAIL_DIR`), and if port
8090 is already taken it tells you so up front — re-run with
`DBTRAIL_PORT=9090 …` to publish the console somewhere else. Prefer to drive
Compose yourself?

```sh
curl -fsSLO https://raw.githubusercontent.com/dbtrail/dbtrail/main/docker-compose.yml
docker compose up -d
docker compose logs -f bintrail
```

The logs print the console URL:

```
Console is running — open it and add the MySQL servers to watch:

    http://127.0.0.1:8090/

First run — open the URL and create your console username and password.
```

Open it and use **+ Add server** (the Servers screen opens itself on a
fresh install): paste the MySQL to watch — host, user, password, optional
schema filter (`host.docker.internal` reaches a MySQL on this same machine
from inside Docker). dbtrail runs the preflight (failures come back as
remediation cards), provisions a dedicated index for that source, and starts
streaming. Repeat per server; everything you add resumes automatically when
the container restarts.

Prefer to start streaming one source immediately at boot? Set `SOURCE_DSN`
in a `.env` next to the compose file — that's optional now, not required.

**The bundled index is a pinned MySQL 8.4** with a generated password — it
holds the forensic record, so **it is your system of record, not a throwaway:
back up its volumes** (`bintrail-index-data` + `bintrail-index-secret`
together; volume loss means re-indexing). dbtrail **ships** that MySQL but
does not **operate** it — disk, backups, and upgrades are yours, as is sizing
(see [Capacity Planning](./capacity.md)). The ship-vs-operate boundary triage
cites is [SUPPORT.md](../SUPPORT.md). See [docker.md](./docker.md) for the
credential mechanism and the `8.0→8.4` upgrade note.

**Bring your own index MySQL** (co-equal path, not an afterthought): set
`INDEX_DSN` in `.env` to a MySQL 8.0+ you operate, and remove the bundled
`index-init` + `index-mysql` services. Same split — dbtrail installs and
migrates only its schema on whatever server you point it at; the contract
floor stays MySQL 8.0+ (only the *bundled* index is 8.4). Want it operated for
you? That's the managed service at [dbtrail.com](https://dbtrail.com).

All the optional knobs (pinned console token, schema filter, image tag)
live in
[`.env.example`](https://github.com/dbtrail/dbtrail/blob/main/.env.example);
the full walkthrough is in [docker.md](./docker.md).

## Try it without installing anything (30 seconds)

Want to *feel* time-travel SQL before wiring anything up? The demo image
bundles MySQL, bintrail, ProxySQL, and a traffic generator in one
evaluation-only container:

```sh
docker run --rm -p 6033:6033 ghcr.io/dbtrail/bintrail-demo
```

Wait for the banner, give the traffic a minute to build history, then query a
row "as of" a minute ago over port 6033. The full walkthrough, credentials,
and more queries are in [demo.md](./demo.md). (Stateless, evaluation-only,
multi-arch — runs natively on amd64 and arm64, including Apple Silicon and
Graviton.)

## Docker image (without Compose)

```sh
docker pull ghcr.io/dbtrail/bintrail:latest
docker run --rm ghcr.io/dbtrail/bintrail:latest --version
```

Multi-arch (`linux/amd64` + `linux/arm64`), signed with cosign, SBOM attached
to every release. The image bundles both `bintrail` and `bintrail-mcp`; the
web console ships as its own image, `ghcr.io/dbtrail/bintrail-console`
(`serve` = read-only console, `watch` = stream + console daemon — what the
Compose stack runs). The PostgreSQL-source binary ships as its own image,
`ghcr.io/dbtrail/bintrail-pg` (beta). See [docker.md](./docker.md) for signature verification,
`docker run` recipes, and the long-running stream container.

## Linux packages

`.deb` and `.rpm` for amd64 and arm64 are attached to every
[release](https://github.com/dbtrail/dbtrail/releases):

```sh
# Debian/Ubuntu
curl -fsSLO https://github.com/dbtrail/dbtrail/releases/latest/download/bintrail_VERSION_linux_amd64.deb
sudo dpkg -i bintrail_*_linux_amd64.deb

# RHEL/Fedora
sudo rpm -i bintrail_VERSION_linux_amd64.rpm
```

(Replace `VERSION` with the release version; `checksums.txt` is cosign-signed.)

The `bintrail` package carries the core CLI + `bintrail-mcp`; the web console
is a separate `bintrail-console` package — install it only where an operator
wants the UI. PostgreSQL-source capture is a separate `bintrail-pg` package
(beta) — install it only on hosts that capture from PostgreSQL.

## Go install

```sh
go install github.com/dbtrail/dbtrail/cmd/bintrail@latest
```

Requires CGO (dbtrail embeds DuckDB for Parquet archive queries).

## Build from source

```sh
git clone https://github.com/dbtrail/dbtrail
cd dbtrail
go build ./cmd/bintrail
```

`make build` builds both `bintrail` and `bintrail-mcp` with version metadata;
`make build-console` builds the `bintrail-console` web-console binary;
`make build-pg` builds the `bintrail-pg` PostgreSQL-source binary.

> macOS binaries and a Homebrew tap are tracked in
> [#349](https://github.com/dbtrail/dbtrail/issues/349) — today the
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

Once it's running, the [Quickstart](quickstart.md) covers querying the index
and generating reversal SQL (`bintrail query` / `bintrail recover`) with worked
examples.

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
