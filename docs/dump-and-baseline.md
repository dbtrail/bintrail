# Dump and Baseline — Using mydumper with dbtrail

dbtrail uses [mydumper](https://github.com/mydumper/mydumper) to create logical dumps of MySQL databases. The dump output is then converted to Parquet files by `bintrail baseline`, producing a point-in-time snapshot of every table that can be stored alongside archived binlog event partitions for long-term audit reconstruction.

This document covers running dumps, converting to Parquet baselines, and scheduling.

---

## Why mydumper?

dbtrail's binlog index captures every change (INSERT, UPDATE, DELETE) but not the initial state of rows that existed before indexing began. A baseline snapshot fills that gap — it records every row as it existed at a known point in time.

mydumper is used instead of `mysqldump` because it:

- Dumps tables in parallel (configurable thread count)
- Produces consistent snapshots using `FTWRL` or `--trx-consistency-only`
- Outputs per-table files that `bintrail baseline` can process independently
- Supports both SQL INSERT and TSV (`*.dat`) output formats

---

## Getting mydumper

**No installation required** — if Docker is available on your system, `bintrail dump` will automatically use the [official mydumper Docker image](https://hub.docker.com/r/mydumper/mydumper) (`mydumper/mydumper`). This is the recommended zero-setup approach.

The resolution order is:

1. If `--mydumper-path` is explicitly set — use that binary (no validation)
2. If a **compiled** `mydumper` binary is found on `$PATH` — use it (shell script wrappers are skipped automatically)
3. If Docker is available — invoke mydumper via `docker run`
4. If none of the above — fail with a clear error message

> **Note:** Do not place a shell script wrapper named `mydumper` on your PATH. dbtrail detects scripts (files starting with `#!`) and skips them to avoid argument-handling issues with volume mounts. If you need to force a specific binary, use `--mydumper-path`.

To pin a specific mydumper Docker image version:

```sh
bintrail dump \
  --mydumper-image mydumper/mydumper:v1.0.3-1 \
  --source-dsn "user:pass@tcp(source-db:3306)/" \
  --output-dir /tmp/mydumper-output
```

<details>
<summary>Manual mydumper installation (advanced)</summary>

If you prefer to install mydumper as a local binary instead of using Docker:

### Ubuntu / Debian

```sh
# Download from the releases page — check for the latest version:
# https://github.com/mydumper/mydumper/releases/latest
wget https://github.com/mydumper/mydumper/releases/download/v1.0.3-1/mydumper_1.0.3-1.jammy_amd64.deb
sudo dpkg -i mydumper_*.deb

# Or from the system repository (may be older)
sudo apt-get install mydumper
```

### RHEL / CentOS / Amazon Linux

```sh
# Check for the latest version: https://github.com/mydumper/mydumper/releases/latest
wget https://github.com/mydumper/mydumper/releases/download/v1.0.3-1/mydumper-1.0.3-1.el9.x86_64.rpm
sudo rpm -i mydumper-*.rpm
```

### macOS

```sh
brew install mydumper
```

### Custom path

If mydumper is installed in a non-standard location, pass its path explicitly:

```sh
bintrail dump --mydumper-path /opt/mydumper/bin/mydumper ...
```

### Verify

```sh
mydumper --version
```

</details>

---

## The dump → baseline pipeline

The pipeline has two steps:

```
Step 1: bintrail dump    →  mydumper output directory (SQL/TSV files per table)
Step 2: bintrail baseline  →  Parquet files (one per table)
```

**Step 1** requires a live connection to the source MySQL server.
**Step 2** operates purely on files — no database connection needed. It can run on a different machine from where the dump was taken.

---

## Step 1: Running a dump (`bintrail dump`)

`bintrail dump` is a thin wrapper around mydumper. It validates inputs, acquires a lockfile to prevent concurrent dumps, and invokes mydumper with the correct flags.

### Basic usage

```sh
bintrail dump \
  --source-dsn "user:pass@tcp(source-db:3306)/" \
  --output-dir /tmp/mydumper-output
```

This dumps all user schemas from the source server into `/tmp/mydumper-output`.

### All flags

| Flag | Default | Description |
|---|---|---|
| `--source-dsn` | *(required)* | DSN for the source MySQL server |
| `--output-dir` | *(required)* | Directory for mydumper output (removed and recreated on each run) |
| `--schemas` | *(all)* | Comma-separated schema filter (e.g. `mydb,otherdb`) |
| `--tables` | *(all)* | Comma-separated table filter (e.g. `mydb.orders,mydb.items`) |
| `--mydumper-path` | `mydumper` | Path to the mydumper binary. When set, skips Docker fallback. |
| `--mydumper-image` | `mydumper/mydumper:latest` | Docker image for mydumper. Used only when no local binary is found. |
| `--threads` | `4` | Number of parallel dump threads |
| `--format` | `text` | Output format: `text` or `json` |

### Schema and table filtering

```sh
# Dump only the 'mydb' schema
bintrail dump \
  --source-dsn "user:pass@tcp(source-db:3306)/" \
  --output-dir /tmp/mydumper-output \
  --schemas mydb

# Dump specific tables
bintrail dump \
  --source-dsn "user:pass@tcp(source-db:3306)/" \
  --output-dir /tmp/mydumper-output \
  --tables mydb.orders,mydb.customers
```

When a single schema is given, dbtrail passes `--database <schema>` to mydumper. When multiple schemas are given, it constructs a regex filter (`--regex ^(s1|s2)\.`). Table filtering uses mydumper's `--tables-list` flag.

### What mydumper flags does dbtrail pass?

dbtrail always passes these flags to mydumper:

| mydumper flag | Purpose |
|---|---|
| `--host`, `--port`, `--user`, `--password` | Connection details (parsed from `--source-dsn`) |
| `--outputdir` | Output directory |
| `--threads` | Parallelism |
| `--compress-protocol` | Compress the MySQL protocol traffic |
| `--complete-insert` | Generate `INSERT INTO table (col1, col2, ...) VALUES (...)` with column names — required for `bintrail baseline` to parse the output correctly |

### Concurrency protection

Only one `bintrail dump` can run at a time. A lockfile at `$TMPDIR/bintrail-dump.lock` prevents concurrent runs. If a previous dump crashed without cleaning up, dbtrail detects the stale lock (by checking if the PID is still alive) and removes it automatically.

### Output directory behavior

The `--output-dir` is **removed and recreated** on each run. Do not point it at a directory containing other files you want to keep.

---

## Step 2: Converting to Parquet (`bintrail baseline`)

Once mydumper finishes, convert the output to Parquet:

```sh
bintrail baseline \
  --input  /tmp/mydumper-output \
  --output /data/baselines
```

### All flags

| Flag | Default | Description |
|---|---|---|
| `--input` | *(required)* | mydumper output directory (from step 1) |
| `--output` | *(required)* | Parquet output base directory |
| `--timestamp` | *(from mydumper metadata)* | Override the snapshot timestamp (ISO 8601) |
| `--tables` | *(all)* | Comma-separated `db.table` filter |
| `--compression` | `zstd` | Parquet compression: `zstd`, `snappy`, `gzip`, `none` |
| `--row-group-size` | `500000` | Rows per Parquet row group |
| `--upload` | *(disabled)* | S3 URL to upload Parquet files after generation |
| `--upload-region` | *(from AWS env)* | AWS region for `--upload` |
| `--baseline-retain` | *(disabled)* | Prune local snapshots older than this (`Nd`/`Nh`) once a durable S3 copy exists (requires `--upload`) |
| `--retry` | `false` | Skip tables whose Parquet file already exists and S3 objects already uploaded |
| `--format` | `text` | Output format: `text` or `json` |

### Output structure

Files are organized as:

```
<output>/<timestamp>/<database>/<table>.parquet
```

For example:

```
/data/baselines/2026-03-02T14-30-00Z/mydb/orders.parquet
/data/baselines/2026-03-02T14-30-00Z/mydb/customers.parquet
```

The timestamp defaults to the `Started dump at:` time from mydumper's metadata file. Override it with `--timestamp` if needed.

### Upload to S3

Generate and upload in one step:

```sh
bintrail baseline \
  --input         /tmp/mydumper-output \
  --output        /tmp/baselines \
  --upload        s3://my-bucket/baselines/ \
  --upload-region us-east-1
```

See [Scenario I in the Practical Guide](guide.md#scenario-i-uploading-baseline-parquet-files-to-s3) for full S3 setup instructions.

### Retrying after a failure

If a baseline run fails partway through (e.g. network error during S3 upload, disk full), re-run with `--retry` to skip work that already completed:

```sh
bintrail baseline \
  --input         /tmp/mydumper-output \
  --output        /tmp/baselines \
  --upload        s3://my-bucket/baselines/ \
  --upload-region us-east-1 \
  --retry
```

With `--retry`:
- **Local Parquet files**: Tables whose output `.parquet` file already exists are skipped.
- **S3 uploads**: Files that already exist in S3 (checked via `HeadObject`) are skipped.

This makes the command safe to re-run without duplicating work.

### Pruning old local snapshots (`--baseline-retain`)

Periodic baselines accumulate under `--output` forever — nothing in `bintrail rotate` or the daemon's rotation loop touches them. On a long-lived host this silently fills the disk, even though every snapshot is already in S3 and therefore redundant on local disk.

`--baseline-retain` reclaims that space after a successful upload:

```sh
bintrail baseline \
  --input           /tmp/mydumper-weekly \
  --output          /data/baselines \
  --upload          s3://my-bucket/baselines/ \
  --upload-region   us-east-1 \
  --baseline-retain 14d
```

It prunes a local snapshot **only** when all of these hold — pruning never risks data:

- **A durable S3 copy exists.** The snapshot's `_SUCCESS` marker must be confirmed present in S3 at the same timestamp prefix. Without `--upload` (or on a local-only setup) the prune is a deliberate, logged no-op — a local snapshot with no S3 copy is the only copy, and the only copy is never deleted. (This mirrors `bintrail rotate`'s `PruneLocalAfterUpload && --archive-s3` rule.)
- **It is not the newest snapshot for any table.** Time-travel resolves a table to the newest snapshot that contains it (`reconstruct`), so the newest snapshot per table is always kept — even when older than the retention window. Pruning only ever narrows how far *back* local Time-travel can reach, never the present.
- **It is past the retention window** (and at least an hour old).
- **It is complete.** A snapshot mid-write or resumable via `--retry` (an `_INCOMPLETE` marker) is never touched.

The S3 copy is **not** pruned — only the redundant local copy. To prune on a long-lived daemon instead of from cron, `bintrail-console watch` takes the same `--baseline-retain` and runs the prune on its rotation cadence. It reclaims both the global `--baseline-dir` (against `--baseline-s3`) and every monitored server's own baseline directory (against that server's S3 prefix) — including the per-server dirs the console **Create baseline** button writes into. Env: `BINTRAIL_BASELINE_RETAIN` (CLI) / `BINTRAIL_CONSOLE_BASELINE_RETAIN` (`watch`).

### No database connection required

`bintrail baseline` reads only files — it never connects to MySQL. This means you can:

- Run the conversion on a different machine from where the dump was taken
- Re-run the conversion with different options (compression, row group size) without re-dumping
- Archive the mydumper output and convert it later

---

## When to run a dump

### Initial setup

Run a dump once when you first set up dbtrail, before starting to index binlog events. This captures the starting state of your data:

```sh
# 1. Dump
bintrail dump \
  --source-dsn "user:pass@tcp(source-db:3306)/" \
  --output-dir /tmp/mydumper-output

# 2. Convert to Parquet
bintrail baseline \
  --input  /tmp/mydumper-output \
  --output /data/baselines

# 3. Then start indexing binlog events
bintrail init --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index"
bintrail snapshot --source-dsn "..." --index-dsn "..."
bintrail stream --index-dsn "..." --source-dsn "..." --server-id 99999
```

### After major schema changes

If you run large DDL migrations (adding/dropping columns, restructuring tables), take a fresh baseline so the Parquet snapshot reflects the new schema.

### Periodic refresh

For audit or compliance purposes, you may want periodic full baselines. A weekly or monthly schedule is typical:

```cron
# Weekly baseline dump at 2am Sunday
0 2 * * 0 root bintrail dump \
  --source-dsn "$SOURCE_DSN" \
  --output-dir /tmp/mydumper-weekly \
  && bintrail baseline \
  --input  /tmp/mydumper-weekly \
  --output /data/baselines \
  --upload s3://my-bucket/baselines/ \
  --baseline-retain 30d \
  >> /var/log/bintrail-baseline.log 2>&1
```

`--baseline-retain 30d` keeps the last month of snapshots on local disk and prunes older ones once they are safely in S3 — see [Pruning old local snapshots](#pruning-old-local-snapshots---baseline-retain) above. Without it, a weekly job grows `/data/baselines` without bound.

### On-demand

Trigger a dump at any time:

```sh
bintrail dump \
  --source-dsn "user:pass@tcp(source-db:3306)/" \
  --output-dir /tmp/mydumper-adhoc \
  --schemas mydb

bintrail baseline \
  --input  /tmp/mydumper-adhoc \
  --output /data/baselines
```

---

## How often?

| Use case | Recommended frequency |
|---|---|
| Initial setup | Once, before first binlog indexing |
| Audit/compliance baselines | Weekly or monthly |
| After major schema changes | On-demand |
| Small, rarely-changing databases | Monthly or quarterly |
| Large, high-write databases | Weekly (with `--schemas` to limit scope) |

The dump frequency depends on your recovery and audit requirements. dbtrail's binlog index captures every change between baselines, so even infrequent baselines provide full coverage when combined with the change log.

---

## Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| `mydumper not found on $PATH and Docker is not available` | Neither mydumper nor Docker is installed | Install Docker (recommended) or install mydumper manually (see above) |
| `mydumper not found at "/custom/path"` | Explicit `--mydumper-path` points to a missing binary | Verify the path is correct and the binary is executable |
| `found mydumper on $PATH but it appears to be a shell script wrapper` | A shell script named `mydumper` is on your PATH (e.g. a Docker wrapper) | Remove the wrapper script — dbtrail handles Docker invocation automatically. Or use `--mydumper-path` to point to the real binary. |
| `another dump is already running` | A previous dump is still running or crashed | Wait for it to finish, or check if the PID in `$TMPDIR/bintrail-dump.lock` is still alive. Stale locks from crashed processes are cleaned up automatically on the next run. |
| `mydumper failed: exit status 2` | mydumper itself encountered an error (wrong credentials, unreachable host, etc.) | Check mydumper's stderr output for details. Verify the `--source-dsn` is correct. |
| Docker: `permission denied` on `/var/run/docker.sock` | Current user is not in the `docker` group | Run `sudo usermod -aG docker $USER` and log out/in, or use `sudo bintrail dump ...` |
| Docker: `Cannot connect to the Docker daemon` | Docker daemon is not running | Start Docker: `sudo systemctl start docker` (Linux) or open Docker Desktop (macOS) |
| Docker: mydumper cannot reach MySQL on localhost | On macOS/Windows, `--network host` does not work as on Linux | Use `host.docker.internal` instead of `localhost` in `--source-dsn` (e.g. `user:pass@tcp(host.docker.internal:3306)/`) |
| Docker: volume mount permission errors | Docker cannot write to the `--output-dir` path | Ensure the output directory's parent exists and is writable. On SELinux systems, add `:z` to the volume mount or use `--security-opt label=disable`. |
| Docker: dump files owned by root | Older dbtrail versions ran the container as root | Upgrade — dbtrail now passes `--user <uid>:<gid>` to `docker run` so dump files are owned by the invoking user. |
| Baseline produces no files | mydumper output directory is empty or has no table data files | Verify the dump ran successfully and the `--schemas`/`--tables` filters match existing tables. |
| `--timestamp: expected ISO 8601 format` | Invalid timestamp override format | Use `2026-03-02T14:30:00Z` or `2026-03-02 14:30:00` format. |
