# Dump and Baseline — Using mydumper with Bintrail

Bintrail uses [mydumper](https://github.com/mydumper/mydumper) to create logical dumps of MySQL databases. The dump output is then converted to Parquet files by `bintrail baseline`, producing a point-in-time snapshot of every table that can be stored alongside archived binlog event partitions for long-term audit reconstruction.

This document covers installing mydumper, running dumps, converting to Parquet baselines, and scheduling.

---

## Why mydumper?

Bintrail's binlog index captures every change (INSERT, UPDATE, DELETE) but not the initial state of rows that existed before indexing began. A baseline snapshot fills that gap — it records every row as it existed at a known point in time.

mydumper is used instead of `mysqldump` because it:

- Dumps tables in parallel (configurable thread count)
- Produces consistent snapshots using `FTWRL` or `--trx-consistency-only`
- Outputs per-table files that `bintrail baseline` can process independently
- Supports both SQL INSERT and TSV (`*.dat`) output formats

---

## Installing mydumper

mydumper is a standalone binary — it is **not** bundled with bintrail. Install it before using `bintrail dump`.

### Ubuntu / Debian

```sh
# From the mydumper releases page (recommended)
wget https://github.com/mydumper/mydumper/releases/latest/download/mydumper_0.16.7-3.jammy_amd64.deb
sudo dpkg -i mydumper_*.deb

# Or from the system repository (may be older)
sudo apt-get install mydumper
```

### RHEL / CentOS / Amazon Linux

```sh
wget https://github.com/mydumper/mydumper/releases/latest/download/mydumper-0.16.7-3.el8.x86_64.rpm
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
| `--mydumper-path` | `mydumper` | Path to the mydumper binary |
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

When a single schema is given, bintrail passes `--database <schema>` to mydumper. When multiple schemas are given, it constructs a regex filter (`--regex ^(s1|s2)\.`). Table filtering uses mydumper's `--tables-list` flag.

### What mydumper flags does bintrail pass?

bintrail always passes these flags to mydumper:

| mydumper flag | Purpose |
|---|---|
| `--host`, `--port`, `--user`, `--password` | Connection details (parsed from `--source-dsn`) |
| `--outputdir` | Output directory |
| `--threads` | Parallelism |
| `--compress-protocol` | Compress the MySQL protocol traffic |
| `--complete-insert` | Generate `INSERT INTO table (col1, col2, ...) VALUES (...)` with column names — required for `bintrail baseline` to parse the output correctly |

### Concurrency protection

Only one `bintrail dump` can run at a time. A lockfile at `$TMPDIR/bintrail-dump.lock` prevents concurrent runs. If a previous dump crashed without cleaning up, bintrail detects the stale lock (by checking if the PID is still alive) and removes it automatically.

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

See [Scenario H in the Practical Guide](guide.md#scenario-h-uploading-baseline-parquet-files-to-s3) for full S3 setup instructions.

### No database connection required

`bintrail baseline` reads only files — it never connects to MySQL. This means you can:

- Run the conversion on a different machine from where the dump was taken
- Re-run the conversion with different options (compression, row group size) without re-dumping
- Archive the mydumper output and convert it later

---

## When to run a dump

### Initial setup

Run a dump once when you first set up bintrail, before starting to index binlog events. This captures the starting state of your data:

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
  >> /var/log/bintrail-baseline.log 2>&1
```

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

The dump frequency depends on your recovery and audit requirements. Bintrail's binlog index captures every change between baselines, so even infrequent baselines provide full coverage when combined with the change log.

---

## Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| `mydumper not found ("mydumper")` | mydumper is not installed or not on `$PATH` | Install mydumper (see above) or use `--mydumper-path` |
| `another dump is already running` | A previous dump is still running or crashed | Wait for it to finish, or check if the PID in `$TMPDIR/bintrail-dump.lock` is still alive. Stale locks from crashed processes are cleaned up automatically on the next run. |
| `mydumper failed: exit status 2` | mydumper itself encountered an error (wrong credentials, unreachable host, etc.) | Check mydumper's stderr output for details. Verify the `--source-dsn` is correct. |
| Baseline produces no files | mydumper output directory is empty or has no table data files | Verify the dump ran successfully and the `--schemas`/`--tables` filters match existing tables. |
| `--timestamp: expected ISO 8601 format` | Invalid timestamp override format | Use `2026-03-02T14:30:00Z` or `2026-03-02 14:30:00` format. |
