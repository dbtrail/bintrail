# Dump and Baseline — Using mydumper with dbtrail

dbtrail uses [mydumper](https://github.com/mydumper/mydumper) to create logical dumps of MySQL databases. The dump output is then converted to Parquet files by `bintrail baseline`, producing a point-in-time snapshot of every table that can be stored alongside archived binlog event partitions for long-term audit reconstruction.

This document covers running dumps, converting to Parquet baselines, and scheduling.

---

## Why mydumper?

dbtrail's binlog index captures every change (INSERT, UPDATE, DELETE) but not the initial state of rows that existed before indexing began. A baseline snapshot fills that gap — it records every row as it existed at a known point in time.

mydumper is used instead of `mysqldump` because it:

- Dumps tables in parallel (configurable thread count)
- Produces a lightweight, lock-minimizing snapshot by default (`--sync-thread-lock-mode NO_LOCK --trx-tables`), so a least-privilege replication user can dump without `BACKUP_ADMIN`/`RELOAD` — see [Cross-table consistency](#cross-table-consistency) for what that trades away
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

This dumps **every accessible schema** into `/tmp/mydumper-output` — bare `bintrail dump` applies no schema filter, so mydumper also tries `mysql`, `sys`, and `performance_schema`. Pass `--schemas mydb,otherdb` to scope it to your data: a least-privilege capture user (no `SHOW VIEW` on the `sys` views) **needs** that filter or the dump fails loudly. (The bundled Compose `baseline` profile excludes system schemas automatically; the bare CLI does not.)

### All flags

| Flag | Default | Description |
|---|---|---|
| `--source-dsn` | *(required)* | DSN for the source MySQL server |
| `--output-dir` | *(required)* | Directory for mydumper output. Never blindly deleted: a non-empty directory that is not a recognizable prior dump is refused; a prior dump is moved aside and restored if the new dump fails (see [Output directory behavior](#output-directory-behavior)) |
| `--schemas` | *(all)* | Comma-separated schema filter (e.g. `mydb,otherdb`) |
| `--tables` | *(all)* | Comma-separated table filter (e.g. `mydb.orders,mydb.items`) |
| `--mydumper-path` | `mydumper` | Path to the mydumper binary. When set, skips Docker fallback. |
| `--mydumper-image` | `mydumper/mydumper:latest` | Docker image for mydumper. Used only when no local binary is found. |
| `--threads` | `4` | Number of parallel dump threads |
| `--encrypt` | `false` | Encrypt dump files at rest using AES-256-CBC (requires `openssl` on `$PATH`) |
| `--encrypt-key` | `~/.config/bintrail/dump.key` | Path to the encryption key file (generate with `bintrail generate-key`) |
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
| `--host`, `--port`, `--user` | Connection details (parsed from `--source-dsn`). The password is **not** passed as a flag — see below. |
| `--outputdir` | Output directory |
| `--threads` | Parallelism |
| `--compress-protocol` | Compress the MySQL protocol traffic |
| `--complete-insert` | Generate `INSERT INTO table (col1, col2, ...) VALUES (...)` with column names — required for `bintrail baseline` to parse the output correctly |

The source password never appears on mydumper's command line, so it is not visible in `ps aux` or (Docker mode) `docker inspect`. With a local mydumper binary it is delivered via `MYSQL_PWD` in the child process environment; in Docker mode it is written to a temporary `0600` MySQL option file bind-mounted read-only into the container, which mydumper reads via `--defaults-file`.

### Cross-table consistency

By default (`--sync-thread-lock-mode NO_LOCK --trx-tables`), every mydumper worker thread opens its own transactional snapshot **independently, with no synchronization barrier between threads**. Each table's snapshot is therefore anchored at *that thread's* instant, not at one shared instant for the whole dump. On a quiet source the skew between threads is negligible; on a write-heavy source it can be milliseconds to a few seconds. Two consequences follow:

- **The dump's recorded binlog coordinates don't correspond exactly to any single thread's actual snapshot instant** — they're an approximation across however many threads ran.
- **A reconstruct that spans multiple tables (e.g. a parent/child pair joined by a foreign key) can read mutually inconsistent state** — the parent table's snapshot may be a few rows ahead of or behind the child table's.

In practice this skew is almost always absorbed by dbtrail's idempotent delta replay and the baseline's second-granularity anchor, but it is a real gap, not a rounding error. It is also worse on non-transactional tables (MyISAM), which get no consistency guarantee at all under `NO_LOCK` — every row can be mid-write when read. mydumper still dumps a MyISAM table under `NO_LOCK` (it does not refuse), it just gives it no consistency guarantee — see the note on the `FTWRL` mode below, which behaves differently for the *same* table.

This is the trade for needing no elevated privilege: a replication user with only `SELECT` + `REPLICATION CLIENT` (no `BACKUP_ADMIN`/`RELOAD`) can run a `NO_LOCK` dump. Both `bintrail dump` (CLI) and the console's **Create baseline** button use this mode by default, and neither downgrades silently — if you need every table anchored at the exact same instant, use the console's opt-in point-consistent mode instead of a `NO_LOCK` dump: see [Creating a baseline from the console](console.md#creating-a-baseline-from-the-console) and the `BINTRAIL_CONSOLE_BASELINE_POINT_CONSISTENT` environment variable in [console.md](console.md). It runs mydumper's built-in `FTWRL` sync mode (`--sync-thread-lock-mode FTWRL --trx-tables`) — one global read lock held just long enough for every worker to open its snapshot at the same instant.

`FTWRL` mode covers **transactional tables only** — the same `--trx-tables` flag as the default mode, but it behaves differently under `FTWRL`: mydumper detects a non-transactional (MyISAM) table and **refuses to dump at all** ("Non transactional table found ... Restart backup using --trx-tables=0"), which the console propagates as the run's error, instead of silently proceeding the way it does under `NO_LOCK`. Verified empirically: the identical MyISAM table dumped successfully (with only a warning) under `NO_LOCK`, and was hard-refused under `FTWRL`, in the same test session — the refusal is gated to an actual "consistent backup attempt" (mydumper's own wording), which `NO_LOCK` explicitly is not attempting and `FTWRL` is.

`FTWRL` mode needs `RELOAD` or the `FLUSH_TABLES` dynamic privilege (for `FLUSH TABLES WITH READ LOCK`) on **every** source flavor, verified against the pinned mydumper build (`v1.0.3-1`) against a real MySQL 8.0 source — plus `BACKUP_ADMIN` (for `LOCK INSTANCE FOR BACKUP`) **only on MySQL/Percona 8.0+**. `BACKUP_ADMIN` is a MySQL 8.0+ dynamic privilege: it does not exist on MariaDB (any version) or MySQL 5.7, and neither of those issues `LOCK INSTANCE FOR BACKUP`, so `FTWRL` there needs only `RELOAD`/`FLUSH_TABLES`. The console detects this from the source's own `SELECT VERSION()` and checks for exactly the privileges that source actually needs **before** invoking mydumper, refusing with a clear, actionable error if any are missing — this is not just belt-and-suspenders: on a MySQL/Percona 8.0+ source, granting `BACKUP_ADMIN` **without** `RELOAD`/`FLUSH_TABLES` does not make mydumper fail cleanly, it makes the pinned build **crash** (a segfault, reproduced on both amd64 and arm64), so the console's own preflight check exists specifically to turn that crash into a clean error instead of ever letting mydumper attempt it half-privileged. Point-consistent mode never silently falls back to `NO_LOCK`. There is currently no CLI (`bintrail dump`) equivalent of this opt-in — it exists only on the console's in-process baseline pipeline.

### Concurrency protection

Only one `bintrail dump` can run at a time. A lockfile at `$TMPDIR/bintrail-dump.lock` prevents concurrent runs. If a previous dump crashed without cleaning up, dbtrail detects the stale lock (by checking if the PID is still alive) and removes it automatically.

### Output directory behavior

`bintrail dump` never blindly deletes the `--output-dir`:

- **Absent or empty** — used as-is (mydumper creates it if needed).
- **Non-empty and recognizable as a prior dump** (contains a `metadata` or `metadata.partial` file, or the `bintrail_dump_started_at_utc` marker) — moved aside to a unique sibling `<dir>.old-<pid>-<nanos>` before the new dump starts. The backup is deleted only after the new dump **succeeds**; if the dump fails, the previous dump is restored in place.
- **Non-empty and anything else** — the dump is **refused**:

  ```
  --output-dir "<dir>" is not empty and does not look like a prior bintrail/mydumper dump (no "metadata" marker); refusing to delete it. Remove it yourself or point --output-dir elsewhere
  ```

  This protects against a typo'd `--output-dir` (or a stray `BINTRAIL_OUTPUT_DIR` picked up from an env file) wiping an arbitrary directory — including baselines that `reconstruct`/`verify` depend on.

Source connectivity is also validated **before** the output directory is touched (`cannot connect to source; refusing to touch --output-dir ...`), so a dump that fails to connect never disturbs the previous dump.

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
| `--encrypt` | `false` | Decrypt encrypted dump files (`.enc`, from `dump --encrypt`) before processing (requires `openssl` on `$PATH`) |
| `--encrypt-key` | `~/.config/bintrail/dump.key` | Path to the decryption key file |
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

The timestamp defaults to the dump's start time, resolved in this order:

1. **The `bintrail dump` marker, if present.** `bintrail dump` records its own UTC wall-clock time immediately before invoking mydumper into a `bintrail_dump_started_at_utc` sidecar in the output directory. This is unambiguous — `bintrail baseline` prefers it whenever present.
2. **mydumper's `Started dump at:` metadata line, otherwise.** This line is written in the **dump host's local time**, but `bintrail baseline` parses it as if it were already UTC. If you produced the mydumper dump yourself (outside `bintrail dump` — e.g. running mydumper directly, or from another tool), the dump host's clock **must** be set to UTC (`TZ=UTC`), or every reconstruct/verify/shim consumer that anchors replay on this snapshot will be off by the host's UTC offset — on a UTC+2 host, for example, deltas in the resulting 2-hour window are silently excluded from replay.

The console's own **Create baseline** trigger runs mydumper and the Parquet conversion in the same process, so it passes its own captured UTC time straight through and is unaffected by the dump host's timezone either way.

Override the resolved timestamp with `--timestamp` if needed.

Each snapshot also records its **binlog anchor** (the file/position/GTID where the deltas on top of it begin) and, per table, a **content digest + row count** in the Parquet metadata (used by [`bintrail verify`](verify.md)). Baseline-anchored consumers (full-table and single-row `reconstruct`, the shim's `_snapshot`, `verify`, and cascade recovery's baseline fallback) fetch deltas using this recorded binlog **position** as the window's exact lower bound, not the snapshot's wall-clock timestamp: a row-event's timestamp reflects when its statement executed, not when its transaction committed, so a transaction that executed just before the snapshot instant but committed (and so was logged) just after it would otherwise be silently missed by a timestamp-only lower bound. Snapshots taken before this position was recorded (or that never recorded one) fall back to the timestamp alone. A `_SUCCESS` marker is written when the conversion completes; a partially-converted snapshot carries an `_INCOMPLETE` marker instead and is excluded from discovery (see [Pruning old local snapshots](#pruning-old-local-snapshots---baseline-retain)).

### At-rest integrity (the `_MANIFEST` sidecar)

Alongside each snapshot, `bintrail baseline` writes a `_MANIFEST` sidecar holding a **CRC-32C** over every Parquet file's bytes. Every local read path that consumes a baseline — full-table and single-row `reconstruct`, cascade recovery, the time-travel shim's `_snapshot`, and `query --include-snapshot` — **re-validates the CRC on every read** and **fails loud** on a mismatch (bit-rot, a truncated/partial write), rather than silently reconstructing from corrupt data. Snapshots created before this feature (no `_MANIFEST`) are read without validation, so it degrades gracefully. S3 read-validation is not yet covered in this release (the bytes can be re-encoded in transit); local reads are.

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

## Step 3 (optional): a newer baseline without a new dump

`bintrail reconstruct --output-format parquet` writes its result **as a baseline snapshot** instead of a SQL dump. Since the index already holds every change since the last snapshot, a fresher snapshot can be folded out of storage you already have — no mydumper run, no connection to the source:

```sh
bintrail reconstruct \
  --index-dsn     "user:pass@tcp(index-db:3306)/bintrail_index" \
  --baseline-dir  /data/baselines \
  --tables        mydb.orders,mydb.users \
  --output-format parquet \
  --output-dir    /data/baselines
```

`--output-dir` is the **baselines root** here, not the destination file: the snapshot lands in `/data/baselines/<timestamp>/<db>/<table>.parquet` with the same `_SUCCESS` / `_MANIFEST` files a converted dump gets, and the next `reconstruct`, `verify` or shim query discovers it as the newest baseline with no configuration change.

Why this matters beyond convenience: reconstructing from a **fresh** snapshot reads a short delta window instead of replaying months of events, so time-travel gets faster and the archive hours the replay depends on stop growing without bound.

**What it inherits.** The emitted snapshot is subject to every full-table reconstruct limit — the table needs a primary key, a PK-changing `UPDATE` in the window refuses the run, and a `TRUNCATE`/`DROP`/`RENAME` between the source snapshot and `--at` refuses it too. A table with no baseline at all is refused rather than degraded: a snapshot folded from deltas alone would silently omit every row the window never touched. Refuse cases point at `bintrail dump` — a real re-dump is the only correct answer for all of them.

**What it deliberately omits.** A dumped snapshot carries a content digest fingerprinting the rows against the live source, which `bintrail verify` compares. A reconstructed snapshot never read the source, so it carries **no** digest — that table is not verifiable against a source through this snapshot until the next real dump. It also carries no GTID set (the index stores GTIDs per event, not as an accumulated executed-set). Both absences are visible in the file's metadata, alongside a `bintrail.snapshot_producer = reconstruct` marker so a reconstructed snapshot stays distinguishable from a dumped one forever.

**Where the deltas resume.** The snapshot records the exact binlog coordinate the next reconstruct starts from — chosen as the position of the first transaction committed after `--at`, not derived from `--at` itself. Binlog row events carry the time a statement *executed*, not the time it *committed*, so a cut made on the timestamp alone can drop a transaction from both the snapshot and the following delta window. Cutting on position on both sides of the seam cannot: what one side ends at is exactly what the other starts from.

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
| `--output-dir ... does not look like a prior bintrail/mydumper dump ... refusing to delete it` | The output directory is non-empty and carries no `metadata`/`metadata.partial`/`bintrail_dump_started_at_utc` marker — dbtrail refuses to delete unrecognized content | Point `--output-dir` at an empty or dedicated dump directory, or remove the existing contents yourself if you are sure they are disposable. |
| `mydumper failed: exit status 2` | mydumper itself encountered an error (wrong credentials, unreachable host, etc.) | Check mydumper's stderr output for details. Verify the `--source-dsn` is correct. |
| Docker: `permission denied` on `/var/run/docker.sock` | Current user is not in the `docker` group | Run `sudo usermod -aG docker $USER` and log out/in, or use `sudo bintrail dump ...` |
| Docker: `Cannot connect to the Docker daemon` | Docker daemon is not running | Start Docker: `sudo systemctl start docker` (Linux) or open Docker Desktop (macOS) |
| Docker: mydumper cannot reach MySQL on localhost | On macOS/Windows, `--network host` does not work as on Linux | Use `host.docker.internal` instead of `localhost` in `--source-dsn` (e.g. `user:pass@tcp(host.docker.internal:3306)/`) |
| Docker: volume mount permission errors | Docker cannot write to the `--output-dir` path | Ensure the output directory's parent exists and is writable. On SELinux systems, add `:z` to the volume mount or use `--security-opt label=disable`. |
| Docker: dump files owned by root | Older dbtrail versions ran the container as root | Upgrade — dbtrail now passes `--user <uid>:<gid>` to `docker run` so dump files are owned by the invoking user. |
| Baseline produces no files | mydumper output directory is empty or has no table data files | Verify the dump ran successfully and the `--schemas`/`--tables` filters match existing tables. |
| `--timestamp: expected ISO 8601 format` | Invalid timestamp override format | Use `2026-03-02T14:30:00Z` or `2026-03-02 14:30:00` format. |
