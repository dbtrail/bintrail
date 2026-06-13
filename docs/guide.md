# dbtrail — Practical Guide for DBAs

dbtrail indexes every INSERT, UPDATE, and DELETE from MySQL ROW-format binary logs into a queryable MySQL database, and generates reversal SQL for recovery — without needing the original binlog files. This guide is for DBAs who need scenario-driven walkthroughs and troubleshooting help.

---

## 0. Where to run dbtrail

dbtrail ingests changes two ways:

- **`bintrail stream`** (and `bintrail up`, which wraps it) connects over the MySQL **replication protocol** — no access to binlog files on disk. This is the default, the only option for managed MySQL (RDS, Aurora, Cloud SQL), and runs anywhere with a TCP path to the source. See [Streaming](streaming.md).
- **`bintrail index`** reads binlog **files** from a local path (`--binlog-dir`). Use it to **backfill** historical files on self-managed MySQL; it never reads remote, SSH, or object-storage paths, and it opens files read-only. See [Indexing](indexing.md).

| | `bintrail index` | `bintrail stream` |
|---|---|---|
| Needs filesystem access to binlogs | Yes | No |
| Works with managed MySQL (RDS/Aurora/Cloud SQL) | No | Yes |
| Backfills historical data | Yes | No — from the start position forward |
| Shape | One-off (run and exit) | Long-running (self-checkpoints) |

Use both together if you like: `index` to backfill old files, then `stream` for ongoing real-time indexing.

---

## 1. Setup Checklist

Before you start:

- [ ] Source MySQL server has `binlog_format = ROW` and `binlog_row_image = FULL`
- [ ] A separate database (or schema) is available for the dbtrail index — it can be on the same server or a different one
- [ ] `bintrail` binary is installed and on your `$PATH`
- [ ] Your index DSN includes the database name: `user:pass@tcp(host:3306)/binlog_index`

**If you have filesystem access to binlog files** (self-managed MySQL, direct disk or NFS mount):
- [ ] Filesystem read access to the source server's binlog files

**If you are using managed MySQL** (RDS, Aurora, Cloud SQL — no binlog file access):
- [ ] Use `bintrail stream` instead of `bintrail index` — it connects over the replication protocol
- [ ] Replication user with `REPLICATION SLAVE` and `REPLICATION CLIENT` privileges on the source
- [ ] Source DSN uses TCP: `user:pass@tcp(host:3306)/` (unix socket is not supported for replication)

---

## 2. First-Time Setup

The [Quickstart](quickstart.md) gets you running — the web console (`+ Add server`), or `bintrail up` on the command line (preflight + init + snapshot + stream). This guide assumes you're up and focuses on the day-to-day scenarios below.

**Tip — skip the repeated flags.** Instead of passing `--index-dsn`/`--source-dsn` on every command, generate a config file once:

```sh
bintrail config init
# Edit .bintrail.env and set BINTRAIL_INDEX_DSN and BINTRAIL_SOURCE_DSN
```

All commands load it automatically; CLI flags take precedence. Use `--global` to write `~/.config/bintrail/config.env` instead.

---

## 3. Scenario Walkthroughs

Each scenario follows: **Situation → Find it → Fix it → Verify**

---

### Scenario A: Someone accidentally deleted rows

**Situation:** A DELETE ran against the wrong rows in production. You need to restore them.

**Find it** — query the index for DELETEs in the window when the accident happened:

```sh
bintrail query \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --schema     mydb \
  --table      orders \
  --event-type DELETE \
  --since      "2026-02-19 14:00:00" \
  --until      "2026-02-19 14:05:00"
```

Review the output to confirm you've identified the right rows. The `row_before` column shows what the row contained before deletion.

**Preview the fix** — dry-run the recovery to see the SQL before applying anything:

```sh
bintrail recover \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --schema     mydb \
  --table      orders \
  --event-type DELETE \
  --since      "2026-02-19 14:00:00" \
  --until      "2026-02-19 14:05:00" \
  --dry-run
```

The output will be `INSERT INTO orders ...` statements reconstructed from the `row_before` images.

**Generate the recovery file:**

```sh
bintrail recover \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --schema     mydb \
  --table      orders \
  --event-type DELETE \
  --since      "2026-02-19 14:00:00" \
  --until      "2026-02-19 14:05:00" \
  --output     recovery.sql
```

**Review and apply:**

```sh
# Always read the file before applying
cat recovery.sql

mysql -u root -p mydb < recovery.sql
```

**Verify** — query the index again or run a `SELECT` against the source to confirm the rows are back.

---

### Scenario B: A bad UPDATE went out — need to roll back column values

**Situation:** An UPDATE incorrectly changed a column (e.g., `status` set to `cancelled` instead of `shipped`). You need to restore the original values.

**Find it** — use `--changed-column` to find UPDATEs that touched the specific column:

```sh
bintrail query \
  --index-dsn     "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --schema        mydb \
  --table         orders \
  --event-type    UPDATE \
  --changed-column status \
  --since         "2026-02-19 09:00:00" \
  --until         "2026-02-19 09:30:00"
```

**Inspect the before/after images** — switch to JSON format to see full row data:

```sh
bintrail query \
  --index-dsn     "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --schema        mydb \
  --table         orders \
  --event-type    UPDATE \
  --changed-column status \
  --since         "2026-02-19 09:00:00" \
  --until         "2026-02-19 09:30:00" \
  --format        json
```

Each event in the JSON output has `row_before` and `row_after` objects so you can confirm what changed.

**Generate reversal SQL** — the generated UPDATE will `SET` the `row_before` values `WHERE` the current row matches `row_after`:

```sh
bintrail recover \
  --index-dsn     "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --schema        mydb \
  --table         orders \
  --event-type    UPDATE \
  --changed-column status \
  --since         "2026-02-19 09:00:00" \
  --until         "2026-02-19 09:30:00" \
  --dry-run
```

**Apply:**

```sh
bintrail recover ... --output recovery.sql
mysql -u root -p mydb < recovery.sql
```

---

### Scenario C: What changed in this table in the last hour?

**Situation:** You need to audit all changes to a table for a post-incident review.

**Query with a time window:**

```sh
bintrail query \
  --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --schema    mydb \
  --table     orders \
  --since     "2026-02-19 13:00:00" \
  --until     "2026-02-19 14:00:00" \
  --limit     500
```

The default table output shows: event type, PK, timestamp, GTID, and changed columns. Each row is one database event.

**Export for further analysis:**

```sh
# JSON — includes full row_before and row_after
bintrail query ... --format json > changes.json

# CSV — for spreadsheets or grep
bintrail query ... --format csv > changes.csv
```

---

### Scenario D: Roll back an entire transaction by GTID

**Situation:** You know a specific transaction (by GTID) caused data corruption and want to reverse all of its changes atomically.

**Find the GTID** — either from `SHOW BINLOG EVENTS` on MySQL, from your application logs, or from bintrail query output (the `gtid` column).

**Inspect what the transaction touched:**

```sh
bintrail query \
  --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --gtid      "3e11fa47-71ca-11e1-9e33-c80aa9429562:42"
```

**Generate reversal SQL for the entire transaction:**

```sh
bintrail recover \
  --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --gtid      "3e11fa47-71ca-11e1-9e33-c80aa9429562:42" \
  --output    recovery.sql
```

The recovery script reverses events in reverse chronological order — the last change in the transaction is undone first.

**Review and apply:**

```sh
cat recovery.sql
mysql -u root -p mydb < recovery.sql
```

---

### Scenario E: Find all changes to a specific row (audit trail)

**Situation:** A customer reports their account data was altered. You need the full history of changes to that row.

**Query by primary key:**

```sh
bintrail query \
  --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --schema    mydb \
  --table     users \
  --pk        12345
```

For composite primary keys, use pipe-delimited values in column ordinal order:

```sh
bintrail query \
  --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --schema    mydb \
  --table     order_items \
  --pk        '12345|2'
```

Results are in chronological order — you can trace the full lifecycle of the row from INSERT through every UPDATE to the final state (or DELETE).

**Export a full audit record:**

```sh
bintrail query \
  --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --schema    mydb \
  --table     users \
  --pk        12345 \
  --format    json > audit-user-12345.json
```

The JSON output includes `row_before` and `row_after` for every event, giving a complete before/after picture for each change.

---

### Scenario F: Disk is filling up — clean old index data

**Situation:** the index database is growing and you need to reclaim space.

Check what you're holding with `bintrail status`, then drop partitions past a retention window:

```sh
# Reclaim space: drop partitions older than 7 days, no replacements
bintrail rotate \
  --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --retain 7d --no-replace
```

Dropping is a single `ALTER TABLE … DROP PARTITION` — far faster than a `DELETE`. Without `--no-replace`, `--retain` keeps a constant rolling window (drops old, adds the same number of future partitions); `--add-future N` extends the range if `p_future` is holding data. Full options and scheduling are in [Rotation and Status](rotation-and-status.md#automating-rotation).

---

### Scenario G: Archiving partitions to S3

**Situation:** keep a long-term, queryable history outside the index by archiving old partitions to Parquet (local or S3) before dropping them.

```sh
bintrail rotate \
  --index-dsn         "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --retain            7d \
  --archive-dir       /tmp/rotate-staging \
  --archive-s3        s3://my-bintrail-archives/events/ \
  --archive-s3-region us-east-1
```

Files are written locally first, then uploaded (Hive-partitioned, Athena/Glue/DuckDB-compatible); re-run with `--retry` after a partial failure. Once archived, `query` and `recover` **discover the archives automatically** (from `archive_state`) and merge them with live results — no extra flags (use `--no-archive` to skip). The full setup — creating and locking down the bucket, the minimum IAM policy, the AWS credential chain — is in [Rotation and Status](rotation-and-status.md#archiving-partitions-to-parquet) and [Upload](upload.md).

---

### Scenario H: Creating a baseline snapshot with mydumper

**Situation:** capture the current full state of your tables — before you start indexing, or as periodic audit snapshots.

```sh
# 1. Dump the source (mydumper; auto-run via Docker if installed)
bintrail dump --source-dsn "user:pass@tcp(source-db:3306)/" --output-dir /tmp/mydumper-output --schemas mydb

# 2. Convert to Parquet (no DB connection — reads files only)
bintrail baseline --input /tmp/mydumper-output --output /data/baselines
```

Output is one `.parquet` per table under `<timestamp>/<schema>/`. Baselines power full-row time-travel (`bintrail reconstruct` and the console's Time-travel view). Full flags, mydumper install, scheduling, and troubleshooting: [Dump and Baseline](dump-and-baseline.md).

---

### Scenario I: Uploading baseline Parquet files to S3

**Situation:** store baseline Parquet (Scenario H) in S3 for long-term retention.

```sh
bintrail baseline --input /path/to/mydumper-output --output /tmp/baselines \
  --upload s3://bintrail-audit-baselines/baselines/ --upload-region us-east-1
```

`--upload` writes locally then uploads (re-run with `--retry` to resume). The IAM policy, credential chain, and `aws s3 sync` / storage-class options are in [Upload](upload.md).

---

### Scenario J: Streaming from managed MySQL (RDS, Aurora, Cloud SQL)

**Situation:** continuous real-time indexing from a managed service with no binlog file access.

Grant `REPLICATION SLAVE, REPLICATION CLIENT` (plus `SELECT` for schema snapshots) on the source — see [the source user](quickstart.md#prerequisites) — then start streaming. `bintrail up` (or `stream`) auto-discovers the current binlog position on first run and resumes from its checkpoint afterward:

```sh
bintrail up \
  --source-dsn "bintrail_repl:secret@tcp(mydb.us-east-1.rds.amazonaws.com:3306)/" \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index"
```

`up` runs the preflight, creates the index, snapshots, streams, and rotates hourly — run it under systemd (`Restart=always`). To replay from an earlier point, use `bintrail stream --start-gtid ... --reset`. RDS gotchas (backup-retention enables binlog, stream from the primary, retention cap), the `--ssl-mode` TLS options, and metrics are in [Streaming](streaming.md); the command-by-command walkthrough is [Quickstart Option B](quickstart.md#option-b--command-line).

---

### Scenario K: Using dbtrail from Claude (AI-assisted investigation)

**Situation:** You want to investigate database changes in natural language from Claude — Claude Code, Claude Desktop, or claude.ai — instead of typing CLI commands.

dbtrail ships an MCP server that exposes `query`, `recover`, `status`, and `list_schema_changes` as AI tools. Once connected, you can ask:

```
"What tables had deletions in the last hour?"
"Show me all changes to the orders table since 2pm today"
"Someone deleted customer 42 — generate SQL to restore them"
"What was the status of order 1234 before the last update?"
"Which customer was modified the most this week?"
```

Claude calls the tools automatically and presents the results. Setup depends on where Claude runs:

- **Claude Code, same machine** (stdio) and **Claude Desktop, remote** (the `proxy.py` bridge): see [mcp-server.md → Connect Claude](mcp-server.md#connect-claude).
- **claude.ai / Claude mobile** (the network Connector, via a gateway you self-host or dbtrail's hosted one — an advanced path): see [mcp-server.md → claude.ai and Claude mobile](mcp-server.md#claudeai-and-claude-mobile).

---

### Scenario L: Debug logging

**Situation:** Something isn't indexing correctly and you need verbose output to diagnose it.

Enable debug-level structured logging with JSON format for easy filtering:

```sh
bintrail --log-level debug --log-format json stream \
  --index-dsn  "..." \
  --source-dsn "..." \
  --server-id  99999 \
  2>debug.log
```

Filter the log with `jq`:

```sh
# Show only errors
tail -f debug.log | jq 'select(.level == "ERROR")'

# Show only events for a specific table
tail -f debug.log | jq 'select(.table == "orders")'

# Show batch flush timing
tail -f debug.log | jq 'select(.msg | contains("batch"))'
```

Redirect stderr only (stdout still shows query output):

```sh
bintrail --log-level debug query --index-dsn "..." --schema mydb --table orders 2>debug.log
```

---

## 4. Keeping dbtrail Running (Day-to-Day)

**Re-indexing new binlog files:** Just run `index --all` again. Files already marked `completed` are skipped automatically — re-running is always safe.

**Running `stream` as a service:** `bintrail stream` is a long-running process — it runs indefinitely and self-checkpoints every 10 seconds (configurable). Run it under systemd with `Type=simple` and `Restart=always` so it automatically recovers from network interruptions. On restart it resumes from the last saved checkpoint in `stream_state` — no `--start-gtid` or `--start-file` needed. See [deployment.md](./deployment.md) for a ready-to-use systemd unit template.

**After schema changes:** If you ran `ALTER TABLE`, `CREATE TABLE`, or `DROP TABLE` on the source, re-run `snapshot` so the indexer has current column metadata:

```sh
bintrail snapshot \
  --source-dsn "user:pass@tcp(source-db:3306)/" \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index"
```

**Health check:** Run `status` at any time to see indexed files, partition sizes, and event counts:

```sh
bintrail status --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index"
```

**Automation:** See [deployment.md](./deployment.md) for ready-to-use recipes with cron, systemd, Docker Compose, and Ansible.

---

## 5. Troubleshooting FAQ

| Problem | Cause | Fix |
|---------|-------|-----|
| `source server has binlog_row_image="MINIMAL"; bintrail requires FULL` | Source server not configured for full row images | Add `binlog_row_image = FULL` to `my.cnf` and restart MySQL. Note: only affects new binlog files written after the restart. |
| `WARNING: column count mismatch for mydb.orders` (logged during index) | Table schema changed since last snapshot | Re-run `bintrail snapshot` — indexing continues but skips this table until the snapshot is updated. |
| `no schema snapshot exists and --source-dsn was not provided` | First `index` run without a prior snapshot and no `--source-dsn` to auto-snapshot | Either add `--source-dsn` to the `index` command (it will auto-snapshot), or run `bintrail snapshot` first. |
| `--index-dsn must include a database name` | DSN is missing the `/database` component | Use format `user:pass@tcp(host:3306)/binlog_index`. The database name is required. |
| `warning: p_future partition contains data` (printed by rotate) | Events arrived beyond the last named partition | Run `bintrail rotate --add-future N` to extend the partition range. |
| `no binlog files found in "/path/to/dir"` | Wrong `--binlog-dir` or binlog files not yet copied | Verify the path with `ls /path/to/dir`. Binlog files are typically named `binlog.000001` etc. Use `docker cp` if the files are inside a container. |
| Recovery SQL uses `WHERE col1 = ? AND col2 = ?` for all columns (verbose) | No schema snapshot available, so dbtrail falls back to matching all columns | Run `bintrail snapshot` — once a snapshot is available, recovery uses the primary key only. |
| `failed to connect to index database: ...` | Wrong DSN, MySQL not running, or network issue | Verify the DSN is correct and test connectivity: `mysql -u user -p -h host -P 3306 binlog_index`. |
| Index files stuck as `in_progress` | Previous `index` run crashed or was killed | Re-run `bintrail index` — `in_progress` files are retried automatically. |
| `auto-discover binlog position: ...` on `bintrail stream` first run | The default auto-discovery (`SHOW BINARY LOG STATUS` / `SHOW MASTER STATUS`) failed — usually because `log_bin=OFF` on the source, or the user lacks `REPLICATION CLIENT` | Enable binary logging on the source (or override with an explicit `--start-file`/`--start-pos` or `--start-gtid`). On RDS, set `binlog_format=ROW` in the parameter group and ensure `backup-retention-period > 0`. |
| Stream replication lag growing | High write rate on source, slow index DB, or large batches | Try increasing `--batch-size` (reduces round-trips), check index DB load, and monitor `bintrail_stream_replication_lag_seconds` via Prometheus. |
| `unix socket; binlog replication requires TCP` | Source DSN uses a unix socket path | Switch `--source-dsn` to TCP format: `user:pass@tcp(host:3306)/`. The replication protocol does not work over unix sockets. |

---

## 6. DSN Quick Reference

Every bintrail command that talks to MySQL uses the same DSN format:

```
user:password@tcp(host:port)/database_name
```

Examples:

```
root:secret@tcp(127.0.0.1:3306)/binlog_index
appuser:p@ssw0rd@tcp(db.internal:3306)/binlog_index
```

**Special characters in passwords** must be URL-encoded. For example, `p@ss#word` becomes `p%40ss%23word`. When in doubt, wrap passwords in single quotes in shell scripts and use a password without special characters for the index DSN.

**Source DSN** (used with `--source-dsn`) does not require a database name since dbtrail reads from `information_schema`:

```
root:secret@tcp(source-db:3306)/
```
