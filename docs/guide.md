# Bintrail — Practical Guide for DBAs

Bintrail indexes every INSERT, UPDATE, and DELETE from MySQL ROW-format binary logs into a queryable MySQL database, and generates reversal SQL for recovery — without needing the original binlog files. This guide is for DBAs who need scenario-driven walkthroughs and troubleshooting help.

---

## 0. Where to Run Bintrail

Where you install and run the `bintrail` binary depends on which indexing mode you use.

---

### Mode A: File-based indexing (`bintrail index`)

`bintrail index` reads binlog files directly from the local filesystem using `--binlog-dir`. There is **no remote file access** — the path must be readable by the process running bintrail.

```
┌─────────────────────────────────────────────────────┐
│  Host with access to binlog files                   │
│                                                     │
│  /var/lib/mysql/binlog.000042   ◄──── bintrail      │
│  /var/lib/mysql/binlog.000043         index         │
└─────────────────────────────────────────────────────┘
                                             │
                                             │ TCP (MySQL protocol)
                                             ▼
                                    ┌─────────────────┐
                                    │  Index Database  │
                                    │  (binlog_index)  │
                                    └─────────────────┘
```

**Typical deployment locations:**

| Where to run bintrail | When to use it |
|---|---|
| On the MySQL server itself | Simplest option for self-managed MySQL. `--binlog-dir /var/lib/mysql`. |
| On a replica host | Point `--binlog-dir` at the replica's own binlog directory (requires `log_bin` enabled on the replica). Useful to avoid load on the primary. |
| On any host with an NFS/CIFS mount | Mount the MySQL data directory read-only and point `--binlog-dir` at the mount point. |
| Inside a Docker container | Mount the binlog directory read-only: `-v /var/lib/mysql:/var/lib/mysql:ro`. |
| After copying files with rsync/SCP | Copy binlog files to a staging directory, run bintrail there, delete afterwards. |

**What does NOT work:**

- Pointing `--binlog-dir` at an SSH or SFTP path (`/ssh/host/var/lib/mysql`) — the flag is a plain filesystem path
- Pointing `--binlog-dir` at a remote HTTP or object-storage URL
- Running bintrail on a machine with no access to the binlog files and hoping `--source-dsn` provides them — `--source-dsn` is only used for schema metadata and format validation, not for reading binlog file content

**Bintrail never modifies binlog files.** It opens them read-only. It is safe to point `--binlog-dir` at the live MySQL data directory.

---

### Mode B: Replication streaming (`bintrail stream`)

`bintrail stream` connects to MySQL over the standard network replication protocol, receiving binlog events as they are committed. It requires **no access to binlog files on disk** — only a TCP connection to MySQL with a replication-privileged user.

```
                    TCP (MySQL replication protocol)
bintrail stream ────────────────────────────────► Source MySQL
                                                  (any location)
       │
       │ TCP (MySQL protocol)
       ▼
┌─────────────────┐
│  Index Database  │
│  (binlog_index)  │
└─────────────────┘
```

**Bintrail can run anywhere** that has a TCP path to the source MySQL server — your laptop, a CI runner, a separate host, or a container.

This is the required mode for managed MySQL services (Amazon RDS, Aurora, Google Cloud SQL, Azure Database for MySQL) where binlog files are not accessible on disk.

---

### Choosing a mode

| | `bintrail index` | `bintrail stream` |
|---|---|---|
| **Requires filesystem access** | Yes — binlog files must be readable locally | No |
| **Requires MySQL TCP access** | Optional (for validation + auto-snapshot) | Yes (always) |
| **Works with managed MySQL** | No (no disk access on RDS/Aurora) | Yes |
| **Backfills historical data** | Yes — indexes any binlog file, including archived ones | No — only events from the starting position forward |
| **Suitable for one-off catchup** | Yes (run and exit) | No (long-running process) |
| **Suitable for continuous indexing** | Yes (cron or timer) | Yes (runs indefinitely, self-checkpoints) |

You can use **both modes together**: use `bintrail index` to backfill historical binlog files, then switch to `bintrail stream` for ongoing real-time indexing from the current position.

---

## 1. Setup Checklist

Before you start:

- [ ] Source MySQL server has `binlog_format = ROW` and `binlog_row_image = FULL`
- [ ] A separate database (or schema) is available for the bintrail index — it can be on the same server or a different one
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

Three commands to get running. Run them once on initial setup.

**Step 1 — Create index tables:**

```sh
bintrail init --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index"
```

Expected output:
```
Database "binlog_index" created (or already exists).
Tables created: binlog_events, schema_snapshots, index_state
Partitions created: 48 hourly partitions + p_future catch-all
```

**Step 2 — Snapshot schema metadata:**

```sh
bintrail snapshot \
  --source-dsn "user:pass@tcp(source-db:3306)/" \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index"
```

Expected output:
```
Snapshotting schemas: mydb, appdb, ...
Snapshot complete: 42 tables captured (snapshot_id=1)
```

**Step 3 — Index binlog files:**

```sh
bintrail index \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --source-dsn "user:pass@tcp(source-db:3306)/" \
  --binlog-dir /var/lib/mysql \
  --all
```

Expected output:
```
Source: binlog_row_image=FULL ✓
Indexing binlog.000042 ... 12345 events indexed
Indexing binlog.000043 ... 8901 events indexed
Done: 2 files, 21246 events total
```

After this, the index is live. See [Section 4](#4-keeping-bintrail-running-day-to-day) for ongoing automation.

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

**Situation:** The bintrail index database is growing large and you need to reclaim space by dropping old partitions.

**Check the current state:**

```sh
bintrail status --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index"
```

The output shows each partition, its hour boundary, and estimated row counts. Identify how many hours of history you're holding.

**Drop old partitions and reclaim space:**

```sh
# Drop partitions older than 7 days without auto-adding replacements
bintrail rotate \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --retain     7d \
  --no-replace
```

Partitions older than the retain threshold are dropped in a single `ALTER TABLE … DROP PARTITION` statement — much faster than `DELETE` on InnoDB. Use `--no-replace` when you genuinely want to reclaim space; without it, `--retain` automatically adds back the same number of future partitions to keep the rolling window size constant.

**Maintain a rolling window (same partition count):**

```sh
# Drop old partitions, auto-add the same number of replacement future ones
bintrail rotate \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --retain     7d
```

**Extend the partition range** if the `p_future` catch-all is holding data:

```sh
bintrail rotate \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --add-future 48
```

---

### Scenario G: Streaming from managed MySQL (RDS, Aurora, Cloud SQL)

**Situation:** You're using a managed MySQL service where you have no filesystem access to binlog files. You want continuous real-time indexing using the replication protocol.

**Prerequisites** — grant replication privileges on the source:

```sql
CREATE USER 'bintrail_repl'@'%' IDENTIFIED BY 'secret';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'bintrail_repl'@'%';
FLUSH PRIVILEGES;
```

Get the current binlog position for the first run:

```sql
SHOW MASTER STATUS;
-- Example output:
-- File: binlog.000123  Position: 4  Executed_Gtid_Set: 3e11fa47-...:1-5000
```

**First run** — one-time setup:

```sh
# Step 1: Create index tables
bintrail init --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index"

# Step 2: Snapshot schema metadata
bintrail snapshot \
  --source-dsn "bintrail_repl:secret@tcp(mydb.us-east-1.rds.amazonaws.com:3306)/" \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index"

# Step 3: Start streaming from the current GTID position
bintrail stream \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --source-dsn "bintrail_repl:secret@tcp(mydb.us-east-1.rds.amazonaws.com:3306)/" \
  --server-id  99999 \
  --start-gtid "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5000" \
  --metrics-addr :9090
```

**Subsequent runs** — the checkpoint is resumed automatically:

```sh
bintrail stream \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --source-dsn "bintrail_repl:secret@tcp(mydb.us-east-1.rds.amazonaws.com:3306)/" \
  --server-id  99999
# No --start-gtid needed — resumed from stream_state checkpoint
```

**Monitor** replication lag and throughput:

```sh
curl -s localhost:9090/metrics | grep bintrail_stream_replication_lag_seconds
```

**Graceful shutdown** — send `SIGTERM` (or Ctrl-C). The current batch is flushed and the checkpoint is written before exit.

**Run as a systemd service** for automatic restart — see the [systemd section in README](../README.md#automating-with-systemd). Use `Type=simple` and `Restart=always` since `stream` is long-running (unlike the one-shot `index` command).

---

### Scenario H: Using bintrail from Claude Desktop (AI-assisted investigation)

**Situation:** You want to use Claude Desktop (or Claude Code on another machine) to investigate database changes in natural language — without typing CLI commands.

Bintrail ships an MCP server that exposes `query`, `recover`, and `status` as AI tools. Once configured, you can ask Claude things like:

- "What got deleted in the last 10 minutes in the orders table?"
- "Bring back customer 289"
- "Which tables had the most activity today?"

Claude calls the tools automatically and presents the results.

---

#### Option A: Claude Code on the same machine (automatic)

If you're using Claude Code in the bintrail repo directory, the `.mcp.json` file registers the MCP server automatically. Just set the DSN:

```bash
export BINTRAIL_INDEX_DSN='user:pass@tcp(127.0.0.1:3306)/binlog_index'
```

The tools appear in Claude Code with no further configuration.

---

#### Option B: Claude Desktop on a different machine (via HTTP + proxy)

This setup lets any machine on the same network use the bintrail tools in Claude Desktop without installing Go.

**Architecture:**

```
Claude Desktop  →  proxy.py (stdio, local)  →  bintrail-mcp --http :8080  →  Index MySQL
```

**On the machine that has bintrail installed:**

```bash
# Build the binary if you haven't already
go build -o bintrail-mcp ./cmd/bintrail-mcp

# Start the HTTP MCP server with the index DSN
BINTRAIL_INDEX_DSN='user:pass@tcp(127.0.0.1:3306)/binlog_index' \
  ./bintrail-mcp --http :8080
```

> Use a process manager (systemd, launchd) or a `tmux` session to keep it running.

**On the remote machine (where Claude Desktop runs):**

1. Copy the proxy script (requires no dependencies):

   ```bash
   scp user@server:~/bintrail/cmd/bintrail-mcp/proxy.py ~/proxy.py
   ```

2. Test the connection before configuring Claude Desktop:

   ```bash
   BINTRAIL_SERVER=http://192.168.1.10:8080/mcp python3 ~/proxy.py <<'EOF'
   {"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}
   {"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}
   EOF
   ```

   You should see two JSON responses. If you do, the server is reachable.

3. Edit Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json`):

   ```json
   {
     "mcpServers": {
       "bintrail": {
         "command": "python3",
         "args": ["/Users/you/proxy.py"],
         "env": { "BINTRAIL_SERVER": "http://192.168.1.10:8080/mcp" }
       }
     }
   }
   ```

4. Restart Claude Desktop.

**Example prompts that work well:**

```
"What tables had deletions in the last hour?"
"Show me all changes to the orders table since 2pm today"
"Someone deleted customer 42 — generate SQL to restore them"
"What was the status of order 1234 before the last update?"
"Which customer was modified the most this week?"
```

**Troubleshooting:**

| Symptom | Cause | Fix |
|---------|-------|-----|
| "Both calls failed" | HTTP server not running or wrong IP | Re-run the curl test above; check firewall |
| Tools fail after server restart | Proxy has stale `Mcp-Session-Id` | Restart Claude Desktop to reset proxy |
| No bintrail tools in Claude Desktop | Config JSON is malformed | Validate JSON; check for missing braces |
| Recovery SQL looks wrong | Schema snapshot is stale | Re-run `bintrail snapshot` on the source |

---

### Scenario J: Debug logging

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

## 4. Keeping Bintrail Running (Day-to-Day)

**Re-indexing new binlog files:** Just run `index --all` again. Files already marked `completed` are skipped automatically — re-running is always safe.

**Running `stream` as a service:** `bintrail stream` is a long-running process — it runs indefinitely and self-checkpoints every 10 seconds (configurable). Run it under systemd with `Type=simple` and `Restart=always` so it automatically recovers from network interruptions. On restart it resumes from the last saved checkpoint in `stream_state` — no `--start-gtid` or `--start-file` needed. See the [README](../README.md#automating-with-systemd) for a ready-to-use systemd unit template.

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

**Automation:** See the [README](../README.md) for ready-to-use recipes with cron, systemd, Docker Compose, and Ansible.

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
| Recovery SQL uses `WHERE col1 = ? AND col2 = ?` for all columns (verbose) | No schema snapshot available, so bintrail falls back to matching all columns | Run `bintrail snapshot` — once a snapshot is available, recovery uses the primary key only. |
| `failed to connect to index database: ...` | Wrong DSN, MySQL not running, or network issue | Verify the DSN is correct and test connectivity: `mysql -u user -p -h host -P 3306 binlog_index`. |
| Index files stuck as `in_progress` | Previous `index` run crashed or was killed | Re-run `bintrail index` — `in_progress` files are retried automatically. |
| `no start position specified; provide --start-file or --start-gtid on the first run` | First `stream` run without a starting position | Run `SHOW MASTER STATUS` on the source and pass the result as `--start-gtid` (GTID mode) or `--start-file` (position mode). On subsequent runs the checkpoint is loaded automatically. |
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

**Source DSN** (used with `--source-dsn`) does not require a database name since bintrail reads from `information_schema`:

```
root:secret@tcp(source-db:3306)/
```
