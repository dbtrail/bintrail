# Bintrail — Practical Guide for DBAs

Bintrail indexes every INSERT, UPDATE, and DELETE from MySQL ROW-format binary logs into a queryable MySQL database, and generates reversal SQL for recovery — without needing the original binlog files. This guide is for DBAs who need scenario-driven walkthroughs and troubleshooting help.

---

## 1. Setup Checklist

Before you start:

- [ ] Source MySQL server has `binlog_format = ROW` and `binlog_row_image = FULL`
- [ ] A separate database (or schema) is available for the bintrail index — it can be on the same server or a different one
- [ ] `bintrail` binary is installed and on your `$PATH`
- [ ] You have filesystem read access to the source server's binlog files (local disk, NFS mount, or `docker cp`)
- [ ] Your index DSN includes the database name: `user:pass@tcp(host:3306)/binlog_index`

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
Partitions created: 7 daily partitions + p_future catch-all
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

The output shows each partition, its date range, and estimated row counts. Identify how many days of history you're holding.

**Drop old partitions:**

```sh
# Keep last 7 days, add 3 new future partitions
bintrail rotate \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --retain     7d \
  --add-future 3
```

Partitions older than 7 days are dropped in a single `ALTER TABLE … DROP PARTITION` statement — much faster than `DELETE` on InnoDB.

**Extend the partition range** if the `p_future` catch-all is holding data:

```sh
bintrail rotate \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --add-future 14
```

---

## 4. Keeping Bintrail Running (Day-to-Day)

**Re-indexing new binlog files:** Just run `index --all` again. Files already marked `completed` are skipped automatically — re-running is always safe.

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
