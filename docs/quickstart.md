# Bintrail Quickstart

Bintrail records every INSERT, UPDATE, and DELETE from MySQL into a searchable index. When something goes wrong, you can find exactly what changed and generate SQL to undo it.

This guide walks through the complete lifecycle from zero to recovery in about 10 minutes.

---

## Before You Start

You need:

- MySQL with `binlog_format = ROW` and `binlog_row_image = FULL` (check with `SHOW VARIABLES LIKE 'binlog_%';`)
- A MySQL database to use as the index (can be on the same server — call it `binlog_index`)
- The `bintrail` binary installed and on your `$PATH`

Set a shorthand for your index DSN so you don't retype it:

```sh
export IDX="root:secret@tcp(127.0.0.1:3306)/binlog_index"
```

Or generate a `.bintrail.env` configuration file and set your DSN there — all commands load it automatically:

```sh
bintrail config init        # creates .bintrail.env in the current directory
# Edit .bintrail.env and uncomment BINTRAIL_INDEX_DSN=...
```

---

## Step 1 — Create the index tables

```sh
bintrail init --index-dsn "$IDX"
```

Expected output:
```
Database "binlog_index" created (or already exists).
Tables created: binlog_events, schema_snapshots, index_state
Partitions created: 48 hourly partitions + p_future catch-all
```

This creates the `binlog_index` database (if it doesn't exist) and all the tables bintrail needs. **Run once.**

---

## Step 2 — Capture your schema

Bintrail needs to know your table structure (column names, primary keys) to build useful output.

```sh
bintrail snapshot \
  --source-dsn "root:secret@tcp(127.0.0.1:3306)/" \
  --index-dsn  "$IDX"
```

Expected output:
```
Snapshotting schemas: mydb, information_schema, ...
Snapshot complete: 42 tables captured (snapshot_id=1)
```

Re-run this any time you `ALTER TABLE` on the source. The old snapshot is kept — bintrail uses the most recent one.

---

## Step 3 — Index your binlog files

```sh
bintrail index \
  --index-dsn  "$IDX" \
  --source-dsn "root:secret@tcp(127.0.0.1:3306)/" \
  --binlog-dir /var/lib/mysql \
  --all
```

Expected output:
```
Source: binlog_row_image=FULL ✓
Indexing binlog.000042 ... 12345 events indexed
Indexing binlog.000043 ... 8901 events indexed
indexing complete files_processed=2 events_indexed=21246
```

`--all` processes every binlog file in `--binlog-dir`. Files already indexed are skipped automatically, so re-running is safe.

> **Using RDS, Aurora, or Cloud SQL?** You don't have access to binlog files on disk. Use `bintrail stream` instead — see [Streaming](./streaming.md).

---

## Step 4 — Query what changed

Now you can search the index. Some examples:

**Everything that happened to the `orders` table in the last hour:**

```sh
bintrail query \
  --index-dsn "$IDX" \
  --schema    mydb \
  --table     orders \
  --since     "2026-02-19 14:00:00" \
  --until     "2026-02-19 15:00:00"
```

**Only DELETEs:**

```sh
bintrail query \
  --index-dsn  "$IDX" \
  --schema     mydb \
  --table      orders \
  --event-type DELETE \
  --since      "2026-02-19 14:00:00"
```

**Full history of a specific row (by primary key):**

```sh
bintrail query \
  --index-dsn "$IDX" \
  --schema    mydb \
  --table     orders \
  --pk        12345
```

**All changes to the `status` column:**

```sh
bintrail query \
  --index-dsn      "$IDX" \
  --schema         mydb \
  --table          orders \
  --changed-column status
```

Add `--format json` to any query to see the full before and after values for each event.

---

## Step 5 — Undo something

When you've found the events you want to reverse, generate the recovery SQL:

**Preview (dry run):**

```sh
bintrail recover \
  --index-dsn  "$IDX" \
  --schema     mydb \
  --table      orders \
  --event-type DELETE \
  --since      "2026-02-19 14:00:00" \
  --until      "2026-02-19 14:05:00" \
  --dry-run
```

This prints the SQL without writing any files. Review it.

**Write to a file:**

```sh
bintrail recover \
  --index-dsn  "$IDX" \
  --schema     mydb \
  --table      orders \
  --event-type DELETE \
  --since      "2026-02-19 14:00:00" \
  --until      "2026-02-19 14:05:00" \
  --output     recovery.sql

cat recovery.sql          # always review before applying
mysql -u root -p mydb < recovery.sql
```

The script is wrapped in `BEGIN`/`COMMIT` and reverses events in reverse chronological order (most recent first).

---

## Step 6 — Keep the index clean

The `binlog_events` table will keep growing. Drop old data with `rotate`:

```sh
# Drop everything older than 7 days (auto-adds replacement future partitions)
bintrail rotate --index-dsn "$IDX" --retain 7d
```

This is fast — it's a metadata operation, not row-by-row deletion. Run it from a cron job:

```sh
# /etc/cron.d/bintrail-rotate
0 * * * * root bintrail rotate --index-dsn "$IDX" --retain 7d
```

---

## Step 7 — Check what's indexed

At any time:

```sh
bintrail status --index-dsn "$IDX"
```

Output:
```
=== Indexed Files ===
FILE              STATUS     EVENTS  STARTED_AT           COMPLETED_AT         ERROR  BINTRAIL_ID
binlog.000042     completed  12345   2026-02-19 10:00:00  2026-02-19 10:00:42  -      abc123...
binlog.000043     completed  8901    2026-02-19 10:00:43  2026-02-19 10:01:12  -      abc123...

=== Partitions ===
PARTITION       LESS_THAN               ROWS (est.)
p_2026021914    2026-02-19 15:00 UTC    12345
...
p_future        MAXVALUE                0
Total events (est.): 21246

=== Summary ===
Server abc123de-0000-0000-0000-000000000001
  Files:  2 completed, 0 in_progress, 0 failed
  Events: 21246 indexed
```

---

## The Full Picture

```
bintrail init          ← run once
       ↓
bintrail snapshot      ← run once, re-run after schema changes
       ↓
bintrail index --all   ← run regularly (cron) or use stream for real-time
       ↓
bintrail query         ← investigate changes
       ↓
bintrail recover       ← generate undo SQL when needed
       ↓
bintrail rotate        ← run hourly to drop old partitions and stay clean
       ↓
bintrail status        ← check at any time
```

---

## Next Steps

| Want to... | Read... |
|---|---|
| Use RDS, Aurora, or Cloud SQL | [Streaming](./streaming.md) |
| Understand the query and recovery options in depth | [Query and Recovery](./query-and-recovery.md) |
| Archive old events to S3 before dropping | [Rotation and Status](./rotation-and-status.md#archiving-partitions-to-parquet) |
| Use AI (Claude) to investigate changes | [MCP Server](./mcp-server.md) |
| Set up cron, systemd, Docker | [Guide](./guide.md) |
| Understand server identity and access flags | [Server Identity](./server-identity.md) |
