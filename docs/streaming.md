# Streaming from MySQL

This page explains `bintrail stream` — the real-time indexing mode that connects to MySQL over the replication protocol instead of reading binlog files directly.

---

## The Source MySQL User

Before streaming (or monitoring a source from the console "+ Add server" form), create a user on the **source** MySQL with the privileges dbtrail needs. Run this on the source:

```sql
CREATE USER 'dbtrail'@'%' IDENTIFIED BY 'strong-password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'dbtrail'@'%';
```

That is the complete, minimal set. Each privilege maps to exactly one thing dbtrail does:

| Privilege | Why dbtrail needs it |
|---|---|
| `REPLICATION SLAVE` | Consume the binlog event stream — dbtrail registers as a replica (`COM_BINLOG_DUMP`). |
| `REPLICATION CLIENT` | Discover the start position and detect gaps: `SHOW BINARY LOG STATUS` / `SHOW MASTER STATUS`, `SHOW BINARY LOGS`, `@@gtid_purged`, `@@gtid_executed`. |
| `SELECT` | Snapshot the schema (column types, primary keys, foreign keys) from `information_schema`, and run the preflight checks. |

dbtrail **never** writes to the source and never locks it. It does not need `RELOAD`, `LOCK TABLES`, `PROCESS`, `SHOW VIEW`, or `EXECUTE`.

**Least-privilege variant** — `SELECT` is the only privilege you can scope to specific schemas (the two `REPLICATION` grants are global-only in MySQL):

```sql
CREATE USER 'dbtrail'@'%' IDENTIFIED BY 'strong-password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'dbtrail'@'%';
GRANT SELECT ON shop.* TO 'dbtrail'@'%';        -- repeat per monitored schema
```

If you scope `SELECT`, it must cover **every column of every monitored table**, or the schema snapshot fails with `no columns found for the requested schemas`.

> The source must also have `binlog_format = ROW` and `binlog_row_image = FULL`. The preflight check (and `bintrail doctor`) refuses to start otherwise — verify with `SHOW VARIABLES LIKE 'binlog_%';`. Set `binlog_row_image = FULL` **server-wide**: the preflight only sees its own connection, so a per-session `SET SESSION binlog_row_image = MINIMAL`/`NOBLOB` on another connection still writes partial images that index as incomplete (unsupported — see [SUPPORT.md](../SUPPORT.md)). Also keep `binlog_row_value_options` clear of `PARTIAL_JSON`, which logs only a JSON diff.

---

## MariaDB as a source (alpha)

bintrail can stream from a **MariaDB** source (the index stays MySQL) — pass
`--source-flavor mariadb`. Everything on this page applies — including gap
detection on resume, which now works for MariaDB in both position and GTID mode.
The MariaDB-specific setup, version support, alpha limitations, and
troubleshooting live on the dedicated page: **[MariaDB](mariadb.md)**.

## PostgreSQL as a source (beta)

bintrail also captures from **PostgreSQL** (beta) via the separate `bintrail-pg`
binary, over logical replication (`pgoutput`) — the index still stays MySQL.
Setup, requirements, the slot/WAL operator boundary, type support, and beta
limitations live on the dedicated page: **[PostgreSQL](postgres.md)**.

---

## The Problem

`bintrail index` reads binlog files from disk. That works well for self-managed MySQL where you have filesystem access, but it doesn't work for managed MySQL services (Amazon RDS, Aurora, Cloud SQL). Those services don't give you file access to the binlog directory.

The solution is to connect to MySQL as if you were a replica. MySQL's replication protocol sends binlog events over the network in real time. `bintrail stream` uses this protocol to receive and index events continuously, without ever touching a file.

---

## MySQL Replication Protocol Basics

When MySQL runs as primary in a replication setup, replicas connect using the `COM_BINLOG_DUMP` command and receive an event stream. The primary sends every event as it commits: `GTIDEvent`, `QueryEvent`, `RowsEvent`, `RotateEvent`, etc.

**GTID vs position mode**: MySQL supports two ways to identify a position in the binlog stream:

- **Position mode** (`--start-file`, `--start-pos`): The traditional approach — a filename and byte offset. Simple but tied to a specific server instance.
- **GTID mode** (`--start-gtid`): Each transaction gets a globally unique ID (`server-uuid:sequence-number`). MySQL tracks which GTIDs have been executed and resumes from the right point even after a failover. Use GTID mode on any setup where GTID replication is enabled (which is most managed MySQL services).

---

## Checkpointing and `stream_state`

The `stream_state` table has exactly one row (enforced by a `CHECK (id = 1)` constraint). It records:

| Column | Description |
|--------|-------------|
| `mode` | `"position"` or `"gtid"` |
| `binlog_file` | Current binlog filename |
| `binlog_position` | Byte offset — in GTID mode, the last processed event; in position mode, the last **safe** statement/commit/DDL boundary, which may trail the last processed event |
| `gtid_set` | Full accumulated GTID set (GTID mode only) |
| `events_indexed` | Running count |
| `last_event_time` | Timestamp of the last indexed event |
| `last_checkpoint` | When the checkpoint was last written |
| `server_id` | The `--server-id` used |
| `bintrail_id` | The server identity this checkpoint belongs to |
| `flavor` | Source flavor — `mysql`/`mariadb` (this command) or `postgres` (written by `bintrail-pg`); selects the recovery SQL dialect |
| `gap_lost_at` / `gap_lost_detail` | Stamped when an unfillable gap permanently lost data — see [Binlog Gap Detection](#binlog-gap-detection) and the [continuity signal](rotation-and-status.md#stream-continuity-no-data-lost) |
| `source_health` | Periodic source-health snapshot (JSON) powering the console replication-health panel (e.g. a PostgreSQL slot's state) |

**GTID accumulation**: In GTID mode, the stream state doesn't store just the latest GTID — it stores the entire accumulated executed GTID set. This is how MySQL replication works: when resuming, you tell MySQL "I've already seen all of these GTIDs, send me everything after."

**Checkpoint interval**: Default 10 seconds, configurable via `--checkpoint`. This is the maximum amount of data that could be re-received from MySQL if the process crashes — batches flush to `binlog_events` independently of the checkpoint ticker (whenever a batch fills or a DDL boundary is hit), so a crash between a flush and the next checkpoint can leave already-indexed events beyond the durable checkpoint. On resume, `bintrail stream` deletes any `binlog_events` rows at or beyond the actual replay start (the saved position, or the gap-auto-advanced position if a purge was detected) before streaming resumes, so the re-received events are re-inserted cleanly instead of duplicating (single-writer index, so this is safe). In GTID mode this also removes any rows from a transaction that was still open — flushed but not yet committed — when the last checkpoint was written, since GTID replay restarts a transaction from its beginning rather than from a byte offset.

**Checkpoint-boundary safety**: in GTID mode the checkpoint advances **only at transaction-commit boundaries**, never mid-transaction, so a saved `gtid_set` is always a set of fully-applied transactions — a crash can't persist a partial GTID that would skip the rest of that transaction on resume. In position mode the persisted `binlog_position` advances **only at a statement/commit/DDL boundary**, never mid-statement — it may trail the last processed event, and on resume nothing is re-applied mid-statement. The guarantee is statement-level (not commit-level) because sources with `gtid_mode=OFF` emit no commit event — which is exactly why it covers non-GTID sources.

### Mode switching

The stream command supports seamless switching between position mode and GTID mode. If a saved checkpoint exists in `stream_state`, the saved mode is used regardless of which `--start-*` flags are passed on the command line. This makes restarts idempotent — you can always pass the same flags without worrying about overriding the saved state.

To explicitly switch modes (e.g. from position to GTID after enabling GTIDs on the source), use the `--reset` flag:

```sh
# Switch from position mode to GTID mode
bintrail stream \
  --index-dsn  "..." \
  --source-dsn "..." \
  --server-id  99999 \
  --start-gtid "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5000" \
  --reset
```

`--reset` clears the saved checkpoint in `stream_state` before starting, forcing the command to use the `--start-file`/`--start-gtid` flags from the command line. Without `--reset`, the saved checkpoint always takes precedence.

**When to use `--reset`:**
- Switching from position mode to GTID mode (or vice versa)
- Forcing a restart from a known position after a disaster recovery
- Skipping ahead past corrupted binlog events

---

## Binlog Gap Detection

When a stream is restarted after downtime, MySQL may have continued generating binlog events that dbtrail did not capture. On every restart from a saved checkpoint, dbtrail automatically detects whether a gap exists and handles it:

### How it works

**Position mode** (`--start-file`/`--start-pos`):
1. Queries `SHOW BINARY LOGS` on the source MySQL.
2. If the checkpoint file still exists in the list, the gap is **fillable** — dbtrail resumes from the checkpoint and replays all missed events before switching to live tailing.
3. If the checkpoint file has been purged, the gap is **unfillable** — dbtrail logs a warning and auto-advances to the earliest available binlog file.

**GTID mode** (`--start-gtid`):
1. Queries `@@gtid_purged` and `@@gtid_executed` from the source MySQL.
2. If the checkpoint GTID set does not intersect with the purged set, the gap is **fillable** — MySQL still has all required binlog events.
3. If the checkpoint includes purged GTIDs, the gap is **unfillable** — dbtrail logs a warning and advances past the purged GTID set.

### What happens during an unfillable gap

When binlogs have been purged and the gap cannot be filled:

1. A warning is logged with details about what was lost:
   ```
   WARN binlog gap detected but CANNOT be filled: required file mysql-bin.000038 has been purged;
   earliest available binlog is mysql-bin.000050; events between these positions are permanently lost
   ```
2. The checkpoint is **immediately updated** to the new (advanced) position. This prevents a crash loop if the stream fails during startup — the next restart will not hit the same purged-binlog error.
3. The stream resumes from the earliest available position.
4. **The loss is recorded durably** — it is not just a log line that scrolls away. The gap is stamped in `stream_state` (`gap_lost_at` / `gap_lost_detail`), so `bintrail status` reports `Continuity: ⚠ GAP LOST` and a loud `=== ⚠ EVENTS PERMANENTLY LOST ===` banner from then on (and `stream.continuity.status: "gap_lost"` under `--format json`), even after the capture process has exited. The index up to the gap stays valid for recovery; resuming capture cleanly requires a re-baseline. To alert on this in CI/cron, run `bintrail status --fail-on-gap` (exits non-zero, fails closed). See [Rotation & Status](rotation-and-status.md#stream-continuity-no-data-lost).

### The `--no-gap-fill` flag

By default, dbtrail auto-advances past unfillable gaps. If you want the stream to **refuse to start** when a gap is detected (so you can investigate and decide how to proceed), use:

```sh
bintrail stream --no-gap-fill --index-dsn "..." --source-dsn "..." --server-id 99999
```

With `--no-gap-fill`, the stream exits with an error if an **unfillable** gap is detected (i.e., required binlogs have been purged). Fillable gaps are always replayed automatically since no data is lost. This flag is useful for self-hosted deployments where data loss must be explicitly acknowledged.

### The `--gap-timeout` flag

Gap-detection queries (`SHOW BINARY LOGS`, `@@gtid_purged`, `@@gtid_executed`) run with a 30-second default timeout. On managed MySQL with **many** binlog files, `SHOW BINARY LOGS` can take longer than this — for example, an Amazon RDS `db.t4g.micro` with 24h retention and high write throughput can accumulate ~300 binlog files and take >10 seconds to enumerate them. If you see:

```
gap detection failed: SHOW BINARY LOGS: context deadline exceeded
(use --reset to skip gap detection and start from a new position)
```

raise the timeout (it only applies to the one-shot startup query, so a higher ceiling has no ongoing cost):

```sh
bintrail stream --gap-timeout 60 --index-dsn "..." --source-dsn "..." --server-id 99999
```

Reducing binlog retention is also a valid mitigation, but loses the ability to fill larger gaps.

### RDS: stream from the primary, not a read-replica

**Important for AWS RDS users:** `bintrail stream` connects as a binlog client and registers itself as a replication slave (`COM_REGISTER_SLAVE`). RDS read-replicas are `read_only=1` by default and reject the registration with:

```
ERROR 1290 (HY000): The MySQL server is running with the --read-only option so it cannot execute this statement
```

Always point `--source-dsn` at the **primary** RDS instance, not a read-replica. If your application reads through a read-replica to spare the primary, dbtrail still needs to stream from the primary — the binlog activity it adds is comparable to a single managed read-replica connection.

### RDS: backup retention enables binlog

**Important for AWS RDS users:** RDS for MySQL only enables binary logging when `backup-retention-period >= 1`. Even with a custom parameter group setting `binlog_format=ROW` and `binlog_row_image=FULL`, `@@log_bin` stays `0` if backup retention is `0`, and `bintrail stream` / `bintrail index` will fail with:

```
ERROR 1381 (HY000): You are not using binary logging
```

`SHOW VARIABLES` happily reports the custom parameter-group values, which makes this easy to miss. Enable binlog by raising backup retention to at least 1 day:

```sh
aws rds modify-db-instance \
  --db-instance-identifier <id> \
  --backup-retention-period 1 \
  --apply-immediately
```

The modification reaches the instance in ~2 minutes. After that, `SELECT @@log_bin` reports `1` but `SHOW BINARY LOGS` may still return an empty set until RDS completes its first automated snapshot (another ~30s–1min). Wait for `SHOW BINARY LOGS` to return at least one row before launching `bintrail stream` — until then auto-discovery returns no row and the streamer exits with `auto-discover binlog position: ...`. Set `binlog retention hours` (see below) so RDS keeps binlogs long enough for dbtrail to index them before purge:

```sql
CALL mysql.rds_set_configuration('binlog retention hours', 48);
```

### Binlog retention requirement

**Important:** Configure your MySQL server's binlog retention to be **at least 2 days**. This gives dbtrail enough time to fill gaps after planned maintenance, restarts, or brief outages. With very short retention (seconds or minutes), binlogs may be purged before dbtrail has a chance to replay them, resulting in permanent data loss.

For MySQL 8.0+:
```sql
SET PERSIST binlog_expire_logs_seconds = 172800;  -- 2 days minimum
```

For MySQL 5.7:
```sql
SET GLOBAL expire_logs_days = 2;  -- 2 days minimum
```

For managed MySQL services (RDS, Aurora, Cloud SQL), check your provider's documentation for binlog retention settings. Amazon RDS defaults to `NULL` (no retention), which means binlogs are purged as soon as they are no longer needed for replication — you **must** set a retention period:

```sql
-- Amazon RDS
CALL mysql.rds_set_configuration('binlog retention hours', 48);
```

RDS caps `binlog retention hours` at **720 (30 days)**. Values above the ceiling are rejected with `ERROR 1644 (45000)`. Longer historical reach is the dbtrail index's job (and `bintrail baseline` for replay anchors before the index window) — not RDS's binlog buffer.

### TLS/SSL for managed MySQL (RDS, Aurora, Cloud SQL)

Managed MySQL services often require TLS. Use `--ssl-mode` to control the connection security:

| Mode | Behavior |
|------|----------|
| `disabled` | No TLS |
| `preferred` (default) | Attempt TLS (no certificate verification), fall back to unencrypted if unavailable |
| `required` | TLS mandatory (no certificate verification), fail if unavailable |
| `verify-ca` | Validate server certificate against CA (no hostname check) |
| `verify-identity` | Full verification (certificate + hostname) |

**Amazon RDS example** (download the [RDS CA bundle](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html) first):

```bash
bintrail stream \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --source-dsn "bintrail_repl:a-strong-password@tcp(mydb.abc123.us-east-1.rds.amazonaws.com:3306)/" \
  --server-id  99999 \
  --start-gtid "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-50000" \
  --ssl-mode   verify-ca \
  --ssl-ca     /path/to/rds-combined-ca-bundle.pem \
  --metrics-addr :9090
```

For **mutual TLS** (client certificate authentication), add `--ssl-cert` and `--ssl-key` to any stream command above:

```bash
bintrail stream \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --source-dsn "bintrail_repl:a-strong-password@tcp(source-db:3306)/" \
  --server-id  99999 \
  --ssl-mode   verify-identity \
  --ssl-ca     /path/to/ca.pem \
  --ssl-cert   /path/to/client-cert.pem \
  --ssl-key    /path/to/client-key.pem
```

---

## Graceful Shutdown

When you send `SIGINT` or `SIGTERM` (or press Ctrl-C), the current in-memory batch is flushed before exit — no events are lost.

---

## Prometheus Metrics

When `--metrics-addr :9090` is set, a Prometheus HTTP endpoint starts at `/metrics`. The stream loop updates these metrics on every event and batch flush:

| Metric | Type | Description |
|--------|------|-------------|
| `bintrail_stream_events_received_total` | Counter | Row events received from the replication stream |
| `bintrail_stream_events_indexed_total` | Counter | Events successfully written to `binlog_events` |
| `bintrail_stream_batch_flushes_total` | Counter | Number of batch INSERT operations |
| `bintrail_stream_checkpoint_saves_total` | Counter | Successful checkpoint writes |
| `bintrail_stream_last_event_timestamp_seconds` | Gauge | Unix timestamp of the last received event |
| `bintrail_stream_replication_lag_seconds` | Gauge | `now() - last_event_timestamp` in seconds |
| `bintrail_stream_errors_total{source,type}` | Counter | Errors by type: `batch_flush`, `checkpoint`, `gtid_update` |
| `bintrail_stream_batch_size` | Histogram | Distribution of events per batch flush |

Every metric in the table above carries a `source` label so concurrent streams
in one process stay distinguishable (the top-level
`bintrail_statement_dml_dropped_total` counter is the exception — see
[observability.md](observability.md#capture-loss-bintrail_statement_dml_dropped_total)). For a standalone `bintrail stream` it is the server's resolved
`bintrail_id` (`default` if unresolved); under `bintrail-console watch` it is the
monitored entry's ID, and the **daemon** serves one `/metrics` endpoint covering
all supervised streams (per-stream endpoints would fight over the bind).

`replication_lag_seconds` is the most useful metric for monitoring — it tells you how far behind the stream is relative to real time. If it grows steadily, the index database can't keep up with the write rate. With multiple sources, alert per label: `bintrail_stream_replication_lag_seconds{source="<entry-id>"}`.

The metrics HTTP server shuts down gracefully (5-second timeout) on command exit.

---

## `bintrail index` vs `bintrail stream`: When to Use Which

| | `bintrail index` | `bintrail stream` |
|---|---|---|
| **Access requirement** | Filesystem access to binlog files | TCP access + replication user |
| **Use case** | Self-managed MySQL, one-time backfill | Managed MySQL (RDS, Aurora, Cloud SQL), continuous |
| **Execution model** | One-shot — processes files and exits | Long-running daemon |
| **Parallelism** | Processes multiple files sequentially | Processes one event at a time |
| **Checkpointing** | Per-file in `index_state` | Periodic timed, in `stream_state` |
| **Start from** | Specific files or `--all` | `--start-file`, `--start-gtid`, or saved checkpoint |
| **Suitable for systemd** | `Type=oneshot` | `Type=simple`, `Restart=always` |

For managed MySQL, `stream` is the only option. For self-managed MySQL, both work — `index` is simpler for batch backfill, `stream` is better for continuous real-time indexing.
