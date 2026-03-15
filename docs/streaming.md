# How the Stream Command Works

This page explains `bintrail stream` — the real-time indexing mode that connects to MySQL over the replication protocol instead of reading binlog files directly.

---

## The Problem

`bintrail index` reads binlog files from disk. That works well for self-managed MySQL where you have filesystem access, but it doesn't work for managed MySQL services (Amazon RDS, Aurora, Cloud SQL). Those services don't give you file access to the binlog directory.

The solution is to connect to MySQL as if you were a replica. MySQL's replication protocol sends binlog events over the network in real time. `bintrail stream` uses this protocol to receive and index events continuously, without ever touching a file.

---

## MySQL Replication Protocol Basics

When MySQL runs as primary in a replication setup, replicas connect using the `COM_BINLOG_DUMP` command and receive an event stream. The primary sends every event as it commits: `GTIDEvent`, `QueryEvent`, `RowsEvent`, `RotateEvent`, etc.

Bintrail impersonates a replica by:

1. Presenting a server ID (set via `--server-id`) that doesn't conflict with any real server.
2. Telling MySQL where to start (a binlog file+position, or a GTID set).
3. Reading the event stream with `go-mysql`'s `BinlogSyncer`.

**GTID vs position mode**: MySQL supports two ways to identify a position in the binlog stream:

- **Position mode** (`--start-file`, `--start-pos`): The traditional approach — a filename and byte offset. Simple but tied to a specific server instance.
- **GTID mode** (`--start-gtid`): Each transaction gets a globally unique ID (`server-uuid:sequence-number`). MySQL tracks which GTIDs have been executed and resumes from the right point even after a failover. Use GTID mode on any setup where GTID replication is enabled (which is most managed MySQL services).

---

## Architecture

```
MySQL source server
       │
       │  replication protocol (TCP)
       │
BinlogSyncer (go-mysql)
       │
StreamParser.Run goroutine  ──────► events chan (buffered 1000) ──► streamLoop (main goroutine)
       │                                                                    │
       └──► parseErrCh (buffered 1)                                        │
                                                                  ticker every 10s
                                                                  → flush batch + save checkpoint
                                                                            │
                                                                       indexer.InsertBatch
                                                                            │
                                                                       index MySQL database
```

The architecture mirrors the file-based indexer: a producer goroutine fills a channel, the main goroutine consumes it. The key difference is that the stream never ends — it runs until you send `SIGINT` or `SIGTERM`.

---

## StreamParser vs Parser: Code Reuse

`internal/parser/` has two types:

- `Parser` — reads binlog files; called from `bintrail index`
- `StreamParser` — reads from a `BinlogStreamer`; called from `bintrail stream`

They look different on the outside but share all the internal logic. Both call the same package-level `handleRows` function for every row event:

```go
// internal/parser/parser.go — shared by both
func handleRows(ctx context.Context, logger *slog.Logger, resolver *metadata.Resolver,
    filters *Filters, binlogEv *replication.BinlogEvent, rowsEv *replication.RowsEvent,
    filename, currentGTID string, out chan<- Event) error { ... }
```

`StreamParser.Run` reads events from the streamer in a loop, calling `streamer.GetEvent(ctx)` which blocks until an event arrives:

```go
// internal/parser/stream.go
for {
    binlogEv, err := streamer.GetEvent(ctx)
    if err != nil {
        if ctx.Err() != nil {
            return nil  // context cancelled — graceful shutdown
        }
        return err
    }
    switch ev := binlogEv.Event.(type) {
    case *replication.RotateEvent:
        currentFile = string(ev.NextLogName)  // track binlog filename changes
    case *replication.GTIDEvent:
        currentGTID = formatGTID(ev.SID, ev.GNO)
    case *replication.RowsEvent:
        handleRows(...)
    }
}
```

`StreamParser` also handles `RotateEvent` — when MySQL switches to a new binlog file, the streamer sends a `RotateEvent` with the new filename. The stream parser updates `currentFile` so the filename is accurate in each emitted `Event`.

---

## The Stream Loop

`streamLoop` in `cmd/bintrail/stream.go` is the main goroutine. It uses a three-way `select`:

```go
for {
    select {
    case <-ctx.Done():
        checkpoint()  // flush + save
        return nil

    case <-ticker.C:
        checkpoint()  // periodic flush + save

    case ev, ok := <-events:
        if !ok {
            checkpoint()
            return nil
        }
        // update state from event
        batch = append(batch, ev)
        if len(batch) >= idx.BatchSize() {
            flush()
        }
    }
}
```

**Why not use `idx.Run` from the indexer?** The file-based `indexer.Run` method handles its own batching and flushes when the channel closes. But the stream never closes — it runs forever. The stream command needs checkpoint control between batches (flush the DB batch, then save position to `stream_state`). So it uses `idx.InsertBatch` (the exported lower-level method) directly, with its own ticker driving checkpoints.

---

## Checkpointing and `stream_state`

The `stream_state` table has exactly one row (enforced by a `CHECK (id = 1)` constraint). It records:

| Column | Description |
|--------|-------------|
| `mode` | `"position"` or `"gtid"` |
| `binlog_file` | Current binlog filename |
| `binlog_position` | Byte offset of last processed event |
| `gtid_set` | Full accumulated GTID set (GTID mode only) |
| `events_indexed` | Running count |
| `last_event_time` | Timestamp of the last indexed event |
| `last_checkpoint` | When the checkpoint was last written |
| `server_id` | The `--server-id` used |

**GTID accumulation**: In GTID mode, the stream state doesn't store just the latest GTID — it stores the entire accumulated executed GTID set. This is how MySQL replication works: when resuming, you tell MySQL "I've already seen all of these GTIDs, send me everything after." The Go type is `*gomysql.MysqlGTIDSet` (in-memory), serialized to a string on checkpoint.

```go
// cmd/bintrail/stream.go
if ev.GTID != "" && state.accGTID != nil {
    state.accGTID.Update(ev.GTID)       // add this GTID to the set
    state.gtidSet = state.accGTID.String()  // serialize for checkpoint
}
```

**Checkpoint upsert**: The `saveCheckpoint` function uses `INSERT … ON DUPLICATE KEY UPDATE` to atomically write-or-update the single row. On the first run, it inserts. On every subsequent checkpoint, it updates. The duplicate key is `id = 1`.

**Checkpoint interval**: Default 10 seconds, configurable via `--checkpoint`. This is the maximum amount of data you'd need to re-index if the process crashes — events between the last checkpoint and the crash are re-indexed on restart (they're deduplicated naturally because the same GTID/position is just re-received from MySQL).

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

When a stream is restarted after downtime, MySQL may have continued generating binlog events that bintrail did not capture. On every restart from a saved checkpoint, bintrail automatically detects whether a gap exists and handles it:

### How it works

**Position mode** (`--start-file`/`--start-pos`):
1. Queries `SHOW BINARY LOGS` on the source MySQL.
2. If the checkpoint file still exists in the list, the gap is **fillable** — bintrail resumes from the checkpoint and replays all missed events before switching to live tailing.
3. If the checkpoint file has been purged, the gap is **unfillable** — bintrail logs a warning and auto-advances to the earliest available binlog file.

**GTID mode** (`--start-gtid`):
1. Queries `@@gtid_purged` and `@@gtid_executed` from the source MySQL.
2. If the checkpoint GTID set does not intersect with the purged set, the gap is **fillable** — MySQL still has all required binlog events.
3. If the checkpoint includes purged GTIDs, the gap is **unfillable** — bintrail logs a warning and advances past the purged GTID set.

### What happens during an unfillable gap

When binlogs have been purged and the gap cannot be filled:

1. A warning is logged with details about what was lost:
   ```
   WARN binlog gap detected but CANNOT be filled: required file mysql-bin.000038 has been purged;
   earliest available binlog is mysql-bin.000050; events between these positions are permanently lost
   ```
2. The checkpoint is **immediately updated** to the new (advanced) position. This prevents a crash loop if the stream fails during startup — the next restart will not hit the same purged-binlog error.
3. The stream resumes from the earliest available position.

### The `--no-gap-fill` flag

By default, bintrail auto-advances past unfillable gaps. If you want the stream to **refuse to start** when a gap is detected (so you can investigate and decide how to proceed), use:

```sh
bintrail stream --no-gap-fill --index-dsn "..." --source-dsn "..." --server-id 99999
```

With `--no-gap-fill`, the stream exits with an error if an **unfillable** gap is detected (i.e., required binlogs have been purged). Fillable gaps are always replayed automatically since no data is lost. This flag is useful for self-hosted deployments where data loss must be explicitly acknowledged.

### Binlog retention requirement

**Important:** Configure your MySQL server's binlog retention to be **at least 2 days**. This gives bintrail enough time to fill gaps after planned maintenance, restarts, or brief outages. With very short retention (seconds or minutes), binlogs may be purged before bintrail has a chance to replay them, resulting in permanent data loss.

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

---

## Graceful Shutdown

When you send `SIGINT` or `SIGTERM` (or press Ctrl-C):

1. The signal handler goroutine calls `cancel()`, which cancels the context.
2. `streamLoop`'s `select` receives `ctx.Done()` and calls `checkpoint()` — flushing the current batch and writing position to `stream_state`.
3. `StreamParser.Run` is blocking on `streamer.GetEvent(ctx)`, which returns an error when the context is cancelled. It returns `nil` (not an error) when `ctx.Err() != nil`.
4. The parser goroutine closes the `events` channel (via `defer close(events)` in the goroutine wrapper).
5. `streamLoop` has already returned; it reads the nil parse error from `parseErrCh` and the command exits cleanly.

This means no events in the current in-memory batch are lost — they're flushed before exit.

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
| `bintrail_stream_errors_total{type}` | Counter | Errors by type: `batch_flush`, `checkpoint`, `gtid_update` |
| `bintrail_stream_batch_size` | Histogram | Distribution of events per batch flush |

`replication_lag_seconds` is the most useful metric for monitoring — it tells you how far behind the stream is relative to real time. If it grows steadily, the index database can't keep up with the write rate.

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
