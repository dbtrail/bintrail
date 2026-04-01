# BYOS — Bring Your Own Storage

In BYOS mode, bintrail splits every parsed binlog event into two streams so that row-level data never leaves the customer's infrastructure:

| Stream | Contains | Destination |
|--------|----------|-------------|
| **Metadata** | pk_hash, schema, table, event type, timestamp, server ID, GTID, changed column names | dbtrail API |
| **Payload** | pk_hash, pk_values, row_before, row_after, changed_columns, schema_version | Customer's S3 bucket (Parquet) |

In hosted mode, everything flows to the index MySQL database as before. The BYOS package (`internal/byos`) is not used.

---

## How the Split Works

After the parser emits a `parser.Event`, `byos.SplitEvent` produces two records:

```
parser.Event
    │
    ├──► MetadataRecord  ──► dbtrail API  (HTTP POST, JSON)
    │       pk_hash            no row data
    │       schema_name        no pk_values
    │       table_name         changed column NAMES only
    │       event_type
    │       event_timestamp
    │       server_id
    │       gtid
    │       changed_columns
    │
    └──► PayloadRecord   ──► Customer S3  (Parquet)
            pk_hash            full before/after images
            pk_values          stays in customer infra
            row_before
            row_after
            changed_columns
            schema_version
```

The correlation key between the two is `pk_hash` — the SHA-256 digest of `pk_values`, matching MySQL's `SHA2(pk_values, 256)` stored generated column. The dbtrail API can reference events by hash without ever seeing the actual primary key values.

---

## Changed Columns

For UPDATE events, `changed_columns` contains the sorted list of column names whose values differ between the before and after images:

```json
{
  "event_type": "UPDATE",
  "changed_columns": ["email", "updated_at"]
}
```

This tells the API *which* columns changed without revealing *what* they changed to. For INSERT and DELETE events, `changed_columns` is null (all columns are affected by definition).

---

## Payload Partitioning

Payload Parquet files are written to the customer's storage backend using this key structure:

```
{server_id}/{schema}.{table}/{date}/events_{nanos}.parquet
```

For example:

```
srv-1/mydb.users/2026-03-31/events_1743379200000000000.parquet
srv-1/mydb.orders/2026-03-31/events_1743379200100000000.parquet
```

Records are grouped by **schema.table** and **UTC date** before writing, so each Parquet file contains events from a single table on a single day. This enables efficient scans by table and date range during recovery.

The Parquet schema includes 10 columns:

| Column | Type | Nullable |
|--------|------|----------|
| `pk_hash` | VARCHAR | No |
| `pk_values` | VARCHAR | No |
| `schema_name` | VARCHAR | No |
| `table_name` | VARCHAR | No |
| `event_type` | VARCHAR | No |
| `event_timestamp` | DATETIME | No |
| `row_before` | JSON | Yes (null for INSERT) |
| `row_after` | JSON | Yes (null for DELETE) |
| `changed_columns` | JSON | Yes (null for INSERT/DELETE) |
| `schema_version` | INT | No |

Files use zstd compression with 500,000-row row groups.

---

## Storage Backend

The `storage.Backend` interface abstracts the underlying object store:

```go
type Backend interface {
    Put(ctx context.Context, key string, r io.Reader) error
    Get(ctx context.Context, key string) (io.ReadCloser, error)
    List(ctx context.Context, prefix string) ([]string, error)
    Delete(ctx context.Context, key string) error
    Exists(ctx context.Context, key string) (bool, error)
}
```

Currently implemented for S3 and S3-compatible services (MinIO, LocalStack) via `storage.S3Backend`. Additional providers (GCS, Azure Blob) can be added as new `Backend` implementations.

Key properties:
- **Transparent prefix**: `S3Config.Prefix` is prepended to all keys automatically
- **Fail-fast construction**: `NewS3Backend` validates bucket access via `HeadBucket` at creation time
- **Key validation**: empty keys and keys with leading `/` are rejected
- **Credentials stay local**: AWS credentials are loaded from the standard SDK chain and never sent to dbtrail

---

## Security Model

The core guarantee of BYOS:

- **No row-level data leaves the customer's infrastructure.** The metadata stream contains only `pk_hash` (irreversible one-way hash), structural fields (schema, table, event type), and column *names* (never values).
- **pk_values, row_before, and row_after** exist only in the payload stream, which is written directly to the customer's own storage backend.
- **AWS credentials** are resolved locally via the standard SDK chain (environment variables, `~/.aws/credentials`, EC2 instance metadata) and are never logged or transmitted to dbtrail.

---

## Local Event Buffer

In BYOS mode, the agent keeps recent events in an **in-memory buffer** so that `resolve_pk` and `recover` commands don't require an S3 round-trip for events that happened in the last few hours.

### How It Works

When the agent runs with `--source-dsn` and `--server-id`, it enters BYOS streaming mode:

```
bintrail agent \
  --api-key "ak_..." --endpoint "wss://api.dbtrail.io/v1/agent" \
  --source-dsn "user:pass@tcp(host:3306)/mydb" \
  --server-id 99999 \
  --buffer-retain "6h"
```

The agent runs two goroutines in a single process:

```
goroutine 1 (stream):  MySQL binlog → parser → in-memory buffer
goroutine 2 (agent):   dbtrail WebSocket → receive commands → query buffer + S3
```

### Query Priority

When a `resolve_pk` or `recover` command arrives from dbtrail:

1. **Buffer** (fastest) — check the in-memory buffer for recent events
2. **MySQL index** — query the dbtrail index database if configured
3. **S3 Parquet** — scan archived Parquet files on the customer's S3

Results from all sources are merged by `event_id`, sorted by timestamp, and deduplicated.

### Configuration

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `--buffer-retain` | `BINTRAIL_BUFFER_RETAIN` | `6h` | How long events stay in the buffer before eviction |
| `--server-id` | `BINTRAIL_SERVER_ID` | — | MySQL replication server ID (required for BYOS streaming) |
| `--start-gtid` | `BINTRAIL_START_GTID` | — | GTID set to start from on first run |
| `--batch-size` | `BINTRAIL_BATCH_SIZE` | `1000` | Events per batch flush |

### Restart Behavior

The buffer is in-memory — on restart it starts empty and fills up from the stream. Recent data that was flushed to S3 before the restart is still available via S3 Parquet queries. The gap between restart and buffer re-fill is covered by S3.

---

## Package Structure

```
internal/
  storage/    # Backend interface + S3 implementation
  buffer/     # In-memory event buffer for BYOS mode
    buffer.go     # Buffer type — Insert, Fetch, ResolvePK, Evict
    parquet.go    # WriteParquet — write ResultRows to Parquet files
  byos/       # Data separation layer
    split.go      # MetadataRecord, PayloadRecord, SplitEvent, PKHash
    metadata.go   # MetadataClient — HTTP client for dbtrail API
    payload.go    # PayloadWriter — Parquet files to storage.Backend
```
