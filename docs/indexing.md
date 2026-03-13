# How Indexing Works

This page explains what happens when you run `bintrail index`. It covers the problem bintrail solves, how it reads and understands binlog data, and how the concurrent parser/indexer pipeline avoids buffering entire files in memory.

---

## The Problem

MySQL binary logs record every INSERT, UPDATE, and DELETE as binary events in a sequential file. They are not queryable. You can't say "show me all changes to the `orders` table in the last hour" — you have to replay the entire file from the beginning, decode each event, and filter manually. And once a binlog file is purged (MySQL rotates them), that history is gone.

Bintrail solves this by reading binlog files once and writing every row event into a queryable MySQL table with full before and after images, where it lives until you explicitly rotate it out.

---

## Step 1: Schema Snapshot and the Resolver

Before parsing a single binlog event, bintrail needs to know what the table columns are called.

Here's the problem: MySQL's ROW format binary log records rows as positional arrays — `[1, "Alice", 42]` — with no column names attached. The column names live in `information_schema.COLUMNS` on the source server, not in the binlog.

The `bintrail snapshot` command captures this metadata and stores it in `schema_snapshots` in the index database. Each row in that table is one column of one table: its name, ordinal position, whether it's a primary key column, its data type, and whether it's a generated column.

At index time, `metadata.NewResolver` loads the entire snapshot into memory as a `map[string]*TableMeta` keyed by `"schema.table"`. Each `TableMeta` holds the column list in ordinal position order, which is exactly the order MySQL puts values in the binlog.

```go
// internal/metadata/metadata.go
func (r *Resolver) MapRow(schema, table string, row []any) (map[string]any, error) {
    tm, err := r.Resolve(schema, table)
    // ...
    named := make(map[string]any, len(row))
    for i, col := range tm.Columns {
        named[col.Name] = row[i]  // positional → named
    }
    return named, nil
}
```

The Resolver lives in memory for the duration of the index run — one lookup per row event with no database queries needed.

**Column count mismatch**: If the source schema changed (an `ALTER TABLE` ran) after the snapshot was taken, the binlog column count won't match the snapshot. The parser detects this per `TABLE_MAP_EVENT` and warns, skipping that table's events rather than corrupting the data. The fix is to re-run `bintrail snapshot`.

### Foreign Key Constraints

In the same snapshot transaction, bintrail also captures foreign key relationships into the `fk_constraints` table. It queries `INFORMATION_SCHEMA.KEY_COLUMN_USAGE` joined with `REFERENTIAL_CONSTRAINTS` on the source server and stores one row per FK column mapping:

| Column | Description |
|--------|-------------|
| `snapshot_id` | Same snapshot_id as the column metadata in `schema_snapshots` |
| `constraint_name` | The FK constraint name (e.g. `fk_orders_customer`) |
| `table_schema` | Schema of the child (referencing) table |
| `table_name` | Child table name |
| `column_name` | Column in the child table |
| `ordinal_position` | Position within a composite FK (1-based) |
| `referenced_table_schema` | Schema of the parent (referenced) table |
| `referenced_table_name` | Parent table name |
| `referenced_column_name` | Column in the parent table |

Composite foreign keys produce multiple rows with increasing `ordinal_position`. Tables with no foreign keys simply contribute zero rows — this is not an error.

**No additional MySQL grants are required.** The same privileges that allow bintrail to read `INFORMATION_SCHEMA.COLUMNS` (which is required for column metadata) also grant visibility into `KEY_COLUMN_USAGE` and `REFERENTIAL_CONSTRAINTS`. MySQL's metadata visibility is row-level, not table-level — if you can see a table's columns, you can see its FK constraints.

The snapshot output includes the FK count:

```
Snapshot complete.
  snapshot_id      : 3
  tables           : 42
  columns          : 318
  fk constraints   : 12
```

---

## Step 2: The Parser

The parser lives in `internal/parser/parser.go`. It wraps `github.com/go-mysql-org/go-mysql/replication` (the go-mysql library), which handles the actual binary decoding.

**What the parser does for each file:**

1. Opens the file and passes it to `replication.BinlogParser.ParseFile`.
2. For each `GTIDEvent`, stores the GTID string. This carries forward to every subsequent row event until the next `GTIDEvent` resets it.
3. For each `QueryEvent`, checks if it's a DDL statement (`ALTER TABLE`, `CREATE TABLE`, etc.) and logs a warning — DDL may invalidate the current snapshot.
4. For each `RowsEvent`, calls `handleRows`.

**`handleRows`** is the central dispatch function (shared with `StreamParser` — more on that in the streaming docs). It:

1. Checks the schema/table against the filter (if `--schemas` or `--tables` was specified).
2. Calls `resolver.Resolve(schema, table)` to get the `TableMeta`.
3. Validates that the binlog column count matches the snapshot.
4. Dispatches to `emitInserts`, `emitDeletes`, or `emitUpdates` based on the event type.

**UPDATE rows are interleaved**: go-mysql delivers UPDATE row data as alternating before/after pairs: `rows[0]` is the before-image of the first changed row, `rows[1]` is the after-image, `rows[2]` is the before-image of the second, and so on. `emitUpdates` steps through in pairs of two (`i += 2`) to reconstruct each full before/after event.

**PK values**: For every event, the parser extracts the primary key values from the row using `BuildPKValues`. The values are joined with pipe (`|`) as delimiter, with `|` → `\|` and `\` → `\\` escaping for composite keys. This is stored as a plain `VARCHAR(512)` — not JSON.

Each matched event becomes a `parser.Event` struct sent onto the channel:

```go
type Event struct {
    BinlogFile string
    StartPos   uint64
    EndPos     uint64
    Timestamp  time.Time
    GTID       string
    Schema     string
    Table      string
    EventType  EventType
    PKValues   string
    RowBefore  map[string]any  // nil for INSERT
    RowAfter   map[string]any  // nil for DELETE
}
```

---

## Step 3: The Indexer

The indexer lives in `internal/indexer/indexer.go`. It reads `parser.Event` values from the channel and writes them to `binlog_events` in batches.

**Batch INSERT**: The indexer accumulates events in a slice up to `--batch-size` (default 1000). When the batch is full, it builds a single multi-row `INSERT` statement with one `(?,?,?,?,?,?,?,?,?,?,?,?)` placeholder group per event. This is far more efficient than 1000 individual INSERTs.

**JSON serialization**: `row_before` and `row_after` are stored as MySQL JSON columns. The indexer calls `marshalRow` to convert each `map[string]any` to JSON.

There's a subtlety here: go-mysql returns MySQL JSON column values as raw `[]byte` containing valid JSON. If the indexer naively called `json.Marshal` on the map, those bytes would be base64-encoded (because Go treats `[]byte` as binary data). `marshalRow` detects `[]byte` values that contain valid JSON and promotes them to `json.RawMessage`, which is then embedded literally:

```go
// internal/indexer/indexer.go
for k, v := range row {
    if b, ok := v.([]byte); ok && json.Valid(b) {
        normalized[k] = json.RawMessage(b)  // embedded as JSON, not base64
    } else {
        normalized[k] = v
    }
}
```

**`pk_hash` and `event_id` are never inserted**: `event_id` is `AUTO_INCREMENT` and `pk_hash` is a `STORED` generated column (`SHA2(pk_values, 256)`). MySQL computes both automatically — the INSERT only specifies the 12 explicit columns.

---

## Step 4: Concurrent Architecture

The most important design decision in the indexer is that the parser and indexer run concurrently. Without this, bintrail would have to parse the entire file into memory before writing anything, which would be a problem for large binlog files.

```
ParseFile goroutine ──► events chan (buffered 1000) ──► idx.Run (main goroutine)
         │                                                      │
         └──► parseErrCh (buffered, size 1)                    │
                                                                │ on error: cancel()
                                                                │ ctx.Done() unblocks parser
```

The code in `indexFile` (`cmd/bintrail/index.go`):

```go
ctx, cancel := context.WithCancel(ctx)
defer cancel()

events := make(chan parser.Event, 1000)
parseErrCh := make(chan error, 1)  // buffered: goroutine never blocks on send

go func() {
    defer close(events)
    parseErrCh <- p.ParseFile(ctx, filename, events)
}()

count, idxErr := idx.Run(ctx, events)
if idxErr != nil {
    cancel()  // tell the parser goroutine to stop
}

parseErr := <-parseErrCh  // wait for parser to finish
```

**Why `parseErrCh` is buffered (size 1):** The parser goroutine sends its error on `parseErrCh` as the last thing it does. If the main goroutine hasn't called `<-parseErrCh` yet (because it's still in `idx.Run`), the goroutine would block forever on an unbuffered channel — a goroutine leak. With size 1, the send always completes immediately.

**Error propagation**: If the indexer fails, it calls `cancel()`. The parser is inside `bp.ParseFile`, which calls the callback for each event; the callback checks `ctx.Err()` before sending. When `ctx.Done()` fires, the parser returns `context.Canceled`. The main goroutine receives this error from `parseErrCh`, but it uses `errors.Is(parseErr, context.Canceled)` to distinguish a real parse error from a clean shutdown triggered by an indexer failure.

---

## Step 5: Index State Tracking

Each file gets a row in `index_state` that tracks its progress. The lifecycle is:

1. Check if the file is already `completed` — if so, skip it entirely.
2. Mark it `in_progress` (upsert via `INSERT … ON DUPLICATE KEY UPDATE`).
3. Parse + index concurrently.
4. On success: mark `completed`.
5. On error: mark `failed` with the error message.

This means `bintrail index --all` is always safe to re-run. Completed files are skipped. Failed or in-progress files (e.g. from a previous crash) are retried.

`index_state` also records the `bintrail_id` of the server that indexed each file. When multiple source servers share one index database, this lets `bintrail status` group indexed files by origin server. See [Server Identity](./server-identity.md) for details.

---

## Full Flow of `bintrail index`

```
1. --source-dsn provided?
      Yes → connect, check binlog_format=ROW, binlog_row_image=FULL, no FK cascades
            resolve server identity → log bintrail_id
      No  → skip validation (warn)

2. Connect to index database

3. Schema snapshot:
      Snapshot exists? → load it with NewResolver(db, snapshotID)
      No snapshot + --source-dsn → call TakeSnapshot automatically
      No snapshot + no --source-dsn → error

4. Build filters from --schemas and --tables

5. Resolve file list:
      --all → scan binlog-dir for files matching *.000000+ pattern, sort ascending
      --files → split comma-separated list

6. For each file (in order):
      a. Already completed? → skip
      b. Mark in_progress in index_state (with bintrail_id)
      c. Launch ParseFile goroutine → events channel
      d. idx.Run consumes events, inserts in batches
      e. Wait for parser to finish
      f. Update index_state: completed or failed
```

---

## Database Destination: `binlog_events`

The indexed events land in `binlog_events`, which is partitioned by `RANGE (TO_SECONDS(event_timestamp))`. Each hourly partition holds all events whose timestamp falls within that hour.

Key columns:

| Column | Type | Notes |
|--------|------|-------|
| `event_id` | `BIGINT UNSIGNED AUTO_INCREMENT` | Row identity |
| `pk_hash` | `CHAR(64)` STORED GENERATED | `SHA2(pk_values, 256)` — the index target |
| `pk_values` | `VARCHAR(512)` | Pipe-delimited PK values |
| `row_before` | `JSON` | Full before-image (`NULL` for INSERT) |
| `row_after` | `JSON` | Full after-image (`NULL` for DELETE) |
| `changed_columns` | `JSON` | Array of column names that differ between before/after |
| `gtid` | `VARCHAR(64)` | `NULL` when GTID not enabled |

The `pk_hash` generated column is what makes PK lookups fast. Queries use `pk_hash = SHA2(?, 256) AND pk_values = ?` — the hash eliminates almost all rows instantly, and the exact `pk_values` comparison guards against SHA-256 collisions (which are astronomically rare but technically possible).
