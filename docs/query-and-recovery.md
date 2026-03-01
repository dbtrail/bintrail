# How Query and Recovery Work

This page explains how `bintrail query` finds indexed events, and how `bintrail recover` turns them into SQL that undoes the original operations.

---

## Query: How It Works

### The Query Engine

The query engine lives in `internal/query/query.go`. Its job is to translate `Options` (a Go struct describing what you're looking for) into a SQL `SELECT` against `binlog_events`, then format the results.

The engine is shared: the CLI `query` command, the CLI `recover` command, and the MCP server all call the same `query.New(db).Fetch(ctx, opts)` entry point.

### Dynamic SQL Builder

`buildQuery` constructs the WHERE clause incrementally. Each filter field in `Options` is optional — nil or zero-valued fields are simply omitted:

| Filter | SQL condition |
|--------|--------------|
| `Schema` | `schema_name = ?` |
| `Table` | `table_name = ?` |
| `PKValues` | `pk_hash = SHA2(?, 256) AND pk_values = ?` |
| `EventType` | `event_type = ?` |
| `GTID` | `gtid = ?` |
| `Since` | `event_timestamp >= ?` |
| `Until` | `event_timestamp <= ?` |
| `ChangedColumn` | `JSON_CONTAINS(changed_columns, ?)` |

The conditions are joined with `AND`. If no filters are provided, there's no `WHERE` clause at all (subject to the `LIMIT`).

Results are always `ORDER BY event_timestamp, event_id` — chronological, with event_id as a tiebreaker for events in the same second.

### PK Lookup Pattern

Primary key lookups use two conditions, not one:

```sql
pk_hash = SHA2(?, 256) AND pk_values = ?
```

`pk_hash` is a `STORED` generated column — MySQL computes it automatically as `SHA2(pk_values, 256)` and indexes it. This means a PK lookup becomes an index scan on the hash, which is extremely fast even across millions of rows and multiple partitions.

The second condition (`pk_values = ?`) is a collision guard. SHA-256 collisions are astronomically unlikely, but the guard costs nothing and makes the query provably correct.

### `changed_column` Filter

The `changed_columns` column is stored as a JSON array (e.g. `["status","updated_at"]`). Filtering on a specific column uses MySQL's `JSON_CONTAINS`:

```sql
JSON_CONTAINS(changed_columns, '"status"')
```

The needle is the JSON string representation of the column name (with quotes). `json.Marshal("status")` produces `"status"` — exactly the right format for `JSON_CONTAINS`.

### Output Formats

Results can be formatted three ways:

**`table`** (default): Uses `text/tabwriter` for aligned columns. Shows `event_id`, `timestamp`, `type`, `schema`, `table`, `pk_values`, `changed_cols`, and `gtid`. Does NOT include `row_before`/`row_after` — the table format is designed to be scannable. Use `--format json` to see full row data.

**`json`**: Each event is a JSON object with all fields including `row_before` and `row_after` as nested objects. Indented for readability. The `event_type` is serialized as a string (`"INSERT"`, `"UPDATE"`, `"DELETE"`), not the raw integer.

**`csv`**: All columns including `row_before`/`row_after` serialized as JSON strings in the CSV cells. Fixed column order matching `csvHeaders`.

---

## Parquet Archive Queries

When rotated partitions have been archived (via `bintrail rotate --archive-dir` or `--archive-s3`), events are no longer in the MySQL index. The `query` command can merge results from these archives with the live index using `--archive-dir` and `--archive-s3`.

### How the Merge Works

When either archive flag is set, the query command takes a different path:

1. **Fetch from MySQL index** — same query as usual, but with no `LIMIT` (`Limit=0` omits the LIMIT clause so no events are dropped before the merge).
2. **Fetch from each archive source** — DuckDB opens the Parquet files via `parquet_scan('glob/*.parquet')`, applies the same filters (schema, table, PK, time range, etc.) in DuckDB SQL, and returns `[]ResultRow`.
3. **Merge** — results from all sources are combined, deduplicated by `event_id` (MySQL wins on duplicates, since it is appended first), sorted by `(event_timestamp, event_id)`, and then the user's `--limit` is applied once.

```
bintrail query --archive-dir /data/archives --since "2026-02-01 00:00:00"
                     │
          ┌──────────┴──────────┐
          ▼                     ▼
   MySQL index            DuckDB (local Parquet)
   (live data)            /data/archives/*.parquet
          │                     │
          └──────────┬──────────┘
                     ▼
              dedup + sort + limit
                     │
                     ▼
               formatted output
```

### Memory Footprint

The merge path loads **all matching rows** from all sources into memory before applying the limit. Filters (schema, table, time range, etc.) bound the result set in practice. For extremely broad queries against large archives, memory usage could be significant — apply at least a `--since`/`--until` range to keep the result set manageable.

### S3 Prerequisites

`--archive-s3` uses DuckDB's `httpfs` extension:

- **AWS credentials** — DuckDB uses the standard credential chain: `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` env vars, `~/.aws/credentials`, or an IAM role. Set credentials before running the query.
- **Outbound internet access** — on first use, DuckDB downloads the `httpfs` extension from its extension registry. In airgapped environments, pre-install it by running `duckdb -c "INSTALL httpfs;"` once on a machine with internet access and copying the extension cache.

---

## Recovery: How It Works

### The Concept

Recovery works because bintrail stores **full before and after images** for every row event. To undo an operation, you simply reverse it:

| Original operation | Reversal |
|-------------------|---------|
| `DELETE` | `INSERT` the deleted row back (from `row_before`) |
| `UPDATE` | `UPDATE` back to `row_before` values, `WHERE` the current state matches `row_after` |
| `INSERT` | `DELETE` the row (using `row_after` to identify it) |

The reversal logic is in `internal/recovery/recovery.go`. It never executes SQL — it only generates a script.

### Reverse Chronological Ordering

Before generating SQL, the generator reverses the event list:

```go
// internal/recovery/recovery.go
slices.Reverse(rows)
```

The most recent event is undone first. This matters for sequences like:

```
INSERT id=5 (at 14:01)
UPDATE id=5: status=draft→published (at 14:02)
UPDATE id=5: status=published→deleted (at 14:03)
```

Reversed: undo the 14:03 UPDATE first, then the 14:02 UPDATE, then the 14:01 INSERT. This is the correct rollback order for any sequence of operations on the same row.

### WHERE Clause Strategy

For `UPDATE` and `DELETE` reversals, the generator needs a `WHERE` clause to identify the correct row in the current database state.

**With a schema snapshot (preferred)**: Uses only the primary key columns from the resolver. This produces a clean, minimal WHERE clause:

```sql
UPDATE `mydb`.`orders` SET `status` = 'draft' WHERE `id` = 42
```

**Without a snapshot (fallback)**: Uses every column in the row image. This is verbose but always correct for tables without duplicate rows:

```sql
UPDATE `mydb`.`orders`
SET `status` = 'draft'
WHERE `id` = 42 AND `status` = 'published' AND `created_at` = '2026-02-19 14:01:00'
```

The resolver is loaded best-effort in the `recover` command — a failure logs a warning and falls back to the all-columns strategy.

**A subtle detail for `UPDATE` reversals**: The `WHERE` clause uses `row_after` (the current database state), not `row_before`. This is correct because the current database reflects the `row_after` state. It also handles the edge case where the `UPDATE` changed the primary key itself — the `WHERE` still finds the right row.

### Generated Column Handling

Generated columns (`STORED` or `VIRTUAL`) are computed by MySQL and cannot be set explicitly. The generator skips them when building `INSERT` SET clauses and UPDATE SET clauses:

```go
// internal/recovery/recovery.go
genCols := g.generatedCols(row.SchemaName, row.TableName)
for _, col := range sortedKeys(row.RowBefore) {
    if genCols[col] {
        continue  // skip generated columns
    }
    // ...
}
```

The `generatedCols` method queries the resolver for columns where `IsGenerated = true`. If the resolver is nil, `generatedCols` returns nil — treated as an empty set, so nothing is skipped. This is safe because the fallback (all-columns WHERE) doesn't need to distinguish generated columns.

### The float64 JSON Round-Trip Gotcha

There's a subtle type coercion issue in recovery. The `row_before` and `row_after` data was stored as JSON, and when the query engine reads it back with `json.Unmarshal` into `map[string]any`, **all numbers become `float64`**.

This is standard Go JSON behavior, but it means an integer ID like `12345` becomes `float64(12345)`, which would naively format as `12345` — correct — but a large integer like `9007199254740993` (beyond float64's exact range) would format incorrectly.

`formatValue` handles this explicitly:

```go
// internal/recovery/recovery.go
case float64:
    if !math.IsInf(val, 0) && !math.IsNaN(val) &&
        val == math.Trunc(val) && math.Abs(val) < 1e15 {
        return strconv.FormatInt(int64(val), 10)  // format as integer
    }
    return strconv.FormatFloat(val, 'f', -1, 64)
```

The `math.Abs(val) < 1e15` guard keeps the conversion in the safe integer range for float64 (which has 53 bits of mantissa). For whole-number floats within this range, the output is an integer literal. For fractional values or very large numbers, it uses decimal notation.

### Output Format

The recovery output is a self-contained SQL script:

```sql
-- Generated by bintrail recover at 2026-02-19 14:30:00 UTC
-- Events to reverse: 3
-- IMPORTANT: Review carefully before applying to production.

BEGIN;

-- [47] reverse DELETE on mydb.orders pk=42 at 2026-02-19 14:03:00 gtid=3e11fa47-...:99
INSERT INTO `mydb`.`orders` (`id`, `status`, `created_at`) VALUES (42, 'draft', '2026-02-19 14:01:00');

-- [46] reverse UPDATE on mydb.orders pk=42 at 2026-02-19 14:02:00 ...
UPDATE `mydb`.`orders` SET `status` = 'draft' WHERE `id` = 42;

COMMIT;
```

Key properties:
- Wrapped in `BEGIN` / `COMMIT` — all changes apply atomically or not at all.
- Comments before each statement showing the original event ID, type, table, PK, timestamp, and GTID.
- Generation errors emit a `-- ERROR ...` comment rather than halting — the script remains runnable (the transaction will roll back on the first error anyway).
- **Never auto-executed**: bintrail only generates the file. Applying it is always a manual step.

---

## The Full Query-to-Recovery Flow

```
bintrail query/recover
        │
        ├── parse flags → query.Options
        │
        ├── query.Engine.Fetch(ctx, opts)
        │       │
        │       ├── buildQuery(opts) → SQL + args
        │       ├── db.QueryContext → *sql.Rows
        │       └── scanRows → []ResultRow
        │              (json.Unmarshal row_before, row_after, changed_columns)
        │
        ├── [query] → format as table/json/csv → stdout
        │
        └── [recover]
                ├── slices.Reverse(rows)
                ├── for each row:
                │       generateStatement(row)
                │           DELETE → generateInsert (from row_before)
                │           UPDATE → generateUpdate (SET row_before WHERE row_after PK)
                │           INSERT → generateDelete (WHERE row_after PK)
                └── write BEGIN ... statements ... COMMIT → file or stdout
```
