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
|--------|---------------|
| `Schema` | `schema_name = ?` |
| `Table` | `table_name = ?` |
| `PKValues` | `pk_hash = SHA2(?, 256) AND pk_values = ?` |
| `EventType` | `event_type = ?` |
| `GTID` | `gtid = ?` |
| `Since` | `event_timestamp >= ?` (+ pruning hint for non-hour-aligned values) |
| `Until` | `event_timestamp <= ?` (+ pruning hint for non-hour-aligned values) |
| `ChangedColumn` | `JSON_CONTAINS(changed_columns, ?)` |
| `Flag` | `EXISTS (SELECT 1 FROM table_flags WHERE schema_name = binlog_events.schema_name AND table_name = binlog_events.table_name AND flag = ?)` |

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

### Partition Pruning Guarantee

MySQL can prune `RANGE (TO_SECONDS(event_timestamp))` partitions when it can compare the query bounds directly against the stored `TO_SECONDS` integer literals. For parameterised datetime comparisons (`event_timestamp >= ?`), the optimizer must infer this — and for non-hour-aligned values it may not.

When `--since` or `--until` has non-zero minutes/seconds (e.g. `15:45:00`), `buildQuery` adds an extra condition using inlined `TO_SECONDS()` integer literals alongside the exact parameterised bound:

```sql
WHERE TO_SECONDS(event_timestamp) >= 63826647000   -- floor to hour: 15:00
  AND event_timestamp >= ?                          -- exact lower bound
  AND TO_SECONDS(event_timestamp) < 63826654800    -- ceil to next hour: 17:00
  AND event_timestamp <= ?                          -- exact upper bound
```

The integer literals are evaluated at parse time, so MySQL can always prune partitions before executing the query — no optimizer inference needed. For hour-aligned ranges (e.g. `15:00:00`–`16:00:00`), no extra conditions are added.

This is transparent to users. The same `--since`/`--until` flags work as before, but queries against non-hour-aligned windows now reliably skip irrelevant partitions.

### Output Formats

Results can be formatted three ways:

**`table`** (default): Uses `text/tabwriter` for aligned columns. Shows `event_id`, `timestamp`, `type`, `schema`, `table`, `pk_values`, `changed_cols`, and `gtid`. Does NOT include `row_before`/`row_after` — the table format is designed to be scannable. Use `--format json` to see full row data.

**`json`**: Each event is a JSON object with all fields including `row_before` and `row_after` as nested objects. Indented for readability. The `event_type` is serialized as a string (`"INSERT"`, `"UPDATE"`, `"DELETE"`), not the raw integer.

**`csv`**: All columns including `row_before`/`row_after` serialized as JSON strings in the CSV cells. Fixed column order matching `csvHeaders`.

---

## Parquet Archive Queries

When rotated partitions have been archived (via `bintrail rotate --archive-dir` or `--archive-s3`), events are no longer in the MySQL index. The `query` and `recover` commands can merge results from these archives with the live index.

### Auto-Discovery (default)

When you run `bintrail rotate --archive-dir` or `--archive-s3`, the archive locations are recorded in the `archive_state` table. Both `query` and `recover` automatically discover these sources — no extra flags needed:

```sh
# Archives are discovered from archive_state automatically
bintrail query \
  --index-dsn  "..." \
  --schema     mydb \
  --table      orders \
  --since      "2026-01-01 00:00:00"
```

### Explicit Archive Flags (override)

You can also specify archive sources explicitly with `--archive-dir` and `--archive-s3`. When these are set, auto-discovery is skipped. **`--bintrail-id` is required** with explicit flags — it scopes the DuckDB glob to that server's archives.

```sh
bintrail query \
  --index-dsn  "..." \
  --archive-s3 s3://my-bintrail-archives/events/ \
  --bintrail-id 3e11fa47-71ca-11e1-9e33-c80aa9429562 \
  --schema     mydb \
  --table      orders \
  --since      "2026-01-01 00:00:00"
```

### `--no-archive` Flag

Use `--no-archive` to disable archive auto-discovery entirely and return MySQL-only results. This is useful when you only want live data or when archive queries are slow:

```sh
bintrail query --index-dsn "..." --schema mydb --table orders --no-archive
```

`--no-archive` cannot be combined with `--archive-dir` or `--archive-s3`.

### Coverage Warnings and Query Planner

When a time range is specified (`--since`/`--until`), the query planner inspects live MySQL partition boundaries and the `archive_state` table to detect coverage gaps — hours where data has been rotated out of MySQL but no archive exists. These gaps are reported as warnings:

```
WARN query covers hours with no data (rotated and not archived): 2026-02-10 00:00 – 2026-02-12 23:00
```

The planner also optimizes routing: if the entire queried time range is covered by archives (no live MySQL partitions needed), the MySQL query is skipped entirely.

### How the Merge Works

When archive sources are available (via auto-discovery or explicit flags), the query command takes a different path:

1. **Fetch from MySQL index** — same query as usual, but with no `LIMIT` (`Limit=0` omits the LIMIT clause so no events are dropped before the merge).
2. **Fetch from each archive source** — DuckDB opens the Parquet files via `parquet_scan('glob/**/*.parquet')`, applies the same filters (schema, table, PK, time range, etc.) in DuckDB SQL, and returns `[]ResultRow`.
3. **Merge** — results from all sources are combined, deduplicated by `event_id` (MySQL wins on duplicates, since it is appended first), sorted by `(event_timestamp, event_id)`, and then the user's `--limit` is applied once.

```
bintrail query --archive-s3 s3://... --bintrail-id <uuid> --since "2026-02-01 00:00:00"
                     │
          ┌──────────┴──────────┐
          ▼                     ▼
   MySQL index            DuckDB (S3 Parquet)
   (live data)            s3://.../bintrail_id=<uuid>/**/*.parquet
          │                     │
          └──────────┬──────────┘
                     ▼
              dedup + sort + limit
                     │
                     ▼
               formatted output
```

### Archive Fetch Error Handling

When an archive source fails — expired AWS credentials, S3 `AccessDenied`, DuckDB `memory_limit` OOM, a corrupted Parquet file, a `Binder Error` on a schema drift — `bintrail query` prints a visible warning to **stderr** regardless of `--log-level` or `--log-format`:

```
Warning: archive query failed for s3://my-bucket/events/bintrail_id=<uuid>: S3 AccessDenied: assume role failed
```

One failure always occupies exactly one stderr line: embedded newlines in the underlying error message (DuckDB Binder errors, AWS SDK error chains) are collapsed to ` | ` separators so `grep`, `systemd-journald`, and other line-oriented consumers don't split a single warning across multiple log entries.

The same record is also emitted as a structured `slog.Warn` with the **raw** (unsanitized) error for JSON-log consumers and full-fidelity debugging — a `--log-format json` pipeline preserves embedded newlines natively.

**Per-source failures are non-fatal.** One broken archive source does not kill the whole query — the loop continues to the next source, any other source that succeeds contributes its rows to the merged result set, and the command exits 0. This is deliberate: operators running against multi-region S3 archives shouldn't lose a regional query because one bucket is temporarily unreachable. If you need a strict all-or-nothing guarantee, use `bintrail reconstruct` (which sets `AllowGaps=false` in its shared `FetchMerged` pipeline).

**Context cancellation is fatal.** If the user presses Ctrl-C, or the parent context times out, or a fetch error wraps `context.Canceled` / `context.DeadlineExceeded`, the archive loop short-circuits immediately and the command exits non-zero with `query canceled: context canceled`. No per-source warnings are printed during a canceled run — a Ctrl-C'd query should exit cleanly, not dump a warning per remaining source. When cancellation fires mid-loop, any rows already accumulated from earlier sources AND any live-MySQL rows fetched before the archive loop started are discarded: a canceled query is an incomplete query, and showing partial results alongside a "canceled" error would invite the operator to treat them as authoritative.

> **History**: `bintrail` versions before 0.4.8 silently swallowed every archive fetch error into a `slog.Warn` (invisible at the default text log format) and continued. A real production incident in early 2026 caused by pre-v0.4.4 Parquet files missing the `connection_id` column produced six days of zero-result queries with exit 0 and no stderr signal — only caught when a user escalated. 0.4.8 fixed the specific `Binder Error` trigger at the `parquetquery` layer; a follow-up fix (#203) surfaced every remaining archive failure mode on stderr so future unknown failures cannot reproduce the same silent-data-loss pattern.

### Memory Footprint

The merge path loads **all matching rows** from all sources into memory before applying the limit. Filters (schema, table, time range, etc.) bound the result set in practice. For extremely broad queries against large archives, memory usage could be significant — apply at least a `--since`/`--until` range to keep the result set manageable.

### S3 Prerequisites

`--archive-s3` uses DuckDB's `httpfs` extension:

- **AWS credentials** — DuckDB uses the standard credential chain: `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` env vars, `~/.aws/credentials`, or an IAM role. Set credentials before running the query.
- **Outbound internet access** — on first use, DuckDB downloads the `httpfs` extension from its extension registry. In airgapped environments, pre-install it by running `duckdb -c "INSTALL httpfs;"` once on a machine with internet access and copying the extension cache.

---

## Recovery: How It Works

The `recover` command also supports archive auto-discovery and the `--no-archive` flag, using the same merge logic as `query`. When archives are available, events are fetched from both MySQL and Parquet, merged, and then passed to the SQL generator via `GenerateSQLFromRows`.

### The Concept

Recovery works because bintrail stores **full before and after images** for every row event. To undo an operation, you simply reverse it:

| Original operation | Reversal |
|--------------------|----------|
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
        ├── resolve archive sources (auto-discovery from archive_state, or explicit flags)
        │   └── query.ResolveArchiveSources(ctx, db) when no explicit flags and no --no-archive
        │
        ├── query planner: Plan(ctx, db, dbName, since, until)
        │   └── warns about coverage gaps, may skip MySQL if fully archived
        │
        ├── if no archives: fast path
        │       └── query.Engine.Run(ctx, opts, format, w) → stdout
        │
        ├── if archives: merge path
        │       ├── query.Engine.Fetch(ctx, opts) → []ResultRow (MySQL, unless planner skips)
        │       ├── queryArchiveSources(ctx, sources, opts, parquetquery.Fetch, os.Stderr)
        │       │       ├── for each source: parquetquery.Fetch(...)
        │       │       ├── on plain error: stderr warning + slog.Warn, continue
        │       │       └── on ctx.Err() or errors.Is(err, context.Canceled|DeadlineExceeded):
        │       │               return (nil, wrapped-ctx-err) — short-circuit
        │       └── query.MergeResults(all, limit) → dedup + sort + limit
        │
        ├── [query] → query.Format(results, format, w) → stdout
        │
        └── [recover]
                ├── recovery.GenerateSQLFromRows(rows, w)
                │       ├── slices.Reverse(rows)
                │       └── for each row:
                │               generateStatement(row)
                │                   DELETE → generateInsert (from row_before)
                │                   UPDATE → generateUpdate (SET row_before WHERE row_after PK)
                │                   INSERT → generateDelete (WHERE row_after PK)
                └── write BEGIN ... statements ... COMMIT → file or stdout
```
