# Querying Parquet Archives with DuckDB CLI

When `bintrail rotate --archive-dir` writes Parquet files (or uploads them to S3), you can query those files directly with the DuckDB CLI. This is useful for debugging archive queries, profiling performance, or inspecting archived data outside of `bintrail query --archive-s3`.

---

## Installing DuckDB

Download the CLI from [duckdb.org/docs/installation](https://duckdb.org/docs/installation). On macOS with Homebrew:

```sh
brew install duckdb
```

Verify with `duckdb --version`.

---

## Parquet Column Schema

Archive Parquet files contain 15 columns (the `pk_hash` stored generated column is omitted):

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | BIGINT | Auto-increment row ID from `binlog_events` |
| `binlog_file` | VARCHAR | Source binlog filename (e.g. `binlog.000042`) |
| `start_pos` | BIGINT | Byte offset where the event starts in the binlog |
| `end_pos` | BIGINT | Byte offset where the event ends |
| `event_timestamp` | TIMESTAMP | When MySQL executed the event (UTC) |
| `gtid` | VARCHAR | GTID if available (nullable) |
| `connection_id` | INT | MySQL connection ID / pseudo_thread_id (nullable) |
| `schema_name` | VARCHAR | Database name |
| `table_name` | VARCHAR | Table name |
| `event_type` | TINYINT | 1 = INSERT, 2 = UPDATE, 3 = DELETE |
| `pk_values` | VARCHAR | Pipe-delimited primary key values |
| `changed_columns` | VARCHAR | JSON array of changed column names (nullable) |
| `row_before` | VARCHAR | JSON object of the row before the event (nullable) |
| `row_after` | VARCHAR | JSON object of the row after the event (nullable) |
| `schema_version` | INT | Schema snapshot version at index time |

---

## Querying Local Parquet Files

Archives written by `bintrail rotate --archive-dir` follow a Hive-partitioned directory layout:

```
/mnt/archives/
  bintrail_id=abc123de-0000-0000-0000-000000000001/
    event_date=2026-02-13/
      event_hour=00/
        events.parquet
      event_hour=01/
        events.parquet
    event_date=2026-02-14/
      ...
```

Query all files under a directory with a glob pattern:

```sql
SELECT * FROM parquet_scan('/mnt/archives/**/*.parquet', hive_partitioning=true)
LIMIT 10;
```

The `hive_partitioning=true` option makes DuckDB recognize `bintrail_id`, `event_date`, and `event_hour` as virtual columns. You can filter on them and DuckDB will skip reading files that don't match:

```sql
-- Only reads Parquet files under event_date=2026-02-13/
SELECT * FROM parquet_scan('/mnt/archives/**/*.parquet', hive_partitioning=true)
WHERE event_date = '2026-02-13'
  AND schema_name = 'mydb'
  AND table_name = 'orders'
LIMIT 10;
```

To query a single server's archives without Hive partitioning, scope the glob:

```sql
SELECT * FROM parquet_scan('/mnt/archives/bintrail_id=abc123de-0000-0000-0000-000000000001/**/*.parquet')
WHERE schema_name = 'mydb' AND table_name = 'orders'
LIMIT 10;
```

---

## Querying S3-Hosted Parquet Files

### Loading the httpfs extension

DuckDB needs the `httpfs` extension to read from S3:

```sql
INSTALL httpfs;
LOAD httpfs;
```

On first use DuckDB downloads the extension from its registry. In airgapped environments, pre-install it on a machine with internet access and copy the extension cache.

### Configuring S3 credentials

Set credentials before querying. DuckDB supports the standard AWS credential chain, but you can also set them explicitly:

```sql
SET s3_region = 'us-east-1';
SET s3_access_key_id = 'AKIA...';
SET s3_secret_access_key = '...';
```

Or rely on environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`) and `~/.aws/credentials` — DuckDB picks these up automatically.

### Running queries

```sql
-- List all archived events for a specific table
SELECT * FROM parquet_scan('s3://my-bucket/archives/**/*.parquet', hive_partitioning=true)
WHERE schema_name = 'mydb' AND table_name = 'users'
LIMIT 10;

-- Scope to a single bintrail server
SELECT * FROM parquet_scan('s3://my-bucket/archives/bintrail_id=abc123de-0000-0000-0000-000000000001/**/*.parquet')
WHERE schema_name = 'mydb' AND table_name = 'users'
LIMIT 10;
```

---

## Inspecting Parquet Files

DuckDB provides metadata functions for examining Parquet file structure without reading the data.

### File schema

```sql
SELECT * FROM parquet_schema('/mnt/archives/bintrail_id=abc123de-0000-0000-0000-000000000001/event_date=2026-02-13/event_hour=00/events.parquet');
```

Shows column names, types, and encoding. Useful for verifying the archive format matches expectations.

### File metadata

```sql
SELECT * FROM parquet_metadata('/mnt/archives/bintrail_id=abc123de-0000-0000-0000-000000000001/event_date=2026-02-13/event_hour=00/events.parquet');
```

Shows row group count, row counts per group, compression codec, and sizes. This helps diagnose performance issues — many small row groups or uncompressed files can slow queries.

These functions also work with S3 paths (after loading `httpfs`):

```sql
SELECT * FROM parquet_metadata('s3://my-bucket/archives/bintrail_id=abc123de-0000-0000-0000-000000000001/event_date=2026-02-13/event_hour=00/events.parquet');
```

---

## Performance Profiling

### EXPLAIN ANALYZE

Measure query execution time and see how DuckDB processes the query:

```sql
EXPLAIN ANALYZE
SELECT * FROM parquet_scan('s3://my-bucket/archives/**/*.parquet', hive_partitioning=true)
WHERE pk_values = '42';
```

The output shows the operator tree with row counts and timing at each stage.

### Filter pushdown

DuckDB pushes certain filters into the Parquet reader so it can skip entire row groups using Parquet min/max statistics. Filters that benefit from pushdown:

| Filter | Pushdown? | Notes |
|--------|-----------|-------|
| `event_timestamp >= ?` / `<= ?` | Yes | Effective when data is sorted by timestamp (bintrail writes in timestamp order) |
| `schema_name = ?` | Yes | Pushed as a predicate on the string column |
| `table_name = ?` | Yes | Same as above |
| `pk_values = ?` | Partial | Pushed down, but row groups contain many distinct PKs so few groups are skipped |
| `event_type = ?` | Yes | Small cardinality (0/1/2) — effective at skipping groups with only one type |
| `json_contains(changed_columns, ?)` | No | Function call — evaluated post-scan |
| Hive partition keys (`event_date`, `event_hour`) | Yes | DuckDB skips entire files that don't match |

To see whether pushdown is happening, check the `EXPLAIN ANALYZE` output for `PARQUET_SCAN` filters vs. `FILTER` operators above it. Filters listed in the `PARQUET_SCAN` node are pushed down; filters in a separate `FILTER` node are applied post-scan.

### Tips for diagnosing slow queries

1. **Add time range filters**: Always include `event_timestamp` (or Hive partition key) bounds. Without them, DuckDB reads every Parquet file.

2. **Check row group size**: Archives written with very small row groups (< 10,000 rows) produce excessive per-group overhead. The default `--row-group-size` in bintrail is large enough to avoid this. Check with `parquet_metadata()`.

3. **Reduce file count**: Scanning thousands of small files is slower than fewer large ones. If you have many hourly partitions archived, consider the Hive partition filters (`event_date`, `event_hour`) to narrow the scan.

4. **Profile PK lookups**: PK lookups (`pk_values = '...'`) can't use an index (Parquet has no indexes), so they scan all row groups. Combine with a time range to limit the scan:

   ```sql
   EXPLAIN ANALYZE
   SELECT * FROM parquet_scan('s3://my-bucket/archives/**/*.parquet', hive_partitioning=true)
   WHERE pk_values = '42'
     AND event_date = '2026-02-13';
   ```

5. **Compare with bintrail query**: Run the same filters through `bintrail query --archive-s3` and through DuckDB CLI to compare results and timing. Differences may reveal filter translation issues.

---

## Example Queries

### Count events per table

```sql
SELECT schema_name, table_name, event_type, COUNT(*) AS cnt
FROM parquet_scan('/mnt/archives/**/*.parquet', hive_partitioning=true)
GROUP BY schema_name, table_name, event_type
ORDER BY cnt DESC;
```

### Find all changes to a specific primary key

```sql
SELECT event_id, event_timestamp, event_type, changed_columns, row_before, row_after
FROM parquet_scan('s3://my-bucket/archives/**/*.parquet', hive_partitioning=true)
WHERE pk_values = '42'
  AND schema_name = 'mydb'
  AND table_name = 'orders'
ORDER BY event_timestamp;
```

### Show event volume by hour

```sql
SELECT date_trunc('hour', event_timestamp) AS hour, COUNT(*) AS events
FROM parquet_scan('/mnt/archives/**/*.parquet', hive_partitioning=true)
WHERE event_date = '2026-02-13'
GROUP BY hour
ORDER BY hour;
```

### Inspect row data for a DELETE

```sql
SELECT event_id, event_timestamp, pk_values, row_before
FROM parquet_scan('/mnt/archives/**/*.parquet', hive_partitioning=true)
WHERE event_type = 3  -- DELETE
  AND schema_name = 'mydb'
  AND table_name = 'users'
ORDER BY event_timestamp DESC
LIMIT 5;
```

### Profile a PK lookup with EXPLAIN ANALYZE

```sql
EXPLAIN ANALYZE
SELECT * FROM parquet_scan('s3://my-bucket/archives/**/*.parquet', hive_partitioning=true)
WHERE pk_values = '42';
```

---

## Troubleshooting Archive Fetch Errors from `bintrail query`

When `bintrail query` reads from a Parquet archive and the read fails, it prints a warning like this to stderr at the default log level:

```
Warning: archive query failed for s3://my-bucket/events/bintrail_id=<uuid>: <error text>
```

The warning is **visible regardless of `--log-level` / `--log-format`** — if you see it once, the archive in question was skipped and the query proceeded with whatever other sources (live MySQL + other archives) succeeded. One bad archive never kills the whole query; only context cancellation (Ctrl-C or deadline expiry) short-circuits the command with a non-zero exit. See [query-and-recovery.md § Archive Fetch Error Handling](query-and-recovery.md#archive-fetch-error-handling) for the full behavior contract.

The subsections below catalogue the common failure modes and how to diagnose each one with the DuckDB CLI.

### Binder Error: column `connection_id` not found

```
Warning: archive query failed for s3://.../bintrail_id=<uuid>: Binder Error: Referenced column "connection_id" not found in FROM clause
```

**Cause**: archive Parquet files written by `bintrail` versions before v0.4.4 lack the `connection_id` column, which was added in v0.4.4 when the indexer started recording `pseudo_thread_id` from `QueryEvent.SlaveProxyID`. v0.4.8 fixed the per-file query to probe the Parquet schema and substitute `NULL::INT32 AS connection_id` when the column is absent, but old Parquet files written by even older bintrail versions might still trigger this on environments that haven't upgraded.

**Diagnose**:

```sql
-- Check which columns the offending file actually has
SELECT * FROM parquet_schema('s3://my-bucket/events/bintrail_id=<uuid>/event_date=2026-01-15/event_hour=14/events.parquet');
```

If `connection_id` is missing, the file was written by a pre-v0.4.4 indexer. Either re-archive from the live index with a current bintrail version, or accept that the column will be NULL for those events in merged results (which is what current bintrail already does).

### S3 AccessDenied / credential errors

```
Warning: archive query failed for s3://.../bintrail_id=<uuid>: IO Error: S3 AccessDenied: ...
```

**Cause**: expired AWS credentials, a mis-scoped IAM role, or a bucket policy change. bintrail uses DuckDB's standard AWS credential chain (env vars → `~/.aws/credentials` → IAM role).

**Diagnose**: reproduce the failure in the DuckDB CLI with the same credentials:

```sh
# First, confirm the AWS CLI can see the bucket:
aws s3 ls s3://my-bucket/events/bintrail_id=<uuid>/

# Then, reproduce the bintrail archive read in DuckDB:
duckdb -c "INSTALL httpfs; LOAD httpfs; SELECT COUNT(*) FROM parquet_scan('s3://my-bucket/events/bintrail_id=<uuid>/**/*.parquet');"
```

If DuckDB reports the same error, the issue is the credential chain, not bintrail. If `aws s3 ls` succeeds but DuckDB fails, DuckDB may be using a different credential profile than the AWS CLI — explicitly set `AWS_PROFILE` or `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_REGION` in the shell running `bintrail query`.

### DuckDB memory_limit exceeded

```
Warning: archive query failed for s3://.../bintrail_id=<uuid>: Out of Memory Error: could not allocate block of size ... (memory_limit is ...)
```

**Cause**: the scan loaded more row groups than fit in DuckDB's `memory_limit` setting. This typically only fires on broad queries (no `--since`/`--until`) against large archives.

**Diagnose + fix**: narrow the time range. Every `bintrail query` invocation against an archive should include at least a `--since`/`--until` window to bound the Parquet scan:

```sh
bintrail query \
  --index-dsn "..." \
  --schema    mydb \
  --table     orders \
  --since     "2026-02-01 00:00:00" \
  --until     "2026-02-08 23:59:59" \
  --archive-s3 s3://my-bucket/events/ \
  --bintrail-id <uuid>
```

You can also raise DuckDB's `memory_limit` in a direct DuckDB CLI session for debugging — bintrail's internal DuckDB instance uses the default (80% of system RAM), so the usual fix is to narrow the query.

### Corrupted Parquet file

```
Warning: archive query failed for s3://.../bintrail_id=<uuid>: IO Error: Failed to open Parquet file ... Invalid Input Error: ...
```

**Cause**: a Parquet file was truncated during upload, corrupted in transit, or never fully written (e.g. `bintrail rotate` was killed mid-archive). `bintrail rotate` writes to a temp file and renames atomically, so a partial file on disk usually indicates external interference.

**Diagnose**: identify the offending file from the archive path, then inspect it with DuckDB:

```sql
SELECT * FROM parquet_metadata('s3://my-bucket/.../events.parquet');
```

If `parquet_metadata` itself errors, the file is unreadable. Either restore it from a backup or re-archive the corresponding hour from the live index (if it's still within the retention window) via `bintrail rotate --archive-dir` against the original source.

### Context canceled / deadline exceeded

```
Error: query canceled: context canceled
```

(Note: this is an **error**, not a warning — it's printed via cobra's error path, not the archive-failure stderr channel. The command exits non-zero.)

**Cause**: the user pressed Ctrl-C, or the parent context (e.g. an orchestrator-imposed timeout) fired. Unlike plain archive failures, cancellation halts the whole query immediately without printing per-source warnings.

**Diagnose**: if you didn't press Ctrl-C, check for a parent-process timeout. Common culprits: a `timeout` wrapper in a shell script, a Kubernetes liveness probe killing the pod, a CI runner with a job-level time budget.

If the cancellation is fired by a context deadline (not Ctrl-C), the wrapped error is `context.DeadlineExceeded` instead of `context.Canceled`. Both short-circuit the archive loop via the same path.

### "Works in DuckDB CLI, fails in bintrail query"

If you can run the same glob directly in the DuckDB CLI but `bintrail query` fails with an archive warning, compare the exact query bintrail issued. Run with `--log-level debug` to see the generated DuckDB SQL:

```sh
bintrail query --index-dsn "..." --archive-s3 s3://... --bintrail-id <uuid> \
  --since "2026-02-01 00:00:00" --log-level debug 2>&1 | grep -i parquet
```

Copy the generated `SELECT ... FROM parquet_scan(...)` into the DuckDB CLI with the same filters and compare. The two should produce identical results; if they don't, the discrepancy is either (a) a filter-translation bug in bintrail (worth filing), or (b) a DuckDB version mismatch between bintrail's embedded DuckDB and your CLI install.
