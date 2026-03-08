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

Archive Parquet files contain 14 columns (the `pk_hash` stored generated column is omitted):

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | BIGINT | Auto-increment row ID from `binlog_events` |
| `binlog_file` | VARCHAR | Source binlog filename (e.g. `binlog.000042`) |
| `start_pos` | BIGINT | Byte offset where the event starts in the binlog |
| `end_pos` | BIGINT | Byte offset where the event ends |
| `event_timestamp` | TIMESTAMP | When MySQL executed the event (UTC) |
| `gtid` | VARCHAR | GTID if available (nullable) |
| `schema_name` | VARCHAR | Database name |
| `table_name` | VARCHAR | Table name |
| `event_type` | TINYINT | 0 = INSERT, 1 = UPDATE, 2 = DELETE |
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
WHERE event_type = 2  -- DELETE
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
