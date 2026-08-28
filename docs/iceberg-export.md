# Iceberg export

`bintrail export iceberg` writes the current state of your tables as Apache
Iceberg tables, and keeps them current by appending only what changed. DuckDB,
Spark, Trino, Athena and Snowflake read Iceberg, so this is the way to put
bintrail's data in front of a reporting engine without a full dump on a timer.

It is an output. bintrail's own storage does not change: the archives,
baselines and index stay what they are (see
[The archives are plain Parquet on purpose](parquet-debugging.md#the-archives-are-plain-parquet-on-purpose)),
and the export reads through them and writes somewhere new. Nothing on the
recovery path links the Iceberg library, and a test in the repository fails if
that ever changes.

## Quick start

You need a baseline snapshot of the tables you want to export (the same one
time-travel uses; see [Dump and Baseline](dump-and-baseline.md)) and the
index.

```bash
# First run: load every table of the newest snapshot, then fold the deltas
bintrail export iceberg \
  --index-dsn "user:pass@tcp(index:3306)/bintrail_index" \
  --baseline-dir /data/baselines \
  --warehouse /data/iceberg

# Read it back
duckdb -c "INSTALL iceberg; LOAD iceberg;
           SELECT status, count(*) FROM iceberg_scan('/data/iceberg/shop/orders') GROUP BY 1;"
```

Run the same command line again whenever you want the tables brought forward.
From cron, hourly is a good default:

```
17 * * * * bintrail export iceberg --index-dsn "..." --baseline-dir /data/baselines --warehouse /data/iceberg
```

Each table lives at `<warehouse>/<schema>/<table>/` with Iceberg's `data/` and
`metadata/` directories and a `version-hint.text`. Point any Iceberg reader at
the table directory.

## What a run does

**The first run** for a table loads its newest baseline snapshot as the
table's first data files, and records the snapshot's binlog position (the
anchor the baseline itself carries) as the table's cursor. Then it continues
as below.

**Every run** fetches the events between the table's cursor and the run's
binlog cut, folds them to the net change per primary key, and commits that as
one Iceberg snapshot:

- an equality-delete file naming every key that was touched, and
- a data file with the current row of every touched key that still exists.

An update is therefore a delete of the old row plus an insert of the new one,
in the same commit. A key changed fifty times in the window costs one delete
row and one data row, not fifty. The table is never rewritten; compaction, if
you want it, is the reader's concern and any engine can do it.

The cursor moves to the run's cut in the same commit as the data. It is stored
in the table's own properties, visible with
`SELECT * FROM iceberg_table_properties('<table dir>')` as
`bintrail.export.binlog_file`, `bintrail.export.binlog_position` and
`bintrail.export.at`. Nothing is written to the index.

Because the commit is atomic, a run that dies after writing files and before
committing leaves the previous snapshot readable and the cursor where it was;
the next run resumes from it and the orphaned files are never referenced.

A window with no events commits nothing: the table keeps its snapshot count
and its cursor.

## What it refuses, and why

Refusals are per table: a refused table does not advance and is retried on the
next run, the other tables commit, and the exit status is non-zero when any
table did not end current. The vocabulary is the same as `baseline refresh`.

| verdict | when | what to do |
|---|---|---|
| `refused-gap` | the window spans events the index permanently lost, or hours rotated out without an archive | there is no flag for this; the missing events are missing. Take a fresh baseline and remove the table directory so the next run reloads from it |
| `refused-ddl` | the table changed shape since it was exported (a column added or dropped), or a TRUNCATE / DROP / RENAME sits in the window | remove the table directory and let the next run reload it from a baseline taken after the change |
| `refused` | anything else; the detail line says what | read the detail line |

Two conditions refuse before anything is written:

- **No primary key**, or a key of type FLOAT, DOUBLE, TIME, BIT, JSON or a
  spatial type. Equality deletes name rows by key, so the export needs one it
  can compare.
- **BIT columns**. The baseline stores them as bytes and the row events store
  them as integers, and the export does not reconcile the two yet.

And one refuses the whole run: an index that holds **more than one source**
(more than one row in `bintrail_servers`). Nothing downstream of the archive
registry attributes an event to a source, so two sources with the same
`schema.table` would interleave in one Iceberg table.

## How the columns map

| MySQL | Iceberg | note |
|---|---|---|
| TINYINT, SMALLINT, MEDIUMINT, INT, YEAR | int | INT UNSIGNED becomes long |
| BIGINT | long | BIGINT UNSIGNED becomes decimal(20,0) |
| FLOAT, DOUBLE | float, double | |
| DECIMAL(p,s) | decimal(p,s) | above 38 digits, string |
| DATETIME, TIMESTAMP | timestamp (no zone) | the value as MySQL shows it, read as UTC wall clock |
| DATE | date | |
| TIME | string | |
| CHAR, VARCHAR, TEXT family, ENUM, SET, JSON | string | ENUM and SET are exported as their labels |
| BINARY, VARBINARY, BLOB family | binary | |
| BIT | refused | |

Zero dates (`0000-00-00`) become NULL, the same choice the baseline writer
makes. A fixed BINARY(n) primary key is exported without its storage padding,
so that the row events, which never carry it, match it.

## Reading the tables

DuckDB:

```sql
INSTALL iceberg; LOAD iceberg;
SELECT * FROM iceberg_scan('/data/iceberg/shop/orders') LIMIT 10;
SELECT * FROM iceberg_snapshots('/data/iceberg/shop/orders');
```

Time travel over the export works with `snapshot_from_id` or
`snapshot_from_timestamp`, since every run is a snapshot.

Two things to know:

- **DuckDB 1.4** applies equality deletes only when the key columns survive
  projection pushdown, so an aggregate straight over `iceberg_scan` can fail
  with `Equality deletes need the relevant columns to be selected`. Select the
  key columns too, or materialize first:
  `CREATE TABLE orders AS SELECT * FROM iceberg_scan('...')`. DuckDB 1.5 lifts
  this.
- **The table is not relocatable by default.** Iceberg metadata stores
  absolute paths. If you copy or sync the warehouse elsewhere (for example
  `bintrail upload` to S3), read it with
  `iceberg_scan('...', allow_moved_paths = true)`.

## Limits in this release

- The warehouse is a local directory. Writing directly to S3 is not supported
  yet; sync the directory and read with `allow_moved_paths`.
- No schema evolution yet. A column added or dropped in the source refuses the
  table (`refused-ddl`); remove the table directory to reload it from a fresh
  baseline. Iceberg tracks columns by id, which is what will make an ADD
  COLUMN a metadata change in a later release.
- Tables are unpartitioned.
- MySQL and MariaDB sources only; `bintrail-pg` does not have the command.
- One writer at a time: the run holds a lock file at the warehouse root and a
  second concurrent run refuses.
- Memory: a run holds one entry per primary key touched in its window, like
  `baseline refresh`. Run it often enough that a window stays a fraction of the
  table.

## Where it runs

This is a one-shot command for your scheduler. It never runs inside
`bintrail-console watch`: that process is the capture plane, and nothing that
competes for its CPU or its S3 bandwidth belongs in it. The DuckDB budget
defaults to the conservative 2 threads and 4 GB; `--ultrafast` is available
here because the process is yours.

Every table whose data was written is recorded in the audit trail as
`cli/export.iceberg`, after the commit is durable.

## Flags

| flag | meaning |
|---|---|
| `--index-dsn` | the index (required) |
| `--baseline-dir` / `--baseline-s3` | where the baseline snapshots are (one required) |
| `--warehouse` | local directory the Iceberg tables live under (required; env `BINTRAIL_ICEBERG_WAREHOUSE`) |
| `--tables` | comma-separated `schema.table` list (default: every table in the newest snapshot) |
| `--at` | export up to this instant (default: now) |
| `--fetch-batch-size` | event page size for the fold (0 = default) |
| `--format` | `text` or `json` |
| `--ultrafast`, `--duckdb-threads`, `--duckdb-memory-limit` | the DuckDB budget for the baseline scan |
