# Verifying a recovery (`bintrail verify`)

`bintrail verify` proves that the recovery chain — a baseline snapshot plus the
indexed binlog deltas on top of it — would faithfully reproduce your data. It
answers the question the rest of dbtrail assumes: *if I reconstructed this table,
would I actually get the right rows back?*

It is read-only and never writes to your source or your index.

## The two modes

### Baseline-anchored (default, drift-free)

Omit `--source-dsn`. `verify` compares the **two most recent baselines** of each
table: it reconstructs the older baseline *forward* — applying indexed binlog
events up to the newer baseline's exact binlog anchor — and fingerprints the
result against the newer baseline.

Both sides are at-rest data (Parquet snapshots), so this mode reads **no live
source** and has **no production impact**. Run it any time after a baseline — for
example right after `bintrail baseline`, or on a schedule (cron/CI). Because
neither side is the live table, it can't be fooled by drift that happened on the
source after capture; it tests the capture-and-reconstruct chain itself.

```sh
# All tables, baselines on local disk
bintrail verify --index-dsn "$IDX" --baseline-dir /data/baselines

# With a row-level drill-down on any mismatch
bintrail verify --index-dsn "$IDX" --baseline-dir /data/baselines --explain
```

### Live-source

Pass `--source-dsn`. For each table, `verify` reconstructs a consistent
point-in-time snapshot and compares it against the **live source** table. This
reads the whole table off the live server, so **run it off-peak**.

```sh
bintrail verify --source-dsn "$SRC" --index-dsn "$IDX" \
  --baseline-s3 s3://bucket/baselines --tables mydb.orders,mydb.users
```

## What it reports

Results are **per table**, one of:

- **match** — the reconstruction reproduces the comparison exactly.
- **mismatch** — they differ. The chain would not reproduce this table; investigate.
- **inconclusive** — verify could not prove the table either way, and this is
  **never reported as a failure**. Causes include: no predecessor baseline yet,
  the index is behind the baseline anchor, an unsupported primary key, a coverage
  gap, or a value class this version cannot yet compare.

## Exit codes (for cron / CI)

- **Non-zero** on any **mismatch** or error, **or** when comparable tables
  existed but none could be proven (all inconclusive). An `inconclusive` table
  never *by itself* fails the run — but a run where *no* table could be proven does.
- **Zero** when a source has only one baseline (no predecessor to compare yet) —
  this is reported, not failed.
- **Non-zero** when *no* baselines are found at all — that's a misconfiguration
  (wrong `--baseline-dir`/`--baseline-s3`), not a "nothing to do."

This makes `bintrail verify` safe to wire straight into a pipeline: a clean exit
means "nothing disproved the recovery chain."

## `--explain` — row-level drill-down

In baseline-anchored mode, add `--explain` to print, below the per-table report,
exactly which rows diverged on a mismatch: the differing primary keys and, for
changed rows, the differing columns with the reconstructed value vs the new
baseline's. It re-runs the same reconstruction the verdict came from (byte-
identical by construction) — no live source, scratch database, or external
diff tool involved.

## Flags

| Flag | Default | Description |
|---|---|---|
| `--index-dsn` | *(required)* | DSN for the index MySQL database |
| `--source-dsn` | *(empty)* | Live source DSN. Pass it for **live-source** mode; omit for **baseline-anchored** mode |
| `--baseline-dir` | *(empty)* | Local directory of baseline Parquet snapshots |
| `--baseline-s3` | *(empty)* | S3 URL prefix of baseline snapshots (e.g. `s3://bucket/baselines/`) |
| `--tables` | *(all)* | Comma-separated `schema.table` list (default: all tables in the latest schema snapshot) |
| `--no-archive` | `false` | Query live MySQL partitions only; skip Parquet archive discovery |
| `--explain` | `false` | On a baseline-anchored mismatch, print a per-row drill-down |

One of `--baseline-dir` or `--baseline-s3` is **required** — `verify` always reads baselines, in both modes.

It also accepts the shared DuckDB tuning flags (`--ultrafast`,
`--duckdb-threads`, `--duckdb-memory-limit`) — see the DuckDB resource tuning
section in [Query & Recovery](query-and-recovery.md).

## How it relates to `recover` and `reconstruct`

- **`recover`** undoes *specific touched rows* from stored before/after images
  (delta-only).
- **`reconstruct`** materializes a *full table or row* at a point in time
  (baseline + deltas).
- **`verify`** doesn't recover anything — it *checks* that the `reconstruct`
  chain is sound, so you find out your recoveries are trustworthy **before** you
  need them.

See also: the stream-continuity "no data lost" signal in
[`bintrail status`](rotation-and-status.md#stream-continuity-no-data-lost), which
verifies the *other* half — that no events were dropped from the capture stream.

## Notes per source

`verify` runs against the MySQL index, so it works for any source family whose
baselines are wired. Baseline-anchored mode requires baselines; **PostgreSQL
baselines are not yet wired** (see [PostgreSQL beta limitations](postgres.md#beta-limitations)),
so `verify` against a PostgreSQL source is not usable in this release.
