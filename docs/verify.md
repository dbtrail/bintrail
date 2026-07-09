# Verifying a recovery (`bintrail verify`)

`bintrail verify` checks that reconstructing a table — merging a baseline
snapshot with the indexed binlog deltas on top of it — reproduces the same
row content as an independent reference (the next baseline, or the live
source) at the specific anchor points being compared. It answers a narrower
question than "would my recoveries work": *at these anchor points, for the
tables and columns compared, does the full-table reconstruction/`_snapshot`
merge agree with the reference?*

It is read-only and never writes to your source or your index.

## What a MATCH does and does not prove

- **The comparison is a 64-bit, additive, non-cryptographic multiset
  fingerprint** over each table's rows — not a cryptographic hash. An
  accidental collision (two different row sets producing the same digest) has
  roughly a 2⁻⁶⁴ chance, and the digest is trivially forgeable by anyone able
  to write rows to the table being compared. A MATCH is strong evidence the
  capture-and-reconstruct pipeline works; it is not tamper-evident or
  forensic-grade proof.
- **It does not exercise the `recover` path.** `verify` reconstructs full-table
  state from the *latest* event per PK (`row_after`, `LimitPerPK=1`).
  `recover`'s reversal SQL additionally depends on `row_before` images,
  DELETE row images, and intermediate events later superseded by a newer event
  on the same PK — none of which a table-content match touches. A corrupt
  `row_before`, a corrupt DELETE image, or a corrupt superseded intermediate
  event can all pass `verify` cleanly while still producing a wrong `recover`
  reversal from that same data.
- **It does not exercise point-in-time reconstruction to an arbitrary instant
  between two anchors.** Baseline-anchored mode compares two baseline anchors
  (or a baseline vs. the live source at scan time); there is no independent
  ground truth for an arbitrary `--at`/`AS OF` instant in between, so nothing
  checks one.
- **It does not cover the restored *schema*.** `CREATE TABLE` definitions,
  indexes, `AUTO_INCREMENT` state, triggers, and collations are outside the
  row-content digest.
- **In default (baseline-anchored) mode, a table with no baseline yet is out
  of scope** — the same tables `reconstruct` also cannot materialize.
  `verify` reports these `inconclusive`, never as proof of correctness.
- **Live-source mode requires a quiescent source.** Any commit against the
  table during the scan reads as drift and reports as MISMATCH, even when the
  reconstruction itself is correct — the live table simply kept moving during
  the read.

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
  gap, a digest-contract skew between a pre-pin baseline and the current scan
  (regenerate the baseline — see below), or a value class this version cannot yet
  compare.

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

## Charset contract: raw stored bytes

The row-content fingerprint is computed over the bytes MySQL returns for each
column. To make that byte stream independent of the connection charset, the
checksum scan pins `character_set_results = binary`, so the server returns each
string column's **raw stored bytes** with no transcoding — the same contract
mydumper's `SET NAMES binary` writes the baseline Parquet under. A `latin1` `é`
(byte `0xE9`) therefore hashes as `0xE9` on both the live-scan side and the
baseline side, instead of the server transcoding it to utf8mb4 (`0xC3 0xA9`) on
one side only. Without this pin, every non-ASCII row of a legacy-charset
(`latin1`, `sjis`, …) table would differ in bytes even under a byte-correct
restore — a permanent, conclusive false **MISMATCH**. The pin removes the
charset dependency by construction.

### Digest version tag

Each digest carries a leading version tag (currently `v2:`) recording the
contract it was computed under — the Go-side encoding plus the MySQL-side
rendering (text protocol, session time zone UTC, and
`character_set_results = binary`). Pinning the binary charset bumped the tag
from `v1:` to `v2:`. Two digests are only byte-comparable when their tags match;
a **version skew** — e.g. a persisted `v1:` baseline digest written before the
pin, compared against a current `v2:` scan — is reported as **inconclusive**
with a "regenerate the baseline" hint, never as a false MISMATCH. Re-run
`bintrail baseline` to refresh the tag: the raw-byte content is unchanged, only
the contract tag differs.

## Notes per source

`verify` runs against the MySQL index, so it works for any source family whose
baselines are wired. Baseline-anchored mode requires baselines; **PostgreSQL
baselines are not yet wired** (see [PostgreSQL beta limitations](postgres.md#beta-limitations)),
so `verify` against a PostgreSQL source is not usable in this release.
