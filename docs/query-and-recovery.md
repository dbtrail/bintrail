# Query and Recovery

How to search indexed events with `bintrail query`, and turn them into SQL that undoes the original operations with `bintrail recover`.

---

## Querying the index

`bintrail query` searches `binlog_events` with optional filters. Results are
ordered by `event_timestamp` (then `event_id` as a tiebreaker) — **oldest first
by default**; pass `--order DESC` for newest-first. With no filters it returns
the first `--limit` events in that order. The same filter set powers
`bintrail recover` and the MCP `query` tool.

Filters: `--schema`, `--table`, `--pk`, `--event-type`, `--gtid`,
`--since` / `--until`, `--changed-column` (events that touched a given column),
`--column-eq` (events where a column has a given value — see below), and
`--flag` (tables/columns labeled via [RBAC flags](server-identity.md)). A
`--pk` lookup is fast and collision-safe — it matches a hash of the PK values
plus the exact values.

### `--column-eq` Filter

Filters events by the value of a column inside the row image. Pass `--column-eq column=value`; repeat the flag to AND multiple entries:

```sh
bintrail query --schema mydb --table orders \
  --column-eq status=active --column-eq order_id=7
```

The generated predicate checks both sides of the event so DELETEs match when the value is in `row_before`:

```sql
JSON_UNQUOTE(JSON_EXTRACT(row_after,  '$.status')) = 'active'
OR
JSON_UNQUOTE(JSON_EXTRACT(row_before, '$.status')) = 'active'
```

`--column-eq` requires `--schema` and `--table`, matching the constraint on `--changed-column`.

**Column names** must match `[A-Za-z0-9_]+`. The column name is interpolated into the JSON path literal (MySQL does not accept bind parameters for paths), so the allowlist keeps the clause safe.

**NULL sentinel.** The literal value `NULL` (unquoted, case-sensitive) matches rows where the column is explicitly JSON null:

```sh
bintrail query --schema mydb --table orders --column-eq deleted_at=NULL
```

Internally this translates to `JSON_TYPE(JSON_EXTRACT(..., '$.deleted_at')) = 'NULL'` on both sides. It does **not** match rows where the column is absent from the row image (FULL row images always include every column, so absence only occurs when the source has `binlog_row_image != FULL`, which `bintrail index` rejects).

The literal value `NULL` is reserved as the JSON-null sentinel — there is currently no escape for matching a column whose value is the four-character string `"NULL"`. If you need that, file an issue.

The same filter is applied to DuckDB when archive auto-discovery routes the query through Parquet files (`json_extract_string` / `json_type`), so merged (live + archive) results stay consistent.

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

### How merged results are deduplicated

When archives are in play, live MySQL and archive (Parquet) results are combined, deduplicated by `event_id` (the live MySQL row wins on a duplicate), sorted chronologically, and **`--limit` is applied once after the merge** — so no events are dropped before deduplication.

### Archive Fetch Error Handling

When an archive source fails — expired AWS credentials, S3 `AccessDenied`, DuckDB `memory_limit` OOM, a corrupted Parquet file, a `Binder Error` on a schema drift — `bintrail query` prints a visible warning to **stderr** regardless of `--log-level` or `--log-format`:

```
Warning: archive query failed for s3://my-bucket/events/bintrail_id=<uuid>: S3 AccessDenied: assume role failed
```

One failure always occupies exactly one stderr line: embedded newlines in the underlying error message (DuckDB Binder errors, AWS SDK error chains) are collapsed to ` | ` separators so `grep`, `systemd-journald`, and other line-oriented consumers don't split a single warning across multiple log entries.

The same record is also emitted as a structured `slog.Warn` with the **raw** (unsanitized) error for JSON-log consumers and full-fidelity debugging — a `--log-format json` pipeline preserves embedded newlines natively.

**Per-source failures are non-fatal.** One broken archive source does not kill the whole query — the loop continues to the next source, any other source that succeeds contributes its rows to the merged result set, and the command exits 0. This is deliberate: operators running against multi-region S3 archives shouldn't lose a regional query because one bucket is temporarily unreachable. If you need a strict all-or-nothing guarantee, use `bintrail reconstruct` (which sets `AllowGaps=false` in its shared `FetchMerged` pipeline and aborts if *any* archive source fails to load, since each source holds deltas no other source carries).

That strictness deliberately includes stale registrations: an `archive_state` row pointing at a local path whose Parquet files were later deleted (or never written) fails the strict query — the planner counted those hours as covered *from `archive_state`*, so a source that can't be read can't be proven harmless. The remediation is `bintrail archive reconcile` (re-syncs `archive_state` with the files actually present; `--prune` removes registrations whose files are gone from every referenced backend); `--allow-gaps` degrades to warn-and-continue if you accept a possibly-incomplete result. The same detection now covers S3: a registered source whose date-scoped listing comes back empty is probed once at the base prefix, and a source with no Parquet anywhere fails loudly instead of contributing silent emptiness. The same applies to a transiently unreachable S3 source (expired credentials, throttling): strict mode prefers a clear error over a silently incomplete reconstruction.

**Context cancellation is fatal.** If the user presses Ctrl-C, or the parent context times out, or a fetch error wraps `context.Canceled` / `context.DeadlineExceeded`, the archive loop short-circuits immediately and the command exits non-zero with `query canceled: context canceled`. No per-source warnings are printed during a canceled run — a Ctrl-C'd query should exit cleanly, not dump a warning per remaining source. When cancellation fires mid-loop, any rows already accumulated from earlier sources AND any live-MySQL rows fetched before the archive loop started are discarded: a canceled query is an incomplete query, and showing partial results alongside a "canceled" error would invite the operator to treat them as authoritative.

> **History**: `bintrail` versions before 0.4.8 silently swallowed every archive fetch error into a `slog.Warn` (invisible at the default text log format) and continued. A real production incident in early 2026 caused by pre-v0.4.4 Parquet files missing the `connection_id` column produced six days of zero-result queries with exit 0 and no stderr signal — only caught when a user escalated. 0.4.8 fixed the specific `Binder Error` trigger at the `parquetquery` layer; a follow-up fix (#203) surfaced every remaining archive failure mode on stderr so future unknown failures cannot reproduce the same silent-data-loss pattern.

### Memory Footprint

The merge path loads **all matching rows** from all sources into memory before applying the limit. Filters (schema, table, time range, etc.) bound the result set in practice. For extremely broad queries against large archives, memory usage could be significant — apply at least a `--since`/`--until` range to keep the result set manageable.

### DuckDB resource tuning (`--ultrafast`)

When `query`, `recover`, or `reconstruct` scan Parquet archives, DuckDB runs under a conservative, container-safe budget by default: **2 threads and a 4 GB memory limit**, spilling to the OS temp directory when exceeded. These defaults keep bintrail alive in small shared containers; on a dedicated box with plenty of RAM they leave performance on the table.

- `--ultrafast` lets DuckDB self-tune to the host: it uses **all CPU cores** and **~80% of physical RAM**, still spilling to the temp directory before hitting the limit. This trades memory-safety for speed — use it for offline recovery on a big machine, **not** in a small container. It does **not** remove the memory limit (which would invite the OOM-killer instead of a graceful spill); it lets DuckDB pick its own RAM-aware limit.
- `--duckdb-threads N` and `--duckdb-memory-limit SIZE` (e.g. `16GB`) override either knob independently. An explicit flag wins over `--ultrafast`, which wins over the default — so you can tune to your box without the all-or-nothing switch.

**S3 archives under `--ultrafast`:** the default path downloads each S3 Parquet file to disk and queries it locally (memory-safe). `--ultrafast` instead reads them **directly via DuckDB's `httpfs` extension** in one parallel multi-file scan — faster (no download round-trip, all files scanned concurrently), but `httpfs` holds each scanned file **in memory, outside the `memory_limit` budget**. Peak RAM is roughly **`largest_file_size × thread_count`**, so on a 32-core host scanning 500 MB files that is ~16 GB. The command logs a warning with the estimate when this path activates. **Lower `--duckdb-threads N` to bound the peak to `N` concurrent files** — under `--ultrafast` on S3, `--duckdb-threads` is a memory-safety knob, not just a speed one. (`--ultrafast` is required for this path; the granular flags alone keep the safe download path.)

Equivalent env vars: `BINTRAIL_ULTRAFAST=1`, `BINTRAIL_DUCKDB_THREADS`, `BINTRAIL_DUCKDB_MEMORY_LIMIT`. These flags affect only the offline CLI commands; the long-lived `shim` and `bintrail-console` daemons always use the safe default.

### S3 Prerequisites

- **Archived events** (`--archive-s3`): objects are listed and downloaded with the **AWS SDK** — the full default credential chain applies (env keys, `~/.aws` profiles incl. SSO, EC2/ECS/EKS IAM roles). DuckDB then scans the downloaded files locally; its `httpfs` extension is not involved on this path — **except under `--ultrafast`**, which reads the files directly via `httpfs` (credentials still resolve through the AWS chain; the bucket region is auto-detected and pinned). See the `--ultrafast` note above for the memory trade-off.
- **Baselines** (`--baseline-s3` / reconstruct / `--include-snapshot`): read through DuckDB `httpfs`, with AWS-SDK-chain credentials from the `aws` extension's `credential_chain` secret, set up automatically (SSO-session profiles have open gaps upstream — [duckdb-aws#125](https://github.com/duckdb/duckdb-aws/issues/125)). When only the `aws` extension is unavailable, reads fall back to static env keys (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` + `AWS_REGION`); if `httpfs` itself cannot load, the read fails outright. Airgapped: extensions cache under `~/.duckdb` **per DuckDB version and platform** — run any bintrail command that touches `s3://` once on a connected machine of the same OS/arch and bintrail release, then copy the cache (a standalone `duckdb` CLI of a different version writes a cache the bundled engine never reads). `BINTRAIL_DUCKDB_NO_AWS_EXT=1` skips the aws-extension setup — use it behind proxies that blackhole the extension registry, where the install attempt can stall for minutes per session.

---

## Recovery

The `recover` command also supports archive auto-discovery and the `--no-archive` flag, using the same merge logic as `query`. When archives are available, events are fetched from both MySQL and Parquet, merged, and then turned into reversal SQL.

### The Concept

Recovery works because dbtrail stores **full before and after images** for every row event. To undo an operation, you simply reverse it:

| Original operation | Reversal |
|--------------------|----------|
| `DELETE` | `INSERT` the deleted row back (from `row_before`) |
| `UPDATE` | `UPDATE` back to `row_before` values, `WHERE` the current state matches `row_after` |
| `INSERT` | `DELETE` the row (using `row_after` to identify it) |

Recovery never executes SQL — it only generates a script you review and apply yourself.

### Reverse Chronological Ordering

Before generating SQL, the generator reverses the event list so the **most recent event is undone first**. This matters for sequences like:

```
INSERT id=5 (at 14:01)
UPDATE id=5: status=draft→published (at 14:02)
UPDATE id=5: status=published→deleted (at 14:03)
```

Reversed: undo the 14:03 UPDATE first, then the 14:02 UPDATE, then the 14:01 INSERT. This is the correct rollback order for any sequence of operations on the same row.

> **Foreign keys are NOT handled.** Reverse-chronological ordering is the only
> ordering `bintrail recover` applies — there is no FK-graph analysis, no
> topological reordering across tables, and the generated script never emits
> `SET FOREIGN_KEY_CHECKS`. Tables with `ON DELETE/UPDATE CASCADE` produce
> side-effect row changes (InnoDB runs cascades below the binlog, MySQL Bug
> #32506, so cascaded child deletes are never captured) that plain `recover`
> cannot reliably undo. `bintrail doctor`, and `stream`/`watch`/`index --source-dsn`,
> warn about them and proceed (cascade schemas index normally). To reconstruct
> cascade-deleted rows, use **`bintrail recover-cascade`** (see below).

### `recover-cascade`: reverse FK ON DELETE CASCADE / SET NULL

`bintrail recover-cascade` reconstructs the side effects of an InnoDB
`ON DELETE CASCADE` or `ON DELETE SET NULL` that were never binlogged. It finds
the deleted parent rows in the index, infers which child rows referenced them in
their last indexed state, and emits reversal SQL:

- **ON DELETE CASCADE** → re-inserts **both** the parents and their
  cascade-deleted descendants (recursing through multi-level cascades).
- **ON DELETE SET NULL** → an **idempotent** `UPDATE` restoring each nulled FK,
  guarded by `... AND fk IS NULL` so a re-run, a manual fix, or a later re-point
  of the child is never clobbered (the child row survives — only its FK was
  nulled — so it is not re-inserted).

All wrapped in `SET FOREIGN_KEY_CHECKS=0/1`. Like `recover`, it only generates
SQL — review before applying.

```bash
bintrail recover-cascade --index-dsn "..." \
  --schema shop --table orders --pk '42' --dry-run
```

- `--table` is the **parent** table whose delete cascaded; `--pk`/`--pks`,
  `--since`/`--until` narrow which deleted parents to process.
- `--lookback` (default `30d`) bounds how far back the last child state is
  searched; `--max-depth` (default 5) bounds cascade recursion.
- **Phase-2 baseline fallback** (`--baseline-dir` or `--baseline-s3`): without a
  baseline, a child untouched within `--lookback` (e.g. an insert-once row from
  months ago) cannot be reconstructed — only children with a binlog event in the
  window are visible. Point at a `bintrail baseline` snapshot and those untouched
  children are recovered from it too, and the binlog window is widened to the
  snapshot time. Tables not covered by the baseline are flagged incomplete.
- **Still best-effort:** baseline augmentation is skipped (and flagged) for a
  table when the index has archived partitions (which the live scan can't see, so
  a child re-parented or deleted in the gap can't be told apart from an untouched
  one) or when one parent has more cascade victims than the per-parent cap. A
  table with no baseline keeps the Phase-1 window limit. When the result is
  provably partial the output is flagged `INCOMPLETE RECOVERY` and the command
  exits non-zero unless `--allow-incomplete` is given. If you have already
  re-created a deleted parent, remove its `INSERT` from the output —
  `FOREIGN_KEY_CHECKS=0` does not suppress primary-key violations.

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

Generated columns (`STORED` or `VIRTUAL`) are computed by MySQL and cannot be set explicitly, so the generated script skips them in `INSERT`/`UPDATE` SET clauses — the script won't fail trying to assign a value MySQL owns.

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
- **Never auto-executed**: dbtrail only generates the file. Applying it is always a manual step.
