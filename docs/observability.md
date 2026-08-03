# Observability

`bintrail stream` (and the `bintrail-console watch` control plane) expose
Prometheus metrics describing both the **live replication pipeline** and the
**state of the index** a DBA cares about for recovery readiness.

Enable the endpoint on a standalone stream with `--metrics-addr`:

```bash
bintrail stream --metrics-addr :9090 --index-dsn ... --source-dsn ... --server-id 100
# scrape http://localhost:9090/metrics
```

Under `bintrail-console watch` the daemon serves one `/metrics` endpoint for
every monitored source; the per-source `source` label keeps them distinct.

Every stream and index metric carries a `source` label (the supervisor's entry
ID when monitored, else the resolved `bintrail_id`, or `default`). The one
exception is the top-level capture-loss counter
[`bintrail_statement_dml_dropped_total`](#capture-loss-bintrail_statement_dml_dropped_total),
which has no `source` label.

## Stream metrics (`bintrail_stream_*`)

The live pipeline — emitted on the hot path as events flow.

| Metric | Type | Meaning |
|---|---|---|
| `bintrail_stream_events_received_total` | counter | Row events received from the replication stream |
| `bintrail_stream_events_indexed_total` | counter | Row events written to `binlog_events` |
| `bintrail_stream_batch_flushes_total` | counter | Batch INSERT operations executed |
| `bintrail_stream_checkpoint_saves_total` | counter | Successful checkpoint saves |
| `bintrail_stream_last_event_timestamp_seconds` | gauge | Timestamp of the last event processed |
| `bintrail_stream_replication_lag_seconds` | gauge | Now minus the last processed event timestamp |
| `bintrail_stream_errors_total` | counter | Errors, by `type` |
| `bintrail_stream_batch_size` | histogram | Events per INSERT batch |

## Capture loss (`bintrail_statement_dml_dropped_total`)

| Metric | Type | Meaning |
|---|---|---|
| `bintrail_statement_dml_dropped_total` | counter | DML statements (INSERT/UPDATE/DELETE/REPLACE/LOAD DATA) seen in the binlog as SQL text instead of row events — the signature of `binlog_format=STATEMENT`/`MIXED` or a session-level override away from ROW. Each one is a change bintrail could **not** capture. |

**Nonzero means changes are being missed.** bintrail validates
`binlog_format=ROW` at startup against the global value, but the variable is
session-settable and MIXED falls back to STATEMENT for non-deterministic DML.
The stream is not aborted — each dropped statement emits a loud warning
(`statement-format DML in binlog — event NOT captured …`) plus one increment
of this counter.

This counter is deliberately **top-level**: it is not part of
`bintrail_stream_*` and carries **no `source` label**, so under
`bintrail-console watch` concurrent streams conflate into one counter. The
paired warning carries the binlog file/position (and the statement type —
never the SQL text, which embeds row values); use the log line to tell which
source produced the drop.

## Index metrics (`bintrail_index_*`)

The **state of the index** — refreshed periodically (default 60s,
`--metrics-scrape-interval`) from a status snapshot, not per event.

| Metric | Type | Meaning |
|---|---|---|
| `bintrail_index_oldest_event_timestamp_seconds` | gauge | Oldest event still in the index — the recovery floor |
| `bintrail_index_newest_event_timestamp_seconds` | gauge | Newest event in the index |
| `bintrail_index_retention_horizon_seconds` | gauge | How far back recovery reaches (now − oldest) |
| `bintrail_index_events_total` | gauge | Approximate event count (information_schema estimate) |
| `bintrail_index_partitions_total{kind}` | gauge | Partition count, `kind` = `active` or `future` |
| `bintrail_index_gap_hours` | gauge | Hours rotated out of MySQL but **not** archived to Parquet — holes in recovery coverage |
| `bintrail_index_storage_bytes{location}` | gauge | Index size, `location` = `mysql` (the `binlog_events` table) or `parquet` (archived partitions) |

> The oldest/newest timestamp gauges are **not published** for an empty index,
> so they never report a misleading 1970 epoch.
>
> `events_total` is deliberately a single aggregate per source — it is **not**
> labelled per schema/table, which would grow unbounded as tables come and go.
> Per-table baseline size is surfaced via `bintrail status --format json`
> instead (the `size_bytes` field on each baseline).
>
> `gap_hours` counts holes **within the retained span** — from the oldest data
> still known (live or archived) up to the newest explicit hourly partition. It
> cannot detect holes entirely *before* the oldest surviving data (nothing
> records that those hours ever existed), and the not-yet-rotated current hours
> (`p_future`) are excluded, so a stream with no rotation loop never false-counts
> them.

## PromQL recipes

**Recovery floor is slipping** — alert if the index no longer reaches back the
required retention (e.g. 7 days):

```promql
bintrail_index_retention_horizon_seconds < 7 * 24 * 3600
```

**Coverage gaps** — any hour rotated out without an archive is unrecoverable:

```promql
bintrail_index_gap_hours > 0
```

**Changes slipping past capture** — statement-format DML the index will never
contain (see [Capture loss](#capture-loss-bintrail_statement_dml_dropped_total)):

```promql
rate(bintrail_statement_dml_dropped_total[5m]) > 0
```

**Index disk growth** — MySQL index size trend, to project a disk-full date
(two separate queries — current size, then the 7-day projection):

```promql
bintrail_index_storage_bytes{location="mysql"}
```

```promql
predict_linear(bintrail_index_storage_bytes{location="mysql"}[6h], 7 * 24 * 3600)
```

**Stream stalled** — replication lag climbing, **or** no events indexed (either
condition is a separate alert):

```promql
bintrail_stream_replication_lag_seconds > 300
```

```promql
rate(bintrail_stream_events_indexed_total[5m]) == 0
```

**Newest event is stale** — the index has stopped advancing even though the
stream is up:

```promql
time() - bintrail_index_newest_event_timestamp_seconds > 600
```

## Permanent data loss is not a metric — alert on `status`

`bintrail_index_gap_hours` counts coverage holes **within the retained span**
(hours rotated out of MySQL but never archived). It does **not** signal a
permanently lost capture stream — an unfillable binlog gap or a lost PostgreSQL
replication slot. That verdict lives in `bintrail status` (the `Continuity:`
line / `stream.continuity.status` JSON field), and the alertable hook is its exit code:

```bash
# in CI/cron — exits non-zero on gap_lost OR an unconfirmable state (fails closed)
bintrail status --index-dsn "$IDX" --fail-on-gap
```

See [the continuity signal](rotation-and-status.md#stream-continuity-no-data-lost).
A Prometheus gauge for this state is not exposed in this release.

## See also

- [rotation-and-status.md](rotation-and-status.md) — `bintrail status` reports
  the same coverage, index size, and per-table baseline size in text/JSON, plus
  the stream-continuity verdict.
- [capacity.md](capacity.md) — sizing math and the `doctor` disk-capacity check.

## Watch health metrics (`bintrail_continuity_*`, `bintrail_verify_*`, `bintrail_rotation_*`)

The `bintrail-console watch` daemon exports its three safety-net conditions —
the same ones the webhook channel notifies on
([console.md](console.md#webhook-notifications)) — as gauges. Each publishes
only while its feature is actually producing verdicts; an **absent series
means "no verdict" — never evaluated, or (for the verify pair) never a
conclusive run — never "healthy"** (the continuity gauge is even unpublished
for an index the watcher cannot reach, so unknown can never read as no-gap).

| Metric | Type | Meaning |
|---|---|---|
| `bintrail_continuity_gap_lost{server}` | gauge | 1 = the stream stamped a permanent capture gap (events in it are unrecoverable). Runs under `--notify-webhook` and/or `--metrics-addr` |
| `bintrail_verify_last_run_timestamp_seconds{server}` | gauge | Unix time of the newest verify run that **succeeded and conclusively verified at least one table** — failed and all-inconclusive runs do not refresh it, so staleness means verification is broken. Re-seeded from the persisted run history at startup |
| `bintrail_verify_tables{server,status}` | gauge | Per-status table counts (`match` / `mismatch` / `inconclusive` / `error`) of that run |
| `bintrail_rotation_healthy` | gauge | 1 = the last built-in rotation cycle neither failed nor deferred unarchived partitions |
| `bintrail_rotation_deferred_partitions` | gauge | Unarchived partitions the last cycle declined to drop |

## Example Prometheus alert rules

The push-based sibling of these metrics is the watch daemon's
`--notify-webhook` (see [console.md](console.md#webhook-notifications)).
For pull-based alerting, a starting rule set over the metrics above:

```yaml
groups:
  - name: bintrail
    rules:
      - alert: BintrailCoverageGap
        expr: bintrail_index_gap_hours > 0
        for: 15m
        labels: {severity: warning}
        annotations:
          summary: "bintrail: hours rotated out of MySQL but not archived — holes in recovery coverage"
      - alert: BintrailStreamDown
        expr: up{job="bintrail"} == 0
        for: 5m
        labels: {severity: critical}
        annotations:
          summary: "bintrail stream/metrics endpoint is down — capture may have stopped"
      - alert: BintrailReplicationLagHigh
        expr: bintrail_stream_replication_lag_seconds > 300
        for: 10m
        labels: {severity: warning}
        annotations:
          summary: "bintrail capture is more than 5 minutes behind the source"
      - alert: BintrailStreamErrors
        expr: rate(bintrail_stream_errors_total[15m]) > 0
        for: 15m
        labels: {severity: warning}
        annotations:
          summary: "bintrail stream is logging errors — check the daemon log"
      - alert: BintrailContinuityGapLost
        expr: bintrail_continuity_gap_lost == 1
        labels: {severity: critical}
        annotations:
          summary: "bintrail: permanent capture gap — events in it are unrecoverable; re-baseline the stream"
      - alert: BintrailVerifyProblems
        expr: bintrail_verify_tables{status=~"mismatch|error"} > 0
        for: 5m
        labels: {severity: critical}
        annotations:
          summary: "bintrail: the last verify run found mismatched or erroring tables"
      - alert: BintrailVerifyStale
        expr: time() - bintrail_verify_last_run_timestamp_seconds > 172800
        labels: {severity: warning}
        annotations:
          summary: "bintrail: no verify run succeeded in 2 days — scheduled verification is broken or persistently failing"
      - alert: BintrailRotationUnhealthy
        expr: bintrail_rotation_healthy == 0
        for: 2h
        labels: {severity: warning}
        annotations:
          summary: "bintrail: built-in rotation keeps failing or deferring — the index is growing"
```

Tune thresholds to your write rate; on a genuinely idle source the lag gauge
grows between writes, so widen its threshold there.
