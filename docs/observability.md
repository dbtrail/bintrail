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

Every metric carries a `source` label (the supervisor's entry ID when
monitored, else the resolved `bintrail_id`, or `default`).

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
