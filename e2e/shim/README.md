# `bintrail shim` end-to-end test

Wire-protocol-level test for the full deployed chain:

```
go test (mysql client) → ProxySQL → bintrail shim → MySQL (bintrail_index)
```

Every other shim test is in-process — this one is the only place we
catch regressions in the MySQL wire-protocol layer, ProxySQL routing
rules, and shim ↔ index DB cooperation as a single integrated unit.

## Running it

```sh
cd e2e/shim
./run.sh
```

`run.sh` sets `SHIM_E2E=1` and runs `go test -tags shim_e2e`. The
test owns the docker-compose lifecycle (build, up, down) so you can
also run it directly:

```sh
SHIM_E2E=1 go test -tags shim_e2e -v ./e2e/shim/...
```

Without `SHIM_E2E=1` the test skips. Without Docker on `PATH` the
test also skips.

## What the stack looks like

- `mysql` (8.0): one container, two schemas:
  - `appdb` — the simulated production DB. `orders` table holds a
    live row with marker values (`sku=LIVE-SKU`, `qty=999`) so the
    passthrough subtest can prove ProxySQL routed there and not to
    the shim. `testuser` is granted SELECT on `appdb.*` so
    ProxySQL can forward passthrough queries with the same
    credentials the client uses.
  - `bintrail_index` — the bintrail row-event index. `binlog_events`
    is hand-seeded with a synthetic `INSERT → UPDATE → DELETE`
    sequence for `appdb.orders` id=42 at known timestamps
    (10:00 / 12:00 / 14:00 UTC on 2026-05-04).
- `proxysql` (official 2.7 image): configured at test startup by
  applying the SQL emitted by `bintrail proxysql-config` to the
  admin port. Three rules route `_flashback`/`_diff`/`_snapshot`
  to the shim hostgroup; everything else hits the passthrough
  hostgroup pointed at the same `mysql` container.
- `bintrail-shim`: runs `bintrail shim` against `bintrail_index`,
  authenticating one tenant (`testuser` / `testpw`). Shares
  `proxysql`'s network namespace (`network_mode: service:proxysql`)
  — a sidecar pattern that mirrors the canonical prod deployment
  and makes the `'127.0.0.1'` shim hostname that
  `bintrail proxysql-config` hardcodes resolve correctly without
  post-processing the SQL.

The test exposes ProxySQL on host ports `127.0.0.1:16032` (admin)
and `127.0.0.1:16033` (client) so it doesn't collide with
locally-running MySQL or ProxySQL.

## What we cover

- `_flashback.<table> AS OF '<ts>'` returns the right post-image
  for the queried instant (multi-event history, mid-stream).
- `_flashback.<table> AS OF '<ts>'` returns zero rows when the
  AS OF instant is before any event.
- `_diff.<table> BETWEEN '<t1>' AND '<t2>'` returns the full
  event stream in order with the right event_type and image
  shape (INSERT lacks row_before, DELETE lacks row_after).
- `_snapshot.<table>` behaves like `_flashback`. (Today they
  share an implementation; pinning the contract here keeps a
  future split deliberate.)
- A non-virtual-schema query (`SELECT * FROM orders WHERE id = 42`)
  is routed to the passthrough hostgroup and returns the live row,
  not the shim's reconstruction. Marker values prove the routing.

## What we deliberately do NOT cover

- The binlog parser → indexer pipeline. `seed.sql` writes the
  `binlog_events` rows by hand so we can pin a deterministic time
  series; the parser/indexer have their own integration tests
  under `internal/parser` and `internal/indexer`.
- The Parquet archive read path. `archive_state` is empty here;
  `internal/parquetquery` covers DuckDB-backed archive reads.
- A `/*+ DBTRAIL_AT='<ts>' */` hint form. The shim's parser
  doesn't recognise it; if support is added, this test should
  grow a subtest.

## Why the data layer is seeded directly

`bintrail shim` reads from the MySQL index via `query.FetchMerged`
and from S3/local Parquet via `parquetquery.Fetch` — there is no
"agent" intermediary. So this test seeds `binlog_events` directly
rather than mocking an HTTP API.
