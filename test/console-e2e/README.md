# console-e2e — headless-Chrome regression guard

`go test` never renders the embedded console SPA (`internal/console/assets/*`),
so a whole class of bugs is invisible to it: CSS cascade (an invisible button),
frontend DOM/state logic (a form that auto-expands), and how the UI *presents*
a backend state (a raw `Unknown database` error wall; the control plane
vanishing when the selected server's index is missing). Every scenario in
`console_e2e.mjs` pins a bug that shipped in 0.13.3 and reached a user.

## Run it

Requires Docker (the shared `bintrail-test-mysql` container, same as the Go
integration suite) and Node.

```sh
# start the test MySQL if it isn't already (see CONTRIBUTING.md)
docker run -d --name bintrail-test-mysql -e MYSQL_ROOT_PASSWORD=testroot \
  -p 13306:3306 mysql:8.4 --binlog-format=ROW --binlog-row-image=FULL \
  --log-bin=binlog --server-id=1

make console-e2e
# or, to use your system Chrome instead of the playwright-managed chromium:
PW_CHANNEL=chrome make console-e2e
```

`run.sh` builds `bintrail-console` and `bintrail`, creates a throwaway index
database, provisions it with the production schema (`bintrail init`) plus a
seeded read fixture — row events, a cascade fixture, and a baseline snapshot
produced by the real `bintrail baseline` converter over a hand-written
mydumper-format dump — launches a source-less `watch` daemon (with
`--baseline-dir` and the baseline-trigger opt-in), seeds a monitored source
whose per-source index is intentionally **not** provisioned (the lifecycle
state that exercises the guards), drives the scenarios, and tears everything
down.

## What each scenario guards

| Scenario | Bug it pins |
|---|---|
| boot: monitor capability reported | caps fetch for a broken/unprovisioned selected server must not degrade to `{}` |
| control plane: Start button / monitor copy | `/api/capabilities` 502 cascade hiding the whole control plane |
| button: primary keeps gradient on hover + text stays white | `.btn:hover` background winning → white text on light gray |
| form: advanced collapsed for a source entry | the optional "BYO index" section auto-expanding for a source entry |
| form: advanced expanded for a BYO-index entry | the other `byoIndex` arm — a no-source entry must show its index fields |
| form: source fields visible | the monitor form rendering for a source entry |
| error: real 1049 reaches the frontend + empty state (×4) | the actual backend 1049 (from the unprovisioned default server) must surface as an actionable empty state, not a raw wall — and `scrubDSNError` must preserve the index db name |
| events: render + redaction (×6) | the primary read workflow over REAL indexed rows (#970): rows render, the diff expands, and the seeded `query_text`/`query_hash` canary never reaches the DOM while `connection_id` passes through (#701 D1) |
| export: JSON/CSV blobs (×6) | the real download buttons, blobs captured at `URL.createObjectURL`: query_text-free, connection_id kept, CSV header in lockstep with `EVENT_CSV_COLUMNS` |
| recover: submit renders SQL (×3) | Recover actually submits and paints the reversal SQL panel — scenario 6 only checks the form DOM exists |
| cascade: banner + counts (×3) | the `cascade_detected` positive-half rendering (#619) — the banner block whose missing `)` once broke the whole SPA |
| schema changes (×9) | the DDL history view (#1443): seeded rows render, same-second DDLs list in binlog order, UTC column, the type filter narrows through the API, the empty state |
| timetravel (×6) | the reconstruct gate + baseline+deltas over a real fixture baseline (#970): event fold, baseline-only row, deleted row |
| overview: window honesty (×2) | `buildOverview` window line uses the fetched window's own bounds, never `status.coverage` (#679/#686) |
| storage: Create-baseline gates (×5) | the button's double-gate (#686): live enabled arm, destination-missing arm, capability-off arm; the fixture snapshot listed |
| telemetry: sample event (×3) | the Storage card's "Show a sample event" fold (#1447): closed by default, opens to a read-only `<pre>` carrying the daemon's `sample_event` string verbatim (never re-serialized in the frontend) |
| no uncaught JS errors | any thrown error over the whole drive |

Adding a scenario: append an `ok(...)`/`bad(...)` block in `console_e2e.mjs`.
A non-zero exit fails CI and writes `console-e2e-failure.png` to the artifact dir.
