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

`run.sh` builds `bintrail-console`, creates a throwaway index database, launches
a source-less `watch` daemon, seeds a monitored source whose per-source index
is intentionally **not** provisioned (the lifecycle state that exercises the
guards), drives the scenarios, and tears everything down.

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
| no uncaught JS errors | any thrown error over the whole drive |

Adding a scenario: append an `ok(...)`/`bad(...)` block in `console_e2e.mjs`.
A non-zero exit fails CI and writes `console-e2e-failure.png` to the artifact dir.
