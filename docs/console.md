# bintrail-console

`bintrail-console serve` serves an embedded, **read-only, single-operator** web
UI over an existing index. It is the MCP server with a web face: the same query,
recovery, and status engines, reached from a browser. Browse indexed row events
with full before/after diffs, and generate recovery (undo) SQL — all without
leaving the terminal that started it.

The console ships as its own binary (and Docker image,
`ghcr.io/dbtrail/bintrail-console`), separate from the core `bintrail` CLI —
install it only where an operator wants the UI.

The console **never executes SQL**. Recover produces a transaction-wrapped
script you copy or download and apply yourself after review, exactly like
`bintrail recover --dry-run`.

> **Scope:** event browsing, recovery-SQL generation, index status, and — when a
> baseline is configured — **single-row point-in-time reconstruct** (a row's full
> state "as of T" plus its history). Full-*table* reconstruction (mydumper output)
> stays in the offline `bintrail reconstruct` CLI.

## Usage

```sh
bintrail-console serve --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index"
```

On start it prints a jupyter-style URL with an access token:

```
Bintrail console (read-only) is running. Open:

    http://127.0.0.1:8090/?token=ab12cd34ef56ab12cd34ef56ab12cd34
```

Or serve it **alongside a live stream** in one process with
`bintrail-console watch` (the daemon formerly known as `bintrail up
--console` — `up`'s preflight + init + stream plus the console and the
multi-server control plane):

```sh
bintrail-console watch --source-dsn "$SRC" --index-dsn "$IDX"
```

`--console-listen` / `--console-token` (or `BINTRAIL_CONSOLE_LISTEN` /
`BINTRAIL_CONSOLE_TOKEN`) customize the bind and token; a single Ctrl-C drains
both the stream and the console. Passing `--baseline-dir` or `--baseline-s3`
(or `BINTRAIL_CONSOLE_BASELINE_DIR` / `BINTRAIL_CONSOLE_BASELINE_S3`) enables
the baseline-gated Time-travel surface here too, so one process serves the live
stream **and** point-in-time reconstruct:

```sh
bintrail-console watch --source-dsn "$SRC" --index-dsn "$IDX" --baseline-dir /var/bintrail/baselines
```

With an S3 baseline (`--baseline-s3`), the `watch` process reads S3 at request
time using the ambient AWS credential chain — same as the standalone console,
but note the plain stream daemon didn't need AWS credentials before.

Open that URL in a browser. A left **sidebar** groups the views (Time-travel
appears only when a baseline is configured), with a **server switcher** at the
top (see [Managing servers](#managing-servers)) and a **⌘K command palette**
(also reachable from the "Search & commands" button) for jumping between views
and searching events:

1. **Overview** (landing) — what changed recently and where: headline counts
   (changes indexed, deletes, tables touched, most recent change), a **Recent
   changes** list (each row opens Events, with an inline **Undo**), and an
   **Activity by table** breakdown. The starting point for "what happened?".
2. **Events** — a smart search box (free text plus `type:`, `pk:`, `col:`, and
   `schema.table` tokens) with an expandable **Filters** panel. Each row expands
   in place to a before→after diff; `j`/`k` move the cursor, `↵` expands, `u`
   jumps to Recover. Results carry **JSON / CSV** export — client-side over the
   rows already on screen, so it stays within the result caps and — like the
   on-screen rows — never includes `connection_id`.
3. **Time-travel** — single-row point-in-time reconstruct, drawn as a timeline
   (baseline snapshot → each change, with a **Restore to this state** jump to
   Recover). Appears **only when a baseline is configured**
   (`--baseline-dir`/`--baseline-s3`); otherwise it is hidden, never shown
   empty. See [Time-travel](#time-travel-reconstruct).
4. **Recover** — filter schema / table / PK / time, preview the affected rows
   with before→after diffs, then **Generate undo SQL** and copy/download the
   script. Arriving via an **Undo** action scopes it to that row and shows a
   context banner. **Nothing is ever executed.**
5. **Status** — index health: partitions, coverage, stream lag, archives.

## Managing servers

The header has a server switcher and a **Servers** button: add, edit, and
remove named connections to dbtrail index databases, and switch every view
between them. The registry is a **local YAML file on the console host**
(`~/.config/bintrail/console-servers.yaml` by default, override with
`--servers-file` / `BINTRAIL_CONSOLE_SERVERS`) — adding a server registers a
connection for browsing; it does **not** start monitoring. Monitoring still
starts with `bintrail up` / `stream` against that server (or from the UI
under `watch` — see the control plane below).

How it behaves:

- **The command-line entry.** `--index-dsn` (or `watch`'s stream index)
  appears as an ephemeral `default (cli)` entry: it is the initial selection,
  is never written to the registry file, and cannot be edited or deleted from
  the UI. With at least one saved server, `--index-dsn` becomes optional — the
  console can start registry-only.
- **Lazy connections.** Saved servers connect on first selection (with an
  eager ping, so a dead server fails the moment you switch to it, not on your
  first query). Editing a server's connection details closes and reopens its
  connection; editing only its baseline/archive settings keeps it.
- **Per-server selection is per-tab.** The selection rides an
  `X-Bintrail-Server` header on each request — there is no server-side "active
  server" — so two browser tabs can watch two different servers.
- **Per-server Time-travel.** The reconstruct gate (baseline configured, no
  RBAC profile, archives enabled) is evaluated per server; the Time-travel view
  appears and disappears as you switch.
- **Test connection.** Each server (saved or being typed) has a write-free
  probe: ping, MySQL version, latency, whether the database looks like a
  dbtrail index, and whether its schema is current.

Security notes specific to the registry:

- The registry file stores full DSNs **including passwords** (`0600`, directory
  `0700`) — the same secret-at-rest class as `shim.yaml` and `dump.key`.
- Passwords never travel to the browser. List/get responses carry parsed
  non-secret fields plus `has_password`; leaving the password blank on an edit
  keeps the stored one.
- **The console never migrates servers added in the UI.** The one schema
  migration (`EnsureSchema`, an idempotent ALTER) runs at startup on the DSN
  you typed on the command line — never on a DSN typed into a browser form. A
  registry index that predates the `connection_id` column returns an
  actionable `422` (run a writer command against it once) instead of being
  silently ALTERed.
- The registry file is the only thing the console ever writes. Its write
  endpoints sit behind the same bearer token and Host-header guard as
  everything else.

The registry file is versioned and forward-compatible: fields written by a
newer dbtrail survive load→edit→save round-trips on an older binary, and a
file written by a newer *schema* version loads read-only rather than being
rewritten lossily.

### Monitoring a source from the UI (the control plane)

Under **`bintrail-console watch`** — and only there — the console is also a
control plane: "+ Add server" with a **source MySQL** (host/user/password,
optional schema filter) runs the `bintrail doctor` preflight inline
(failures come back as remediation cards), provisions a dedicated index
database for that source (`bintrail_idx_<id>` on the daemon's index server:
`CREATE DATABASE` + tables + schema migration, done by the daemon — the
console's request handlers still never migrate anything), and starts a
supervised binlog stream. Auto-start: a green preflight starts streaming
immediately; warnings (e.g. short binlog retention) show but don't block.

- Each monitored source gets **its own index database** — per-source state
  (checkpoints, snapshots) stays structurally isolated, and the server
  switcher lists it like any other connection.
- The supervisor reconciles **desired state** (`monitor_desired` in the
  registry) at boot: restart the daemon and monitoring resumes from each
  stream's saved checkpoint.
- A per-entry **advisory lock** (`GET_LOCK`) on the index server makes a
  second daemon refuse to double-stream the same entry.
- **Stream states**: `PENDING` covers launch through the stream's first
  checkpoint (connecting, snapshotting, finding the start position) — the
  badge only says `RUNNING` once the stream has proven it is attached and
  writing. Two unhealthy-but-alive variants surface what used to be silent:
  `STALLED` (connected but no checkpoint/batch progress for 5+ minutes) and
  `LOST POSITION` (binlogs were purged past the saved position while the
  stream was behind or stopped — it auto-advanced and events in the gap are
  permanently lost; the record is durable, survives daemon restarts, and is
  cleared only by an explicit Stop on the entry).
- A failing stream is retried with exponential backoff (15s → 5m) and shows
  as `FAILED` with the scrubbed error; `Stop` requires an explicit click, and
  deleting or re-pointing a *running* entry is refused (409) until stopped.
  A stream that crash-loops continuously for 6 hours stops retrying
  (permanent `FAILED`, the message says it gave up) — press Start to re-arm
  it after fixing the cause.
- The add-server preflight warns (amber, never blocks) when the new source
  **looks like a replica or duplicate of an already-monitored one** — GTID
  lineage comparison; monitoring both would double-index the same changes.
  Detection needs `gtid_mode=ON`; in position mode the check is skipped.
- With `--metrics-addr`, the daemon serves one Prometheus `/metrics` endpoint
  for all supervised streams; every series carries a `source` label set to
  the entry ID (see [streaming.md](streaming.md)).
- Registry fields: `source_dsn` (replication credentials — a secret with the
  same masking/keep-password discipline as the index DSN; `source_dsn: ""`
  clears it), `source_server_id` (0 = derived), `schemas`, `monitor_desired`.

The standalone read-only `bintrail-console serve` never offers any of this:
the `monitor` capability is false and the verbs return 403 there.

## Flags

| Flag | Default | Description |
|---|---|---|
| `--index-dsn` | — | DSN for the index MySQL database. Becomes the ephemeral `default` entry. Required only when the server registry is empty. |
| `--listen` | `127.0.0.1:8090` | Bind address. `:8090` avoids the MCP server's `:8080`. |
| `--token` | auto-generated | Access token. Auto-generated for loopback binds; **required** for non-loopback. |
| `--no-archive` | `false` | Disable Parquet archive auto-discovery (MySQL-only results). Also **disables Time-travel** — reconstruct needs archive access to verify coverage. |
| `--profile` | — | RBAC profile: deny tables / redact columns. Forces `--no-archive` and **disables Time-travel** (baseline reads bypass redaction). |
| `--allowed-hosts` | — | Extra hostnames accepted in the `Host` header (for reverse-proxy setups). IP literals and `localhost` are always allowed. |
| `--baseline-dir` | — | Local directory of baseline Parquet snapshots. Enables the Time-travel (reconstruct) surface. |
| `--baseline-s3` | — | S3 prefix of baseline snapshots (`s3://bucket/prefix/`). Enables Time-travel. |
| `--servers-file` | `~/.config/bintrail/console-servers.yaml` | Path to the server registry YAML managed from the UI. |

### Environment variables

- `BINTRAIL_INDEX_DSN` — same as `--index-dsn` (shared with other commands).
- `BINTRAIL_CONSOLE_LISTEN` — same as `--listen`.
- `BINTRAIL_CONSOLE_TOKEN` — same as `--token`.
- `BINTRAIL_CONSOLE_BASELINE_DIR` — same as `--baseline-dir`.
- `BINTRAIL_CONSOLE_BASELINE_S3` — same as `--baseline-s3`.
- `BINTRAIL_CONSOLE_SERVERS` — same as `--servers-file`.

Precedence is the usual CLI flag > environment variable > default. The five
`BINTRAIL_CONSOLE_*` variables apply equally to `bintrail-console watch` (where
the matching flags are `--console-listen`, `--console-token`, `--baseline-dir`,
`--baseline-s3`, `--console-servers-file`).

## Security model

The binary has no Supabase/RBAC backend to lean on, so the console defends
itself:

- **Loopback by default + token required.** A random 128-bit token is generated
  for loopback binds and printed in the URL. Binding to a non-loopback address
  (`0.0.0.0`, a LAN IP, …) **requires** an explicit `--token` or the command
  refuses to start.
- **Constant-time token compare** (`crypto/subtle`).
- **Bearer header on the API.** `/api/*` requires the token in the
  `Authorization: Bearer …` header — never a cookie alone — so a cross-site
  form POST cannot carry ambient credentials to `/api/recover`. The page shell
  loads without a token (it reads the token from the URL to bootstrap its
  requests).
- **No CORS headers.** Requests are same-origin only.
- **Host-header allowlist.** Requests whose `Host` is a domain name are
  rejected (only IP literals and `localhost` pass), which defeats DNS-rebinding
  attacks against the local bind.
- **Result caps.** Every query is bounded — events default 100 (max 1000),
  recover default 1000 (max 10000). Never unlimited.
- **`/api/healthz`** is the only unauthenticated endpoint (a liveness probe).

## Open-core boundary

The console exposes only the free **query_explorer** surface (event query +
recovery-SQL generation), available on every tier. It deliberately stays out of
the paid **forensics** surface (who-changed / attribution): the events API drops
`connection_id` — the MySQL `pseudo_thread_id` of the writing transaction — from
every response. There is no gating, RBAC, or license code in the binary; the
boundary is simply what the API chooses to serve.

## API

All endpoints return JSON. `/api/*` (except `healthz`) require
`Authorization: Bearer <token>`.

| Method & path | Purpose |
|---|---|
| `GET /api/healthz` | Liveness probe (no token). |
| `GET /api/status` | Index status (same payload as `bintrail status --format json`). |
| `GET /api/schemas` | Distinct schemas. `?schema=<name>` → that schema's tables. |
| `GET /api/events` | Event browser. Query params: `schema, table, pk, event_type, gtid, since, until, changed_column, order, limit`. |
| `POST /api/recover` | Undo-SQL generation. JSON body with the same filter fields (requires at least `schema`; an `order` field is accepted but ignored — recover always processes oldest-first). Returns `{sql, statement_count, row_count, warnings}`. |
| `GET /api/capabilities` | Reports enabled optional surfaces for the **selected server**, e.g. `{"reconstruct": true}`. The frontend uses it to show/hide baseline-gated tabs on every switch. |
| `GET /api/reconstruct` | Single-row point-in-time reconstruct (baseline-gated **per server**; 404 when not configured). Query params: `schema, table, pk, at, history, allow_gaps`. Returns `{found, deleted, state, history, baseline_time, event_count, warnings}`. |
| `GET /api/servers` | List servers (masked: parsed host/port/user/dbname + `has_password`, never a DSN or password) plus `default_id`. |
| `POST /api/servers` | Add a server to the registry (validates, does not connect; never runs DDL). |
| `GET /api/servers/{id}` | One masked entry (prefills the edit form). |
| `PUT /api/servers/{id}` | Edit. Omitted password = keep stored; `""` = clear; value = replace. `409` for the command-line entry. |
| `DELETE /api/servers/{id}` | Remove from the registry and close its cached connection. `409` for the command-line entry. |
| `POST /api/servers/{id}/test`, `POST /api/servers/test` | Write-free reachability probe (short timeout): `{ok, server_version, dbname, latency_ms, has_index, schema_current}`. Accepts an unsaved candidate body; with `{id}`, a blank password merges the stored one. |
| `POST /api/servers/{id}/monitor/start` | Supervisor only (403 on the standalone console): doctor preflight → on green, record intent + provision + stream. Returns `{doctor, started, monitor}`. |
| `POST /api/servers/{id}/monitor/stop` | Supervisor only: clear intent, drain the stream (final checkpoint), release the advisory lock. |
| `GET /api/servers/{id}/monitor` | Supervisor only: `{monitor: {state, last_error, since}}` — `stopped\|pending\|running\|stalled\|lost_position\|failed`. |

Every data endpoint (`status`, `schemas`, `events`, `recover`, `capabilities`,
`reconstruct`) targets the server named by the `X-Bintrail-Server` request
header; without the header they target the default entry. Selection is
stateless — concurrent clients can each target a different server.

### Time-travel (reconstruct)

When `--baseline-dir` or `--baseline-s3` is set, the console can reconstruct a
single row's **full state at a point in time** — the baseline snapshot merged
with the binlog deltas after it — and show the row's history. PK column names are
read from the schema snapshot, so you only pass the value(s) (pipe-delimited for
composite keys). The response cleanly distinguishes three outcomes: the row's
state, the row *deleted as of T* (`deleted: true`), and *no baseline row* for
that PK (`found: false`).

Three gates protect it, all enforced at the endpoint (not just by hiding the tab):

- **Baseline required** — without `--baseline-dir`/`--baseline-s3`, `GET
  /api/reconstruct` returns 404 and the tab is hidden.
- **No active RBAC profile** — the baseline row is read directly (it does not
  pass through the query engine's column redaction), so a `--profile` disables
  reconstruct, mirroring the way a profile forces `--no-archive`.
- **Archives enabled** — `--no-archive` disables reconstruct. The gap check that
  makes reconstruct fail loud can only verify coverage of rotated-out hours if
  their archives are actually fetched; with archives off, an archived-but-rotated
  hour would be skipped yet counted as covered, producing a silently-wrong state.

Unlike events/recover browsing, reconstruct treats a coverage gap between the
baseline and the target time as a **hard error (422)**, not a warning: a missing
hour means a silently-wrong reconstructed state, not just a few deltas missing
from a script. Pass `allow_gaps=true` to override (best-effort), mirroring the
CLI's `--allow-gaps`. A single row touched by more than 10,000 events in the
`[baseline, at]` window is also refused (422) rather than reconstructed from a
truncated prefix.

> **Archive-source failures fail loudly here (#377):** when several archive
> sources are configured and *any* of them fails to load, `query.FetchMerged`
> aborts the strict-mode (`allow_gaps=false`) fetch — the request returns 500
> naming the failed source — instead of folding an incomplete delta set into a
> 200. Pass `allow_gaps=true` to fall back to warn-and-continue.

### Coverage gaps and incomplete data

Both `/api/events` and `/api/recover` report coverage gaps (hours rotated out of
MySQL with no archive) in a `warnings` array rather than failing the request —
matching the CLI `recover`, which warns and continues so a human can review the
script. The recover screen renders those warnings prominently, so an
incomplete-coverage undo is flagged to the operator rather than silently
presented as complete.

One residual limitation: a few failure modes are logged server-side but not
surfaced to the browser (this matches the CLI `recover`, which warns to stderr
and continues; both apply only to these permissive `AllowGaps=true` endpoints —
the reconstruct endpoint fails loudly instead, see above):

- some of several configured archive sources fail to load, and
- the query planner itself fails to run (gap detection is skipped entirely).

In both cases you get results without a coverage caveat in the response. Watch
the server log when running with archives configured.

## Build

The frontend is vanilla HTML/CSS/JS with **zero third-party assets** (see
`internal/console/assets/VENDOR.md`) and is embedded via `//go:embed`. There is
no Node build step; `make build` (CGO, required for DuckDB) produces a single
self-contained binary.
