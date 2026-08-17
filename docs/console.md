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

On start it prints the URL to open. On a fresh console the first visit is a
**"create your password"** screen (see [Password login](#password-login));
after that, you sign in:

```
Bintrail console (read-only) is running. Open:

    http://127.0.0.1:8090/

First run — open the URL and create your console username and password.
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

For a TLS-requiring source (RDS, Aurora, Cloud SQL), `watch`'s own stream
accepts `--ssl-mode` / `--ssl-ca` / `--ssl-cert` / `--ssl-key` (env
`BINTRAIL_SSL_MODE` / `BINTRAIL_SSL_CA` / `BINTRAIL_SSL_CERT` /
`BINTRAIL_SSL_KEY`), same semantics as `bintrail stream` — see
[streaming.md → TLS/SSL for managed MySQL](streaming.md#tlsssl-for-managed-mysql-rds-aurora-cloud-sql).
The default stays `--ssl-mode preferred` (opportunistic, no certificate
verification).

> **Scope of `--ssl-mode` under `watch`.** The flag encrypts `watch`'s embedded
> **stream** connections — the source replication and the index *write*. It does
> **not** currently cover the console's own index *reads* (the connections the
> multi-server manager opens to serve the web UI) or the index reads behind the
> embedded flashback port (`--flashback-listen`). Encrypt those index
> connections by adding a `tls=` parameter to their DSN (`...?tls=true`, or
> `?tls=skip-verify` for self-signed dev certs) — the same knob the offline read
> commands use. (The flashback port's *inbound* MySQL-protocol listener — the
> client→port leg — has no TLS option in this release; `tls=` only covers its
> backend index reads.) If your index MySQL is reached over loopback or a
> private network this is moot; over an untrusted network, set `tls=` on the
> index DSN in addition to `--ssl-mode`.

Open that URL in a browser. A left **sidebar** groups the views (Time-travel
appears only when a baseline is configured), with a **server switcher** at the
top (see [Managing servers](#managing-servers)) and a **⌘K command palette**
(also reachable from the "Search & commands" button) for jumping between views
and searching events:

1. **Overview** (landing) — what changed recently and where: a **Restore
   coverage** card answering "to when can I restore, right now?" — any point
   between the delta-coverage floor and the last *indexed* event (never the
   wall clock; the capture-lag chip says how close to now that edge is), the
   continuity verdict, and — with a baseline source configured — the
   full-table restore window plus any table whose newest baseline predates
   coverage. It degrades loudly on `gap_lost`/`unavailable`/`unknown`/an
   empty index, and full-table coverage that could not be evaluated says so
   instead of rendering as "nothing broken".
   Below it: headline counts (changes indexed, deletes, tables touched, most
   recent change), a **Recent changes** list (each row opens Events, with an
   inline **Undo**), and an **Activity by table** breakdown. The
   window-scoped counts cover the **live retention** — exactly what the live
   index still holds, derived from its oldest partition — and come from a
   server-side materialization refreshed about every 30 minutes; each tile
   states its window and an "as of …" refresh time, so a cached number is
   never presented as live. The page paints progressively: the frame and
   per-card placeholders appear immediately, and each card fills as its own
   fetch lands, so one slow aggregate cannot hold the whole page. The
   starting point for "what happened?". For history older than the live
   retention, use Events with time filters or `bintrail query`.
2. **Events** — a smart search box (free text plus `type:`, `pk:`, `col:`, and
   `schema.table` tokens) with an expandable **Filters** panel. Each row expands
   in place to a before→after diff; `j`/`k` move the cursor, `↵` expands, `u`
   jumps to Recover. Results are **paged**: `‹ Newer` / `Older ›` walk the
   stream a page at a time, and the header states where you are
   (`showing 1–100 of more`, or `showing 201–247 of 247 (end)` once you reach
   the bottom) rather than restating the page size. Paging is keyset-based —
   the cost of page 40 is the cost of page 1 — and each page is still bound by
   the same result cap; editing any filter returns you to page 1.
   **Export JSON / Export CSV** export *every match of the current search*, up
   to the endpoint's 1000-event cap, not just the page on screen; if the search
   has more than that, the console says so instead of handing you a silent
   prefix. Rows and exports include `connection_id` (the transaction's
   originating thread number) but never `query_text`/`query_hash`.
3. **Time-travel** — single-row point-in-time reconstruct, drawn as a timeline
   (baseline snapshot → each change, with a **Restore to this state** jump to
   Recover). Appears **only when a baseline is configured**
   (`--baseline-dir`/`--baseline-s3`); otherwise it is hidden, never shown
   empty. See [Time-travel](#time-travel-reconstruct).
4. **Recover** — filter schema / table / PK / time, preview the affected rows
   with before→after diffs, then **Generate undo SQL** and copy/download the
   script. Arriving via an **Undo** action scopes it to that row and shows a
   context banner. When you undo a `DELETE` on a foreign-key **parent** whose
   children InnoDB cascade-deleted *below* the binlog (MySQL ≤ 8.x / MariaDB) —
   or an `UPDATE` of a referenced key whose `ON UPDATE` cascade rewrote them just
   as invisibly —
   Recover **auto-detects** it and folds the invisible children into the same
   script — no separate tab, no extra step. **Nothing is ever executed.** See
   [Recover and cascade](#cascade-recovery).
5. **Status** — index health: partitions, coverage, stream lag, archives, and a
   first-class **stream-continuity** signal — a green "✓ No gaps in captured
   stream" badge when the captured range is contiguous, or a red "⚠ Events
   permanently lost" record when an unfillable gap (or a lost PostgreSQL slot)
   was detected. Both fire for any source family. See
   [the continuity signal](rotation-and-status.md#stream-continuity-no-data-lost).
6. **Settings** (under `watch` only) — **Storage** (rotation policy,
   per-source S3 archiving, baseline snapshots, AWS credential signals, and a
   usage-telemetry opt-out — see
   [The Storage page](#the-storage-page)) and **Rotation** (opens the
   rotation dialog).

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
  appears as an ephemeral entry labeled by its database name, e.g.
  `bintrail_index (cli)`: it is never written to the registry file and cannot
  be edited or deleted from the UI. With at least one saved server,
  `--index-dsn` becomes optional — the console can start registry-only.
  **It is a connection to the daemon's own index database, not a monitored
  source** — under source-less `watch` nothing ever streams into it (each
  added source gets its own per-source database). For that reason a
  source-less `watch` daemon **hides it entirely**: a fresh install lists no
  servers (the switcher shows "no servers yet" and the Servers dialog is
  empty), and the views render against the internal index underneath until
  the first server is added (a note under the switcher says so — the
  internal index is not guaranteed empty, e.g. after restarting without a
  previous `SOURCE_DSN`); the default selection is then the first server
  with a source configured (or the first saved one). The cli entry remains
  visible where it actually carries data — `serve`, and `watch` with
  `--source-dsn` (the main stream writes into it) — labeled by its database
  name and sorted last in the switcher.
- **Lazy connections.** Saved servers connect on first selection (with an
  eager ping, so a dead server fails the moment you switch to it, not on your
  first query). Editing a server's connection details closes and reopens its
  connection; editing only its baseline/archive settings keeps it.
- **Per-server selection is per-tab.** The selection rides an
  `X-Bintrail-Server` header on each request — there is no server-side "active
  server" — so two browser tabs can watch two different servers.
- **Per-server Time-travel.** The reconstruct gate (baseline configured, no
  RBAC profile, archives enabled) is evaluated per server; the Time-travel view
  appears and disappears as you switch. A server whose registry entry has no
  baseline of its own inherits the process-wide `--baseline-dir`/`--baseline-s3`
  (when set), so servers added from the UI get Time-travel and verify under a
  single-baseline-dir deployment without extra configuration.
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
- The registry file, the [auth file](#password-login) (written by
  set-password), and the [managed MCP token file](#mcp-endpoint)
  (`~/.config/bintrail/console-mcp-token.yaml`, SHA-256 only) are the only
  things the console ever writes. Their write
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

The **source user** you paste into the form needs `REPLICATION SLAVE,
REPLICATION CLIENT, SELECT` on the source MySQL — the form spells out the
exact `CREATE USER` / `GRANT` to copy. dbtrail never writes to the source, and
capture never locks it. The **Create baseline** button needs one privilege
more, `LOCK TABLES`, because a baseline is point-consistent by default; without
it capture keeps running and only the baseline is refused, naming the exact
`GRANT`. On RDS/Aurora set `BINTRAIL_CONSOLE_BASELINE_LOCK_MODE=lock-all` —
`ftwrl` needs `BACKUP_ADMIN`, which managed MySQL will not grant. Full
per-privilege breakdown and the least-privilege (schema-scoped `SELECT`)
variant: [streaming.md](streaming.md#the-source-mysql-user).

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
  for all supervised streams; every stream series carries a `source` label set
  to the entry ID (see [streaming.md](streaming.md)). Exception: the capture-loss
  counter `bintrail_statement_dml_dropped_total` has **no** `source` label, so
  concurrent streams conflate into one counter — see
  [observability.md](observability.md).
- **Archive to S3** (the `Archive to S3` field on a monitored source): set an
  `s3://bucket/prefix/` destination and the daemon's built-in rotation
  **uploads that source's rotated partitions as Parquet before dropping them**,
  so the forensic record survives the retention window and stays queryable —
  the console auto-discovers the archive on the next query, no extra config.
  Partitions are staged locally (`--archive-staging-dir` /
  `BINTRAIL_CONSOLE_ARCHIVE_STAGING`, default a temp dir), uploaded, then
  pruned. The S3 upload uses the **ambient AWS credential chain** (`AWS_*`
  env, `~/.aws`, or an instance/role) — the same credentials the console needs
  to read the archive back; there is no per-source credential. Archiving for a
  source begins once its identity (`bintrail_id`) is resolved (right after its
  first stream connect); until then it rotates drop-only, and the
  protect-unarchived guard never drops un-uploaded data. A persistently failing
  upload (bad bucket/credentials) keeps partitions undropped and escalates to a
  loud Error after a few cycles — the index does not silently lose data, but it
  does stop shrinking, so fix the bucket/credentials. The archived Parquet is
  unencrypted; rely on bucket-level SSE/policy. Archive to S3 ≠ Baseline S3
  (the latter is read-side Time-travel input).
- Registry fields: `source_dsn` (replication credentials — a secret with the
  same masking/keep-password discipline as the index DSN; `source_dsn: ""`
  clears it), `source_server_id` (0 = derived), `schemas`, `monitor_desired`,
  `archive_s3` (the bucket above — non-secret, round-trips in the masked DTO),
  and per-source TLS: `ssl_mode` / `ssl_ca` / `ssl_cert` / `ssl_key` (same
  semantics as `bintrail stream`'s `--ssl-*` flags — see
  [streaming.md → TLS/SSL for managed MySQL](streaming.md#tlsssl-for-managed-mysql-rds-aurora-cloud-sql)).
  Setting these in the registry is the only way to get `verify-ca` / mutual
  TLS on a "+ Add server" source; an empty `ssl_mode` means the default
  `preferred` (opportunistic, no certificate verification).
  `ssl_ca`/`ssl_cert`/`ssl_key` are certificate/key file paths **on the
  daemon host**, not secrets.

The standalone read-only `bintrail-console serve` never offers any of this:
the `monitor` capability is false and the verbs return 403 there.

### Configuring rotation from the UI

`bintrail-console watch` runs the built-in rotation loop that keeps the index
from growing without bound: it drops binlog partitions older than a **retention
window** every **interval**, keeping a few **future partitions** ready. Under
`watch` you can tune that policy from the console — the sidebar's
**Settings → Rotation** entry, or **⌘K → "Configure rotation…"** — without
editing flags or restarting:

- **Live:** changes apply on the loop's next cycle. Retention and future-partition
  count take effect immediately; a changed interval re-tunes the schedule.
- **Global, one schedule:** the loop is a single shared ticker, so the policy
  applies to **every** index the daemon rotates (the boot index and every
  monitored source). Per-source retention is not offered — the schedule is one.
- **Override vs default:** the saved policy lives in the local console registry
  (`console-servers.yaml` — the only file the console writes). When nothing is saved the panel shows the
  daemon's `--rotate-retain` / `--rotate-interval` / `--rotate-add-future`
  (`BINTRAIL_ROTATE_*`) values as the **effective default**.
- **Disabling** rotation entirely stays a daemon-level decision
  (`--rotate-retain off`); the panel tunes a running loop rather than turning it
  off. A retain like `off` is rejected at save.
- The standalone `bintrail-console serve` hides the panel and refuses the write
  (HTTP 403) — only the daemon running the loop consumes the policy.

### The Storage page

Under `watch` the sidebar grows a **Settings → Storage** page that gathers
everything S3/baseline-related in one place (it was previously scattered
across the rotation dialog and the per-server edit form):

- **Rotation** — the effective policy (override vs daemon defaults) with an
  edit shortcut to the rotation dialog.
- **S3 archiving per source** — every monitored server with its
  `Archive to S3` destination (or `drop-only` when none), with a shortcut into
  that server's edit form. The boot (cli) index always rotates drop-only.
- **Baseline snapshots** — a read-only listing of the **selected server's**
  baseline source (`baseline_dir` / `baseline_s3`): each snapshot's timestamp,
  age, table count, and (local sources) the binlog coordinates its deltas
  start from. The empty states explain how to produce a first baseline
  (`bintrail dump` → `bintrail baseline`). When the **Create baseline** button
  is enabled (see below) it sits in this panel's header.
- **Automatic baseline refresh** — when `--baseline-refresh-interval` is set,
  the Baseline snapshots panel reports the daemon's last automatic refresh for
  the selected server: how many tables it published, or that it published
  nothing and why. A refusal there is the fail-closed contract working (a
  capture gap, a schema change), not a broken daemon — nothing was overwritten
  and the next run retries. The refresh is opt-in on its own — it does not
  require, and does not enable, the **Create baseline** button.
- **Query in DuckDB** — a one-click download of `views.sql`: a ready-made
  DuckDB schema over the selected server's own Parquet — an `events` view
  across every archive source registered in `archive_state`, plus one
  `state_<schema>_<table>` view per table in the newest baseline snapshot. It
  is the same file `bintrail views` writes. **The console does not run it.**
  You get a text file; your own DuckDB executes it, in your process, on your
  machine — which is why unrestricted SQL over your lake needs no sandbox, no
  timeout and no result cap here. No credentials appear in the file (S3 uses
  your AWS credential chain), so it is safe to share or commit. The card is
  hidden for a server with archives disabled, for one with nothing archived and
  no baseline yet, and while an access-control profile is active — the file
  maps straight onto the unredacted Parquet a profile exists to withhold.
- **Usage telemetry** — the current state of dbtrail's metadata-only usage
  telemetry and a one-click opt-out. Turning it off stops this `watch` daemon's
  beacons immediately (no restart) and records the machine-wide choice, exactly
  like `bintrail telemetry off`. When an environment variable (`DO_NOT_TRACK`,
  `BINTRAIL_TELEMETRY`) or the `--telemetry` flag already controls it, the card
  says so and defers to that. See [TELEMETRY.md](./TELEMETRY.md).

### The SQL panel (opt-in)

The **Query in DuckDB** card above hands you a file to run in your *own* DuckDB.
The **SQL panel** is the other half of that trade: a query box that runs the SQL
**inside the console daemon** and returns the rows, so you never leave the
browser. Because that SQL now executes server-side, it is **off by default** and
guarded — start the console with `BINTRAIL_CONSOLE_SQL_PANEL=1` to expose it. It
appears as a **SQL** item in the sidebar for any server that has an archive or
baseline layout to query.

What you can query is exactly what the `views.sql` schema defines: an `events`
view across every archive source, plus one `state_<schema>_<table>` view per
table in the newest baseline snapshot. Type a `SELECT`, press **Run** (or
`⌘/Ctrl+Enter`), and the results come back in a capped table. A long query is
**cancelable** — the **Cancel** button aborts it and interrupts the query in the
daemon; closing the tab or navigating away does the same.

The panel is deliberately constrained, and every constraint is enforced
server-side (the UI only mirrors it):

- **Read-only.** Only a single `SELECT` runs. Writes, `COPY`, `ATTACH`,
  `INSTALL`, `CREATE SECRET`, `SET`, and multi-statement input are refused —
  classified by DuckDB's own parser, not a text filter.
- **Views only.** Query the pre-built `events` and `state_<schema>_<table>`
  views. The `FROM` clause accepts those views (and a few pure generators like
  `range`) and nothing else: every file reader (`read_parquet`, `read_csv`,
  `FROM '…/file.parquet'`, …) and every dynamic-SQL function (`query`,
  `query_table`, …) is refused by an allowlist that fails closed. So the views —
  which omit the paid forensics columns, exactly as the rest of the console
  does — are the one data path.
- **Filesystem-sandboxed.** Under the views, the DuckDB session may touch *only*
  the resolved archive/baseline roots for the selected server (local paths and
  `s3://` prefixes alike); everything else — other buckets, arbitrary URLs,
  local files outside the roots — is denied, and the sandbox configuration is
  locked so no query can widen it.
- **Bounded.** Conservative memory/thread budget (never the `--ultrafast`
  profile — the daemon may also be capturing your stream), a hard 60-second
  query timeout, a result-row cap matching the events API, and **one query at a
  time** per process (a second concurrent request gets `429`).
- **Access-control aware.** Refused outright while a data profile is active:
  free-form SQL reads the unredacted Parquet directly and cannot honor
  per-column redaction, so the whole surface is withheld (the same stance the
  `views.sql` download takes).
- **Audited.** Every statement that reaches the engine is recorded on the audit
  seam (`console` / `sql.run`) with the SQL text, its outcome (`ok` / `refused` /
  `error`) and row count — including one the read-only gate refuses. Only a
  statement the client aborts mid-flight is not recorded.

No AWS credentials are ever exposed to the query; S3 reads use the daemon's
ambient credential chain, scoped to the allowed prefixes.

#### Creating a baseline from the console

By default the console only *lists* baselines — you produce them with the
`bintrail dump` → `bintrail baseline` CLI (or the compose `baseline` profile).
A **Create baseline** button can run that pipeline for a monitored server
straight from the Storage page, and it only runs on the `watch` daemon:

- A bare `watch` invocation (no compose) has this **opt-in** and off by
  default — start it with `BINTRAIL_CONSOLE_BASELINE_TRIGGER=1`. The bundled
  compose stack flips this to **on by default**; set `BASELINE_TRIGGER=0` in
  `.env` to opt out there.
- The server must have **both** a source DSN and a baseline destination
  (`baseline_dir` or `baseline_s3`) configured; the button 400s otherwise.
- Clicking it runs **dump → convert → upload entirely in-process**: the console
  image bundles `mydumper` and runs it as a **local subprocess** — it never
  mounts the docker socket, so a console compromise can never escalate to
  host-root. The source DSN stays inside the process (never written to disk or
  any HTTP response). One baseline at a time per server (409 while one runs).
- For an `s3://` destination the dump is staged under
  `BINTRAIL_CONSOLE_BASELINE_STAGING` (default a temp dir), uploaded, then
  discarded; for a local `baseline_dir` it is written there. Region and
  credentials come from the daemon's ambient AWS chain, like every other S3
  access. When it finishes the new snapshot appears in the listing.
- **For MySQL/MariaDB sources, this button's dump IS point-consistent across
  tables by default** (`--sync-thread-lock-mode FTWRL`), which needs `RELOAD`
  (or `FLUSH_TABLES`) plus `BACKUP_ADMIN` on MySQL/Percona 8.0+. The daemon
  checks those privileges before launching mydumper and refuses with an
  actionable error if any are missing — it never silently falls back to a
  weaker mode. On managed MySQL such as RDS, `BACKUP_ADMIN` cannot be
  granted at all, so use `BINTRAIL_CONSOLE_BASELINE_LOCK_MODE=lock-all` — equally
  point-consistent, needs only `LOCK TABLES`. Or `=safe-no-lock` to dump
  without any of them (it aborts rather than write a snapshot stitched from several
  instants, so expect it to refuse on a write-active source) or `=no-lock` to
  accept such a snapshot; only under `no-lock` does the daemon log a warning
  when a dump spans more than one table. See
  [Cross-table consistency](dump-and-baseline.md#cross-table-consistency).
- Like the Create-baseline button, this only runs on the `watch` daemon; a
  bare invocation needs `BINTRAIL_CONSOLE_VERIFY_TRIGGER=1`, while the bundled
  compose stack enables it **by default** (`VERIFY_TRIGGER=0` in `.env` opts
  out). Unlike baseline creation it starts no subprocess and, in its default
  mode, reads no live source — the same in-process baseline/index reads the
  console already does for Time-travel — so it skips the opt-in-then-flip
  cycle baseline-trigger went through.
- **Baseline-anchored** (the default) compares the two most recent baseline
  snapshots, drift-free — no live source read. It needs a baseline
  destination configured (same precondition as Time-travel) and at least two
  snapshots; with only one, the run reports a benign "nothing to compare yet"
  note rather than an error.
- **Live-source** reconstructs each table to a consistent snapshot of the
  actual source and compares — it needs the server's source DSN and reads the
  *whole table* off production, so the panel warns to run it off-peak (see
  [verify.md](verify.md)'s own warning). It only appears in the mode selector
  when the server has a source DSN configured.
- Per-table results are colored match / mismatch / inconclusive / error, the
  same four outcomes `bintrail verify` reports. A mismatch found by a
  baseline-anchored run gets an **Explain** button — an on-demand,
  never-precomputed row-level drill-down (`--explain`'s console equivalent),
  re-run only when clicked. Live-source mismatches have no explain support
  (mirroring the CLI). Because it REBUILDS the table to diff it, the work
  runs on the daemon and the console polls for it — minutes on a large
  table — behind a busy dialog that says so. Closing that dialog only stops
  the waiting: the drill-down keeps computing, and reopening **Explain**
  picks up the finished result if it is still held. It is not held
  indefinitely: the first reopen consumes it, and a finished result may be
  dropped to make room once many tables have been explained — in both cases
  reopening simply recomputes, and nothing is lost but the wait. A new verify
  run is different: it discards (and cancels) the previous run's drill-downs,
  because each explains the snapshot pair its own run compared, and Explain
  reports no explainable mismatch until a *baseline-anchored* run reports the
  table as a mismatch again — a live-source or check-recovery-inputs run
  clears the drill-downs without being able to replace them. Failures are
  logged by the daemon whether or not anyone is still polling.
- **Check recovery inputs** is the console face of
  [`bintrail verify --check recover`](verify.md): index-only — no baseline
  and no source read — walking each primary key's event chain over the last
  30 days. It is the one mode that works on a server with no baseline
  location configured.
- One run at a time per server (409 while one is in flight). The live,
  pollable result is in-memory, but every finished run also lands in a
  persisted **run history** (`console-verify-history.json` next to the server
  registry file, last 20 runs per server, served at
  `GET /api/servers/{id}/verify/history`) — the panel shows "last verified"
  and the recent trend from it across restarts.
- Verify's baseline/live-source reads carry no RBAC redaction (like Time-travel's),
  so the panel and its endpoints are unavailable whenever an RBAC profile
  (`--profile`) is active.

**Scheduled verification** runs the same engine on a timer, so backups
re-prove themselves without external cron glue: start the daemon with
`--verify-interval 24h` (or `BINTRAIL_CONSOLE_VERIFY_INTERVAL`) and every
cycle verifies each registry server, sequentially — baseline-anchored where a
baseline location is configured (the server's own, or the process-wide
`--baseline-dir`/`--baseline-s3`), the index-only recovery-inputs check
otherwise, so no server is silently skipped. `--verify-tables a.b,c.d`
narrows every scheduled run to those tables. Setting an interval implies the
Verification panel (no separate `BINTRAIL_CONSOLE_VERIFY_TRIGGER` needed),
and one cycle also runs shortly after startup. Every outcome — including
cycles skipped because a run was already in flight — lands in the run
history above. The schedule covers **registry servers** (added from the
console UI); a source configured only through command-line flags/env is not
in the registry and is not covered — the daemon warns every cycle it finds
nothing to verify.

#### Webhook notifications

`--notify-webhook <url>` (or `BINTRAIL_CONSOLE_NOTIFY_WEBHOOK`) makes the
`watch` daemon POST a small JSON payload when something means "your safety
net has a hole", so the operator hears about it instead of discovering it at
restore time:

- `continuity_gap_lost` (**critical**) — a stream stamped `gap_lost`: events
  in the gap are permanently unrecoverable. Checked every few minutes for the
  command-line index **and** every registry server.
- `verify_problem` — a verify run (manual or scheduled) found mismatches
  (**critical**) or errors / failed to run (**warning**).
- `rotation_unhealthy` (**warning**) — a built-in rotation cycle failed or
  kept deferring unarchived partitions; either way the index is not
  shrinking when it should.
- `baseline_stale` (**critical**) — a table's newest baseline predates the
  oldest available delta coverage: full-table restore through the missing
  window is impossible until a fresh baseline is taken. Checked hourly for
  every server with a baseline location configured.

The payload is `{event, severity, server, summary, details, timestamp}` —
generic JSON, no per-vendor adapters: Slack, PagerDuty, ntfy etc. all accept
it through their inbound-webhook integrations. Notifications are
**edge-triggered**: one POST on the transition into a bad state, a reminder
at most every 24 h while it persists (for verify problems: on the next run
that still finds them), and one `resolved: true` event (severity
`info`) on recovery. Delivery is best-effort — bounded retries off the hot
path, never blocking capture. A dead or wrong endpoint surfaces as
rate-limited log warnings, and an event that could not be delivered is not
re-queued: the next 24 h edge repeat re-sends a still-active condition. Edge
state is in-memory — after a daemon restart a still-active condition
re-fires (loud side errs safe), but a recovery that happened *across* the
restart produces no `resolved` event. The same three conditions are also
exported as Prometheus gauges under `--metrics-addr`
(`bintrail_continuity_*` / `bintrail_verify_*` / `bintrail_rotation_*`) —
see the metrics tables and example alert rules in
[observability.md](observability.md).

- **AWS credentials** — which ambient credential signals the daemon process
  can see: env keys (presence only, never values), `AWS_PROFILE`,
  `AWS_REGION`, a shared `~/.aws` config, ECS task-role / EKS IRSA markers.
  **The console never stores AWS keys.** S3 *uploads* and archived-event
  *reads* use the AWS default credential chain of the daemon (environment,
  shared profile incl. SSO, or an IAM role; EC2/ECS/EKS roles work even when
  nothing shows as set, since instance roles are not detectable without a
  metadata call). Baseline listings/reads from `s3://` ride DuckDB httpfs with
  AWS-SDK-chain credentials via the `aws` extension's `credential_chain`
  secret (set up automatically; SSO-session profiles have known upstream
  gaps, and hosts where the extension cannot install fall back to static
  environment keys).

## Flags

| Flag | Default | Description |
|---|---|---|
| `--index-dsn` | — | DSN for the index MySQL database. Becomes the ephemeral `default` entry. Required only when the server registry is empty. |
| `--listen` | `127.0.0.1:8090` | Bind address. `:8090` avoids the MCP server's `:8080`. |
| `--token` | — (none) | Opt-in static token for API automation. **Never generated** — humans sign in with the console password. One of the credentials that makes a non-loopback bind legal. |
| `--no-archive` | `false` | Disable Parquet archive auto-discovery (MySQL-only results). Also **disables Time-travel** — reconstruct needs archive access to verify coverage. |
| `--profile` | — | RBAC profile: deny tables / redact columns. Forces `--no-archive` and **disables Time-travel** (baseline reads bypass redaction). |
| `--allowed-hosts` | — | Extra hostnames accepted in the `Host` header (for reverse-proxy setups). IP literals and `localhost` are always allowed. |
| `--baseline-dir` | — | Local directory of baseline Parquet snapshots. Enables the Time-travel (reconstruct) surface. |
| `--baseline-s3` | — | S3 prefix of baseline snapshots (`s3://bucket/prefix/`). Enables Time-travel. |
| `--servers-file` | `~/.config/bintrail/console-servers.yaml` | Path to the server registry YAML managed from the UI. |
| `--auth-file` | `~/.config/bintrail/console-auth.yaml` | Console credential file enabling password login (see below). Created with `bintrail-console user set-password`, never by the server on its own. |
| `--tls-cert` / `--tls-key` | — | Serve the console over HTTPS (PEM files, both-or-neither). Rotation = restart; no ACME. |
| `--allow-setup` | `false` | Allow browser first-run password setup on a non-loopback bind (assert the bind is access-controlled, e.g. host-loopback published). Loopback always allows setup. |

### Environment variables

- `BINTRAIL_INDEX_DSN` — same as `--index-dsn` (shared with other commands).
- `BINTRAIL_CONSOLE_LISTEN` — same as `--listen`.
- `BINTRAIL_CONSOLE_TOKEN` — same as `--token`.
- `BINTRAIL_CONSOLE_BASELINE_DIR` — same as `--baseline-dir`.
- `BINTRAIL_CONSOLE_BASELINE_S3` — same as `--baseline-s3`.
- `BINTRAIL_CONSOLE_SERVERS` — same as `--servers-file`.
- `BINTRAIL_CONSOLE_AUTH` — same as `--auth-file`.
- `BINTRAIL_CONSOLE_TLS_CERT` / `BINTRAIL_CONSOLE_TLS_KEY` — same as `--tls-cert` / `--tls-key`.
- `BINTRAIL_CONSOLE_ALLOWED_HOSTS` — comma-separated, same as `--allowed-hosts`.
- `BINTRAIL_CONSOLE_ALLOW_SETUP` — `1`/`true`, same as `--allow-setup`.
- `BINTRAIL_CONSOLE_SQL_PANEL` — `1`/`true` enables the **SQL** page: a
  server-side, read-only SQL query box over the selected server's Parquet (see
  [The SQL panel](#the-sql-panel-opt-in)). Off by default; works on both `serve`
  and `watch`. Unlike the `views.sql` download, this executes SQL **inside the
  daemon**, so it runs in a locked-down DuckDB sandbox — read [The SQL panel](#the-sql-panel-opt-in)
  before enabling it on a shared or public bind.
- `BINTRAIL_CONSOLE_ARCHIVE_STAGING` (`watch` only) — local staging dir for the
  Archive-to-S3 feature, same as `--archive-staging-dir`. AWS credentials for
  the upload come from the ambient chain (`AWS_*` / `~/.aws` / role).
- `BINTRAIL_CONSOLE_BASELINE_TRIGGER` (`watch` only) — `1`/`true` enables the
  **Create baseline** button (runs `mydumper` → convert → upload in-process;
  see [The Storage page](#the-storage-page)). Off by default for a bare
  `watch` invocation; the bundled compose stack sets this on by default (see
  [docker.md](docker.md) — `BASELINE_TRIGGER=0` in `.env` opts out there).
- `BINTRAIL_CONSOLE_BASELINE_STAGING` (`watch` only) — local staging dir for
  S3-destined baselines created by that button (default a temp subdir).
- `BINTRAIL_CONSOLE_BASELINE_LOCK_MODE` (`watch` only) — `ftwrl` (default),
  `lock-all`, `safe-no-lock` or `no-lock`. Selects how mydumper synchronizes its worker
  threads onto one instant for the **Create baseline** button. The default is
  point-consistent and needs `RELOAD`/`FLUSH_TABLES` (plus `BACKUP_ADMIN` on
  MySQL/Percona 8.0+). `lock-all` is also point-consistent and needs only
  `LOCK TABLES` — **the mode to use on RDS/Aurora**, where `BACKUP_ADMIN`
  cannot be granted and `ftwrl` therefore cannot run. `safe-no-lock` needs no
  elevated privilege but aborts rather than write a torn snapshot; `no-lock`
  accepts one. A baseline is the seed state
  `reconstruct` merges deltas onto, which is why the weaker modes must be named
  explicitly. An unrecognized value disables baseline DUMPS only — capture and
  the periodic refresh keep running.
- `BINTRAIL_CONSOLE_NOTIFY_WEBHOOK` (`watch` only) — same as
  `--notify-webhook`: URL for JSON notifications on lost continuity, verify
  problems, and unhealthy rotation (see
  [Webhook notifications](#webhook-notifications)).
- `BINTRAIL_CONSOLE_VERIFY_INTERVAL` (`watch` only) — same as
  `--verify-interval`: enables scheduled verification on that cadence
  (e.g. `24h`, `7d`; see
  [Running verification from the console](#running-verification-from-the-console)).
- `BINTRAIL_CONSOLE_VERIFY_TABLES` (`watch` only) — same as `--verify-tables`.
- `BINTRAIL_CONSOLE_VERIFY_TRIGGER` (`watch` only) — `1`/`true` enables the
  Storage page's **Verification** panel (runs `bintrail verify` in-process;
  see [Running verification from the console](#running-verification-from-the-console)).
  Off by default for a bare `watch` invocation; the bundled compose stack sets
  this on by default (see [docker.md](docker.md) — `VERIFY_TRIGGER=0` in
  `.env` opts out there).
- `BINTRAIL_CONSOLE_FLASHBACK_LISTEN` (`watch` only) — same as `--flashback-listen`
  (e.g. `127.0.0.1:3308`): serve an embedded MySQL-protocol time-travel port for
  every monitored server, routed by the connection username. Off by default;
  requires a console token. See [Time-travel over the MySQL protocol](#time-travel-over-the-mysql-protocol-flashback-port).

There is deliberately **no** environment variable for the password itself —
env vars leak through `docker inspect`, `ps e`, and `/proc`; the password is
set interactively or via `--password-stdin`, never inlined.

Precedence is the usual CLI flag > environment variable > default. The
`BINTRAIL_CONSOLE_*` variables apply equally to `bintrail-console watch` (where
the matching flags are `--console-listen`, `--console-token`, `--baseline-dir`,
`--baseline-s3`, `--console-servers-file`, `--console-auth-file`,
`--console-tls-cert`, `--console-tls-key`, `--console-allowed-hosts`,
`--console-allow-setup`).

## Password login

**Username + password is the primary way in.** On a fresh loopback console
with no credential, the first browser visit shows a **"create your password"**
screen; you set it once and you're signed in. Every later visit is a normal
sign-in. (Prefer the terminal, or setting it up before first launch? Run
`bintrail-console user set-password`.)

```console
$ bintrail-console user set-password
New console password: ********
Retype to confirm: ********
Console password set for user "admin" (~/.config/bintrail/console-auth.yaml).
A running server accepts it on the next login — no restart needed.
```

- **First-run setup is loopback-only.** The unauthenticated `POST
  /api/auth/setup` endpoint (which the "create your password" screen calls)
  is enabled only on a loopback bind — where reaching it already implies local
  access — and **self-disables the instant a password exists**. It is also
  enabled by `--allow-setup` (`BINTRAIL_CONSOLE_ALLOW_SETUP`), the assertion
  the Docker stack makes because it binds `0.0.0.0` inside the container but
  publishes the port on the host's loopback only. A non-loopback bind with no
  credential and no `--allow-setup` is refused — set the password from the
  shell first.
- The credential lives in a 0600 YAML file (`version`, `username`, a
  bcrypt-cost-12 `password_bcrypt`, `updated_at`) — same envelope and atomic
  write as the server registry. One user; multi-user/RBAC/SSO is dbtrail.
- A successful login (or first-run setup) mints an **in-memory session token**
  (24 h absolute, 8 h idle, max 16 concurrent) the SPA uses as its Bearer
  credential. Sessions die on logout, on password change (which revokes all
  of them), and on process restart — nothing session-shaped touches disk.
- The login response also sets the session token as an **HttpOnly
  `bintrail_session` cookie** (`Secure; SameSite=Lax; Path=/`, `Max-Age`
  matching the session's absolute expiry), so opening a console link in a new
  tab or window is already signed in — same store, same expiry, same
  revocation as the Bearer path; the cookie holds nothing new. Logout revokes
  the session server-side *and* expires the cookie, killing every tab at once.
  The `Secure` flag is safe on the local first-run flow (browsers treat
  loopback as a secure context); **operators terminating TLS at a reverse
  proxy** should serve the console over `https://` end-to-end from the
  browser's point of view, or the browser will drop the cookie and each tab
  falls back to its own login (the Bearer flow keeps working either way).
- **An opt-in static token** for automation: set `--token` /
  `BINTRAIL_CONSOLE_TOKEN` explicitly and scripts/curl/CI can call the API with
  it. It is never generated for you, and it is not the human path — when a
  token *is* set, the browser never sees the setup screen (the token is the
  credential).
- Login, setup, and password-change are throttled (per-IP 5 failures/min and
  20/15 min, 30/min globally, `Retry-After` on 429) and bcrypt-verified in
  constant time with no username enumeration. Loopback peers are exempt from
  the **global** window only — the deliberate no-self-DoS guarantee for the
  on-host operator; the per-IP windows still apply. There is no lockout — locking
  the single user out would hand an attacker a denial-of-service against the
  operator.
- **Rotate or reset:** from the UI (⌘K → "Change console password", revokes
  every other session immediately) or re-run `user set-password` (overwrites;
  applies on the next login, live sessions ride out their TTL). **Forgot the
  password?** Shell access is the recovery path — re-run `user set-password`.
  `user remove` deletes the file (a loopback console then returns to first-run
  setup; a non-loopback one refuses its next restart until a credential is set
  again). `user status` shows what is configured without printing secrets.
- Off-loopback password logins over plain HTTP are warned about at startup:
  use `--tls-cert`/`--tls-key` or terminate TLS at a reverse proxy (with
  `--allowed-hosts`).

### External login providers

Embedding distributions — builds that construct their console binary from the
importable `consoleapp` package (`cmd/bintrail-console` is a thin `main()`
over `consoleapp.Main`) — may install an external login flow (e.g. OIDC
single sign-on) through the `ext.ConsoleAuth` seam: call `ext.SetConsoleAuth`
once from `main()` before `consoleapp.Main`, like `ext.SetAuditSink`. When a
provider is installed, the sign-in screen adds a **"Continue with \<name\>"**
button, and a successful external login mints the same in-memory session a
password login does (same lifetime, logout, and revocation). An installed
provider also counts as a valid sole credential for a non-loopback bind —
first-run browser setup stays loopback-gated regardless. The standalone
`bintrail-console` binary has no provider installed: the button never
appears, and the provider routes (`/api/auth/ext/*`) simply require a normal
credential like any other `/api` path.

### Extension views

Embedding distributions — builds that construct their console binary from the
importable `consoleapp` package — may add one additional view to the console
through the `ext.ConsoleView` seam: call `ext.SetConsoleView` once from
`main()` before `consoleapp.Main`, like `ext.SetConsoleAuth`. An installed view
contributes a nav item, a frontend module, and its own authenticated data API;
the console reveals the nav item, routes to it, and loads the module in the same
page (same origin, not an iframe).

The view's static assets are served **unauthenticated** at `/ext/<id>/` (the
code always ships, like the console's own `app.js`), while its data routes at
`/api/ext/<id>/` require the same bearer credential as every other `/api` path
and are **refused while an access-control profile is active** (the console can't
guarantee a third-party handler honors table-deny / column-redaction rules, so
it withholds the whole surface under a profile). Each data route reads the index
of the server currently selected in the switcher, with the operator's profile
applied.

The standalone `bintrail-console` binary ships **no extension views**: no nav
item appears, `/api/capabilities` advertises none, and `/ext/*` and
`/api/ext/*` are absent from the router entirely.

## Security model

The binary has no Supabase/RBAC backend to lean on, so the console defends
itself:

- **Loopback by default + a credential required.** On a loopback bind the
  console prompts you to create a password on first run — no credential is ever
  auto-generated for you. Binding to a non-loopback address (`0.0.0.0`, a LAN
  IP, …) **requires** an explicit `--token` or a configured console password, or
  the command refuses to start.
- **Constant-time credential checks** (`crypto/subtle` for the token;
  sessions are looked up by SHA-256 of the presented value, so raw session
  tokens never live server-side; unknown-username logins still burn a full
  bcrypt compare so timing cannot enumerate the username).
- **Bearer header first; the session cookie is the tab-bridging sibling.**
  `/api/*` accepts the credential (static token or login session) in the
  `Authorization: Bearer <token>` header, or — for browser navigation — the
  HttpOnly session cookie a login sets. A request carrying a Bearer header is
  judged on that header alone (no cookie fallback), so scripted access is
  unchanged. Cookie-authenticated **state-changing** requests (anything but
  GET/HEAD/OPTIONS) must additionally send `Content-Type: application/json`,
  which an HTML form cannot produce — combined with `SameSite=Lax`, a
  cross-site form POST cannot carry the ambient cookie to `/api/recover`.
  Login itself requires `Content-Type: application/json` too — login-CSRF
  dies the same way. The page shell loads without a token (it reads the token
  from the URL, or probes the session cookie, to bootstrap its requests).
- **No CORS headers.** Requests are same-origin only.
- **Host-header allowlist.** Requests whose `Host` is a domain name are
  rejected (only IP literals and `localhost` pass), which defeats DNS-rebinding
  attacks against the local bind.
- **Static security headers** on every response: `Referrer-Policy:
  no-referrer` (keeps the `?token=` bootstrap URL out of Referer headers),
  `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`.
- **Brute-force throttling** on the two bcrypt-verifying endpoints (login and
  change-password), counting failures per client IP plus a global window.
  Loopback socket peers are exempt from the **global** window only (per-IP
  windows still apply), so a remote attacker can never rate-limit the on-host
  operator out. The exemption keys on the real socket peer —
  `X-Forwarded-For` / `X-Real-IP` are never trusted — so it cannot be spoofed
  remotely. Consequence: behind a same-host reverse proxy the socket peer *is*
  loopback, so the global 30/min backstop does not apply and only the per-IP
  buckets throttle — and all proxied clients share that one bucket; rate-limit
  at the proxy if that matters to you.
- **Result caps.** Every query is bounded — events default 100 (max 1000),
  recover default 1000 (max 10000). Never unlimited.
- **Unauthenticated endpoints**: `/api/healthz` (liveness), `GET /api/auth`
  (does password login exist — what the login form's presence would reveal
  anyway), `POST /api/auth/login`, and — only during first-run setup —
  `POST /api/auth/setup`. Everything else needs a Bearer
  credential or the login session cookie.

## PostgreSQL sources

The console reads only the **index**, never the source database, so it works
identically for a PostgreSQL source captured by `bintrail-pg`: the index schema
is the same. It does adapt its **presentation** to the source family (reported
per server as `source` in [`/api/capabilities`](#api), derived from
`stream_state.flavor`):

- **Stream vocabulary.** A PostgreSQL stream shows its cursor as an **LSN** (and
  labels the source "PostgreSQL · logical replication") instead of MySQL binlog
  file / position / GTID. Slot and publication *names* are capture-side
  configuration and are not stored in the index, so the console does not show
  them.
- **Permanent-loss badge.** The Status page surfaces the durable loss record
  (`stream_state.gap_lost_at`) — for PostgreSQL, an invalidated/lost replication
  slot; for MySQL, an unfillable binlog gap. The index is valid only up to that
  point and capture must be re-baselined to resume.
- **Connection-id note.** PostgreSQL logical replication (`pgoutput`) carries
  no backend connection id, so `connection_id` is empty for PostgreSQL sources
  — no console or capture setting can add it. The Events page says so for
  PostgreSQL sources rather than leaving it an unexplained gap.
- **Replication-health panel.** The Status page shows the replication slot's
  WAL-retention state (`wal_status`, retained WAL, the safe margin before
  invalidation) and whether every published table is at `REPLICA IDENTITY FULL`.
  The console is still index-only: it never queries the source. Instead the
  streaming daemon (`bintrail-pg stream` / `watch`) polls the source every ~30s
  and persists a snapshot to the index (`stream_state.source_health`), which the
  console renders. Because a snapshot can outlive a stopped daemon, the panel
  shows **how recently it was checked** and **degrades a stale snapshot** (older
  than ~90s) to muted with a warning — a frozen "reserved" must never read as
  live-healthy. If the daemon cannot read the source at all (for example a
  standby, where the slot-retention metrics are unavailable), the panel shows
  **probe failing** with the reason rather than disappearing. For an on-demand,
  always-live check, use `bintrail-pg doctor`.

## MCP endpoint

The console serves the same six read-only MCP tools as
[`bintrail-mcp`](mcp-server.md) — `query`, `recover`, `recover_cascade`,
`reconstruct`, `status`, `list_schema_changes` — over **Streamable HTTP**, on
both `bintrail-console serve` and `bintrail-console watch`:

| URL | Target |
|---|---|
| `/mcp` | The console's **default server** (same selection rules as the browser UI). |
| `/mcp/{id-or-name}` | A named server from the registry (`default` = the command-line entry). Unknown → `404`. |

MCP clients cannot reliably send custom headers, so the server choice lives in
the URL path (mirroring how the [time-travel port](time-travel-sql.md) routes
by username) instead of the `X-Bintrail-Server` header.

Point any Streamable-HTTP-capable MCP client at it with the console token as a
Bearer credential:

```json
{
  "mcpServers": {
    "bintrail-console": {
      "type": "http",
      "url": "http://127.0.0.1:8090/mcp",
      "headers": { "Authorization": "Bearer <console token>" }
    }
  }
}
```

Rules that differ from the standalone `bintrail-mcp` server:

- **A token is required.** Password login is a browser credential and cannot
  authenticate a headless MCP client, so `/mcp` needs either the static
  `--token` / `BINTRAIL_CONSOLE_TOKEN` **or a managed MCP token generated
  from Settings → Connect AI** (#1052) — one click from an authenticated
  browser session, no flags, no restart. The managed token is persisted as a
  SHA-256 hash only (`~/.config/bintrail/console-mcp-token.yaml`, `0600`,
  atomic write, versioned envelope with the registry's read-only-if-newer
  contract), its plaintext is shown exactly once at generation, and it is
  **scoped to `/mcp` alone** — it cannot drive the browser API (registry
  CRUD, monitor verbs, or its own rotation). Rotate/revoke from the same
  card take effect on the next request, including for sibling console
  processes sharing the file: every `/mcp` request re-validates the
  credential, so a rotated-away or revoked token stops authenticating
  immediately. An MCP *session* is additionally bound to the credential that
  created it — no other token can continue it, so a session opened by a
  since-rotated token is orphaned outright (its already-open stream can idle
  out but never be driven again) and is discarded after the idle timeout:
  sessions with no request for **30 minutes** are closed (clients keep a
  session alive with pings, or transparently re-initialize).
- **A managed token carries the grants of the session that minted it.** Each
  tool call requires the same permission as its `/api` counterpart — `query`
  and `list_schema_changes` need `query:execute`, `recover` needs
  `recover:execute`, `reconstruct` needs `reconstruct:execute`, `status`
  needs `status:read` — checked against the permission set recorded into the
  token file at mint time, so a session cannot mint its way past what the
  API would refuse it directly. A call the token's grants do not cover fails
  with a tool error naming the missing permission. Tokens minted by a
  full-access session (the static token, a password login — every session in
  a plain OSS install) record no cap and keep the full read surface, and
  **tokens minted before grants were recorded behave the same** (they were
  all minted by full-access sessions); rotate the token to stamp the current
  session's grants. The static `--token` / `BINTRAIL_CONSOLE_TOKEN` is
  environment-owned and always full-access.
- **`index_dsn`, `profile`, `baseline_dir` and `baseline_s3` tool parameters are
  rejected.** Connections, the baseline location and the RBAC posture are all
  managed by the console process — an authenticated MCP client cannot point the
  console at an arbitrary DSN or storage prefix, nor change redaction rules.
- **`reconstruct` is gated per server**, on the same signal as the Time-travel
  tab and `/api/reconstruct`: that server needs a baseline location configured,
  with archives enabled and no access-control profile active (baseline reads
  aren't redacted). Otherwise the tool refuses with that explanation.
- **The console's read boundary applies.** Result caps match the API (events
  100 default / 1000 max, recover 1000 / 10000), each server's archive and
  baseline posture is honored, and `query_text` / `query_hash` are withheld
  from query results exactly as on the events API.

The host-header allowlist (`--allowed-hosts`) covers `/mcp` like every other
route.

The UI's **Settings → Connect AI** page assembles all of this for you: the
ready-to-copy `/mcp` URL for the selected server (the per-server form when
more than one server is registered), the `.mcpb` bundle download for the
running version, and the raw-config fallback above. Its **Access token** card
generates, rotates, and revokes the managed MCP token; the plaintext is
displayed exactly once, at generation, and never stored. For the
start-to-finish walkthrough (bundle install included), see
[Connect an AI assistant](connect-ai.md).

## API

All endpoints return JSON except `GET /api/views.sql`, which serves a SQL file. `/api/*` (except `healthz`) require
`Authorization: Bearer <token>`.

| Method & path | Purpose |
|---|---|
| `GET /api/healthz` | Liveness probe (no token). |
| `GET /api/auth` | Auth mode (no token): `{"password_login": bool, "setup": bool}` — `setup` true means first-run create-password is open. |
| `POST /api/auth/login` | Exchange `{username, password}` for a session: `{token, expires_at}`. Rate-limited; requires `Content-Type: application/json`. |
| `POST /api/auth/setup` | First-run only (loopback / `--allow-setup`, self-disables once a password exists): create the password, returns a session. |
| `POST /api/auth/logout` | Revoke the presented session (static token → 204 no-op). |
| `POST /api/auth/password` | Set (first time; requires static-token auth) or rotate (`current_password` verified) the console password. Revokes all sessions and returns a fresh one. |
| `GET /api/status` | Index status (same payload as `bintrail status --format json`). |
| `GET /api/coverage` | Live RPO summary: restorable delta window `[delta_from, delta_to]`, `lag_seconds`, `continuity`; with a baseline source, `full_table_status` (`ok`/`unknown`), `full_table_from` and `broken_tables` (profile-restricted sessions get the delta half only). |
| `GET /api/activity` | Window aggregate behind the Overview tiles: counts by event type, distinct tables touched, and a per-table breakdown. The window **is the live retention** — derived from the oldest live `binlog_events` partition, so the counts cover exactly what the live index still holds and read the live tier only (no archive scan, and nothing archived can fall inside the window by construction). Returns `{label, since, until, refreshed_at, total, inserts, updates, deletes, other, tables, top_tables, complete, notes}`. The aggregate is a **server-side materialization** refreshed when older than ~30 minutes (a stale copy is served immediately while one recompute runs in the background); `refreshed_at` is when it was computed, and the UI renders it on the tiles ("as of …") so a cached number is never presented as live. `complete: false` means the counts are knowably a floor (an index with a pathological table count trips the grouping cap) and `notes` says so; the UI marks the affected tiles "partial". RBAC deny rules are applied, so a denied table contributes to neither the counts nor `top_tables`, and each deny profile gets its own materialization. |
| `GET /api/schemas` | Schemas known to the index: those observed in `binlog_events` **plus** those in the latest schema snapshot, so a schema whose partitions have all been rotated out to Parquet/S3 is still listed (the archives still answer `/api/events` and `/api/recover`). `schemas` is that full union; `snapshot_only` (when present) is the subset with no live events observed — the UI labels these "snapshot only" since queries against them may return nothing; `snapshot_unavailable: true` means the snapshot half was skipped because the schema resolver failed to load (check the server log), so archive-only schemas may be missing from the list. The snapshot half is skipped under `--no-archive` or an active `--profile`, where archived data is unreachable anyway. Note this answers *which schemas this index knows of*, not *which have data in a given window* — for that, see `bintrail status`'s continuity verdict. `?schema=<name>` → that schema's tables. |
| `GET /api/events` | Event browser. Query params: `schema, table, pk, event_type, gtid, since, until, changed_column, order, limit`. |
| `POST /api/recover` | Undo-SQL generation. JSON body with the same filter fields (requires at least `schema`; an `order` field is accepted but ignored — recover always processes oldest-first). Returns `{sql, statement_count, row_count, warnings, notes}`. When the target is a foreign-key **parent** whose `DELETE` cascaded below the binlog (MySQL/MariaDB index only), cascade victims are **auto-detected** and folded into the same script; the response then also carries `{cascade_detected, victim_count, set_null_count}` (see [Recover and cascade](#cascade-recovery)). |
| `POST /api/recover-cascade` | Cascade-recovery SQL generation (reverse FK `ON DELETE CASCADE` / `SET NULL` side effects). JSON body: `schema, table` (the **parent**), `pk, pks, since, until, lookback, max_depth, allow_incomplete`. Returns `{sql, statement_count, victim_count, set_null_count, complete, incomplete}` — text only, never executed. Returns `403` under an active RBAC redaction profile (see [Cascade recovery](#cascade-recovery)). |
| `GET /api/capabilities` | Reports enabled optional surfaces for the **selected server**, e.g. `{"reconstruct": true, "recover_cascade": true, "recover_cascade_baseline": false, "source": "mysql", "auth": {"password_set": true, "auth_kind": "session"}}`. The frontend uses it to show/hide gated tabs on every switch (`reconstruct` → Time-travel) and to gate the logout affordance (`auth_kind` says how this request authenticated). `recover_cascade` reports whether cascade synthesis is available (false under an RBAC redaction profile) — it gates the standalone `POST /api/recover-cascade` endpoint; the **Recover** tab's auto-detection follows the same server-side rule. `source` (`"mysql"` or `"postgresql"`, read from the index's `stream_state.flavor`) drives **source-aware presentation** only — never a gate; see [PostgreSQL sources](#postgresql-sources). |
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
| `GET /api/rotation` | Effective global rotation policy: `{retain, interval, add_future, source, enabled}` — `source` is `"override"` (console-saved) or `"default"` (daemon `--rotate-*`). |
| `PUT /api/rotation` | Supervisor only (403 on the standalone console): save a global rotation override `{retain, interval, add_future}` (validated; `off` rejected). Applies live on the next cycle. |
| `GET /api/baselines` | Read-only listing of the **selected server's** baseline snapshots, grouped per snapshot: `{configured, source, kind, reconstruct, snapshots: [{time, age_hours, tables, binlog_file, binlog_pos, gtid_set}]}` (coordinates local-only, capped at 50 snapshots). `502` when the configured source is unreadable. |
| `GET /api/views.sql` | **Not JSON** — a `text/plain` DuckDB schema over the selected server's Parquet (the same output as `bintrail views`), served as a `views.sql` attachment. Nothing is executed here; the file runs in your own DuckDB. 404 when archives are disabled or nothing is archived yet, 403 while an access-control profile is active. |
| `POST /api/sql` | Runs a read-only `SELECT` over the selected server's Parquet **inside the daemon**, in a locked-down DuckDB sandbox, returning `{columns, rows, row_count, truncated, elapsed_ms}`. Opt-in (`BINTRAIL_CONSOLE_SQL_PANEL=1`) — `403` otherwise. `403` while a profile is active; `404` when archives are disabled or there is nothing to query; `422` for a non-`SELECT`, a statement error, or the timeout; `429` when another query is already running. Cancellation is by aborting the request. See [The SQL panel](#the-sql-panel-opt-in). |
| `GET /api/storage` | Process-global storage context: `{aws: {access_key_env, profile, region_env, shared_config, container_creds, web_identity}}` — presence booleans and non-secret names only, never credential values. |
| `GET /api/profiles` | RBAC data-profile **names** defined on the selected server's index: `{"profiles": ["..."]}`, sorted; empty on a legacy index without the table. Vocabulary for administration panels (e.g. a settings-surface profile picker) — never the rules or flagged tables/columns behind a name. |

Every data endpoint (`status`, `schemas`, `events`, `recover`,
`recover-cascade`, `capabilities`, `reconstruct`, `baselines`) targets the
server named by the `X-Bintrail-Server` request header; without the header they
target the default entry (`storage` is the one process-global exception).
Selection is stateless — concurrent clients can each target a different server.

### Cascade recovery

On MySQL 8.x and earlier (and all MariaDB), InnoDB enforces a foreign-key `ON
DELETE CASCADE` / `ON DELETE SET NULL` *below* the binary log — only the parent
`DELETE` is logged, so the cascaded child deletes and SET-NULL updates are never
recorded as events. A plain undo of the parent `DELETE` would re-create the
parent but leave those children gone. The `ON UPDATE` cascades have the same
blind spot: only the parent `UPDATE` is logged, so undoing it alone would leave
every child foreign key pointing at the key the cascade wrote.

**Recover handles this automatically — there is no separate tab.** When you
generate undo SQL for a `DELETE` — or for an `UPDATE` of a referenced key — on a
table that is a foreign-key **parent**, the console detects it (one index lookup
of the recorded FK graph, matched to the referential action the reversed events
can actually trigger) and folds the invisible children into the **same** script:
`INSERT`s for the cascade-deleted children, idempotent guarded `UPDATE`s
(`… AND fk IS NULL`) for SET-NULL'd foreign keys, and guarded `UPDATE`s
(`… AND fk = <new key>`) for the foreign keys an `ON UPDATE CASCADE` rewrote, all
wrapped in `SET FOREIGN_KEY_CHECKS=0/1`. A **CASCADE detected**
banner above the result reports how many children, SET-NULL restores and FK
restores were included. Copy or download it; **nothing is ever executed.**
Detection only fires
for a MySQL/MariaDB index (PostgreSQL logical replication captures cascade
deletes as real events — no blind spot to reconstruct) and only when the matched
rows actually contain an event on the target table that its FK rules say could
have cascaded. The same engine is exposed
for scripting as `bintrail recover-cascade` and `POST /api/recover-cascade` (with
explicit `lookback` / `max-depth` knobs) — see
[query-and-recovery.md](query-and-recovery.md) for the full mechanism, the
baseline Phase-2 fallback, and the coverage limits.

**Coverage is surfaced, never hidden.** Phase-1 (live binlog window) recovery is
partial by construction — a child not touched within `lookback` (default `30d`)
and not in a baseline cannot be reconstructed. When the result is provably
partial (a coverage gap, a per-parent overflow, or archived-out partitions the
live scan can't see) the warnings carry a prominent **provably partial** notice
listing every caveat — and the same caveats are embedded in the generated SQL's
preamble, so a partial recovery can never read as a full restore even after you
copy or download it.

**Under an RBAC redaction profile**, cascade synthesis is **disabled** (it does
not yet pass through the query engine's column redaction, so synthesizing child
rows could leak redacted/denied data): the parent-only undo is still generated,
but with an explicit warning that cascade children are **not** included — it is
never silently presented as a full restore. The standalone `POST
/api/recover-cascade` endpoint returns `403` in that mode. When a baseline is
configured, the `recover_cascade_baseline` capability signals that Phase-2
(recovering children untouched within the window) is active for the selected
server.

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
> 200. Pass `allow_gaps=true` to fall back to warn-and-continue — with the
> skipped source reported in the response `warnings` (#1281).

### Time-travel over the MySQL protocol (flashback port)

The reconstruct tab above is HTTP/browser-oriented. For a `mysql` client or an
application that speaks `AS OF` SQL, `bintrail-console watch --flashback-listen
<addr>` opens an embedded MySQL-protocol port that serves the `_flashback` /
`_snapshot` / `_diff` virtual schemas for **every monitored server** — routed by
the connection username (the server's registry id or display name), authenticated
with the console token. It replaces running a separate `bintrail shim` per
per-source index: the daemon already resolves each server's `bintrail_idx_<id>`
and baseline, so one port covers them all. A token is required (`--console-token`
/ `BINTRAIL_CONSOLE_TOKEN`) because MySQL-protocol auth cannot use the console's
password store. Full setup, routing, and the `_snapshot` baseline-parity edge:
[docs/time-travel-sql.md → the embedded port](time-travel-sql.md#the-embedded-port-multi-source).

### Coverage gaps and incomplete data

Both `/api/events` and `/api/recover` report coverage gaps (hours rotated out of
MySQL with no archive) in a `warnings` array rather than failing the request —
matching the CLI `recover`, which warns and continues so a human can review the
script. The recover screen renders those warnings prominently, so an
incomplete-coverage undo is flagged to the operator rather than silently
presented as complete.

On an archive-heavy server the default Events browse (newest first, no time
filter) is answered from the live index alone whenever the live index fills
the whole page: rotation only archives hours **older** than the oldest live
partition, so archived events cannot appear in a newest-first page the live
index already filled. When that shortcut is taken the response says so with an
entry in `notes` ("This page was answered from the live index; the registered
archives were not read…"; `/api/recover` carries the same fact in its own
words when a filled reversal window elides the archives). On `/api/events`
and `/api/recover` — and only there — `notes` is the informational sibling of
`warnings`: it carries benign audit facts — this one records a
completeness-preserving optimization, nothing is missing from the page — and
the console renders it as a muted line, not as an alert. Cautionary facts
(coverage gaps, a session's archive exclusion, divergence findings) stay in
`warnings`. Do not generalize the field name across endpoints:
`/api/activity`'s pre-existing `notes` is CAUTIONARY — it explains a
`complete: false` aggregate. Older pages, time filters that reach archived
hours, and pages the live index cannot fill read the archives exactly as
before.

One residual limitation: a few failure modes are logged server-side but not
surfaced to the browser (this matches the CLI `recover`, which warns to stderr
and continues; both apply only to these permissive `AllowGaps=true` endpoints —
the reconstruct endpoint fails loudly by default, and when you opt into
`allow_gaps` it reports these same conditions as response warnings instead):

- some of several configured archive sources fail to load, and
- the query planner itself fails to run (gap detection is skipped entirely).

In both cases you get results without a coverage caveat in the response. Watch
the server log when running with archives configured. (One narrower reconstruct
case remains log-only: the straddling-transaction probe re-fetches with gaps
allowed, so a source failing only during that probe is logged server-side.)
