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
5. **Cascade recovery** — reverse a foreign-key `ON DELETE CASCADE` / `SET NULL`
   that InnoDB ran *below* the binlog: give it the **parent** table whose delete
   cascaded and **Generate cascade recovery SQL** (copy/download). A provably
   partial recovery shows a prominent **INCOMPLETE** banner listing every caveat,
   so it never reads as a full restore. Free tier, but **hidden — its route
   redirects to Overview — under an RBAC redaction profile**. See
   [Cascade recovery](#cascade-recovery).
6. **Status** — index health: partitions, coverage, stream lag, archives.
7. **Settings** (under `watch` only) — **Storage** (rotation policy,
   per-source S3 archiving, baseline snapshots, AWS credential signals — see
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

The **source user** you paste into the form needs `REPLICATION SLAVE,
REPLICATION CLIENT, SELECT` on the source MySQL — the form spells out the
exact `CREATE USER` / `GRANT` to copy. dbtrail never writes to or locks the
source. Full per-privilege breakdown and the least-privilege (schema-scoped
`SELECT`) variant: [streaming.md](streaming.md#the-source-mysql-user).

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
  `archive_s3` (the bucket above — non-secret, round-trips in the masked DTO).

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
  (`bintrail dump` → `bintrail baseline`).
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
- `BINTRAIL_CONSOLE_ARCHIVE_STAGING` (`watch` only) — local staging dir for the
  Archive-to-S3 feature, same as `--archive-staging-dir`. AWS credentials for
  the upload come from the ambient chain (`AWS_*` / `~/.aws` / role).

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
- **An opt-in static token** for automation: set `--token` /
  `BINTRAIL_CONSOLE_TOKEN` explicitly and scripts/curl/CI can call the API with
  it. It is never generated for you, and it is not the human path — when a
  token *is* set, the browser never sees the setup screen (the token is the
  credential).
- Login, setup, and password-change are throttled (per-IP 5 failures/min and
  20/15 min, 30/min globally, `Retry-After` on 429) and bcrypt-verified in
  constant time with no username enumeration. There is no lockout — locking
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

## Security model

The binary has no Supabase/RBAC backend to lean on, so the console defends
itself:

- **Loopback by default + a credential required.** A random 128-bit token is
  generated for loopback binds and printed in the URL. Binding to a
  non-loopback address (`0.0.0.0`, a LAN IP, …) **requires** an explicit
  `--token` or a configured console password, or the command refuses to start.
- **Constant-time credential checks** (`crypto/subtle` for the token;
  sessions are looked up by SHA-256 of the presented value, so raw session
  tokens never live server-side; unknown-username logins still burn a full
  bcrypt compare so timing cannot enumerate the username).
- **Bearer header on the API — never a cookie.** `/api/*` requires the
  credential (static token or login session) in the `Authorization: Bearer …`
  header, so a cross-site form POST cannot carry ambient credentials to
  `/api/recover`. Login itself requires `Content-Type: application/json`,
  which an HTML form cannot send — login-CSRF dies the same way. The page
  shell loads without a token (it reads the token from the URL to bootstrap
  its requests).
- **No CORS headers.** Requests are same-origin only.
- **Host-header allowlist.** Requests whose `Host` is a domain name are
  rejected (only IP literals and `localhost` pass), which defeats DNS-rebinding
  attacks against the local bind.
- **Static security headers** on every response: `Referrer-Policy:
  no-referrer` (keeps the `?token=` bootstrap URL out of Referer headers),
  `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`.
- **Brute-force throttling** on the two bcrypt-verifying endpoints (login and
  change-password), counting failures per client IP plus a global window.
  `X-Forwarded-For` is never trusted; behind a reverse proxy all clients share
  one bucket — rate-limit at the proxy if that matters to you.
- **Result caps.** Every query is bounded — events default 100 (max 1000),
  recover default 1000 (max 10000). Never unlimited.
- **Unauthenticated endpoints**: `/api/healthz` (liveness), `GET /api/auth`
  (does password login exist — what the login form's presence would reveal
  anyway), `POST /api/auth/login`, and — only during first-run setup —
  `POST /api/auth/setup`. Everything else needs a Bearer
  credential.

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
| `GET /api/auth` | Auth mode (no token): `{"password_login": bool, "setup": bool}` — `setup` true means first-run create-password is open. |
| `POST /api/auth/login` | Exchange `{username, password}` for a session: `{token, expires_at}`. Rate-limited; requires `Content-Type: application/json`. |
| `POST /api/auth/setup` | First-run only (loopback / `--allow-setup`, self-disables once a password exists): create the password, returns a session. |
| `POST /api/auth/logout` | Revoke the presented session (static token → 204 no-op). |
| `POST /api/auth/password` | Set (first time; requires static-token auth) or rotate (`current_password` verified) the console password. Revokes all sessions and returns a fresh one. |
| `GET /api/status` | Index status (same payload as `bintrail status --format json`). |
| `GET /api/schemas` | Distinct schemas. `?schema=<name>` → that schema's tables. |
| `GET /api/events` | Event browser. Query params: `schema, table, pk, event_type, gtid, since, until, changed_column, order, limit`. |
| `POST /api/recover` | Undo-SQL generation. JSON body with the same filter fields (requires at least `schema`; an `order` field is accepted but ignored — recover always processes oldest-first). Returns `{sql, statement_count, row_count, warnings}`. |
| `POST /api/recover-cascade` | Cascade-recovery SQL generation (reverse FK `ON DELETE CASCADE` / `SET NULL` side effects). JSON body: `schema, table` (the **parent**), `pk, pks, since, until, lookback, max_depth, allow_incomplete`. Returns `{sql, statement_count, victim_count, set_null_count, complete, incomplete}` — text only, never executed. Returns `403` under an active RBAC redaction profile (see [Cascade recovery](#cascade-recovery)). |
| `GET /api/capabilities` | Reports enabled optional surfaces for the **selected server**, e.g. `{"reconstruct": true, "recover_cascade": true, "recover_cascade_baseline": false, "auth": {"password_set": true, "auth_kind": "session"}}`. The frontend uses it to show/hide gated tabs on every switch (`reconstruct` → Time-travel, `recover_cascade` → Cascade recovery) and to gate the logout affordance (`auth_kind` says how this request authenticated). |
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
| `GET /api/storage` | Process-global storage context: `{aws: {access_key_env, profile, region_env, shared_config, container_creds, web_identity}}` — presence booleans and non-secret names only, never credential values. |

Every data endpoint (`status`, `schemas`, `events`, `recover`,
`recover-cascade`, `capabilities`, `reconstruct`, `baselines`) targets the
server named by the `X-Bintrail-Server` request header; without the header they
target the default entry (`storage` is the one process-global exception).
Selection is stateless — concurrent clients can each target a different server.

### Cascade recovery

On MySQL 8.x and earlier (and all MariaDB), InnoDB enforces a foreign-key `ON
DELETE CASCADE` / `ON DELETE SET NULL` *below* the binary log — only the parent
`DELETE` is logged, so the cascaded child deletes and SET-NULL updates are
invisible to the normal **Recover** tab. The **Cascade recovery** tab
reconstructs them: give it the **parent** table whose delete cascaded (schema /
table / PK, plus optional `since`/`until`, `lookback`, `max-depth`) and
**Generate cascade recovery SQL** — `INSERT`s for the cascade-deleted children
and idempotent guarded `UPDATE`s (`… AND fk IS NULL`) for SET-NULL'd foreign
keys, wrapped in `SET FOREIGN_KEY_CHECKS=0/1`. Copy or download it; **nothing is
ever executed.** It is the in-console face of `bintrail recover-cascade` — see
[query-and-recovery.md](query-and-recovery.md) for the full mechanism, the
baseline Phase-2 fallback, and the coverage limits.

**Coverage is surfaced, never hidden.** Phase-1 (live binlog window) recovery is
partial by construction — a child not touched within `lookback` and not in a
baseline cannot be reconstructed. When the result is provably partial (a coverage
gap, a per-parent overflow, or archived-out partitions the live scan can't see)
the tab shows a prominent **INCOMPLETE** banner listing every caveat — and the
same caveats are embedded in the generated SQL's preamble, so a partial recovery
can never read as a full restore even after you copy or download it.

**Gating.** Cascade recovery is the free `query_explorer` tier, shown whenever
the `recover_cascade` capability is true — which is whenever `recover` is,
**except under an active RBAC redaction profile**. Cascade victim synthesis does
not (yet) pass through the query engine's column redaction, so a `--profile`
disables it: the tab is hidden, its route redirects to Overview, and `POST
/api/recover-cascade` returns `403`. When a baseline is configured, the
`recover_cascade_baseline` capability signals that Phase-2 (recovering children
untouched within the window) is active for the selected server.

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
