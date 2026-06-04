# bintrail console

`bintrail console` serves an embedded, **read-only, single-operator** web UI over
an existing index. It is the MCP server with a web face: the same query,
recovery, and status engines, reached from a browser. Browse indexed row events
with full before/after diffs, and generate recovery (undo) SQL — all without
leaving the terminal that started it.

The console **never executes SQL**. Recover produces a transaction-wrapped
script you copy or download and apply yourself after review, exactly like
`bintrail recover --dry-run`.

> **Scope (MVP):** event browsing, recovery-SQL generation, and index status.
> Point-in-time full-table reconstruction (baseline + deltas) is **not** part of
> the console yet — use the offline `bintrail reconstruct` for that.

## Usage

```sh
bintrail console --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index"
```

On start it prints a jupyter-style URL with an access token:

```
Bintrail console (read-only) is running. Open:

    http://127.0.0.1:8090/?token=ab12cd34ef56ab12cd34ef56ab12cd34
```

Open that URL in a browser. Three tabs:

1. **Recover** (landing) — filter schema / table / PK / time, preview the
   affected rows with before→after diffs, then **Generate undo SQL** and
   copy/download the script.
2. **Events** — broader filters (event type, changed column, GTID, limit).
3. **Status** — index health: partitions, coverage, stream lag, archives.

## Flags

| Flag | Default | Description |
|---|---|---|
| `--index-dsn` | — (required) | DSN for the index MySQL database. |
| `--listen` | `127.0.0.1:8090` | Bind address. `:8090` avoids the MCP server's `:8080`. |
| `--token` | auto-generated | Access token. Auto-generated for loopback binds; **required** for non-loopback. |
| `--no-archive` | `false` | Disable Parquet archive auto-discovery (MySQL-only results). |
| `--profile` | — | RBAC profile: deny tables / redact columns. Forces `--no-archive`. |
| `--allowed-hosts` | — | Extra hostnames accepted in the `Host` header (for reverse-proxy setups). IP literals and `localhost` are always allowed. |

### Environment variables

- `BINTRAIL_INDEX_DSN` — same as `--index-dsn` (shared with other commands).
- `BINTRAIL_CONSOLE_LISTEN` — same as `--listen`.
- `BINTRAIL_CONSOLE_TOKEN` — same as `--token`.

Precedence is the usual CLI flag > environment variable > default.

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

### Coverage gaps and incomplete data

Both `/api/events` and `/api/recover` report coverage gaps (hours rotated out of
MySQL with no archive) in a `warnings` array rather than failing the request —
matching the CLI `recover`, which warns and continues so a human can review the
script. The recover screen renders those warnings prominently, so an
incomplete-coverage undo is flagged to the operator rather than silently
presented as complete.

One residual limitation: a few failure modes are logged server-side but not
surfaced to the browser, because `query.FetchMerged` exposes no signal for them
(this matches the CLI `recover`, which warns to stderr and continues):

- some of several configured archive sources fail to load, and
- the query planner itself fails to run (gap detection is skipped entirely).

In both cases you get results without a coverage caveat in the response. Watch
the server log when running with archives configured.

## Build

The frontend is vanilla HTML/CSS/JS with **zero third-party assets** (see
`internal/console/assets/VENDOR.md`) and is embedded via `//go:embed`. There is
no Node build step; `make build` (CGO, required for DuckDB) produces a single
self-contained binary.
