# Time-Travel SQL Setup

This walkthrough takes you from zero to running a working time-travel query against your MySQL:

```sql
SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00' WHERE id = 12345;
```

The query is answered by `bintrail shim`, an in-process MySQL-protocol server (a subcommand of the `bintrail` binary) that intercepts the virtual `_flashback`, `_diff`, and `_snapshot` schemas and resolves them against your dbtrail index plus any rotated archives (local directory or `s3://` prefix). ProxySQL sits in front of both your real MySQL and the shim, routing each query to the right backend. The shim only cares that the index exists and `archive_state` is current — whatever keeps `binlog_events` populated (typically `bintrail stream`).

```
┌─────────────┐     :6033       ┌──────────┐    real query     ┌────────────┐
│ your app    ├────────────────►│ ProxySQL ├──────────────────►│ MySQL      │
└─────────────┘                 │          │                   └────────────┘
                                │          │  _flashback.*     ┌────────────┐
                                │          ├──────────────────►│ bintrail   │
                                │          │  _diff.*          │ shim       │
                                │          │  _snapshot.*      │ (:3308)    │
                                └──────────┘                   └────────────┘
```

## Two ways to run time-travel SQL

There are two ways to put `AS OF` SQL in front of your data; they differ only in
*how the client connects*:

1. **A dedicated terminal — point `mysql` straight at the shim (no ProxySQL).**
   Simplest. The shim already speaks the MySQL protocol, so an analyst connects
   a `mysql` client directly to it and runs `_flashback` / `_snapshot` / `_diff`
   queries. Trade-off: that connection answers *only* time-travel queries (a
   normal `SELECT` against a real table returns `ER_NOT_SUPPORTED_YET`, 1235).
   Use it when a person or tool just needs to read historical state.
2. **Transparent routing — ProxySQL in front (the rest of this guide).** Needed
   only when an application's *normal* connection must mix live queries and
   `AS OF` queries on the same endpoint. ProxySQL routes virtual-schema queries
   to the shim and everything else to your real MySQL.

### The dedicated terminal

**With the Docker Compose stack**, the shim ships as the opt-in `flashback`
profile. It serves the boot `SOURCE_DSN` source, so set `SOURCE_DSN` in `.env`
and bring it up *with* the full stack (`up -d`, not `up -d shim`, so the
streaming `bintrail` service comes along — see [docker.md](./docker.md)):

```sh
SHIM_USER=analyst SHIM_PASSWORD='pick-a-strong-one' \
  docker compose --profile flashback up -d
```

**Standalone** (the shim is a subcommand of the core `bintrail` binary), run it
against your index — no ProxySQL, and the source MySQL need not even be
reachable (the shim reads the index). `init-shim` reads `BINTRAIL_SOURCE_DSN`
and `BINTRAIL_SERVER_ID` from the environment (or `.bintrail.env`):

```sh
export BINTRAIL_SOURCE_DSN='user:pass@tcp(your-db:3306)/yourdb' BINTRAIL_SERVER_ID=prod-1
bintrail init-shim --out shim.yaml          # then fill in mysql_user + mysql_password
bintrail shim --shim-config shim.yaml \
  --index-dsn 'user:pass@tcp(127.0.0.1:3306)/bintrail_index'
```

Either way, connect a plain `mysql` client to the shim's port (default `3308`)
and query the virtual schemas:

```sh
mysql -h 127.0.0.1 -P 3308 -u analyst -p
mysql> USE myapp;
mysql> SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00' WHERE id = 12345;
mysql> SELECT * FROM _snapshot.orders  AS OF '2026-05-02 10:00:00';   -- full table (needs a baseline)
```

`_snapshot.*` (the complete table as it was) requires a baseline configured with
`--baseline-dir` / `--baseline-s3` (or `BASELINE_DIR` on the compose profile);
without one, `_flashback.*` returns only rows with binlog activity in the
retained window. The statement shapes and their semantics are identical on both
paths — see **Step 6 — Run a time-travel query** below.

---

The ProxySQL walkthrough below takes about 10 minutes on a fresh Ubuntu 22.04 or Amazon Linux 2023 host that already has a populated dbtrail index.

---

## Prerequisites

Before starting, you need:

- **A populated dbtrail index.** Some process is keeping `binlog_events` current — typically `bintrail stream`. If rotated hours have been archived, `archive_state` points at the local directory or `s3://` prefix where the Parquet files live. If you haven't set any of this up yet, see [`docs/streaming.md`](streaming.md) and [`docs/rotation-and-status.md`](rotation-and-status.md).
- **A `.bintrail.env` file** with `BINTRAIL_SOURCE_DSN`, `BINTRAIL_INDEX_DSN`, and `BINTRAIL_SERVER_ID` set. `bintrail config init` scaffolds one.
- **The `bintrail` binary** on the host. The shim is a subcommand — there is no second binary to download.
- **Root or `sudo` access** on the host.
- **A writable bintrail config directory.** The generator commands below (`bintrail init-shim`, `bintrail proxysql-config`) and the `mysql … < proxysql-setup.sql` redirect both run as the operator (not root), so the directory holding `.bintrail.env`, `shim.yaml`, and `proxysql-setup.sql` must be owned by the operator. If you keep these in a root-owned `/etc/bintrail`, run `sudo chown $(whoami):$(whoami) /etc/bintrail` once at install time.
- **The `mysql` client** installed on the host (used to apply ProxySQL config below).
- **A MySQL user your application will use to connect through ProxySQL.** This is *not* the replication user the streamer uses — it's a regular application user that ProxySQL authenticates against. Pick a username and a strong password; you'll need both below.

---

## Step 1 — Generate `shim.yaml`

`bintrail init-shim` scaffolds the file from your existing `.bintrail.env`:

```sh
cd /etc/bintrail   # or wherever your .bintrail.env lives
bintrail init-shim --out shim.yaml
```

The generated file has one tenant block populated from your `.bintrail.env`, plus two TODO lines for the application credentials:

```yaml
listen: '127.0.0.1:3308'

tenants:
  - server_id: '...'        # from BINTRAIL_SERVER_ID
    source_dsn: '...'       # from BINTRAIL_SOURCE_DSN
    # TODO: fill in your application's MySQL credentials
    # mysql_user: app_user
    # mysql_password: '<cleartext>'
```

Edit `shim.yaml`, uncomment the two TODO lines, and paste the values:

```yaml
    mysql_user: app_user
    mysql_password: 'your-app-password'
```

`bintrail proxysql-config` recomputes the SHA1 hash ProxySQL needs from `mysql_password` automatically — you do not need to run a manual SHA1 recipe.

> **Auth note**: both `bintrail shim` and ProxySQL validate the application's password against the same `mysql_password`. The default is `mysql_native_password`; `caching_sha2_password` is opt-in via `--auth-method` (see Step 4). The shim's listen address defaults to `127.0.0.1:3308` so it is not reachable from the network. Treat `shim.yaml` as you'd treat `.bintrail.env` — it contains a password and ships at 0o600.

---

## Step 2 — Install ProxySQL

ProxySQL 2.6 (LTS) is the recommended release.

### Ubuntu / Debian

```sh
sudo apt-get update
sudo apt-get install -y wget lsb-release gnupg ca-certificates
sudo install -d -m 0755 /etc/apt/keyrings
wget -qO- https://repo.proxysql.com/ProxySQL/repo_pub_key | \
  sudo gpg --dearmor -o /etc/apt/keyrings/proxysql.gpg
echo "deb [signed-by=/etc/apt/keyrings/proxysql.gpg] https://repo.proxysql.com/ProxySQL/proxysql-2.6.x/$(lsb_release -sc)/ ./" \
  | sudo tee /etc/apt/sources.list.d/proxysql.list
sudo apt-get update
sudo apt-get install -y proxysql=2.6.*
sudo systemctl enable --now proxysql
```

### RHEL / Amazon Linux 2023

```sh
sudo tee /etc/yum.repos.d/proxysql.repo >/dev/null <<'EOF'
[proxysql_repo]
name=ProxySQL 2.6.x repository
baseurl=https://repo.proxysql.com/ProxySQL/proxysql-2.6.x/centos/9
gpgcheck=1
gpgkey=https://repo.proxysql.com/ProxySQL/repo_pub_key
EOF
sudo dnf install -y proxysql-2.6.*
sudo systemctl enable --now proxysql
```

After install, ProxySQL listens on:
- **`:6032`** — admin port (used to apply config). Default credentials are `admin / admin`. Change them in `/etc/proxysql.cnf` before exposing this port to anything other than localhost.
- **`:6033`** — MySQL protocol port your application connects to.

---

## Step 3 — Apply the ProxySQL config

`bintrail proxysql-config` reads `BINTRAIL_SOURCE_DSN` from `.bintrail.env` and `shim.yaml` from the previous step and emits a deterministic SQL script:

```sh
bintrail proxysql-config --out proxysql-setup.sql
```

The script tells you exactly how to apply it:

```text
ProxySQL setup SQL written to proxysql-setup.sql
Apply it: mysql -u admin -P 6032 -h <proxysql-host> < proxysql-setup.sql
```

If ProxySQL is on the same host (typical):

```sh
mysql -u admin -p -h 127.0.0.1 -P 6032 < proxysql-setup.sql
```

The script wraps its DML in `BEGIN`/`COMMIT` and finishes with `LOAD ... TO RUNTIME` and `SAVE ... TO DISK`, so the new routing is live immediately and survives a ProxySQL restart. **Re-running the script is safe** — it scopes its DELETEs to dbtrail-owned hostgroups (990, 991) and rule IDs (990001-990006), so it never touches operator-managed config.

Verify ProxySQL accepted the config — you should see exactly two rows, one for hostgroup 990 (your real MySQL — `hostname` reflects whatever you have in `BINTRAIL_SOURCE_DSN`) and one for hostgroup 991 (the shim, always `127.0.0.1:3308`):

```sh
mysql -u admin -p -h 127.0.0.1 -P 6032 -e \
  "SELECT hostgroup_id, hostname, port FROM runtime_mysql_servers WHERE hostgroup_id IN (990,991);"
```

---

## Step 4 — Run `bintrail shim` under systemd

Create `/etc/systemd/system/bintrail-shim.service`:

```ini
[Unit]
Description=bintrail shim - time-travel SQL backend for ProxySQL
Documentation=https://github.com/dbtrail/dbtrail/blob/main/docs/time-travel-sql.md
After=network-online.target proxysql.service
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/etc/bintrail
EnvironmentFile=/etc/bintrail/.bintrail.env
ExecStart=/usr/local/bin/bintrail shim --shim-config /etc/bintrail/shim.yaml
Restart=on-failure
RestartSec=5s

StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

> A copy of this unit ships at `deploy/bintrail-shim.service` in the dbtrail repo.

The unit reads `BINTRAIL_INDEX_DSN` from `/etc/bintrail/.bintrail.env` (the same file your other `bintrail` commands use) so the shim can answer queries against your index. The DSN must include the index database name (e.g. `…/bintrail_index`) — the shim refuses to start otherwise. Append `--allow-gaps` to `ExecStart` to warn-and-continue on archive failures or coverage gaps instead of returning a MySQL error to the client; the default is strict because the wire protocol has no warning channel.

**Auth method on MySQL 8.4+.** If your MySQL has `mysql_native_password` disabled (the default since 8.4), append `--auth-method=caching_sha2_password` to `ExecStart` (or `Environment=BINTRAIL_AUTH_METHOD=caching_sha2_password`):

```ini
ExecStart=/usr/local/bin/bintrail shim --shim-config /etc/bintrail/shim.yaml --auth-method=caching_sha2_password
```

Requires ProxySQL **2.7+** between the application and the shim — the LTS 2.6 line isn't verified to negotiate SHA2 against backends, so operators on 2.6 keep the default (`mysql_native_password`). The application user used by ProxySQL must match the chosen scheme: `IDENTIFIED WITH mysql_native_password BY '<password>'` for the default path, `IDENTIFIED WITH caching_sha2_password BY '<password>'` for the opt-in. `sha256_password` is also accepted by `--auth-method` if your environment requires it. The same 2.7+ requirement applies when ProxySQL fronts an **8.4 source** directly (its `caching_sha2_password` backend), set via `proxysql-config --backend-auth-plugin caching_sha2_password`.

> **dbtrail's own connections to MySQL 8.4 need no auth flag.** The ProxySQL requirement above is only about ProxySQL negotiating `caching_sha2_password` to a backend. dbtrail's *index* connection (`--index-dsn`) and its *source replication* handshake (`bintrail stream`/`up`) both complete `caching_sha2_password` over a plaintext network on their own — with no flag, no TLS, and no ProxySQL in the path. This is what the bundled MySQL 8.4 index uses by default.

Enable and start:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now bintrail-shim
sudo systemctl status bintrail-shim
```

You should see `active (running)`. Tail the log if not:

```sh
journalctl -u bintrail-shim -f
```

The shim should report `shim listening addr=127.0.0.1:3308 tenants=N` once it has loaded `shim.yaml`.

---

## Step 5 — Point your application at ProxySQL

Change your application's MySQL connection string from the real MySQL port (`:3306`) to ProxySQL's MySQL port (`:6033`). The credentials are the `mysql_user` / `mysql_password` pair from `shim.yaml` (cleartext — same value the shim and ProxySQL both validate against).

For example, with the Go MySQL driver:

```go
// before:
db, _ := sql.Open("mysql", "app_user:your-app-password@tcp(127.0.0.1:3306)/myapp")

// after:
db, _ := sql.Open("mysql", "app_user:your-app-password@tcp(127.0.0.1:6033)/myapp")
```

Normal queries (`SELECT * FROM orders WHERE id = 1`) still go to your real MySQL, transparently. Only queries that reference `_flashback.*`, `_diff.*`, or `_snapshot.*` are routed to the shim.

---

## Step 6 — Run a time-travel query

Connect through ProxySQL:

```sh
mysql -u app_user -p -h 127.0.0.1 -P 6033 myapp
```

Six statement shapes are recognised:

```sql
-- Row state at a point in time (point-lookup, fast):
SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00' WHERE id = 12345;

-- The same, on the REAL table name — the AS OF clause must END the
-- statement (#385). Rewritten internally to _flashback (binlog-only):
SELECT * FROM orders WHERE id = 12345 AS OF '2026-05-02 10:00:00';

-- Optimizer-hint form on the real table name (ORM-friendly: survives query
-- builders that would reject AS OF syntax). `*`-only — a column list here
-- is a parse error:
SELECT /*+ DBTRAIL_AT='2026-05-02 10:00:00' */ * FROM orders WHERE id = 12345;

-- Full-table reconstruction at AS OF (no WHERE). Against _snapshot (with a
-- baseline configured) this is every row that existed at that instant;
-- against _flashback it is only rows with binlog activity in the retained
-- window (see the _snapshot vs _flashback note under Limitations):
SELECT * FROM _snapshot.orders  AS OF '2026-05-02 10:00:00';
SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00';

-- All events for one row in a time window:
SELECT * FROM _diff.orders BETWEEN '2026-05-01' AND '2026-05-02' WHERE id = 12345;
```

> **The hint form is the only one that degrades silently.** `/*+ DBTRAIL_AT=... */`
> is 100% valid vanilla MySQL (an unknown optimizer hint raises a warning, not an
> error), so if the query never reaches the shim — the client points at MySQL's
> port instead of ProxySQL's `:6033`, rules 990001-990006 are missing (e.g.
> ProxySQL restarted without `SAVE MYSQL QUERY RULES TO DISK`), or a
> lower-`rule_id` operator rule intercepts first — MySQL executes it against the
> live table and returns **present-day data with no error**. You believe you are
> reading the past; you are reading the present. The other shapes fail loud on
> the same misroute (`_flashback.*` → `ER_BAD_DB` 1049; bare `AS OF` → 1064), so
> prefer them whenever the client can emit them — reserve the hint form for
> ORM/query-builder paths that reject `AS OF` syntax, and verify routing after
> any ProxySQL change:
>
> ```sh
> bintrail doctor --source-dsn "$SRC" --proxysql-admin 'admin:<pass>@tcp(127.0.0.1:6032)/'
> ```
>
> The check confirms all six dbtrail rules are live in
> `runtime_mysql_query_rules` (advisory `WARN` when they aren't — it never
> changes doctor's exit code).

Every quoted time literal — in `AS OF`, `DBTRAIL_AT`, and the `BETWEEN` bounds — accepts four absolute formats (`'2026-05-02 10:00:00'`, RFC 3339 `'2026-05-02T10:00:00Z'`, zone-less `'2026-05-02T10:00:00'`, and date-only `'2026-05-02'`), plus `'now'` and relative forms `'<n> seconds|minutes|hours|days ago'` (e.g. `AS OF '5 minutes ago'`), resolved against the wall clock at parse time. Larger units (weeks, months) are deliberately not parsed — spell them as days.

**All three zone-less forms (`'2026-05-02 10:00:00'`, the zone-less RFC-3339-shaped literal, and date-only) are interpreted as UTC** — only the `Z`-suffixed RFC 3339 form is unambiguous by construction. There is no per-session override: a `SET time_zone = ...` sent by the client is accepted (returned `OK`, matching a real MySQL server's handshake noise) but has **no effect** on how the shim parses `AS OF` literals — it is treated as connection setup noise and silently ignored. If your monitoring or incident timeline is in local time, convert to UTC before writing the literal, or use the unambiguous `Z`-suffixed form.

**1-second granularity.** Timestamps are compared and stored at one-second resolution; a literal with sub-second precision has no finer effect than truncating to the second.

**Server-side prepared statements are not supported.** The shim has no `COM_STMT_PREPARE` handling and returns error 1105 ("not supported now") for it. Drivers/ORMs that prepare statements by default against the shim's port — MySQL Connector/J with `useServerPrepStmts=true`, .NET's `MySqlConnector`, Perl's `DBD::mysql` with `mysql_server_prepare` — fail on the very first query unless configured to use client-side (text-protocol) statements instead.

On the `_flashback` / `_snapshot` shapes a column list may replace `*` and the optional `TIMESTAMP` keyword may follow `AS OF` (Oracle / SQL Server convention):

```sql
SELECT id, email, name FROM _flashback.users AS OF TIMESTAMP '2026-05-02 10:00:00' WHERE id = 1;
```

The column list accepts bare identifiers only; backticks, schema-qualified columns (`users.id`), and aliases (`id AS user_id`) are not yet parsed and surface as `ER_PARSE_ERROR`. Columns the row image is missing (e.g. dropped post-event) come back as `NULL` — matching MySQL's behaviour after an `ALTER TABLE DROP COLUMN`.

For a **full-table** `SELECT *` (no column list), dbtrail unions the columns present across the reconstructed rows, so a column that existed at the queried time but was **dropped afterward** still appears — with its historical values — rather than being silently hidden by the current (narrower) schema.

The WHERE column must match the table's primary key. A WHERE on a non-PK column is rejected with a parser error rather than silently returning the wrong row.

`SHOW TABLES FROM _flashback / _diff / _snapshot` returns every table in the current schema that dbtrail has schema knowledge of (the newest snapshot per table, so PostgreSQL-source indexes — which record one table per snapshot — list all their tables too). A table that was since dropped at the source still appears: its indexed history remains queryable with `AS OF`. This lets an interactive `mysql>` session explore the virtual schemas:

```sql
USE myapp;
SHOW TABLES FROM _flashback;
```

`_diff` returns the full per-PK event history within the requested window — there is no implicit row cap. If a single hot row produced thousands of events, you'll get all of them in one response; if that's too much for one query, narrow the `BETWEEN` range.

Full-table `_flashback` / `_snapshot` queries are buffered and capped at 100,000 rows; exceeding the cap surfaces as `ER_TOO_BIG_SELECT` (code 1104) with a hint to narrow the AS OF range or add a PK filter. DELETE events are correctly suppressed — rows that did not exist at the AS OF instant don't appear in the resultset (same semantic as Oracle's `AS OF`). For ad-hoc filtering, joins, or aggregations, pipe the resultset to `duckdb`, `pandas`, or any tool that consumes a `SELECT *` stream — the shim deliberately stays a forensic point-lookup + full-table tool, not a SQL planner.

The shim resolves the row by replaying the relevant binlog events from your dbtrail MySQL index. If the timestamp falls outside the index's retention (because hourly partitions have been rotated to S3), the shim auto-discovers the Parquet archives via `archive_state` and merges results from both sources — same machinery `bintrail query` and `bintrail recover` already use.

---

## Troubleshooting

### `ERROR 1045: Access denied for user 'app_user'@'…'`

ProxySQL is rejecting your credentials. Confirm your app is connecting with the cleartext value of `mysql_password` from `shim.yaml`. If `shim.yaml` was edited, re-apply the ProxySQL config so the regenerated SHA1 reaches the live `mysql_users` table:

```sh
rm -f proxysql-setup.sql
bintrail proxysql-config --out proxysql-setup.sql
mysql -u admin -p -h 127.0.0.1 -P 6032 < proxysql-setup.sql
```

If the username comes through but the connection still fails, check `bintrail shim`'s log: it logs which usernames are in the allowlist at startup, and a connection from an unknown username is rejected.

### `_flashback.t doesn't exist` (or query goes to MySQL instead of the shim)

The query rule isn't matching. Inspect the routing:

```sh
mysql -u admin -p -h 127.0.0.1 -P 6032 \
  -e "SELECT rule_id, match_pattern, destination_hostgroup FROM runtime_mysql_query_rules WHERE rule_id BETWEEN 990001 AND 990006;"
```

You should see six rows targeting hostgroup 991 (one each for `_flashback.*`, `_diff.*`, `_snapshot.*`, the `/*+ DBTRAIL_AT=... */` hint-comment shape, `SHOW TABLES FROM` the virtual schemas, and the end-anchored bare `... AS OF '<ts>'` shape). If they're missing, re-apply `proxysql-setup.sql`. If they're present but the query still goes to MySQL, double-check that no operator rule with a smaller `rule_id` is intercepting `_flashback.*` first (ProxySQL evaluates rules in `rule_id` order). `bintrail doctor --proxysql-admin '<admin-dsn>'` runs the six-rules check for you (advisory `WARN` when any is missing or inactive).

Note this failure mode is only *visible* for the `_flashback.*`/`_diff.*`/`_snapshot.*` and bare `AS OF` shapes, which error out when misrouted to MySQL. The `/*+ DBTRAIL_AT */` hint form produces **no error at all** on the same misroute — MySQL treats the hint as unknown-and-ignorable and returns current data (see the warning under Step 6). If time-travel results ever look suspiciously like the present, check the rules above before trusting them.

### `connection refused` on the shim's port

`bintrail shim` isn't running, or it's listening on a different port than `shim.yaml`'s `listen` directive.

```sh
systemctl status bintrail-shim
ss -tlnp | grep 3308
```

If `bintrail-shim` is dead, `journalctl -u bintrail-shim -n 100` shows why. Common causes: missing or unreadable `shim.yaml`, missing `BINTRAIL_INDEX_DSN`, a `mysql_password` value that's not a valid YAML string (quote it).

### MySQL error codes the shim returns

The shim emits typed wire codes so ORMs and monitoring can distinguish *user input* errors from *server fault* errors — a 1105 spike no longer means "any time-travel query failed". Codes you may see:

- **1064 `ER_PARSE_ERROR`** — a query mentions `_flashback` / `_snapshot` / `_diff` but doesn't match any supported shape (missing `AS OF`, missing `BETWEEN`, missing `USE <db>`, unparseable timestamp). Same code MySQL itself returns for any SQL syntax error.
- **1235 `ER_NOT_SUPPORTED_YET`** — a non-virtual-schema query reached the shim (typically a direct connection to `:3308` bypassing ProxySQL). Hostgroup routing is misconfigured.
- **1526 `ER_NO_PARTITION_FOR_GIVEN_VALUE`** — two causes, distinguished by the message. Either the AS OF or BETWEEN range falls outside what this index retains (rotated out of MySQL with no archive coverage) — narrow the time range or check `archive_state` and the shim's `--allow-gaps` flag. Or a **full-table `_snapshot`** query found the configured baseline unusable (no baseline snapshot at-or-before the AS OF instant, or a primary key the baseline merge can't canonicalize) and refused rather than return a partial table — create or re-run `bintrail baseline` so a snapshot covers the AS OF, or query `_flashback` for a binlog-only view.
- **1045 `ER_ACCESS_DENIED_ERROR`** — credential mismatch (see the section above).
- **1104 `ER_TOO_BIG_SELECT`** — full-table `_flashback` / `_snapshot` returned more than 100,000 rows. Narrow the AS OF or add a `WHERE <pk> = <value>` to fall back to the point-lookup path.
- **1105 `ER_UNKNOWN_ERROR`** — real internal failure (DB timeout, archive S3 outage, build-resultset bug). This is the catch-all "the server is broken, retry" signal; persistent 1105s warrant inspecting the shim log.

### Time-travel query returns empty

Three causes produce an empty `_flashback` / `_snapshot` resultset:

1. The row had no event at-or-before the requested timestamp.
2. The latest event at-or-before the timestamp is a DELETE — the row did not exist at AS OF (Oracle `AS OF` semantic; matches the full-table path).
3. A coverage gap or archive-fetch failure under `--allow-gaps`. Without that flag the shim returns a typed MySQL error instead — `ER_NO_PARTITION_FOR_GIVEN_VALUE` (1526) for coverage gaps, `ER_UNKNOWN_ERROR` (1105) for archive-fetch failures — so on the default strict configuration the empty resultset never indicates a gap or an archive outage.

To distinguish cases 1 and 2, query `_diff` for the per-PK history: it returns every event (including the DELETE's `row_before`), so a row that was deleted produces at least one row in the diff resultset while a row that never existed produces zero. Or check the indexer is keeping up:

```sh
journalctl -u bintrail-stream -n 200
```

The dbtrail index retains the most recent hours via partition rotation; older data is in S3 (auto-discovered via `archive_state`). See [`docs/rotation-and-status.md`](rotation-and-status.md) for rotation and archive cadence.

### Operator already has users in hostgroup 990

`bintrail proxysql-config` scopes its DELETE to `mysql_users WHERE default_hostgroup = 990` — any pre-existing user in that hostgroup will be removed when the script is applied. If you have application users you want to keep separate from dbtrail-managed routing, place them in a different hostgroup before running the script. Hostgroup 990 is reserved for dbtrail; see the comment header at the top of the generated `proxysql-setup.sql` for the full list of resources the script manages.

---

## Limitations

- **Single source MySQL per shim.** The current `bintrail shim` is one-tenant-per-instance. If you have multiple source MySQLs you want time-travel SQL against, run one shim per instance with separate listen ports and separate ProxySQL hostgroups.
- **No TLS termination on the shim port.** `bintrail shim` accepts plain MySQL protocol on `127.0.0.1:3308` by default. If you need TLS between ProxySQL and the shim, terminate at ProxySQL or via an `stunnel` sidecar.
- **`_snapshot` is baseline-aware; `_flashback` is binlog-only.** Start `bintrail shim` with `--baseline-dir <dir>` or `--baseline-s3 s3://bucket/prefix/` (the snapshots produced by `bintrail baseline`) to enable it. Both the **single-row** (`WHERE <pk> = <value>`) and **full-table** (no WHERE) `_snapshot` shapes then seed row state from the baseline at-or-before the AS OF instant and apply post-snapshot binlog events on top — so a row that existed at AS OF but was never touched within the retained binlog window still resolves, and full-table `_snapshot` returns the table's complete row state at AS OF (never-touched baseline rows, rows updated/inserted after the baseline, with rows deleted after it dropped). `_flashback` deliberately stays binlog-only — its full-table form returns only rows with binlog activity in the retained window — so the two schemas have distinct, observable semantics. With no baseline source configured, `_snapshot` degrades to the binlog-only `_flashback` behaviour (a `Debug` log notes the fallback). The baseline match is supported for **integer, `YEAR`, `DECIMAL`/`NUMERIC`, string (`CHAR`/`VARCHAR`/`TEXT`/`ENUM`/`SET`), and `DATETIME`/`TIMESTAMP`/`DATE` PKs** (the shim pins the DuckDB session to UTC so temporal-PK matches resolve deterministically on any host timezone). **`FLOAT`/`DOUBLE`, `BINARY`/`VARBINARY`/`BLOB`, `BIT`, `JSON`, and spatial PK types can't be matched against the baseline** (their Parquet representation doesn't match reliably against a string literal), and neither can a table whose primary key can't be resolved from the indexed snapshot. The **single-row** shape still falls back to binlog-only in these cases (a `Warn` notes the unsupported PK type). The **full-table** shape does not: with a baseline source configured, it **refuses with `ER_NO_PARTITION_FOR_GIVEN_VALUE` (1526)** instead of silently returning a partial table (only rows with binlog activity in the window, indistinguishable from a complete one) — the error message points at `_flashback` for a binlog-only view. Full-table `_snapshot` also refuses with the same code when **no baseline snapshot exists at-or-before the AS OF instant**: take a baseline covering that instant (`bintrail baseline`), or query `_flashback`. For tables keyed by an unsupported PK type, or to write the reconstructed table to mydumper-format files, use the offline `bintrail reconstruct` command (its full-table mode holds the whole window in memory and warns past `--warn-event-threshold` — see [Memory Footprint](query-and-recovery.md#memory-footprint)). Full-table `_snapshot` buffers the reconstructed table in memory and is bounded by the same row cap as `_flashback` (see the next limitation).
- **Full-table reconstruction is buffered, not streamed.** The MVP buffers up to 100,000 rows per query and surfaces overflow as `ER_TOO_BIG_SELECT` (1104). A streaming wire-protocol path (no row cap) is deferred until an operator reports the cap as a real bottleneck. PK-filtered point-lookups are unaffected.
- **`_snapshot` refuses across a TRUNCATE/DROP/RENAME.** `TRUNCATE TABLE`/`DROP TABLE`/`RENAME TABLE` emit no row events, so a baseline merge spanning one of these statements would silently resurrect pre-DDL rows as if they still existed at AS OF. Both the single-row and full-table `_snapshot` paths check `schema_changes` for such a statement between the baseline snapshot and AS OF and return `ER_UNKNOWN_ERROR` (1105) naming the DDL type and timestamp instead ([#764](https://github.com/dbtrail/dbtrail/issues/764)); re-baseline the table after the DDL to resume. `_flashback` is unaffected — it never reads a baseline.
- **No JOINs, aggregations, or non-PK WHERE filters inside the shim.** Run them outside on the resultset (`duckdb`, `pandas`, `awk`). The shim's job is to deliver correct historical row state; SQL execution against that state is the operator's tool of choice.
- **ENUM/SET labels are decoded with the snapshot in effect at each event.** Binlog row images store ENUMs as ordinals and SETs as bitmasks; the shim (and the console Time-travel / `bintrail reconstruct` surfaces) map them back to labels using the schema snapshot whose capture time most recently precedes the event — so an enum reshaped between two events renders each event under its own definition. Remaining caveats: events older than the *first* snapshot decode with that first snapshot, and a change made between an ALTER and the next snapshot decodes with the pre-ALTER definition (stream mode auto-snapshots on DDL, so that window is normally seconds). An ordinal beyond the selected definition is returned as the raw number — the forensic ground truth, also visible in `bintrail query`'s JSON output, which is deliberately left unmapped.
- **ProxySQL itself is not provisioned by dbtrail.** `bintrail proxysql-config` only writes routing rules; you install and harden ProxySQL itself (admin password, frontend TLS, monitoring) using the standard ProxySQL docs.
- **The bare `AS OF` rule (990006) has a small residual false-positive surface.** The rule is end-anchored — only statements that *finish* with `AS OF '<text>'` route to the shim, so `AS OF` inside a string literal mid-query stays on passthrough (covered by an e2e guard test against real ProxySQL). The irreducible residue: a benign statement whose **final token** is a string literal of the exact form `AS OF '<text>'` would route to the shim and fail (the shim has no passthrough). If you hit that in practice, parenthesise or reorder the predicate — or delete rule 990006 from `mysql_query_rules` and use the `_flashback.`/hint forms instead. Note ProxySQL's `$` anchor assumes the default `re_modifiers` (CASELESS, no multiline); adding `GLOBAL`/multiline modifiers to the rule weakens the anchor to end-of-line.
- **The bare `AS OF` form is `*`-only and trailing-only.** Column lists stay on the `_flashback`/`_snapshot` virtual schemas, and the AS OF clause must end the statement (an AS-OF-before-WHERE variant would forfeit the end anchor — the false-positive defense above). The bare form rewrites to `_flashback` (binlog-only); for baseline-aware lookups use `_snapshot`.
