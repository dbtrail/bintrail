# Time-Travel SQL Setup

This walkthrough takes you from zero to running a working time-travel query against your MySQL:

```sql
SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00' WHERE id = 12345;
```

The query is answered by `bintrail shim`, an in-process MySQL-protocol server (a subcommand of the `bintrail` binary) that intercepts the virtual `_flashback`, `_diff`, and `_snapshot` schemas and resolves them against your bintrail index plus any rotated archives (local directory or `s3://` prefix). ProxySQL sits in front of both your real MySQL and the shim, routing each query to the right backend. The setup is the same whether you populate the index with `bintrail stream` (hosted) or `bintrail agent` (BYOS) — the shim only cares that the index exists and `archive_state` is current.

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

The whole walkthrough takes about 10 minutes on a fresh Ubuntu 22.04 or Amazon Linux 2023 host that already has a populated bintrail index.

---

## Prerequisites

Before starting, you need:

- **A populated bintrail index.** Some process is keeping `binlog_events` current — typically `bintrail stream` (hosted) or `bintrail agent` (BYOS). If rotated hours have been archived, `archive_state` points at the local directory or `s3://` prefix where the Parquet files live. If you haven't set any of this up yet, see [`docs/streaming.md`](streaming.md) and [`docs/rotation-and-status.md`](rotation-and-status.md).
- **A `.bintrail.env` file** with `BINTRAIL_SOURCE_DSN`, `BINTRAIL_INDEX_DSN`, and `BINTRAIL_SERVER_ID` set. `bintrail config init` scaffolds one.
- **The `bintrail` binary** on the host. The shim is a subcommand — there is no second binary to download.
- **Root or `sudo` access** on the host.
- **The `mysql` client** installed on the host (used to apply ProxySQL config below).
- **A MySQL user your application will use to connect through ProxySQL.** This is *not* the replication user the streamer uses — it's a regular application user that ProxySQL authenticates against. Pick a username and a strong password; you'll need both below.

---

## Step 1 — Generate `shim.yaml`

`bintrail init-shim` scaffolds the file from your existing `.bintrail.env`:

```sh
cd /etc/bintrail   # or wherever your .bintrail.env lives
sudo bintrail init-shim --out shim.yaml
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
sudo apt-get install -y wget lsb-release gnupg
wget -qO- https://repo.proxysql.com/ProxySQL/repo_pub_key | sudo apt-key add -
echo "deb https://repo.proxysql.com/ProxySQL/proxysql-2.6.x/$(lsb_release -sc)/ ./" \
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
sudo bintrail proxysql-config --out proxysql-setup.sql
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

The script wraps its DML in `BEGIN`/`COMMIT` and finishes with `LOAD ... TO RUNTIME` and `SAVE ... TO DISK`, so the new routing is live immediately and survives a ProxySQL restart. **Re-running the script is safe** — it scopes its DELETEs to bintrail-owned hostgroups (990, 991) and rule IDs (990001-990003), so it never touches operator-managed config.

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
Documentation=https://github.com/dbtrail/bintrail/blob/main/docs/time-travel-sql.md
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

> A copy of this unit ships at `deploy/bintrail-shim.service` in the bintrail repo.

The unit reads `BINTRAIL_INDEX_DSN` from `/etc/bintrail/.bintrail.env` (the same file your agent uses) so the shim can answer queries against your index. The DSN must include the index database name (e.g. `…/bintrail_index`) — the shim refuses to start otherwise. Append `--allow-gaps` to `ExecStart` to warn-and-continue on archive failures or coverage gaps instead of returning a MySQL error to the client; the default is strict because the wire protocol has no warning channel.

**Auth method on MySQL 8.4+.** If your MySQL has `mysql_native_password` disabled (the default since 8.4), append `--auth-method=caching_sha2_password` to `ExecStart` (or `Environment=BINTRAIL_AUTH_METHOD=caching_sha2_password`):

```ini
ExecStart=/usr/local/bin/bintrail shim --shim-config /etc/bintrail/shim.yaml --auth-method=caching_sha2_password
```

Requires ProxySQL **2.7+** between the application and the shim — the LTS 2.6 line isn't verified to negotiate SHA2 against backends, so operators on 2.6 keep the default (`mysql_native_password`). The application user used by ProxySQL must match the chosen scheme: `IDENTIFIED WITH mysql_native_password BY '<password>'` for the default path, `IDENTIFIED WITH caching_sha2_password BY '<password>'` for the opt-in. `sha256_password` is also accepted by `--auth-method` if your environment requires it.

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

Four statement shapes are recognised:

```sql
-- Row state at a point in time (point-lookup, fast):
SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00' WHERE id = 12345;

-- Full-table reconstruction at AS OF (no WHERE) — every row that
-- existed at that instant. Same shape works against _snapshot:
SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00';
SELECT * FROM _snapshot.orders  AS OF '2026-05-02 10:00:00';

-- All events for one row in a time window:
SELECT * FROM _diff.orders BETWEEN '2026-05-01' AND '2026-05-02' WHERE id = 12345;
```

`_diff` returns the full per-PK event history within the requested window — there is no implicit row cap. If a single hot row produced thousands of events, you'll get all of them in one response; if that's too much for one query, narrow the `BETWEEN` range.

Full-table `_flashback` / `_snapshot` queries are buffered and capped at 100,000 rows; exceeding the cap surfaces as `ER_TOO_BIG_SELECT` (code 1104) with a hint to narrow the AS OF range or add a PK filter. DELETE events are correctly suppressed — rows that did not exist at the AS OF instant don't appear in the resultset (same semantic as Oracle's `AS OF`). For ad-hoc filtering, joins, or aggregations, pipe the resultset to `duckdb`, `pandas`, or any tool that consumes a `SELECT *` stream — the shim deliberately stays a forensic point-lookup + full-table tool, not a SQL planner.

The shim resolves the row by replaying the relevant binlog events from your bintrail MySQL index. If the timestamp falls outside the index's retention (because hourly partitions have been rotated to S3), the shim auto-discovers the Parquet archives via `archive_state` and merges results from both sources — same machinery `bintrail query` and `bintrail recover` already use.

---

## Troubleshooting

### `ERROR 1045: Access denied for user 'app_user'@'…'`

ProxySQL is rejecting your credentials. Confirm your app is connecting with the cleartext value of `mysql_password` from `shim.yaml`. If `shim.yaml` was edited, re-apply the ProxySQL config so the regenerated SHA1 reaches the live `mysql_users` table:

```sh
sudo rm proxysql-setup.sql
sudo bintrail proxysql-config --out proxysql-setup.sql
mysql -u admin -p -h 127.0.0.1 -P 6032 < proxysql-setup.sql
```

If the username comes through but the connection still fails, check `bintrail shim`'s log: it logs which usernames are in the allowlist at startup, and a connection from an unknown username is rejected by `TenantAuth.CheckUsername`.

### `_flashback.t doesn't exist` (or query goes to MySQL instead of the shim)

The query rule isn't matching. Inspect the routing:

```sh
mysql -u admin -p -h 127.0.0.1 -P 6032 \
  -e "SELECT rule_id, match_pattern, destination_hostgroup FROM runtime_mysql_query_rules WHERE rule_id BETWEEN 990001 AND 990003;"
```

You should see three rows targeting hostgroup 991. If they're missing, re-apply `proxysql-setup.sql`. If they're present but the query still goes to MySQL, double-check that no operator rule with a smaller `rule_id` is intercepting `_flashback.*` first (ProxySQL evaluates rules in `rule_id` order).

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
- **1526 `ER_NO_PARTITION_FOR_GIVEN_VALUE`** — the AS OF or BETWEEN range falls outside what this index retains (rotated out of MySQL with no archive coverage). Operators narrow the time range or check `archive_state` and the shim's `--allow-gaps` flag.
- **1045 `ER_ACCESS_DENIED_ERROR`** — credential mismatch (see the section above).
- **1104 `ER_TOO_BIG_SELECT`** — full-table `_flashback` / `_snapshot` returned more than 100,000 rows. Narrow the AS OF or add a `WHERE <pk> = <value>` to fall back to the point-lookup path.
- **1105 `ER_UNKNOWN_ERROR`** — real internal failure (DB timeout, archive S3 outage, build-resultset bug). This is the catch-all "the server is broken, retry" signal; persistent 1105s warrant inspecting the shim log.

### Time-travel query returns empty

Three causes produce an empty `_flashback` / `_snapshot` resultset:

1. The row had no event at-or-before the requested timestamp.
2. The latest event at-or-before the timestamp is a DELETE — the row did not exist at AS OF (Oracle `AS OF` semantic; matches the full-table path).
3. A coverage gap or archive-fetch failure under `--allow-gaps`. Without that flag the shim returns a typed MySQL error instead — `ER_NO_PARTITION_FOR_GIVEN_VALUE` (1526) for coverage gaps, `ER_UNKNOWN_ERROR` (1105) for archive-fetch failures — so on the default strict configuration the empty resultset never indicates a gap or an archive outage.

To distinguish cases 1 and 2, query `_diff` for the per-PK history: it returns every event (including the DELETE's `row_before`), so a row that was deleted produces at least one row in the diff resultset while a row that never existed produces zero. Or check the agent is keeping up:

```sh
journalctl -u bintrail-agent -n 200
```

The bintrail index retains the most recent hours via partition rotation; older data is in S3 (auto-discovered via `archive_state`). See [`docs/storage.md`](storage.md) for the buffer query priority and S3 flush cadence.

### Operator already has users in hostgroup 990

`bintrail proxysql-config` scopes its DELETE to `mysql_users WHERE default_hostgroup = 990` — any pre-existing user in that hostgroup will be removed when the script is applied. If you have application users you want to keep separate from bintrail-managed routing, place them in a different hostgroup before running the script. Hostgroup 990 is reserved for bintrail; see the comment header at the top of the generated `proxysql-setup.sql` for the full list of resources the script manages.

---

## Limitations

- **Single source MySQL per shim.** The current `bintrail shim` is one-tenant-per-instance. If you have multiple source MySQLs you want time-travel SQL against, run one shim per instance with separate listen ports and separate ProxySQL hostgroups.
- **No TLS termination on the shim port.** `bintrail shim` accepts plain MySQL protocol on `127.0.0.1:3308` by default. If you need TLS between ProxySQL and the shim, terminate at ProxySQL or via an `stunnel` sidecar.
- **`_snapshot` is currently a synonym for `_flashback`.** The shim reserves the `_snapshot.*` virtual schema for a future baseline-lookup integration (the `bintrail dump` / `bintrail baseline` pipeline) so it can answer for rows that have never appeared in binlog events. Today both schemas resolve to "row state at the most recent event at-or-before the given timestamp".
- **Full-table reconstruction is buffered, not streamed.** The MVP buffers up to 100,000 rows per query and surfaces overflow as `ER_TOO_BIG_SELECT` (1104). A streaming wire-protocol path (no row cap) is deferred until an operator reports the cap as a real bottleneck. PK-filtered point-lookups are unaffected.
- **No JOINs, aggregations, or non-PK WHERE filters inside the shim.** Run them outside on the resultset (`duckdb`, `pandas`, `awk`). The shim's job is to deliver correct historical row state; SQL execution against that state is the operator's tool of choice.
- **ProxySQL itself is not provisioned by bintrail.** `bintrail proxysql-config` only writes routing rules; you install and harden ProxySQL itself (admin password, frontend TLS, monitoring) using the standard ProxySQL docs.
