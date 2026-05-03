# BYOS Time-Travel SQL Setup

This walkthrough takes a BYOS customer from zero to running a working time-travel query against their own MySQL:

```sql
SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00' WHERE id = 12345;
```

The query is answered by `bintrail shim`, an in-process MySQL-protocol server (a subcommand of the `bintrail` binary you already have) that intercepts the virtual `_flashback`, `_diff`, and `_snapshot` schemas and resolves them against your bintrail index plus your customer-side S3 archives. ProxySQL sits in front of both your real MySQL and the shim, routing each query to the right backend.

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

The whole walkthrough takes about 10 minutes on a fresh Ubuntu 22.04 or Amazon Linux 2023 host that already runs the bintrail agent.

---

## Prerequisites

Before starting, you need:

- **A running BYOS deployment.** The `bintrail agent` is already streaming from your source MySQL into your S3 bucket and the dbtrail metadata API. If you haven't done this yet, follow [`docs/storage.md`](storage.md) (data separation) and the BYOS architecture section of [`docs/deployment.md`](deployment.md) first.
- **A `.bintrail.env` file** with `BINTRAIL_SOURCE_DSN` and `BINTRAIL_SERVER_ID` set. If you ran `bintrail config init` for your BYOS install you already have this.
- **The `bintrail` binary** on the host (the same one running the agent). The shim is a subcommand — there is no second binary to download.
- **Root or `sudo` access** on the host.
- **The `mysql` client** installed on the host (used to apply ProxySQL config and to generate the password hash below).
- **A MySQL user your application will use to connect through ProxySQL.** This is *not* the replication user the agent uses — it's a regular application user that ProxySQL authenticates against. Pick a username and a strong password; you'll need both below.

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
    # TODO: SHA1 hex of mysql_user's password
    # mysql_pass_sha1:
```

You need to fill in **`mysql_user`** and **`mysql_pass_sha1`**. The username is whatever you want your application to use. The SHA1 is in ProxySQL's `*HEX` format. The portable way to compute it (works on any OS, no MySQL version assumptions):

```sh
printf 'your-app-password' \
  | sha1sum | cut -d' ' -f1 \
  | xxd -r -p | sha1sum | cut -d' ' -f1 \
  | tr 'a-f' 'A-F' | sed 's/^/*/'
# *30D6BC64B4B66AC024BDC6551C3B28BB49320725
```

(ProxySQL stores `mysql_native_password`'s double-SHA1 with a `*` prefix.)

Edit `shim.yaml`, uncomment the two TODO lines, and paste the values:

```yaml
    mysql_user: app_user
    mysql_pass_sha1: '*30D6BC64B4B66AC024BDC6551C3B28BB49320725'
```

> **Auth note**: `bintrail shim` validates only the *username* against this file. ProxySQL holds the password gate (`mysql_pass_sha1`). The shim's listen address defaults to `127.0.0.1:3308` so it is not reachable from the network — but any process on the same host with a known tenant username can still connect, so treat host-local users as trusted. ProxySQL on the same host is the legitimate caller.

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
Description=bintrail shim - BYOS time-travel SQL backend for ProxySQL
Documentation=https://github.com/dbtrail/bintrail/blob/main/docs/byos-time-travel-sql.md
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

The unit reads `BINTRAIL_INDEX_DSN` from `/etc/bintrail/.bintrail.env` (the same file your agent uses) so the shim can answer queries against your index. Make sure that variable is set in the env file before enabling the service.

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

Change your application's MySQL connection string from the real MySQL port (`:3306`) to ProxySQL's MySQL port (`:6033`). The credentials are the `mysql_user` / `mysql_pass_sha1` pair from `shim.yaml` — the *plaintext* password you generated in step 1, not the SHA1.

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

Three statement shapes are recognised:

```sql
-- Row state at a point in time:
SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00' WHERE id = 12345;

-- Same shape, reserved for future baseline-lookup integration:
SELECT * FROM _snapshot.orders  AS OF '2026-05-02 10:00:00' WHERE id = 12345;

-- All events for one row in a time window:
SELECT * FROM _diff.orders BETWEEN '2026-05-01' AND '2026-05-02' WHERE id = 12345;
```

`_diff` returns the full per-PK event history within the requested window — there is no implicit row cap. If a single hot row produced thousands of events, you'll get all of them in one response; if that's too much for one query, narrow the `BETWEEN` range.

The shim resolves the row by replaying the relevant binlog events from your bintrail MySQL index. If the timestamp falls outside the index's retention (because hourly partitions have been rotated to S3), the shim auto-discovers the customer-side Parquet archives via `archive_state` and merges results from both sources — same machinery `bintrail query` and `bintrail recover` already use.

---

## Troubleshooting

### `ERROR 1045: Access denied for user 'app_user'@'…'`

ProxySQL is rejecting your credentials. Re-run the SHA1 recipe from step 1 against your app's password and compare against `mysql_pass_sha1` in `shim.yaml`:

```sh
printf 'your-app-password' \
  | sha1sum | cut -d' ' -f1 \
  | xxd -r -p | sha1sum | cut -d' ' -f1 \
  | tr 'a-f' 'A-F' | sed 's/^/*/'
```

Then re-apply the ProxySQL config (the `mysql_users` row is regenerated from `shim.yaml`):

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

If `bintrail-shim` is dead, `journalctl -u bintrail-shim -n 100` shows why. Common causes: missing or unreadable `shim.yaml`, missing `BINTRAIL_INDEX_DSN`, a `mysql_pass_sha1` value that's not a valid YAML string (quote it).

### Time-travel query returns empty

Either the row didn't exist at that timestamp, or the requested time falls in a gap between the live bintrail index and the S3 archive. Check the agent's recent log output:

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
- **ProxySQL itself is not provisioned by bintrail.** `bintrail proxysql-config` only writes routing rules; you install and harden ProxySQL itself (admin password, frontend TLS, monitoring) using the standard ProxySQL docs.
