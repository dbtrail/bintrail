# MariaDB as a source (alpha)

bintrail can capture from a **MariaDB** server while the index database stays
MySQL. This is an **alpha** capability: the happy path is verified end-to-end
against real MariaDB, but it has documented limitations (below) and narrower
version/topology coverage than the MySQL path. Read the limitations before
pointing it at production.

**Scope:** MariaDB is supported as a **source** (the database you capture
changes from). The **index** — where bintrail stores the indexed events — stays
**MySQL**. Pointing the index at MariaDB is not supported.

---

## Quickstart

Opt in with `--source-flavor mariadb` (or `BINTRAIL_SOURCE_FLAVOR=mariadb`). The
flag defaults to `mysql`, so every existing MySQL command is unchanged.

**Live streaming** (the common case — works against managed MariaDB too):

```bash
bintrail stream \
  --source-dsn 'dbtrail:pw@tcp(mariadb-host:3306)/' \
  --source-flavor mariadb \
  --index-dsn 'user:pw@tcp(index-host:3306)/binlog_index' \
  --server-id 200 --schemas shop
```

**File-based backfill** of on-disk MariaDB binlogs:

```bash
bintrail index \
  --binlog-dir /var/lib/mysql --files mariadb-bin.000042 \
  --source-dsn 'dbtrail:pw@tcp(mariadb-host:3306)/shop' \
  --index-dsn 'user:pw@tcp(index-host:3306)/binlog_index'
```

`query`, `recover`, and `reconstruct` then work exactly as they do for a MySQL
source — they read the index, which is flavor-agnostic.

---

## Requirements

Identical to a MySQL source (see [Streaming → The Source MySQL User](streaming.md#the-source-mysql-user)):

| Requirement | Notes |
|---|---|
| `binlog_format = ROW` | Validated at preflight; `bintrail` refuses to start otherwise. |
| `binlog_row_image = FULL` | Set it **server-wide** (`SHOW VARIABLES LIKE 'binlog_row_image';`). MariaDB defaults to `FULL`, but verify. |
| `log_bin = ON` | Binary logging must be enabled. |
| Source user grants | `REPLICATION SLAVE, REPLICATION CLIENT, SELECT` — the same set as MySQL. |

> MariaDB does not have a `server_uuid` system variable. You will see a benign
> `WARN … Unknown system variable 'server_uuid'` at startup — bintrail proceeds
> without a `bintrail_id`. This is expected on MariaDB and not an error.

---

## Version support

| Version | Status |
|---|---|
| **MariaDB 11.4** | **Tested** in CI (the primary target). |
| MariaDB 10.6 LTS – 11.3 | Expected to work; **not yet covered by CI**. |
| MariaDB < 10.6 | Not supported. |

---

## What works

- **Live capture** in both **position mode** and **GTID mode** (MariaDB
  `domain-server-seq` GTIDs, e.g. `0-1-100`).
- **GTID resume with gap detection** — restart and bintrail re-reads the saved
  MariaDB GTID set and continues where it left off. On resume it verifies the
  source still retains the binlogs needed: MariaDB has no `@@gtid_purged`, so the
  purge floor is derived from `BINLOG_GTID_POS` over the oldest surviving binlog.
  A purged-binlog gap raises the data-loss alarm (or, with `--no-gap-fill`,
  refuses to start) in **both position and GTID mode**, and multi-domain GTID
  sets are compared per domain.
- **File-based `bintrail index`** over MariaDB binlog files.
- All row events (INSERT/UPDATE/DELETE) with before/after images, including
  `UNSIGNED`, `DECIMAL`, and DDL detection / auto-snapshot.
- MariaDB-only binlog events (`Annotate_rows`, `Gtid_list`, `Binlog_checkpoint`)
  are skipped transparently.

---

## Alpha limitations

- **The source flavor is fixed per checkpoint.** Resuming a saved MariaDB
  checkpoint requires the same `--source-flavor mariadb`. A mismatch is rejected
  with an actionable error; use `--reset` to start fresh.
- **Compressed row events are skipped.** With `log_bin_compress = ON`, MariaDB's
  compressed row events are not yet decoded — bintrail logs a loud warning
  (`rows_skipped`) rather than indexing them. Leave `log_bin_compress = OFF` on
  the source for now.
- **Mid-capture primary failover is untested.** Gap detection compares GTID
  sequences per domain (correct for single-server and multi-domain topologies),
  but a primary failover that changes the `server_id` *within* a domain mid-stream
  has not been validated against a live multi-server MariaDB cluster.
- **Not wired into the console / control plane.** The console "+ Add server"
  form and `bintrail-console watch` do not yet pass the MariaDB flavor — use the
  `bintrail stream` CLI for MariaDB sources in this release.
- **No BYOS agent support.** `bintrail agent` does not yet support MariaDB.
- **Index-on-MariaDB is out of scope** — the index database stays MySQL.

---

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `WARN … Unknown system variable 'server_uuid'` | Expected — MariaDB has no `server_uuid`. Benign; bintrail proceeds without a `bintrail_id`. |
| `invalid Mysql GTID` when starting against MariaDB | You omitted `--source-flavor mariadb` — the MariaDB GTID set was parsed as a MySQL set. Add the flag. |
| `WARN source flavor mismatch: configured … detected …` | `--source-flavor` doesn't match the server's actual flavor. Set it to match. |
| `saved checkpoint is source flavor "mariadb" but "mysql" was requested` | You resumed a MariaDB checkpoint without `--source-flavor mariadb`. Add the flag, or `--reset` to start fresh. |
| `MariaDB GTID gap detected but CANNOT be filled` | The source purged binlogs your checkpoint still needed. bintrail auto-advances past the lost range and records the data loss durably; pass `--no-gap-fill` to refuse to start instead. Raise `binlog_expire_logs_seconds` to give bintrail more time to resume. |
| `auto-discover binlog position` errors on an old MariaDB | Ensure `log_bin = ON` and the source user has `REPLICATION CLIENT`. |

---

## See also

- [Streaming](streaming.md) — the full `bintrail stream` reference (TLS, gap
  detection, RDS gotchas, metrics) — all of it applies to a MariaDB source.
- [DBA guide](guide.md) — day-to-day recovery scenarios.
- [Query & Recovery](query-and-recovery.md) — querying history and generating
  reversal SQL (flavor-agnostic — same for MariaDB and MySQL sources).
