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
- **GTID resume** — restart and bintrail re-reads the saved MariaDB GTID set and
  continues where it left off.
- **File-based `bintrail index`** over MariaDB binlog files.
- All row events (INSERT/UPDATE/DELETE) with before/after images, including
  `UNSIGNED`, `DECIMAL`, and DDL detection / auto-snapshot.
- MariaDB-only binlog events (`Annotate_rows`, `Gtid_list`, `Binlog_checkpoint`)
  are skipped transparently.

---

## Alpha limitations

- **Prefer position mode for resume.** Both position and GTID capture work, but
  MariaDB **GTID gap detection** — the alarm that fires when a resume would skip
  past binlogs the source has already purged — is **not yet implemented**. A
  GTID-mode resume past purged binlogs proceeds with a loud warning instead of
  the data-loss alarm. **Position mode keeps full gap detection** and is the
  recommended resume mode for MariaDB. If you want a hard stop on unverifiable
  gaps, pass `--no-gap-fill`: in MariaDB GTID mode it **refuses to start** rather
  than proceeding blind.
- **The source flavor is fixed per checkpoint.** Resuming a saved MariaDB
  checkpoint requires the same `--source-flavor mariadb`. A mismatch is rejected
  with an actionable error; use `--reset` to start fresh.
- **Compressed row events are skipped.** With `log_bin_compress = ON`, MariaDB's
  compressed row events are not yet decoded — bintrail logs a loud warning
  (`rows_skipped`) rather than indexing them. Leave `log_bin_compress = OFF` on
  the source for now.
- **Multi-domain GTID is untested.** Capture works, but resume/gap behavior
  under multiple GTID domains has not been validated — prefer position mode.
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
| `--no-gap-fill is set but GTID gap detection is unavailable for a MariaDB source` | Expected guard. Resume in **position mode**, or drop `--no-gap-fill`. |
| `auto-discover binlog position` errors on an old MariaDB | Ensure `log_bin = ON` and the source user has `REPLICATION CLIENT`. |

---

## See also

- [Streaming](streaming.md) — the full `bintrail stream` reference (TLS, gap
  detection, RDS gotchas, metrics) — all of it applies to a MariaDB source.
- [DBA guide](guide.md) — day-to-day recovery scenarios.
- [Query & Recovery](query-and-recovery.md) — querying history and generating
  reversal SQL (flavor-agnostic — same for MariaDB and MySQL sources).
