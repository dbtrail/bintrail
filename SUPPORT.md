# Support scope

This document is the canonical statement of what the bintrail project does
and does not support. Issue triage links here; reports outside this scope
are closed with a pointer to this file.

## The contract

Bintrail's contract with your infrastructure is one line:

> **A reachable MySQL 8.0+ via `--index-dsn`.**

Bintrail installs and versions **its own schema** on that server — databases
(`CREATE DATABASE IF NOT EXISTS`), tables, and idempotent migrations, via
`init`, `up`, and the console control plane. It never installs, embeds,
tunes, supervises, or operates a MySQL **server process**. That boundary is
architectural and permanent.

## In scope (we own this)

- The bintrail binaries and their commands, flags, and documented behavior.
- The index **schema**: its tables, migrations, and data correctness
  (every row event, full before/after images).
- Bintrail's own tooling for operating the index *data*: `rotate`, `status`,
  `doctor`, `archive reconcile`, Parquet archives and their queries.
- The web console, the MCP server, the time-travel shim.
- The Docker images we publish and the bundled docker-compose **as an
  evaluation/quickstart environment** (see below).

## Out of scope (the operator owns this)

The **operation of the index MySQL server** is the operator's responsibility
in the free core — all of it:

- Sizing, InnoDB tuning, and capacity planning (see
  [deployment.md §3](docs/deployment.md)).
- Backups and restore of the index server, and replication of the index
  itself.
- Disk-full conditions, corruption, and crash recovery of the server.
- MySQL version upgrades, and distribution/managed-flavor quirks
  (RDS, Aurora, Cloud SQL, MariaDB) of the **index** server.

"Supported" means: MySQL 8.0+ reached via a DSN, tested against the pinned
evaluation image and the versions our CI runs. The operator's index server
is not part of our defect matrix.

> Want the index hosted, sized, backed up, and operated for you? That is
> exactly what [dbtrail](https://dbtrail.com) is.

## The bundled compose MySQL is evaluation-grade

The `index-mysql` container in the root `docker-compose.yml` exists so the
four-line quickstart works with zero prerequisites. It is **not** a
production system of record:

- Single unreplicated volume, default credentials, no backup story.
- **Volume loss = re-index.** Nothing in it is recoverable by us.
- For production, bring your own index MySQL (`INDEX_DSN` in `.env`) and
  operate it like the system of record it becomes.

## Reporting issues

Bugs in bintrail's binaries, schema, tooling, console, or docs: please open
an issue with reproduction steps — those are always in scope. If your report
is about the index MySQL server's own operation (disk, backups, upgrades,
corruption), see the list above first.
