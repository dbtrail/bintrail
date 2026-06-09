---
title: "From 5 commands to 2: bringing the bintrail onboarding wall down"
slug: from-5-commands-to-2
date: 2026-05-28
tags: [mysql, bintrail, dba, dx]
description: The old bintrail quickstart had a 5-command wall. Most onboarding failures happened at step 3, where the source MySQL wasn't configured the way bintrail expects and the error message gave you a debugging job instead of a fix. Two new commands collapse that wall — `bintrail doctor` and `bintrail up`.
---

The old bintrail quickstart looked like this:

```sh
bintrail init --index-dsn ...
bintrail snapshot --source-dsn ... --index-dsn ...
bintrail index --binlog-dir /var/lib/mysql --source-dsn ... --index-dsn ... --all
bintrail query ...
bintrail recover ...
```

Five commands. Most people made it through the first two fine. Step three was where they hit a wall — and the wall was almost never about the `index` command itself. It was about the source MySQL: `binlog_format` was `MIXED` instead of `ROW`, or `binlog_row_image` was `MINIMAL`, or the connecting user didn't have `REPLICATION SLAVE`, or `log_bin` was off entirely, or the user was on RDS with no binlog file path because `bintrail index` doesn't work there (you want `bintrail stream`).

Bintrail's response to all of these was a single-line error. Technically correct, operationally hostile: it told you what was wrong but not what to do about it.

I shipped two commands this week to collapse that wall: `bintrail doctor` and `bintrail up`.

## `bintrail doctor` — preflight that talks back

Run it before anything else:

```sh
bintrail doctor \
  --source-dsn "user:pass@tcp(source:3306)/" \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index"
```

If your source MySQL is missing something, you don't get a one-line death message. You get the GRANT statement to copy-paste:

```
✗ REPLICATION SLAVE + CLIENT grants (missing: REPLICATION SLAVE)
    Run on the source MySQL as a privileged user (e.g. root):

      GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'bintrail'@'%';
      FLUSH PRIVILEGES;

    REPLICATION SLAVE lets bintrail stream binlog events.
    REPLICATION CLIENT lets it run SHOW BINARY LOGS / SHOW MASTER STATUS for gap detection.
```

Or the SET GLOBAL statement, or the my.cnf snippet, or the `CREATE DATABASE` clause — whichever applies. Eight checks today, all in the same shape: name, status, detail, *remediation*. The remediation is the part that turns a debugging task into a copy-paste.

Checks covered in this first cut:

1. Source MySQL connection (with a hint about RDS security groups when ports are unreachable)
2. `log_bin` enabled
3. `binlog_format = ROW`
4. `binlog_row_image = FULL`
5. Binlog retention ≥ 2 days (warns on shorter retention because gap-fill needs the window)
6. `REPLICATION SLAVE` + `REPLICATION CLIENT` grants
7. No FK CASCADE constraints (warn, not fail — reversal SQL gets messy across cascades)
8. Schema visibility from `information_schema`
9. Index DB write access (if `--index-dsn` is provided)

Two output modes: human (`--format text`, the default) and machine (`--format json`). The JSON mode is meant for CI: drop `bintrail doctor` into a workflow and it becomes a self-service "is our staging MySQL still configured correctly" gate.

Exit code is zero only when nothing failed. Warnings don't fail the run, so `doctor` is safe to run unattended.

## `bintrail up` — one command, three phases

Once doctor is green, the second wall used to be: now do init, then snapshot, then stream, in that order, with both DSNs each time, and oh by the way pick a unique `--server-id` that doesn't collide with your existing replicas.

That's now:

```sh
bintrail up \
  --source-dsn "user:pass@tcp(source:3306)/" \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index"
```

Three things happen, in order:

1. **Preflight** — runs the same checks as `bintrail doctor`. If any required check fails, `up` refuses to start. You can override with `--skip-doctor` for the cases where you've already verified externally.
2. **Init** — creates the index tables if they don't exist. Idempotent: if they already exist, nothing happens.
3. **Stream** — starts the replication stream. Auto-snapshots the source schema if no snapshot exists yet (this was already a feature inside `stream`, just not surfaced in the workflow). Resumes from the last checkpoint on restart.

The `--server-id` problem: it's required by MySQL replication, but most users don't have a free integer lying around. `bintrail up` derives one deterministically from your source DSN (SHA-256 of host:port:user, mapped into the 100M-4.2B range where production replicas almost never live). Same DSN → same ID every time → clean resume on restart. You can still pass `--server-id` explicitly when you need to.

## What this didn't fix

Honest about the rest of the friction surface so you don't think this is the whole story:

- **Distribution.** `go install` still requires a Go toolchain. There's no published Docker image yet, no Homebrew tap, no `.deb`/`.rpm`. GoReleaser is wired up but not publishing — that's the next batch.
- **Index database provisioning.** You still need a separate MySQL for the index. For evaluation, an "appliance" Docker image that ships MySQL + bintrail in one container would collapse this. Not built yet.
- **The schema of the source itself.** `bintrail` cares about your tables having primary keys, no FK cascades, and binlog-friendly column types. Doctor warns on FK cascades; the rest is still a manual review.
- **K8s deployment.** No Helm chart yet. The systemd units in `deploy/` cover bare-metal but the operator/sidecar story is open.

## Why this matters

The 5-command quickstart was the first thing every prospective user saw. The friction wasn't the count of commands — it was that the failures all happened at step 3, after they'd already invested time on steps 1 and 2, and the error message gave them homework instead of a fix.

Collapsing it to 2 commands (doctor, up) does two things at once:
1. The first command's whole job is *to tell you what's wrong with your environment* and how to fix it. If your environment is ready, you spent 200ms and got a green checkmark. If it isn't, you got copy-paste remediation. There is no "I'll come back to it" outcome.
2. The second command makes setup idempotent. Re-running `bintrail up` is always safe — re-runs skip work that's already done and resume the stream from its checkpoint. That removes a class of "did I run the right command in the right order" anxiety.

If you've onboarded bintrail before and hit any of the gotchas listed above, I'd love to hear which one cost you the most time. The `doctor` check list is going to grow, and the right next checks are the ones that match real onboarding failures rather than my theoretical model of them.

## What's next on the friction front

Roughly in order of impact-to-effort ratio:

1. **Published Docker image** (`ghcr.io/dbtrail/bintrail`) signed with cosign, with SBOM. Removes the Go-toolchain dependency for the most common path.
2. **Homebrew tap** + GoReleaser deb/rpm targets. Same dependency removal, different audiences.
3. **Appliance image** — single container with bintrail + MySQL index preconfigured, for evaluation/demo. `docker run dbtrail/bintrail-appliance` and you have time-travel SQL.
4. **Helm chart for sidecar deployment** under Percona Operator and the official MySQL Operator. Makes "Bintrail = a feature of my operator-managed MySQL" a true statement.
5. **PMM dashboard** that consumes bintrail's Prometheus metrics and surfaces coverage, gaps, retention horizon, and recovery readiness as panels DBAs already look at.

If you want to follow along, the work is happening in [github.com/dbtrail/dbtrail](https://github.com/dbtrail/dbtrail). The shorter the quickstart, the better the project — and I think there's still room to halve it again.
