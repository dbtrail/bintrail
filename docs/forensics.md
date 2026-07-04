# Forensics — who changed this?

dbtrail's index always tells you **what** changed: every row, with its full
before and after images. Forensics answers the next question: **who** did it —
which database user, from which host, with which client program — and, when
the source is configured for it, the exact SQL statement.

```console
$ bintrail who-changed --index-dsn "$IDX" --source-dsn "$SRC" \
    --schema shop --table orders --pk 42
```

Each matching change comes back with a name attached — or an honest
"unknown", together with the reason and the queries you could run yourself.
dbtrail never guesses silently: every answer is labeled with where it came
from and how much you can trust it.

## The one fact that explains everything on this page

**The binlog records a connection *number*, not a name.** MySQL writes which
connection made each change (`connection_id`), but not who was behind it.
Everything forensics does is turn that number back into a name, using the best
evidence available *at the moment you ask*. How good that evidence is depends
almost entirely on one choice: **whether an audit plugin is writing a log on
your source server.**

## What to expect — with and without an audit plugin

Three realistic setups:

- **A — plain capture.** You run `bintrail stream` (or `bintrail agent`), no
  audit plugin.
- **B — capture + identity cache.** You run `bintrail up` or
  `bintrail-console watch`, no audit plugin. These daemons also poll the
  source's live session list twice a second and save what they see into the
  index, so a name can outlive its session. (Plain `stream`/`agent` do not
  start this poller today — tracked in
  [#751](https://github.com/dbtrail/dbtrail/issues/751).)
- **C — audit plugin installed.** Any supported audit plugin writes a log on
  the source (see [below](#with-an-audit-plugin-the-full-answer)).

| Question | A: plain | B: + identity cache | C: + audit plugin |
|---|---|---|---|
| Who changed this row, if the session is **still connected**? | ✅ name (corroborated) | ✅ name (corroborated) | ✅ name (**exact**) |
| Who changed it, if the session **already disconnected**? | ❌ unknown | ✅ usually — if the poller saw the session while it lived (corroborated) | ✅ name (**exact**) |
| Who changed it, **days ago / while nothing was running**? | ❌ | ❌ beyond the cache retention (default 24 h) | ✅ within the audit log's retention |
| Can dbtrail **prove** that session was open at that moment? | ❌ | ❌ | ✅ — the log's CONNECT/DISCONNECT records bracket each session |
| Very short sessions (connect → write → disconnect in < 0.5 s)? | ❌ | ⚠️ can slip between two polls | ✅ |
| Which **SQL statement** made the change? | ✅ if `binlog_rows_query_log_events=ON` — independent of all of this, see [below](#the-statement-itself-query_text) | same | same, plus the audit log's own copy |

The honest summary: **without an audit plugin, dbtrail can only name sessions
it (or MySQL) was watching at the right moment. With one, it can name sessions
after the fact and prove the match.** If "who did this?" matters to you
operationally, install the audit plugin — `bintrail doctor` and the console's
Forensics page tell you exactly how for your server flavor.

## How to read the confidence labels

Every attribution carries a label. In plain terms:

- **exact** — dbtrail can prove it: the audit log shows that connection
  belonged to this user *and was open when the change happened*. Id-number
  reuse cannot fool this.
- **corroborated** — the name matches the connection number, but dbtrail
  cannot prove that session was open at the event's moment. Usually right;
  connection numbers are reused (especially after a server restart), so treat
  it as strong evidence, not proof.
- **heuristic** — more than one candidate matched (e.g. two sessions on the
  same number within the same second) and dbtrail picked the most likely one.
  It says so instead of pretending certainty.

Answers also carry **notes** — plain-language caveats that apply to your
result (a truncated audit read, an unreachable source, a coverage gap). They
are part of the answer, not log noise: if a note says a source was NOT
consulted, don't read the result as "that source had nothing".

## Without an audit plugin: what performance_schema can and cannot do

`performance_schema` is MySQL's built-in live view of the server. It is the
best free evidence source, and it has three hard limits that no tool can work
around — they shape everything in column A/B above:

1. **It forgets a session the instant it disconnects.** The user, host, and
   client program of a disconnected session are simply gone. That is why the
   `up`/`watch` daemons poll it twice a second and save what they see (the
   *identity cache*, kept for `--attribution-retention`, default 24 h): so a
   name survives its session. A session shorter than the gap between two
   polls can still be missed.
2. **Its statement history is a short ring buffer with no clock.** By default
   the server keeps roughly the last 10,000 statements *server-wide*, and the
   entries carry no wall-clock timestamps — on a busy server that is seconds
   to minutes of history, and it cannot be filtered by time of day. This is
   why `bintrail user-activity` shows *recent* statements and quietly cannot
   honor "between 14:00 and 15:00" on the live path, and why the durable
   what-happened answers always come from dbtrail's own index instead.
3. **It cannot be switched on after the fact.** `performance_schema=ON/OFF`
   is fixed at server startup (on RDS/Aurora: parameter group + reboot). It is
   ON by default on MySQL 8.0+, but OFF by default on MariaDB.

dbtrail's role here is deliberately modest: it reads performance_schema
(read-only, never changes your server), caches identities while it runs, and
tells you honestly when the evidence ran out.

## With an audit plugin: the full answer

An audit plugin writes every connection (and optionally every statement) to a
log file on the source. That file is what performance_schema can never be — a
**durable record**. With it, dbtrail:

- names sessions long after they disconnected;
- **brackets** each connection number with its CONNECT..DISCONNECT records, so
  an event is only attributed to an identity whose session actually contained
  it — this is what earns the `exact` label and defeats id-number reuse;
- keeps working across dbtrail restarts (the evidence lives on your server,
  not in dbtrail's memory).

Supported families:

| Server | Plugin | Notes |
|---|---|---|
| Percona Server | `audit_log` | free; JSON/CSV/XML formats. The newer `audit_log_filter` plugin is detected but not yet readable — [#747](https://github.com/dbtrail/dbtrail/issues/747) |
| MariaDB | `server_audit` | free; also the dialect used by RDS MySQL/MariaDB |
| RDS / Aurora MySQL | MariaDB Audit Plugin (via option group / advanced auditing) | dbtrail reads the logs through the AWS API — needs `rds:DescribeDBLogFiles` + `rds:DownloadDBLogFilePortion` on the host's IAM role. (A CloudWatch Logs reader exists in the library but is not used by `who-changed` today.) |
| MySQL Community | none built in | MySQL Enterprise Audit exists (commercial); Percona Server is a free drop-in alternative |

Run `bintrail doctor --source-dsn "$SRC"` — it detects what you have and
prints copy-pasteable setup SQL / my.cnf snippets per flavor. The console's
**Forensics** page shows the same setup guide. dbtrail only ever *reads*: it
never installs plugins or changes settings on your server.

Two honest caveats:

- **Retention is yours to manage.** dbtrail can never attribute an event whose
  audit records were already rotated away or pruned — on RDS/Aurora instance
  storage that can be a matter of hours. If you need long forensic reach,
  size the audit log's rotation accordingly (or export to CloudWatch with an
  explicit retention).
- **Format coverage is uneven today.** The MariaDB/RDS/Aurora/CloudWatch path
  is well tested; several Percona and MySQL Enterprise on-disk formats
  currently parse incompletely or not at all — tracked in
  [#745](https://github.com/dbtrail/dbtrail/issues/745),
  [#746](https://github.com/dbtrail/dbtrail/issues/746),
  [#748](https://github.com/dbtrail/dbtrail/issues/748).

## The statement itself (`query_text`)

Independent of everything above, MySQL can write the originating SQL statement
into the binlog itself: set `binlog_rows_query_log_events=ON` (MariaDB:
`binlog_annotate_row_events`, on by default). dbtrail then indexes the
statement **durably** next to each row change — this is the highest-fidelity
"what", available even when no identity source can supply the "who".
`who-changed` shows it alongside the attribution. `bintrail doctor` checks
this flag too.

## What no tool can tell you

Worth being clear about, because these are limits of MySQL and of network
reality — not of dbtrail, and not fixable by any product:

- **Behind a connection pooler or proxy** (ProxySQL, RDS Proxy, app-side
  pools): the database sees the pool's backend session. The database username
  usually survives, but the *client host* is the proxy's, and with
  multiplexing many application users share one backend connection — no
  server-side evidence can split them apart. If you need per-application-user
  attribution behind a pool, it has to come from the application's own logs.
- **Older than your evidence.** No audit log ⇒ no history. Audit log rotated
  away ⇒ that window is gone. Nobody can read a deleted record.
- **From a replica's binlog**: connection numbers belong to the replica's
  replication applier, not the original client. Run `who-changed` against an
  index captured from the primary.
- **A determined insider**: a privileged session can set a fake
  `pseudo_thread_id`. The binlog connection number is corroborating evidence,
  not courtroom proof — dbtrail's answers say this in their notes.
- **PostgreSQL sources**: Postgres's logical replication stream carries no
  connection identity at all, so `who-changed` does not exist for
  `bintrail-pg` (see [PostgreSQL source](postgres.md)).

## The commands

| Command | What it answers | Needs |
|---|---|---|
| `bintrail who-changed` | "Who changed these rows?" — the main forensic command. Attributes indexed changes via audit log → live sessions → identity cache, labels each answer, explains every gap. Without `--source-dsn` only index-side evidence is used. | `--index-dsn`; `--source-dsn` recommended |
| `bintrail user-activity --user X` | "What is this user running **right now / very recently**?" — live view, short window, no time filter (see limit 2 above). | `--source-dsn` |
| `bintrail connection-history` | "Who is connected right now?" (its fallback SQL adds cumulative per-account connection totals to run yourself) | `--source-dsn` |

All accept `--format json`. (Looking for DDL history? That is served durably
from the index, not from performance_schema: `bintrail query --event-type ddl`
— see [DDL tracking](ddl-tracking.md).) The web console has the same surface on its
**Forensics** page (who-changed, the investigation queries, a capabilities check,
and the setup guide). Note: the console refuses forensics queries while an
RBAC access profile is active, because forensic output contains unredacted
SQL and session identity.

The identity cache is controlled by `--attribution-retention` on
`bintrail up` / `bintrail-console watch` (`BINTRAIL_ATTRIBUTION_RETENTION`;
`0` disables it). Cached identities are stored in the index
(`connection_cache` table) and swept hourly.

## Privileges

The user in `--source-dsn` needs `SELECT` on `performance_schema`. Reading a
local audit log needs filesystem access to the log path; reading RDS/Aurora
audit logs needs the two IAM actions listed above. As everywhere in dbtrail:
read-only — doctor and the setup guide print remediation for you to apply,
and never execute it.
