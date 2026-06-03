#!/usr/bin/env python3
"""
Adaptive write throttle for MySQL via ProxySQL, driven by binlog change volume.

Trigger:   replica apply lag (Seconds_Behind_Source) on one or more read
           replicas of the same primary.
Diagnosis: which table is generating the most binlog volume right now, read
           straight from the bintrail index (the `binlog_events` table that
           `bintrail stream` populates) with a plain GROUP BY.
Action:    apply a per-rule delay in ProxySQL to the WRITE rule that matches
           the hot table.

With several replicas the diagnosis and the action stay shared: every replica
replays the same primary's binlog, so the hot table is the same, and there is
one ProxySQL in front of that primary. Only the trigger is per-replica. We
engage when the WORST replica crosses the lag threshold and release when EVERY
replica is back under it. A replica that is unreachable is logged and skipped,
never aborting the others (this assumes one primary; replicas of different
primaries would need a throttle per primary).

This script is pure open-source: it talks to MySQL/ProxySQL only. It needs a
running `bintrail stream` writing to a `binlog_events` index, but no hosted
service, API key, or network calls beyond your own database and proxy.

Targets:   ProxySQL 3.0.x (Stable tier), MySQL 8.4 LTS, Python 3.11+.
Deps:      pip install pymysql
"""

import re
import signal
import sys
import time

import pymysql


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROXYSQL = dict(host="127.0.0.1", port=6032, user="admin", password="admin")
# One entry per replica to watch. All must replicate from the SAME primary
# (the one ProxySQL fronts and bintrail indexes).
REPLICAS = [
    dict(host="mysql-replica-a", port=3306, user="monitor", password="monitor"),
    dict(host="mysql-replica-b", port=3306, user="monitor", password="monitor"),
]
# The bintrail index DB: where `bintrail stream` writes the binlog_events table.
INDEX = dict(host="127.0.0.1", port=3306, user="bintrail", password="bintrail",
             database="bintrail_index")

LAG_THRESHOLD_S = 3        # engage throttling at or above this replica lag
CLEAR_THRESHOLD_S = 1      # release throttling at or below this (hysteresis band)
VOLUME_WINDOW_S = 60       # how far back in the binlog we look for the hot table
DELAY_MS = 1               # per-query delay applied while throttling
THROTTLE_RULE_ID = 100     # dedicated, table-targeted throttle rule
POLL_INTERVAL_S = 1.0      # how often we check replica lag (cheap, local)
HOTSPOT_REFRESH_S = 10     # how often we re-query the index for the hot table


# ---------------------------------------------------------------------------
# Trigger: replica apply lag (MySQL 8.4)
# ---------------------------------------------------------------------------
def get_replica_lag(conn):
    """Return Seconds_Behind_Source as an int, or None when replication is not
    reporting a value. NULL is NOT zero: it means the applier thread is not
    running, or has drained the relay log while the receiver is stopped, i.e.
    replication may be broken. Callers must treat None as 'unknown', not 'caught
    up'."""
    with conn.cursor() as cur:
        cur.execute("SHOW REPLICA STATUS")   # 8.0.22+ / 8.4 wording
        row = cur.fetchone()
    if not row:
        return None
    val = row.get("Seconds_Behind_Source")
    return int(val) if val is not None else None


class Replica:
    """A watched replica that connects lazily. A replica that is down at startup
    (or that drops and comes back) does not stop the daemon: we (re)connect on
    demand and raise on failure so the caller can mark it broken for this tick."""

    def __init__(self, cfg):
        self.cfg = cfg
        self.label = f"{cfg['host']}:{cfg.get('port', 3306)}"
        self.conn = None

    def lag(self):
        if self.conn is None:
            self.conn = pymysql.connect(
                autocommit=True, cursorclass=pymysql.cursors.DictCursor, **self.cfg
            )
        try:
            self.conn.ping(reconnect=True)
            return get_replica_lag(self.conn)
        except Exception:
            try:
                self.conn.close()
            finally:
                self.conn = None       # force a fresh connect next tick
            raise

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None


def worst_replica_lag(replicas):
    """Read every replica's lag. Returns (worst_lag, laggard_label, broken):
    worst_lag is the highest numeric Seconds_Behind_Source seen this tick (None
    if no replica reported a number), laggard_label names that replica, and
    broken is a list of (label, reason) for replicas that returned NULL or were
    unreachable. One bad replica never aborts the others."""
    lags, broken = [], []
    for rep in replicas:
        try:
            lag = rep.lag()
        except Exception as exc:
            broken.append((rep.label, str(exc) or "unreachable"))
            continue
        if lag is None:
            broken.append((rep.label, "NULL / not applying"))
        else:
            lags.append((lag, rep.label))
    if not lags:
        return None, None, broken
    worst_lag, label = max(lags)        # the worst (highest) lag drives the decision
    return worst_lag, label, broken


# ---------------------------------------------------------------------------
# Diagnosis: hottest table by binlog volume, from the bintrail index
# ---------------------------------------------------------------------------
def top_table_by_volume(index):
    """Return (schema, table) for the table with the most binlog events in the
    last VOLUME_WINDOW_S seconds, or None if the window was genuinely empty.
    Raises on a DB error (the caller keeps the previous hot table rather than
    treating an error as 'no activity').

    The whole lookup runs against your own index. The connection is pinned to
    UTC because bintrail stores binlog event timestamps in UTC; comparing
    against UTC_TIMESTAMP() keeps the time window correct regardless of the
    index server's timezone."""
    sql = (
        "SELECT schema_name, table_name, COUNT(*) AS c "
        "FROM binlog_events "
        "WHERE event_timestamp >= UTC_TIMESTAMP() - INTERVAL %s SECOND "
        "GROUP BY schema_name, table_name "
        "ORDER BY c DESC LIMIT 1"
    )
    index.ping(reconnect=True)
    with index.cursor() as cur:
        cur.execute(sql, (VOLUME_WINDOW_S,))
        row = cur.fetchone()
    if not row:
        return None
    return row[0], row[1]


# ---------------------------------------------------------------------------
# Action: throttle / unthrottle a specific table in ProxySQL
# ---------------------------------------------------------------------------
def set_throttle(conn, schema, table, delay_ms):
    """Refresh a single dedicated throttle rule. The match anchors on a WRITE
    verb so we never delay SELECTs (reads generate no binlog and don't cause
    apply lag), and includes the schema so a same-named table in another schema
    isn't caught. The schema qualifier is optional because many apps issue
    unqualified writes against their default database - accept that unqualified
    writes to a same-named table elsewhere may also match (a digest-matching
    limitation; use match_pattern on the raw query if you need stricter).

    apply=0 lets your routing rules keep firing. ProxySQL evaluates rules by
    ascending rule_id and an apply=1 rule stops evaluation, so this rule_id must
    sort before any apply=1 routing rule (or your routing rules must use
    apply=0).

    ProxySQL supports both PCRE (default since v1.4.0) and RE2; the pattern uses
    only features both engines accept and avoids lookbehind/lookahead. We get a
    left boundary by consuming a separator char class [\\s,(] right before the
    table token, so `orders` does NOT match inside `customer_orders` (the char
    before it is `_`, not a separator). Residual limitation of digest matching:
    a column literally named like the table (e.g. `SET orders=?`) can still
    match; use match_pattern on the raw query if that bites you."""
    sch = re.escape(schema)
    tbl = re.escape(table)
    pattern = (
        rf"(?is)^\s*(?:INSERT|UPDATE|DELETE|REPLACE)\b"
        rf".*[\s,(]`?(?:{sch}`?\.`?)?{tbl}`?\b"
    )
    with conn.cursor() as cur:
        cur.execute(
            """REPLACE INTO mysql_query_rules
                   (rule_id, active, match_digest, delay, apply)
               VALUES (%s, 1, %s, %s, 0)""",
            (THROTTLE_RULE_ID, pattern, delay_ms),
        )
        cur.execute("LOAD MYSQL QUERY RULES TO RUNTIME")


def clear_throttle(conn):
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM mysql_query_rules WHERE rule_id=%s", (THROTTLE_RULE_ID,)
        )
        cur.execute("LOAD MYSQL QUERY RULES TO RUNTIME")


def rule_hits(conn):
    """Number of queries the throttle rule has matched (for sanity-checking that
    the regex actually catches live traffic). NOTE: stats_mysql_query_rules.hits
    is eventually-consistent - it lags the live counter by ~1 query, so only
    compare it across the HOTSPOT_REFRESH_S interval, never query-to-query."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT hits FROM stats_mysql_query_rules WHERE rule_id=%s",
            (THROTTLE_RULE_ID,),
        )
        row = cur.fetchone()
    return row[0] if row else 0


# ---------------------------------------------------------------------------
# Loop
# ---------------------------------------------------------------------------
def tick(proxysql, replicas, index, state):
    """One control iteration across all replicas. Engage when the worst replica
    lag crosses the threshold; release when every replica is back under it.
    Raises on an unexpected ProxySQL/index failure; main() catches and retries."""
    lag, laggard, broken = worst_replica_lag(replicas)
    for label, why in broken:
        print(f"WARN: replica {label} lag unknown ({why})", file=sys.stderr)

    # No replica reported a numeric lag: we have no lag signal at all. Throttling
    # on no signal is wrong, so drop any active throttle and wait.
    if lag is None:
        if state["throttling"]:
            proxysql.ping(reconnect=True)
            clear_throttle(proxysql)
            state.update(throttling=False, hot=None, rule_table=None)
            print("cleared throttle (no replica lag signal)")
        return

    print(f"worst lag={lag}s ({laggard})")

    if lag >= LAG_THRESHOLD_S:
        now = time.monotonic()
        if state["hot"] is None or (now - state["last_hotspot"]) >= HOTSPOT_REFRESH_S:
            # Stamp the time BEFORE the call so an index outage backs off on the
            # HOTSPOT_REFRESH_S cadence instead of hammering every POLL_INTERVAL_S.
            state["last_hotspot"] = now
            new_hot = top_table_by_volume(index)   # raises on error -> caught by main
            if new_hot is not None:
                state["hot"] = new_hot
            else:
                print(f"WARN: index shows no recent writes; keeping {state['hot']}",
                      file=sys.stderr)
            if state["throttling"]:
                # Sanity: is the live rule actually matching traffic?
                if rule_hits(proxysql) == state["last_hits"]:
                    print("WARN: throttle rule matched 0 new queries - check the "
                          "digest/regex and rule ordering", file=sys.stderr)
                state["last_hits"] = rule_hits(proxysql)

        hot = state["hot"]
        if hot:
            schema, table = hot
            # Only reload rules when the target actually changes (LOAD ... TO
            # RUNTIME rebuilds the whole rule chain under a lock - don't do it
            # every second).
            if not state["throttling"] or table != state["rule_table"]:
                proxysql.ping(reconnect=True)
                set_throttle(proxysql, schema, table, DELAY_MS)
                state.update(throttling=True, rule_table=table, last_hits=rule_hits(proxysql))
                print(f"throttling {schema}.{table} by {DELAY_MS}ms")

    elif state["throttling"] and lag <= CLEAR_THRESHOLD_S:
        # worst lag <= clear threshold means EVERY replica is back under it.
        proxysql.ping(reconnect=True)
        clear_throttle(proxysql)
        state.update(throttling=False, hot=None, rule_table=None)
        print(f"all replicas back to <= {CLEAR_THRESHOLD_S}s (worst {lag}s), throttle removed")
    # Band CLEAR_THRESHOLD_S < lag < LAG_THRESHOLD_S: hold current state.


def main():
    proxysql = pymysql.connect(autocommit=True, **PROXYSQL)
    replicas = [Replica(cfg) for cfg in REPLICAS]
    # Pin the index session to UTC so the time window matches the stored
    # (UTC) binlog event timestamps.
    index = pymysql.connect(
        autocommit=True, init_command="SET time_zone = '+00:00'", **INDEX
    )

    state = {"throttling": False, "hot": None, "rule_table": None,
             "last_hotspot": 0.0, "last_hits": 0}

    def shutdown(*_):
        # Always try to lift the throttle on exit - and never report success if
        # we couldn't (a false "cleared" is worse than a loud failure).
        try:
            clear_throttle(proxysql)
        except Exception as exc:
            print(f"ERROR: failed to clear throttle on shutdown - rule "
                  f"{THROTTLE_RULE_ID} may still be ACTIVE. Remove manually: "
                  f"DELETE FROM mysql_query_rules WHERE rule_id={THROTTLE_RULE_ID}; "
                  f"({exc})", file=sys.stderr)
            sys.exit(1)
        finally:
            proxysql.close()
            for rep in replicas:
                rep.close()
            index.close()
        print("throttle cleared, bye")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    while True:
        try:
            tick(proxysql, replicas, index, state)
        except Exception as exc:
            print(f"WARN: iteration failed, will retry: {exc}", file=sys.stderr)
        time.sleep(POLL_INTERVAL_S)


if __name__ == "__main__":
    main()
