#!/usr/bin/env python3
"""
Adaptive write throttle for MySQL via ProxySQL, driven by binlog change
intelligence (dbtrail).

Trigger:   replica apply lag (Seconds_Behind_Source) on a read replica.
Diagnosis: which table is generating the most binlog volume right now,
           asked to dbtrail's count_events MCP tool.
Action:    apply a per-rule delay in ProxySQL to the WRITE rule that matches
           the hot table.

Targets:   ProxySQL 3.0.x (Stable tier), MySQL 8.4 LTS, Python 3.11+.
Deps:      pip install pymysql requests

NOTE (verify against your gateway before production):
  The dbtrail MCP endpoint (per the server code) requires an `initialize`
  handshake that returns an `Mcp-Session-Id` header, which must be sent on
  every subsequent `tools/call`. This script does that handshake. If your
  gateway runs in a stateless mode, the session header is simply ignored.
"""

import json
import re
import signal
import sys
import time
from datetime import datetime, timedelta, timezone

import pymysql
import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROXYSQL = dict(host="127.0.0.1", port=6032, user="admin", password="admin")
REPLICA = dict(host="mysql-replica", port=3306, user="monitor", password="monitor")

DBTRAIL_MCP_URL = "https://api.dbtrail.com/mcp"
DBTRAIL_TOKEN = "REPLACE_WITH_YOUR_OAUTH2_BEARER_TOKEN"   # JWT or bt_... API key
DBTRAIL_SERVER_ID = "REPLACE_WITH_YOUR_SERVER_ID"
MCP_PROTOCOL_VERSION = "2025-06-18"

LAG_THRESHOLD_S = 3        # engage throttling at or above this replica lag
CLEAR_THRESHOLD_S = 1      # release throttling at or below this (hysteresis band)
VOLUME_WINDOW_S = 60       # how far back in the binlog we look for the hot table
DELAY_MS = 1               # per-query delay applied while throttling
THROTTLE_RULE_ID = 100     # dedicated, table-targeted throttle rule
POLL_INTERVAL_S = 1.0      # how often we check replica lag (cheap, local)
HOTSPOT_REFRESH_S = 10     # how often we re-ask dbtrail for the hot table
HTTP_TIMEOUT_S = 10


# ---------------------------------------------------------------------------
# Trigger: replica apply lag (MySQL 8.4)
# ---------------------------------------------------------------------------
def get_replica_lag(conn):
    """Return Seconds_Behind_Source as an int, or None when replication is not
    reporting a value. NULL is NOT zero: it means the applier thread is not
    running, or has drained the relay log while the receiver is stopped - i.e.
    replication may be broken. Callers must treat None as 'unknown', not 'caught
    up'."""
    with conn.cursor() as cur:
        cur.execute("SHOW REPLICA STATUS")   # 8.0.22+ / 8.4 wording
        row = cur.fetchone()
    if not row:
        return None
    val = row.get("Seconds_Behind_Source")
    return int(val) if val is not None else None


# ---------------------------------------------------------------------------
# Diagnosis: dbtrail MCP client (session-aware) + hot-table lookup
# ---------------------------------------------------------------------------
class DbtrailMCP:
    """Minimal HTTP JSON-RPC client for the dbtrail MCP gateway.

    Performs the `initialize` handshake and carries the Mcp-Session-Id on
    subsequent calls. Adapt auth / protocol version to your gateway."""

    def __init__(self, url, token):
        self.url = url
        self.token = token
        self.http = requests.Session()
        self.session_id = None
        self._req_id = 0

    def _headers(self):
        h = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.session_id:
            h["Mcp-Session-Id"] = self.session_id
        return h

    def _rpc(self, method, params=None):
        self._req_id += 1
        body = {"jsonrpc": "2.0", "id": self._req_id, "method": method}
        if params is not None:
            body["params"] = params
        resp = self.http.post(
            self.url, json=body, headers=self._headers(), timeout=HTTP_TIMEOUT_S
        )
        sid = resp.headers.get("Mcp-Session-Id")
        if sid:
            self.session_id = sid
        resp.raise_for_status()
        return resp.json()

    def initialize(self):
        self._rpc(
            "initialize",
            {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "proxysql-throttle", "version": "1.0"},
            },
        )
        # The MCP spec expects an `initialized` notification before tools/call.
        # It's a notification (no id, no response). Best-effort.
        body = {"jsonrpc": "2.0", "method": "notifications/initialized"}
        self.http.post(
            self.url, json=body, headers=self._headers(), timeout=HTTP_TIMEOUT_S
        )

    def count_events(self, server_id, group_by, since, until):
        resp = self._rpc(
            "tools/call",
            {
                "name": "count_events",
                "arguments": {
                    "server_id": server_id,
                    "group_by": group_by,
                    "since": since,
                    "until": until,
                },
            },
        )
        return _extract_groups(resp)


def _extract_groups(rpc_response):
    """Pull count_events groups[] out of the MCP response, and RAISE on any
    error rather than masking it as 'no data'. A masked error would silently
    stop throttling while the replica keeps lagging - the opposite of what we
    want."""
    if "error" in rpc_response:                      # JSON-RPC transport error
        raise RuntimeError(f"dbtrail RPC error: {rpc_response['error']}")
    result = rpc_response.get("result", {})
    if isinstance(result, dict) and result.get("isError"):
        text = "".join(b.get("text", "") for b in result.get("content", []))
        raise RuntimeError(f"dbtrail tool error: {text or 'unknown'}")
    if isinstance(result, dict) and "groups" in result:
        return result["groups"]
    for block in result.get("content", []):
        if block.get("type") == "text":
            parsed = json.loads(block["text"])       # let a parse failure raise
            if "groups" in parsed:
                return parsed["groups"]
    return []   # only for a genuinely valid, empty response


def top_table_by_volume(mcp):
    """Return (schema, table) for the hottest table in the last VOLUME_WINDOW_S
    seconds, or None if the window was genuinely empty. Raises on transport /
    tool error (caller decides whether to keep stale state).

    count_events returns absolute counts, not a rate; we sort DESC ourselves
    instead of trusting response ordering (the MCP layer does not guarantee it).
    Pydantic on the server side accepts ISO-8601 with a +00:00 offset and
    normalizes to UTC, so datetime.isoformat() is fine."""
    now = datetime.now(timezone.utc)
    since = (now - timedelta(seconds=VOLUME_WINDOW_S)).isoformat()
    until = now.isoformat()
    groups = mcp.count_events(DBTRAIL_SERVER_ID, ["schema", "table"], since, until)
    if not groups:
        return None
    groups.sort(key=lambda g: g.get("count", 0), reverse=True)
    top = groups[0]
    return top.get("schema"), top.get("table")


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
def tick(proxysql, replica, mcp, state):
    """One control iteration. Raises on any unexpected failure; main() catches,
    logs, and retries so a transient blip never kills the daemon."""
    replica.ping(reconnect=True)
    lag = get_replica_lag(replica)

    # Broken / unknown replication: surface loudly and drop any active throttle
    # (lag-based throttling is meaningless when the replica isn't applying).
    if lag is None:
        print("WARN: replica lag is NULL - replication stopped/broken", file=sys.stderr)
        if state["throttling"]:
            proxysql.ping(reconnect=True)
            clear_throttle(proxysql)
            state.update(throttling=False, hot=None, rule_table=None)
            print("cleared throttle (lag unknown)")
        return

    print(f"lag={lag}s")

    if lag >= LAG_THRESHOLD_S:
        now = time.monotonic()
        if state["hot"] is None or (now - state["last_hotspot"]) >= HOTSPOT_REFRESH_S:
            # Stamp the time BEFORE the call so a dbtrail outage backs off on the
            # HOTSPOT_REFRESH_S cadence instead of hammering every POLL_INTERVAL_S.
            state["last_hotspot"] = now
            new_hot = top_table_by_volume(mcp)   # raises on error -> caught by main
            if new_hot is not None:
                state["hot"] = new_hot
            else:
                print(f"WARN: dbtrail returned no hot table; keeping {state['hot']}",
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
        proxysql.ping(reconnect=True)
        clear_throttle(proxysql)
        state.update(throttling=False, hot=None, rule_table=None)
        print(f"lag back to {lag}s, throttle removed")
    # Band CLEAR_THRESHOLD_S < lag < LAG_THRESHOLD_S: hold current state.


def main():
    proxysql = pymysql.connect(autocommit=True, **PROXYSQL)
    replica = pymysql.connect(
        autocommit=True, cursorclass=pymysql.cursors.DictCursor, **REPLICA
    )
    mcp = DbtrailMCP(DBTRAIL_MCP_URL, DBTRAIL_TOKEN)
    try:
        mcp.initialize()
    except Exception as exc:
        print(f"FATAL: dbtrail MCP initialize failed: {exc}", file=sys.stderr)
        sys.exit(1)

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
            replica.close()
        print("throttle cleared, bye")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    while True:
        try:
            tick(proxysql, replica, mcp, state)
        except Exception as exc:
            print(f"WARN: iteration failed, will retry: {exc}", file=sys.stderr)
        time.sleep(POLL_INTERVAL_S)


if __name__ == "__main__":
    main()
