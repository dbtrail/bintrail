#!/usr/bin/env bash
# Orchestrates the console frontend E2E: build the binaries, point them at the
# shared test MySQL, launch a source-less `watch` daemon, seed a monitored
# source whose per-source index is intentionally NOT provisioned (the
# lifecycle state that exercises the regression guards), then drive headless
# Chrome (console_e2e.mjs). Tears everything down on exit.
#
# The boot index ($IDX_DB) is additionally provisioned as a REAL read fixture
# (#970/#686/#619): `bintrail init` creates the production schema, seeded row
# events + a cascade fixture land in real hourly partitions, and a baseline
# snapshot is produced by the real `bintrail baseline` converter over a
# hand-written mydumper-format dump. The scenarios reach it through the
# "byo-idx" registry server; the daemon-level --baseline-dir makes that server
# reconstruct-capable (Time-travel).
#
# Usage:  test/console-e2e/run.sh
# Env:
#   BINTRAIL_TEST_DSN   base DSN, default root:testroot@tcp(127.0.0.1:13306)
#   MYSQL_CONTAINER     docker container running test MySQL, default bintrail-test-mysql
#   CONSOLE_BIN         path to a prebuilt bintrail-console (else it is built)
#   PW_CHANNEL          playwright browser channel (e.g. "chrome"); default bundled chromium
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"

BASE_DSN="${BINTRAIL_TEST_DSN:-root:testroot@tcp(127.0.0.1:13306)}"
MYSQL_CONTAINER="${MYSQL_CONTAINER:-bintrail-test-mysql}"
PORT="${E2E_PORT:-8090}"
TOKEN="${E2E_TOKEN:-e2e-console-token}"
IDX_DB="bintrail_e2e_idx"
FIX_SCHEMA="e2eshop"
SERVERS_FILE="$(mktemp -t console-e2e-servers.XXXXXX.yaml)"
DUMP_DIR="$(mktemp -d -t console-e2e-dump.XXXXXX)"
BASELINE_ROOT="$(mktemp -d -t console-e2e-baseline.XXXXXX)"
export E2E_ARTIFACT_DIR="${E2E_ARTIFACT_DIR:-${RUNNER_TEMP:-/tmp}}"

mysql_exec() { docker exec -i "$MYSQL_CONTAINER" mysql -uroot -ptestroot "$@" 2>/dev/null; }

DAEMON_PID=""
cleanup() {
  [ -n "$DAEMON_PID" ] && kill "$DAEMON_PID" 2>/dev/null || true
  mysql_exec -e "DROP DATABASE IF EXISTS $IDX_DB;" >/dev/null 2>&1 || true
  rm -f "$SERVERS_FILE" 2>/dev/null || true
  rm -rf "$DUMP_DIR" "$BASELINE_ROOT" 2>/dev/null || true
}
trap cleanup EXIT

echo "==> build bintrail-console"
CONSOLE_BIN="${CONSOLE_BIN:-}"
if [ -z "$CONSOLE_BIN" ]; then
  CONSOLE_BIN="$ROOT/bintrail-console"
  (cd "$ROOT" && go build -o "$CONSOLE_BIN" ./cmd/bintrail-console)
fi

echo "==> build bintrail (fixture provisioning: init + baseline conversion)"
CLI_BIN="$ROOT/bintrail"
(cd "$ROOT" && go build -o "$CLI_BIN" ./cmd/bintrail)

echo "==> fresh index database $IDX_DB"
mysql_exec -e "DROP DATABASE IF EXISTS $IDX_DB; CREATE DATABASE $IDX_DB;"

# Fixture timestamps, anchored in the PREVIOUS UTC hour so the whole event
# window (baseline anchor -> reconstruct `at`) lives inside one hour whose
# partition exists — the planner classifies hours without a named partition as
# gaps (p_future doesn't count), and a gap 422s the strict Time-travel path.
# Computed with node (already required by this harness) because BSD and GNU
# `date` disagree on relative-time flags.
IFS='|' read -r P_HOUR H_HOUR H_NEXT PN_P PN_H TS_SNAP TS_INS TS_UPD TS_DEL TT_AT <<EOF
$(node -e '
const H = new Date(); H.setUTCMinutes(0, 0, 0);
const P = new Date(H.getTime() - 3600e3), N = new Date(H.getTime() + 3600e3);
const f = (d) => d.toISOString().slice(0, 19).replace("T", " ");
const pn = (d) => "p_" + d.toISOString().slice(0, 13).replace(/[-T]/g, "");
const m = (min) => f(new Date(P.getTime() + min * 60e3));
console.log([f(P), f(H), f(N), pn(P), pn(H), m(5), m(10), m(20), m(30), m(35)].join("|"));')
EOF

echo "==> provision the boot index with the production schema (bintrail init)"
"$CLI_BIN" init --index-dsn "${BASE_DSN}/${IDX_DB}" --partitions 4 >/dev/null

# init partitions forward from the current hour; the fixture events live in the
# PREVIOUS hour, so split the first partition to give that hour a named one.
mysql_exec "$IDX_DB" -e "ALTER TABLE binlog_events REORGANIZE PARTITION $PN_H INTO (
  PARTITION $PN_P VALUES LESS THAN (TO_SECONDS('$H_HOUR')),
  PARTITION $PN_H VALUES LESS THAN (TO_SECONDS('$H_NEXT')));"

echo "==> seed row events + cascade fixture into $IDX_DB"
# Mirrors internal/console's seedCascadeConsole plus an orders lifecycle. The
# UPDATE carries query_text/query_hash (a canary the frontend must NEVER see —
# they are redacted at the DTO layer) and connection_id=777 (which MUST pass
# through since #701 D1). The cascade child deletes are deliberately NOT
# indexed — that blind spot is what /api/recover's cascade synthesis repairs.
mysql_exec "$IDX_DB" <<SQL
INSERT INTO schema_snapshots (snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, is_nullable) VALUES
  (1,'$P_HOUR','$FIX_SCHEMA','orders','id',1,'PRI','int','NO'),
  (1,'$P_HOUR','$FIX_SCHEMA','orders','status',2,'','varchar','YES'),
  (1,'$P_HOUR','$FIX_SCHEMA','orders','email',3,'','varchar','YES'),
  (1,'$P_HOUR','$FIX_SCHEMA','parent','id',1,'PRI','int','NO'),
  (1,'$P_HOUR','$FIX_SCHEMA','child','id',1,'PRI','int','NO'),
  (1,'$P_HOUR','$FIX_SCHEMA','child','pid',2,'','int','YES');
INSERT INTO binlog_events (binlog_file, start_pos, end_pos, event_timestamp, connection_id, schema_name, table_name, event_type, pk_values, changed_columns, row_before, row_after, query_text, query_hash) VALUES
  ('binlog.000001',100,200,'$TS_INS',NULL,'$FIX_SCHEMA','orders',1,'3',NULL,NULL,'{"id":3,"status":"new","email":"c@example.com"}',NULL,NULL),
  ('binlog.000001',200,300,'$TS_UPD',777,'$FIX_SCHEMA','orders',2,'1','["status"]','{"id":1,"status":"new","email":"a@example.com"}','{"id":1,"status":"shipped","email":"a@example.com"}','UPDATE orders SET status = ''shipped'' /* e2e-canary-query-text */',SHA2('e2e-canary-query-text',256)),
  ('binlog.000001',300,400,'$TS_DEL',NULL,'$FIX_SCHEMA','orders',3,'2',NULL,'{"id":2,"status":"new","email":"b@example.com"}',NULL,NULL,NULL),
  ('binlog.000001',400,500,'$TS_INS',NULL,'$FIX_SCHEMA','child',1,'10',NULL,NULL,'{"id":10,"pid":1}',NULL,NULL),
  ('binlog.000001',500,600,'$TS_INS',NULL,'$FIX_SCHEMA','child',1,'11',NULL,NULL,'{"id":11,"pid":1}',NULL,NULL),
  ('binlog.000001',600,700,'$TS_UPD',NULL,'$FIX_SCHEMA','parent',3,'1',NULL,'{"id":1}',NULL,NULL,NULL);
INSERT INTO fk_constraints (snapshot_id, constraint_name, schema_name, table_name, column_name, ordinal_position, referenced_schema_name, referenced_table_name, referenced_column_name, delete_rule, update_rule) VALUES
  (1,'fk_child_parent','$FIX_SCHEMA','child','pid',1,'$FIX_SCHEMA','parent','id','CASCADE','RESTRICT');
SQL

echo "==> build a baseline snapshot (real 'bintrail baseline' over a mydumper-format dump)"
# Baseline rows 1 and 2 predate the events above (anchor binlog.000001:50 at
# TS_SNAP); row 4 is never touched by any event — Time-travel resolving it
# proves the baseline half of baseline+deltas, not just the event fold.
# The tab before Log:/Pos: is load-bearing (ParseMetadata cuts "\tLog: ").
printf 'Started dump at: %s\nSHOW MASTER STATUS:\n\tLog: binlog.000001\n\tPos: 50\nFinished dump at: %s\n' "$TS_SNAP" "$TS_SNAP" > "$DUMP_DIR/metadata"
cat > "$DUMP_DIR/$FIX_SCHEMA.orders-schema.sql" <<'SQL'
CREATE TABLE `orders` (
  `id` int NOT NULL,
  `status` varchar(32) DEFAULT NULL,
  `email` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
SQL
cat > "$DUMP_DIR/$FIX_SCHEMA.orders.00000.sql" <<'SQL'
INSERT INTO `orders` VALUES (1,"new","a@example.com"),(2,"new","b@example.com"),(4,"new","d@example.com");
SQL
"$CLI_BIN" baseline --input "$DUMP_DIR" --output "$BASELINE_ROOT" >/dev/null

echo "==> launch source-less watch daemon on :$PORT"
BINTRAIL_CONSOLE_TOKEN="$TOKEN" BINTRAIL_CONSOLE_BASELINE_TRIGGER=1 "$CONSOLE_BIN" watch \
  --index-dsn "${BASE_DSN}/${IDX_DB}" \
  --console-listen "127.0.0.1:$PORT" \
  --console-token "$TOKEN" \
  --console-servers-file "$SERVERS_FILE" \
  --baseline-dir "$BASELINE_ROOT" \
  --console-allow-setup >"$E2E_ARTIFACT_DIR/console-e2e-daemon.log" 2>&1 &
DAEMON_PID=$!

echo "==> wait for /api/healthz"
for i in $(seq 1 30); do
  if curl -fsS -o /dev/null "http://127.0.0.1:$PORT/api/healthz" 2>/dev/null; then break; fi
  if ! kill -0 "$DAEMON_PID" 2>/dev/null; then
    echo "daemon exited early; log:" >&2; cat "$E2E_ARTIFACT_DIR/console-e2e-daemon.log" >&2; exit 1
  fi
  sleep 1
done

echo "==> seed a monitored source (per-source index left unprovisioned)"
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"name":"wp","source_host":"127.0.0.1","source_port":"13306","source_user":"dbtrail","source_password":"x"}' \
  "http://127.0.0.1:$PORT/api/servers" >/dev/null

echo "==> install node deps"
(cd "$HERE" && npm install --no-audit --no-fund --silent)
if [ "${PW_CHANNEL:-}" = "" ]; then
  echo "==> install playwright chromium"
  (cd "$HERE" && npx --yes playwright install chromium >/dev/null)
fi

echo "==> drive headless Chrome"
cd "$HERE"
CONSOLE_URL="http://127.0.0.1:$PORT" CONSOLE_TOKEN="$TOKEN" \
  E2E_FIX_SCHEMA="$FIX_SCHEMA" E2E_TT_AT="$TT_AT" node console_e2e.mjs
