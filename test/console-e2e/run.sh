#!/usr/bin/env bash
# Orchestrates the console frontend E2E: build the binary, point it at the
# shared test MySQL, launch a source-less `watch` daemon, seed a monitored
# source whose per-source index is intentionally NOT provisioned (the
# lifecycle state that exercises the regression guards), then drive headless
# Chrome (console_e2e.mjs). Tears everything down on exit.
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
SERVERS_FILE="$(mktemp -t console-e2e-servers.XXXXXX.yaml)"
export E2E_ARTIFACT_DIR="${E2E_ARTIFACT_DIR:-${RUNNER_TEMP:-/tmp}}"

mysql_exec() { docker exec -i "$MYSQL_CONTAINER" mysql -uroot -ptestroot "$@" 2>/dev/null; }

DAEMON_PID=""
cleanup() {
  [ -n "$DAEMON_PID" ] && kill "$DAEMON_PID" 2>/dev/null || true
  mysql_exec -e "DROP DATABASE IF EXISTS $IDX_DB;" >/dev/null 2>&1 || true
  rm -f "$SERVERS_FILE" 2>/dev/null || true
}
trap cleanup EXIT

echo "==> build bintrail-console"
CONSOLE_BIN="${CONSOLE_BIN:-}"
if [ -z "$CONSOLE_BIN" ]; then
  CONSOLE_BIN="$ROOT/bintrail-console"
  (cd "$ROOT" && go build -o "$CONSOLE_BIN" ./cmd/bintrail-console)
fi

echo "==> fresh index database $IDX_DB"
mysql_exec -e "DROP DATABASE IF EXISTS $IDX_DB; CREATE DATABASE $IDX_DB;"

echo "==> launch source-less watch daemon on :$PORT"
BINTRAIL_CONSOLE_TOKEN="$TOKEN" "$CONSOLE_BIN" watch \
  --index-dsn "${BASE_DSN}/${IDX_DB}" \
  --console-listen "127.0.0.1:$PORT" \
  --console-token "$TOKEN" \
  --console-servers-file "$SERVERS_FILE" \
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

# Forensics who-changed happy-path fixture (#708): one real row in the boot
# index ($IDX_DB, already schema-migrated by the daemon's own EnsureSchema on
# startup). The "byo-idx" server created below by scenario 5 points at this
# exact database with no source configured — reusing it here gives the
# who-changed scenarios both a working index AND the "no source" capability
# state without a second fixture.
echo "==> seed one binlog_events row for the forensics who-changed scenarios"
mysql_exec -e "
USE $IDX_DB;
INSERT INTO binlog_events (binlog_file, start_pos, end_pos, event_timestamp, connection_id, schema_name, table_name, event_type, pk_values, changed_columns, row_before, row_after, schema_version)
VALUES ('bin.000001', 4, 40, NOW(), 4242, 'fx_e2e', 'probe', 2, '1', JSON_ARRAY('val'), JSON_OBJECT('val','a'), JSON_OBJECT('val','b'), 0);
"

echo "==> install node deps"
(cd "$HERE" && npm install --no-audit --no-fund --silent)
if [ "${PW_CHANNEL:-}" = "" ]; then
  echo "==> install playwright chromium"
  (cd "$HERE" && npx --yes playwright install chromium >/dev/null)
fi

echo "==> drive headless Chrome"
cd "$HERE"
CONSOLE_URL="http://127.0.0.1:$PORT" CONSOLE_TOKEN="$TOKEN" node console_e2e.mjs
