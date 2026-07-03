#!/usr/bin/env bash
# managed-pg-smoke.sh — end-to-end smoke of the bintrail-pg capture pipeline
# against ANY PostgreSQL DSN (#594).
#
# The "managed" part of the managed-PostgreSQL smoke matrix is the
# *provisioning* (RDS / Aurora parameter groups, reboot semantics, replication
# grants); the smoke itself is DSN-agnostic — point it at stock PostgreSQL, RDS,
# Aurora, or Cloud SQL and it exercises the identical pipeline:
#
#   1. operator setup on the source  (table + REPLICA IDENTITY FULL + publication
#                                     — the same SQL docs/postgres.md tells users
#                                     to run; bintrail validates, never creates)
#   2. bintrail-pg doctor            (preflight verdict must be exit 0)
#   3. bintrail-pg stream            (creates the slot, streams into the MySQL index)
#   4. INSERT + UPDATE + DELETE      (on the source, as an ordinary client)
#   5. assert all 3 events land in binlog_events (via `bintrail-pg query`)
#   6. bintrail-pg recover --dry-run (assert the reversal SQL undoes the DELETE
#                                     with a re-INSERT of the full row image)
#   7. teardown                      (stop stream, `bintrail-pg reset` drops the
#                                     slot + checkpoint, drop publication/table)
#
# Required environment:
#   SMOKE_PG_URL      ordinary PostgreSQL URL, e.g.
#                     postgres://postgres:pw@host:5432/smoke?sslmode=require
#                     (the replication DSN is derived by appending
#                     replication=database)
#   SMOKE_INDEX_DSN   MySQL DSN of a DEDICATED index database that already
#                     exists (this script bootstraps the index tables but does
#                     not create the database), e.g.
#                     root:pw@tcp(127.0.0.1:13306)/smoke_rds
#
# Optional environment:
#   SMOKE_BINTRAIL_PG   path to the bintrail-pg binary   (default: bintrail-pg)
#   SMOKE_PSQL          psql invocation                  (default: psql)
#                       e.g. "docker exec -i some-pg-container psql"
#   SMOKE_SLOT          replication slot name            (default: bintrail_smoke_slot)
#   SMOKE_PUBLICATION   publication name                 (default: bintrail_smoke_pub)
#   SMOKE_TABLE         test table name (schema public)  (default: bintrail_smoke_events)
#   SMOKE_SERVER_ID     --server-id for the stream       (default: 4594)
#   SMOKE_TIMEOUT       seconds to wait for events       (default: 120)
#   SMOKE_KEEP_SOURCE   set to 1 to skip the source-side teardown (debugging)
#
# Exit code: 0 only when every step passes. All failures are loud.

set -u -o pipefail

: "${SMOKE_PG_URL:?SMOKE_PG_URL is required (ordinary PostgreSQL URL)}"
: "${SMOKE_INDEX_DSN:?SMOKE_INDEX_DSN is required (MySQL DSN of an existing, dedicated index database)}"

BINTRAIL_PG=${SMOKE_BINTRAIL_PG:-bintrail-pg}
PSQL=${SMOKE_PSQL:-psql}
SLOT=${SMOKE_SLOT:-bintrail_smoke_slot}
PUBLICATION=${SMOKE_PUBLICATION:-bintrail_smoke_pub}
TABLE=${SMOKE_TABLE:-bintrail_smoke_events}
SERVER_ID=${SMOKE_SERVER_ID:-4594}
TIMEOUT=${SMOKE_TIMEOUT:-120}

# Derive the replication DSN (walsender mode) from the ordinary URL.
if [[ "$SMOKE_PG_URL" == *\?* ]]; then
  REPL_URL="${SMOKE_PG_URL}&replication=database"
else
  REPL_URL="${SMOKE_PG_URL}?replication=database"
fi

WORKDIR=$(mktemp -d "${TMPDIR:-/tmp}/bintrail-pg-smoke.XXXXXX")
STREAM_PID=""
FAILED=0

log()  { printf '\n== %s ==\n' "$*"; }
fail() { printf 'SMOKE FAIL: %s\n' "$*" >&2; FAILED=1; exit 1; }

run_sql() { # run_sql "<sql>" — ordinary SQL on the source, fails loud
  echo "$1" | $PSQL "$SMOKE_PG_URL" -v ON_ERROR_STOP=1 -q -At \
    || fail "source SQL failed: $1"
}

cleanup() {
  local rc=$?
  set +e
  log "teardown"
  if [[ -n "$STREAM_PID" ]] && kill -0 "$STREAM_PID" 2>/dev/null; then
    kill -TERM "$STREAM_PID"
    for _ in $(seq 1 20); do kill -0 "$STREAM_PID" 2>/dev/null || break; sleep 1; done
    kill -0 "$STREAM_PID" 2>/dev/null && kill -KILL "$STREAM_PID"
    wait "$STREAM_PID" 2>/dev/null
  fi
  if [[ "${SMOKE_KEEP_SOURCE:-0}" != "1" ]]; then
    # Drop slot + index checkpoint (the documented two-system teardown), then
    # the operator-created objects. Best-effort: the source may already be gone.
    "$BINTRAIL_PG" reset --query-dsn "$SMOKE_PG_URL" --index-dsn "$SMOKE_INDEX_DSN" \
      --slot "$SLOT" --force >/dev/null 2>&1
    echo "DROP PUBLICATION IF EXISTS ${PUBLICATION}; DROP TABLE IF EXISTS public.${TABLE};" \
      | $PSQL "$SMOKE_PG_URL" -v ON_ERROR_STOP=0 -q -At >/dev/null 2>&1
  fi
  if [[ $rc -ne 0 && -s "$WORKDIR/stream.log" ]]; then
    echo "--- last stream log lines ---" >&2
    tail -30 "$WORKDIR/stream.log" >&2
  fi
  rm -rf "$WORKDIR"
  exit $rc
}
trap cleanup EXIT

log "0. source sanity"
SRC_VERSION=$(run_sql "SELECT version();") || exit 1
WAL_LEVEL=$(run_sql "SHOW wal_level;") || exit 1
echo "source:    $SRC_VERSION"
echo "wal_level: $WAL_LEVEL"
[[ "$WAL_LEVEL" == "logical" ]] || fail "wal_level is '$WAL_LEVEL', want 'logical' (on RDS/Aurora set rds.logical_replication=1 in the parameter group and reboot)"

log "1. operator setup (table + REPLICA IDENTITY FULL + publication)"
run_sql "DROP PUBLICATION IF EXISTS ${PUBLICATION};"
run_sql "DROP TABLE IF EXISTS public.${TABLE};"
run_sql "CREATE TABLE public.${TABLE} (id int PRIMARY KEY, note text NOT NULL, amount numeric(10,2));"
run_sql "ALTER TABLE public.${TABLE} REPLICA IDENTITY FULL;"
run_sql "CREATE PUBLICATION ${PUBLICATION} FOR TABLE public.${TABLE};"
echo "created public.${TABLE} (RI FULL) + publication ${PUBLICATION}"

log "2. bintrail-pg doctor"
if ! "$BINTRAIL_PG" doctor \
    --query-dsn "$SMOKE_PG_URL" \
    --slot "$SLOT" --publication "$PUBLICATION" \
    --tables "public.${TABLE}" | tee "$WORKDIR/doctor.log"; then
  fail "doctor reported a FAIL (see output above)"
fi

log "3. bintrail-pg stream (background)"
"$BINTRAIL_PG" stream \
  --index-dsn "$SMOKE_INDEX_DSN" \
  --repl-dsn "$REPL_URL" \
  --query-dsn "$SMOKE_PG_URL" \
  --slot "$SLOT" --publication "$PUBLICATION" \
  --server-id "$SERVER_ID" \
  --schemas public --tables "public.${TABLE}" \
  --checkpoint 2 \
  >"$WORKDIR/stream.log" 2>&1 &
STREAM_PID=$!

# Wait for the slot to exist and be active before mutating (the stream creates it).
for i in $(seq 1 "$TIMEOUT"); do
  kill -0 "$STREAM_PID" 2>/dev/null || { cat "$WORKDIR/stream.log" >&2; fail "stream exited early"; }
  ACTIVE=$(run_sql "SELECT active FROM pg_replication_slots WHERE slot_name = '${SLOT}';") || exit 1
  [[ "$ACTIVE" == "t" ]] && break
  sleep 1
  [[ $i -eq $TIMEOUT ]] && fail "slot ${SLOT} not active after ${TIMEOUT}s"
done
echo "slot ${SLOT} active; stream pid ${STREAM_PID}"

log "4. INSERT + UPDATE + DELETE on the source"
run_sql "INSERT INTO public.${TABLE} (id, note, amount) VALUES (1, 'managed smoke row', 12.34);"
run_sql "UPDATE public.${TABLE} SET note = 'managed smoke row v2', amount = 56.78 WHERE id = 1;"
run_sql "DELETE FROM public.${TABLE} WHERE id = 1;"
echo "3 mutations executed"

log "5. assert the 3 events land in binlog_events"
QUERY_JSON=""
for i in $(seq 1 "$TIMEOUT"); do
  QUERY_JSON=$("$BINTRAIL_PG" query --index-dsn "$SMOKE_INDEX_DSN" \
    --schema public --table "$TABLE" --pk 1 --format json --no-archive 2>/dev/null)
  N=$(printf '%s' "$QUERY_JSON" | grep -o '"event_type"' | wc -l | tr -d ' ')
  [[ "$N" -ge 3 ]] && break
  kill -0 "$STREAM_PID" 2>/dev/null || { cat "$WORKDIR/stream.log" >&2; fail "stream exited while waiting for events"; }
  sleep 1
  [[ $i -eq $TIMEOUT ]] && fail "only ${N}/3 events indexed after ${TIMEOUT}s"
done
for et in INSERT UPDATE DELETE; do
  printf '%s' "$QUERY_JSON" | grep -q "\"event_type\": \"${et}\"" \
    || fail "no ${et} event indexed for public.${TABLE} pk=1"
done
echo "3/3 events indexed (INSERT, UPDATE, DELETE):"
printf '%s\n' "$QUERY_JSON" | grep -E '"event_type"|"pk_values"' | sed 's/^ *//'

log "6. bintrail-pg recover --dry-run (reversal SQL)"
RECOVER_SQL=$("$BINTRAIL_PG" recover --index-dsn "$SMOKE_INDEX_DSN" \
  --schema public --table "$TABLE" --pk 1 --dry-run --no-archive) \
  || fail "recover failed"
printf '%s\n' "$RECOVER_SQL"
# Reversal order is most-recent-first: undo DELETE (re-INSERT the full row image),
# undo UPDATE (restore the before-image), undo INSERT (DELETE).
printf '%s' "$RECOVER_SQL" | grep -q "INSERT INTO \"public\".\"${TABLE}\"" \
  || fail "reversal SQL lacks the re-INSERT that undoes the DELETE"
printf '%s' "$RECOVER_SQL" | grep -q "managed smoke row v2" \
  || fail "re-INSERT does not carry the deleted row's full before-image"
printf '%s' "$RECOVER_SQL" | grep -q "UPDATE \"public\".\"${TABLE}\"" \
  || fail "reversal SQL lacks the UPDATE that restores the before-image"
printf '%s' "$RECOVER_SQL" | grep -q "DELETE FROM \"public\".\"${TABLE}\"" \
  || fail "reversal SQL lacks the DELETE that undoes the INSERT"
echo "reversal SQL verified (re-INSERT with full before-image, UPDATE, DELETE)"

log "SMOKE PASS"
