#!/usr/bin/env bash
# Bintrail demo entrypoint:
#   1. Wait for demo.customers to exist (schema ready)
#   2. bintrail init
#   3. bintrail snapshot
#   4. Capture GTID start position
#   5. exec bintrail stream (PID 1 for clean SIGTERM handling)

set -euo pipefail

INDEX_DSN="${INDEX_DSN:-root:demo@tcp(mysql:3306)/bintrail_index}"
SOURCE_DSN="${SOURCE_DSN:-root:demo@tcp(mysql:3306)/}"
SCHEMAS="${SCHEMAS:-demo,sbtest}"
SERVER_ID="${SERVER_ID:-99999}"
BATCH_SIZE="${BATCH_SIZE:-500}"
CHECKPOINT="${CHECKPOINT:-5}"
METRICS_ADDR="${METRICS_ADDR:-}"

log() { echo "[bintrail] $(date '+%H:%M:%S') $*"; }

# Parse SOURCE_DSN (format: user:pass@tcp(host:port)/db) for mysql CLI
SRC_USER=$(echo "$SOURCE_DSN" | sed -n 's/^\([^:]*\):.*/\1/p')
SRC_PASS=$(echo "$SOURCE_DSN" | sed -n 's/^[^:]*:\([^@]*\)@.*/\1/p')
SRC_HOST=$(echo "$SOURCE_DSN" | sed -n 's/.*tcp(\([^:)]*\).*/\1/p')
SRC_PORT=$(echo "$SOURCE_DSN" | sed -n 's/.*tcp([^:]*:\([^)]*\)).*/\1/p')

mysql_q() {
    mysql -h"$SRC_HOST" -P"$SRC_PORT" -u"$SRC_USER" -p"$SRC_PASS" \
          --protocol=tcp --silent -e "$1"
}

# ── 1. Wait for demo schema ─────────────────────────────────
log "Waiting for demo.customers..."
until mysql_q "SELECT 1 FROM demo.customers LIMIT 1" &>/dev/null; do
    sleep 2
done
log "Schema ready."

# ── 2. bintrail init ────────────────────────────────────────
log "Running: bintrail init"
bintrail init \
    --index-dsn "$INDEX_DSN" \
    --partitions 7

# ── 3. bintrail snapshot ────────────────────────────────────
log "Running: bintrail snapshot"
bintrail snapshot \
    --source-dsn "$SOURCE_DSN" \
    --index-dsn  "$INDEX_DSN" \
    --schemas    "$SCHEMAS"

# ── 4. Capture GTID start position ─────────────────────────
log "Capturing GTID start position..."
START_GTID=$(mysql_q "SELECT @@global.gtid_executed" 2>/dev/null || true)

# ── 5. exec bintrail stream ─────────────────────────────────
# Build args array so optional --metrics-addr and --start-gtid are handled
# cleanly without shell word-splitting issues.
STREAM_ARGS=(
    --index-dsn   "$INDEX_DSN"
    --source-dsn  "$SOURCE_DSN"
    --server-id   "$SERVER_ID"
    --batch-size  "$BATCH_SIZE"
    --checkpoint  "$CHECKPOINT"
    --schemas     "$SCHEMAS"
)

if [ -n "$START_GTID" ]; then
    STREAM_ARGS+=(--start-gtid "$START_GTID")
else
    log "Warning: gtid_executed is empty — starting stream without --start-gtid"
fi

if [ -n "$METRICS_ADDR" ]; then
    STREAM_ARGS+=(--metrics-addr "$METRICS_ADDR")
fi

log "Running: bintrail stream (args: ${STREAM_ARGS[*]})"

# Using 'exec' so Docker SIGTERM goes directly to bintrail (PID 1),
# triggering its graceful shutdown + checkpoint flush.
exec bintrail stream "${STREAM_ARGS[@]}"
