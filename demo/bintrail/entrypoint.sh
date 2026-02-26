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

mysql_q() {
    mysql -hmysql -P3306 -uroot -pdemo --protocol=tcp --silent -e "$1"
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

if [ -z "$START_GTID" ]; then
    log "Warning: gtid_executed is empty — starting stream without --start-gtid"
    log "Running: bintrail stream (no GTID)"
    exec bintrail stream \
        --index-dsn   "$INDEX_DSN" \
        --source-dsn  "$SOURCE_DSN" \
        --server-id   "$SERVER_ID" \
        --batch-size  "$BATCH_SIZE" \
        --checkpoint  "$CHECKPOINT" \
        --schemas     "$SCHEMAS" \
        ${METRICS_ADDR:+--metrics-addr "$METRICS_ADDR"}
fi

log "Start GTID: $START_GTID"

# ── 5. exec bintrail stream ─────────────────────────────────
# Using 'exec' so Docker SIGTERM goes directly to bintrail (PID 1),
# triggering its graceful shutdown + checkpoint flush.
log "Running: bintrail stream"
exec bintrail stream \
    --index-dsn   "$INDEX_DSN" \
    --source-dsn  "$SOURCE_DSN" \
    --server-id   "$SERVER_ID" \
    --start-gtid  "$START_GTID" \
    --batch-size  "$BATCH_SIZE" \
    --checkpoint  "$CHECKPOINT" \
    --schemas     "$SCHEMAS" \
    ${METRICS_ADDR:+--metrics-addr "$METRICS_ADDR"}
