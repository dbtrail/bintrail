#!/usr/bin/env bash
# Smoke test for the bintrail demo image (#350).
#
# Builds the image, boots it, waits for the stack, lets the traffic
# generator build ~70s of history, then asserts the acceptance flow:
# a time-travel query for orders id=1 returns a non-empty PREVIOUS
# state that differs from the live row.
#
# Not part of `go test ./...` — needs Docker and ~3 minutes. Run:
#   demo/image/smoke-test.sh            # build + test
#   SKIP_BUILD=1 demo/image/smoke-test.sh   # reuse the last image

set -euo pipefail

IMAGE="${IMAGE:-bintrail-demo:smoke}"
PORT="${PORT:-16033}"
NAME="bintrail-demo-smoke"

log() { echo "[smoke] $(date '+%H:%M:%S') $*"; }

if [ -z "${SKIP_BUILD:-}" ]; then
    # Build for the host's native architecture (Percona Server + ProxySQL
    # both ship arm64, so this works on Graviton/Apple Silicon too). Override
    # with PLATFORM=linux/amd64 to cross-build a specific arch.
    log "Building image${PLATFORM:+ (${PLATFORM})}..."
    docker build ${PLATFORM:+--platform "$PLATFORM"} -f demo/image/Dockerfile -t "$IMAGE" .
fi

cleanup() {
    docker rm -f "$NAME" &>/dev/null || true
}
trap cleanup EXIT
cleanup

log "Starting demo image (ProxySQL on :$PORT)..."
docker run -d --name "$NAME" -p "$PORT:6033" "$IMAGE" >/dev/null

mysql_demo() {
    mysql -h127.0.0.1 -P"$PORT" -udemo -pdemo --protocol=tcp --connect-timeout=5 -N demo -e "$1" 2>/dev/null
}

log "Waiting for the stack (ProxySQL passthrough answering)..."
for i in $(seq 1 120); do
    if ! docker inspect -f '{{.State.Running}}' "$NAME" 2>/dev/null | grep -q true; then
        log "FAIL: container exited during boot"
        docker logs "$NAME" | tail -40
        exit 1
    fi
    mysql_demo "SELECT 1" &>/dev/null && break
    sleep 2
done
mysql_demo "SELECT 1" &>/dev/null || { log "FAIL: stack never answered on :$PORT"; docker logs "$NAME" | tail -40; exit 1; }
log "Stack is answering. Letting traffic build history (70s)..."
sleep 70

LIVE=$(mysql_demo "SELECT status, total FROM orders WHERE id = 1")
log "live row:        ${LIVE:-<empty>}"
[ -n "$LIVE" ] || { log "FAIL: live row missing"; exit 1; }

PAST=$(mysql_demo "SELECT status, total FROM _flashback.orders AS OF '1 minute ago' WHERE id = 1")
log "row 1 minute ago (virtual): ${PAST:-<empty>}"

# The bare tagline form (#385): time-travel on the REAL table name —
# the literal acceptance query of issue #350. NOTE: the bare form is
# `*`-only (column lists stay on the virtual schemas). Both forms are
# pinned to the same deterministic historical image in e2e/shim, so
# non-empty is the right assertion here.
BARE=$(mysql_demo "SELECT * FROM orders WHERE id = 1 AS OF '1 minute ago'" || true)
[ -n "$BARE" ] || { log "FAIL: bare AS OF form returned no row"; docker logs "$NAME" | tail -40; exit 1; }
log "bare AS OF row:   $BARE"
log "row 1 minute ago: ${PAST:-<empty>}"
[ -n "$PAST" ] || { log "FAIL: time-travel query returned no row"; docker logs "$NAME" | tail -40; exit 1; }

# The traffic generator bumps total by 1.00 every ~5s, so live and
# 1-minute-ago MUST differ once a minute of history exists.
if [ "$LIVE" = "$PAST" ]; then
    log "FAIL: historical row equals live row (no history applied?)"
    exit 1
fi

# Spot-check the hint form too. NOTE: the hint form only supports the
# `*` projection (the parser keeps the SELECT * shape, #313) — a column
# list here would 1064.
HINT=$(mysql_demo "SELECT /*+ DBTRAIL_AT='30 seconds ago' */ * FROM orders WHERE id = 1" || true)
[ -n "$HINT" ] || { log "FAIL: DBTRAIL_AT hint form returned no row"; exit 1; }

log "PASS: time-travel returns a previous row state distinct from live."
