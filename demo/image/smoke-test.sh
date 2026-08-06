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

# ── Telemetry guard, static half (#1055) ────────────────────
# Checked before the build so a dropped guard fails in a second rather
# than three minutes. Both layers are pinned: either one alone satisfies
# the live check below, so only asserting the effect would let one rot
# unnoticed.
grep -q '^export DO_NOT_TRACK=1' demo/image/entrypoint.sh \
    || { log "FAIL: entrypoint.sh no longer exports DO_NOT_TRACK=1"; exit 1; }
grep -q '^ENV DO_NOT_TRACK=1' demo/image/Dockerfile \
    || { log "FAIL: Dockerfile no longer sets ENV DO_NOT_TRACK=1"; exit 1; }

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

# ── Telemetry guard, live half (#1055) ──────────────────────
# Assert the MECHANISM on the running processes, not sampled network
# connections: a telemetry POST lasts milliseconds, so a sampled
# connection check would pass by luck — false confidence is worse than
# no check. DO_NOT_TRACK=1 in the environ of every bintrail process is
# deterministic and catches the realistic regression (the export moved
# below a child's start, or a child spawned with a scrubbed env).
# Processes are identified by their executable (/proc/PID/exe), NOT by a
# substring of their command line: this scanning shell's own argv contains
# the script text below, so a cmdline match would count the scanner itself
# and the "no bintrail process found" assertion could never fire. Matching
# the exe also skips transient `mysql ... bintrail_index` clients.
log "Asserting the telemetry guard on live processes..."
GUARD=$(docker exec "$NAME" sh -c '
    total=0; guarded=0
    for d in /proc/[0-9]*; do
        case "$(readlink "$d/exe" 2>/dev/null)" in
            */bintrail) total=$((total+1)) ;;
            *) continue ;;
        esac
        if tr "\0" "\n" < "$d/environ" 2>/dev/null | grep -qx "DO_NOT_TRACK=1"; then
            guarded=$((guarded+1))
        fi
    done
    echo "$total $guarded"') || { log "FAIL: could not inspect processes in the container"; exit 1; }
read -r TOTAL GUARDED <<<"$GUARD"
log "bintrail processes: ${TOTAL:-0}, of them with DO_NOT_TRACK=1: ${GUARDED:-0}"
[ "${TOTAL:-0}" -ge 1 ] \
    || { log "FAIL: no bintrail process found — the guard check proved nothing"; exit 1; }
[ "${TOTAL:-0}" = "${GUARDED:-0}" ] \
    || { log "FAIL: $((TOTAL - GUARDED)) bintrail process(es) missing DO_NOT_TRACK=1"; exit 1; }

LIVE=$(mysql_demo "SELECT status, total FROM orders WHERE id = 1")
log "live row:        ${LIVE:-<empty>}"
[ -n "$LIVE" ] || { log "FAIL: live row missing"; exit 1; }

PAST=$(mysql_demo "SELECT status, total FROM _flashback.orders AS OF '1 minute ago' WHERE id = 1")
log "row 1 minute ago (virtual): ${PAST:-<empty>}"

# The bare tagline form (#385): time-travel on the REAL table name —
# the literal acceptance query of issue #350. NOTE: the bare form is
# `*`-only (column lists stay on the virtual schemas). Both forms are
# pinned to the same deterministic historical image in test/shim, so
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
