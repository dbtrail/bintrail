#!/usr/bin/env bash
# Traffic generator for the bintrail demo (evaluation-only).
#
# A trimmed demo/traffic/traffic.sh: no sysbench, no compose-restart
# watchdogs (entrypoint.sh supervises). Every ~5s cycle makes a mixed
# INSERT/UPDATE/DELETE workload on the demo schema, INCLUDING a
# deterministic mutation of orders id=1 — random-row updates alone
# would make the README's `AS OF` query flaky (a row only has
# queryable history once it is touched after the stream started).

set -euo pipefail

MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:-demo}"
MYSQL_PASS="${MYSQL_PASS:-demo}"
MYSQL_DB="${MYSQL_DB:-demo}"

# Connection hardening carried over from demo/traffic/traffic.sh: caps
# on handshake, server-side read/write timeouts, and an outer wall-clock
# kill so a half-open socket can never wedge the loop silently.
mysql_cmd() {
    timeout 60 mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASS" \
          --protocol=tcp --silent \
          --connect-timeout=10 \
          --init-command="SET SESSION net_read_timeout=30, net_write_timeout=30, max_execution_time=15000" \
          "$@"
}

# Churn is best-effort: a transient failure (lock-wait timeout, brief
# connection blip, the outer `timeout 60` firing) must NOT abort the
# loop — under set -e an unguarded failure would exit this script, and
# the entrypoint's fail-loud supervisor would then tear down the whole
# demo image over a hiccup in non-essential traffic. Log and keep going;
# the next cycle retries the same statement shapes anyway.
sql() {
    mysql_cmd "$MYSQL_DB" -e "$1" \
        || log "statement failed (rc=$?, transient?), continuing"
}

log() { echo "[traffic] $(date '+%H:%M:%S') $*"; }

CYCLE=0
log "Starting traffic loop..."

while true; do
    CYCLE=$((CYCLE + 1))

    # 1. Deterministic mutation of orders id=1 — guarantees the
    #    "SELECT ... AS OF '1 minute ago' WHERE id = 1" demo query
    #    always has a previous state to return.
    sql "UPDATE orders
         SET status = ELT(1 + FLOOR(RAND()*5), 'pending','processing','shipped','delivered','cancelled'),
             total  = ROUND(total + 1.00, 2)
         WHERE id = 1;"

    # 2. New customers
    for i in 1 2; do
        TS=$(date '+%s%N')
        sql "INSERT INTO customers (name, email, tier)
             VALUES ('User_${CYCLE}_${i}', 'u${TS}_${i}@demo.test',
                     ELT(1 + FLOOR(RAND()*4), 'bronze','silver','gold','platinum'))"
    done

    # 3. Orders for the most recent customers
    sql "
    SET @max = (SELECT MAX(id) FROM customers);
    INSERT INTO orders (customer_id, status, total)
    SELECT id,
           ELT(1 + FLOOR(RAND()*5), 'pending','processing','shipped','delivered','cancelled'),
           ROUND(RAND()*200 + 10, 2)
    FROM customers
    WHERE id > @max - 2;"

    # 4. Order items (exercises the STORED generated column line_total)
    sql "
    INSERT INTO order_items (order_id, product_id, quantity, unit_price)
    SELECT o.id,
           FLOOR(RAND()*5)+1,
           FLOOR(RAND()*5)+1,
           ROUND(RAND()*100+5, 2)
    FROM (SELECT id FROM orders ORDER BY id DESC LIMIT 2) AS recent
    JOIN orders o ON o.id = recent.id;"

    # 5. Random customer updates (fires trg_customers_after_update → audit_log)
    sql "
    UPDATE customers
    SET tier = ELT(1 + FLOOR(RAND()*4), 'bronze','silver','gold','platinum')
    ORDER BY RAND()
    LIMIT 2;"

    # 6. Random order-status churn
    sql "
    UPDATE orders
    SET status = ELT(1 + FLOOR(RAND()*5), 'pending','processing','shipped','delivered','cancelled')
    ORDER BY RAND()
    LIMIT 3;"

    # 7. Child-first delete of one old synthetic customer (DELETE events
    #    across 3 tables; seed customers id<=5 are kept so the demo data
    #    stays recognizable).
    sql "
    SET @del_id = (SELECT id FROM customers WHERE id > 5 AND name LIKE 'User\\_%' ORDER BY RAND() LIMIT 1);
    DELETE FROM order_items WHERE order_id IN (SELECT id FROM orders WHERE customer_id = @del_id);
    DELETE FROM orders WHERE customer_id = @del_id;
    DELETE FROM customers WHERE id = @del_id;"

    if (( CYCLE % 12 == 1 )); then
        log "cycle $CYCLE"
    fi
    sleep 5
done
