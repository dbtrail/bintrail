#!/usr/bin/env bash
# Generates realistic + adversarial MySQL traffic for bintrail demo.
# Runs two concurrent workloads:
#   1. sysbench oltp_read_write on the sbtest database (background)
#   2. Custom chaos loop on the demo database (foreground)

set -euo pipefail

MYSQL_HOST="${MYSQL_HOST:-mysql}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASS="${MYSQL_PASS:-demo}"
MYSQL_DB="${MYSQL_DB:-demo}"

mysql_cmd() {
    mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASS" \
          --protocol=tcp --silent "$@"
}

sql() {
    mysql_cmd "$MYSQL_DB" -e "$1"
}

log() { echo "[traffic] $(date '+%H:%M:%S') $*"; }

# ── Wait for MySQL ──────────────────────────────────────────
log "Waiting for MySQL..."
until mysql_cmd -e "SELECT 1" &>/dev/null; do sleep 1; done
log "MySQL ready."

# ── Wait for demo schema ────────────────────────────────────
log "Waiting for demo.customers..."
until mysql_cmd demo -e "SELECT 1 FROM customers LIMIT 1" &>/dev/null; do sleep 1; done
log "Schema ready."

# ── sysbench prepare + run ──────────────────────────────────
SYSBENCH_ARGS=(
    --db-driver=mysql
    --mysql-host="$MYSQL_HOST"
    --mysql-port="$MYSQL_PORT"
    --mysql-user="$MYSQL_USER"
    --mysql-password="$MYSQL_PASS"
    --mysql-db=sbtest
    --tables=4
    --table-size=1000
)

log "Cleaning up any existing sysbench tables (idempotent on restart)..."
sysbench oltp_read_write "${SYSBENCH_ARGS[@]}" cleanup 2>/dev/null || true

log "Preparing sysbench tables..."
sysbench oltp_read_write "${SYSBENCH_ARGS[@]}" prepare

log "Starting sysbench workload (background)..."
sysbench oltp_read_write "${SYSBENCH_ARGS[@]}" \
    --threads=2 \
    --time=0 \
    --report-interval=30 \
    run &
SYSBENCH_PID=$!

# ── Chaos loop ─────────────────────────────────────────────
CYCLE=0
log "Starting chaos loop..."

while true; do
    CYCLE=$((CYCLE + 1))
    log "Chaos cycle $CYCLE"

    # 1. Create 5 new customers
    for i in $(seq 1 5); do
        TS=$(date '+%s%N')
        TIER=$(echo "bronze silver gold platinum" | tr ' ' '\n' | shuf -n1)
        sql "INSERT INTO customers (name, email, tier)
             VALUES ('User_${CYCLE}_${i}', 'u${TS}_${i}@demo.test', '$TIER')"
    done

    # 2. Create orders for the 5 most-recent customers
    #    The trg_orders_after_insert trigger fires on each INSERT → upserts customer_stats
    sql "
    SET @max = (SELECT MAX(id) FROM customers);
    INSERT INTO orders (customer_id, status, total)
    SELECT id,
           ELT(1 + FLOOR(RAND()*5), 'pending','processing','shipped','delivered','cancelled'),
           ROUND(RAND()*200 + 10, 2)
    FROM customers
    WHERE id > @max - 5;"

    # 3. Add order items (exercises STORED generated column line_total)
    # Note: MySQL 8.0 does not support LIMIT inside IN subqueries; use a JOIN instead.
    sql "
    INSERT INTO order_items (order_id, product_id, quantity, unit_price)
    SELECT o.id,
           FLOOR(RAND()*5)+1,
           FLOOR(RAND()*5)+1,
           ROUND(RAND()*100+5, 2)
    FROM (SELECT id FROM orders ORDER BY id DESC LIMIT 5) AS recent
    JOIN orders o ON o.id = recent.id;"

    # 4. Update 3 random customers (fires trg_customers_after_update → audit_log)
    sql "
    UPDATE customers
    SET tier = ELT(1 + FLOOR(RAND()*4), 'bronze','silver','gold','platinum'),
        name = CONCAT(name, ' ')
    ORDER BY RAND()
    LIMIT 3;"

    # 5. Update 5 random order statuses
    sql "
    UPDATE orders
    SET status = ELT(1 + FLOOR(RAND()*5), 'pending','processing','shipped','delivered','cancelled')
    ORDER BY RAND()
    LIMIT 5;"

    # 6. Insert products (VIRTUAL price_with_tax + STORED price_category)
    #    Parser will warn about column count mismatch for this table (VIRTUAL col)
    for i in $(seq 1 3); do
        PRICE=$(awk "BEGIN{printf \"%.2f\", $((RANDOM % 300 + 1)) + 0.99}")
        sql "INSERT INTO products (name, base_price, tax_rate)
             VALUES ('Product_${CYCLE}_${i}', $PRICE, 0.10)"
    done

    # 7. Insert into PK-less tables (exercises allColsWhere recovery fallback)
    sql "INSERT INTO event_log (event_type, source, payload)
         VALUES ('cycle', 'traffic', JSON_OBJECT('cycle', $CYCLE, 'ts', NOW()))"
    sql "INSERT INTO metrics (metric_name, value, tags)
         VALUES ('active_customers',
                 (SELECT COUNT(*) FROM customers),
                 JSON_OBJECT('cycle', $CYCLE))"
    sql "INSERT INTO metrics (metric_name, value, tags)
         VALUES ('total_orders',
                 (SELECT COUNT(*) FROM orders),
                 JSON_OBJECT('cycle', $CYCLE))"

    # 8. Explicit child-first delete — removes 1 random customer and all its children
    #    Order matters: order_items → orders → customer_stats → customers
    #    Generates DELETE events across 4 tables in one transaction
    sql "
    SET @del_id = (SELECT id FROM customers ORDER BY RAND() LIMIT 1);
    DELETE FROM order_items WHERE order_id IN (SELECT id FROM orders WHERE customer_id = @del_id);
    DELETE FROM orders WHERE customer_id = @del_id;
    DELETE FROM customer_stats WHERE customer_id = @del_id;
    DELETE FROM customers WHERE id = @del_id;"

    # 9. Delete old PK-less rows (PK-less DELETE events)
    sql "DELETE FROM event_log WHERE event_type = 'cycle' ORDER BY created_at LIMIT 3"
    sql "DELETE FROM metrics WHERE metric_name = 'active_customers' ORDER BY recorded_at LIMIT 2"

    sleep 5
done
