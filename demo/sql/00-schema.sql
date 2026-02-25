-- ============================================================
-- Bintrail demo schema
-- Exercises: explicit child-first deletes, triggers, generated columns
-- (STORED vs VIRTUAL), PK-less tables, sysbench OLTP
-- ============================================================

CREATE DATABASE IF NOT EXISTS demo;
USE demo;

-- ── Core tables ────────────────────────────────────────────

CREATE TABLE customers (
    id          INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(120)    NOT NULL,
    email       VARCHAR(200)    NOT NULL UNIQUE,
    tier        ENUM('bronze','silver','gold','platinum') NOT NULL DEFAULT 'bronze',
    created_at  DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE orders (
    id          INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    customer_id INT UNSIGNED    NOT NULL,
    status      ENUM('pending','processing','shipped','delivered','cancelled') NOT NULL DEFAULT 'pending',
    total       DECIMAL(10,2)   NOT NULL DEFAULT 0.00,
    created_at  DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id)
        REFERENCES customers(id) ON DELETE RESTRICT
) ENGINE=InnoDB;

CREATE TABLE order_items (
    id          INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    order_id    INT UNSIGNED    NOT NULL,
    product_id  INT UNSIGNED    NOT NULL,
    quantity    INT UNSIGNED    NOT NULL DEFAULT 1,
    unit_price  DECIMAL(10,2)   NOT NULL,
    -- STORED generated column: appears in binlog
    line_total  DECIMAL(10,2)   GENERATED ALWAYS AS (quantity * unit_price) STORED,
    CONSTRAINT fk_items_order FOREIGN KEY (order_id)
        REFERENCES orders(id) ON DELETE RESTRICT
) ENGINE=InnoDB;

-- Products table: has both STORED and VIRTUAL generated columns.
-- VIRTUAL column (price_with_tax) is NOT written to the binlog.
-- This means parser sees fewer columns than the snapshot → column-count
-- mismatch warning. All other tables index normally.
CREATE TABLE products (
    id              INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name            VARCHAR(200)    NOT NULL,
    base_price      DECIMAL(10,2)   NOT NULL,
    tax_rate        DECIMAL(5,4)    NOT NULL DEFAULT 0.10,
    -- VIRTUAL: excluded from binlog (demonstrates parser mismatch warning)
    price_with_tax  DECIMAL(10,2)   GENERATED ALWAYS AS (base_price * (1 + tax_rate)) VIRTUAL,
    -- STORED: included in binlog
    price_category  VARCHAR(10)     GENERATED ALWAYS AS (
        CASE
            WHEN base_price < 10   THEN 'budget'
            WHEN base_price < 100  THEN 'mid'
            ELSE 'premium'
        END
    ) STORED,
    created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- Customer aggregate stats — FK to customers
CREATE TABLE customer_stats (
    customer_id     INT UNSIGNED    NOT NULL PRIMARY KEY,
    order_count     INT UNSIGNED    NOT NULL DEFAULT 0,
    total_spent     DECIMAL(12,2)   NOT NULL DEFAULT 0.00,
    last_order_at   DATETIME,
    CONSTRAINT fk_stats_customer FOREIGN KEY (customer_id)
        REFERENCES customers(id) ON DELETE RESTRICT
) ENGINE=InnoDB;

-- Audit log written by trigger (no FK, append-only)
CREATE TABLE audit_log (
    id          INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    table_name  VARCHAR(64)     NOT NULL,
    record_id   INT UNSIGNED    NOT NULL,
    action      VARCHAR(16)     NOT NULL,
    old_values  JSON,
    new_values  JSON,
    created_at  DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- ── PK-less tables ─────────────────────────────────────────
-- Recovery uses allColsWhere fallback (all columns in WHERE clause)

CREATE TABLE event_log (
    event_type  VARCHAR(64)     NOT NULL,
    source      VARCHAR(128)    NOT NULL,
    payload     JSON,
    created_at  DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE metrics (
    metric_name VARCHAR(128)    NOT NULL,
    value       DOUBLE          NOT NULL,
    tags        JSON,
    recorded_at DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- ── Triggers ───────────────────────────────────────────────

DELIMITER ;;

-- After an order is inserted: upsert customer_stats
CREATE TRIGGER trg_orders_after_insert
AFTER INSERT ON orders
FOR EACH ROW
BEGIN
    INSERT INTO customer_stats (customer_id, order_count, total_spent, last_order_at)
        VALUES (NEW.customer_id, 1, NEW.total, NEW.created_at)
    ON DUPLICATE KEY UPDATE
        order_count   = order_count + 1,
        total_spent   = total_spent + NEW.total,
        last_order_at = NEW.created_at;
END;;

-- After a customer is updated: write to audit_log
CREATE TRIGGER trg_customers_after_update
AFTER UPDATE ON customers
FOR EACH ROW
BEGIN
    INSERT INTO audit_log (table_name, record_id, action, old_values, new_values)
    VALUES (
        'customers',
        OLD.id,
        'UPDATE',
        JSON_OBJECT('name', OLD.name, 'email', OLD.email, 'tier', OLD.tier),
        JSON_OBJECT('name', NEW.name, 'email', NEW.email, 'tier', NEW.tier)
    );
END;;

DELIMITER ;

-- ── Seed data ──────────────────────────────────────────────

INSERT INTO customers (name, email, tier) VALUES
    ('Alice Andersen',   'alice@example.com',   'gold'),
    ('Bob Bakker',       'bob@example.com',      'silver'),
    ('Carol Chen',       'carol@example.com',    'platinum'),
    ('David Diaz',       'david@example.com',    'bronze'),
    ('Eve Eriksson',     'eve@example.com',       'silver');

INSERT INTO products (name, base_price, tax_rate) VALUES
    ('Widget Standard',  9.99,  0.10),
    ('Widget Pro',       49.99, 0.10),
    ('Widget Enterprise',199.99,0.10),
    ('Gadget Basic',     14.99, 0.08),
    ('Gadget Plus',      79.99, 0.08);

INSERT INTO orders (customer_id, status, total) VALUES
    (1, 'delivered', 59.97),
    (2, 'shipped',   49.99),
    (3, 'processing',249.98),
    (1, 'pending',   14.99);

INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
    (1, 1, 3,  9.99),
    (2, 2, 1, 49.99),
    (3, 3, 1,199.99),
    (3, 4, 1, 14.99),
    (4, 4, 1, 14.99);

INSERT INTO event_log (event_type, source, payload) VALUES
    ('startup', 'demo', JSON_OBJECT('version', '1.0')),
    ('config_change', 'demo', JSON_OBJECT('key', 'timeout', 'old', 30, 'new', 60));

INSERT INTO metrics (metric_name, value, tags) VALUES
    ('query_latency_ms', 12.5, JSON_OBJECT('host', 'db1', 'env', 'demo')),
    ('connections_active', 42, JSON_OBJECT('host', 'db1'));

-- sysbench database (tables created by sysbench prepare)
CREATE DATABASE IF NOT EXISTS sbtest;
