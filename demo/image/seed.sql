-- ============================================================
-- Bintrail demo image seed (evaluation-only)
--
-- demo/sql/00-schema.sql minus the sysbench database, plus the
-- two users the demo image needs:
--
--   bintrail@127.0.0.1  — what `bintrail up` and `bintrail shim`
--                         connect as. ALL on *.* covers the doctor
--                         preflight (REPLICATION SLAVE/CLIENT) and
--                         lets `init` create the bintrail_index DB.
--   demo@%              — the evaluator's login, forwarded by
--                         ProxySQL to both the passthrough MySQL
--                         backend and the shim. DML on demo.* so
--                         evaluators can make their own changes and
--                         then time-travel them.
-- ============================================================

CREATE USER 'bintrail'@'127.0.0.1' IDENTIFIED WITH mysql_native_password BY 'bintrail';
GRANT ALL PRIVILEGES ON *.* TO 'bintrail'@'127.0.0.1';

CREATE USER 'demo'@'%' IDENTIFIED WITH mysql_native_password BY 'demo';

CREATE DATABASE IF NOT EXISTS demo;
GRANT SELECT, INSERT, UPDATE, DELETE ON demo.* TO 'demo'@'%';
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
    -- RESTRICT, not CASCADE: bintrail's preflight refuses FK cascades
    -- (cascaded child writes are invisible in the binlog as statements).
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

CREATE TABLE products (
    id              INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name            VARCHAR(200)    NOT NULL,
    base_price      DECIMAL(10,2)   NOT NULL,
    tax_rate        DECIMAL(5,4)    NOT NULL DEFAULT 0.10,
    created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP
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

-- ── Trigger ────────────────────────────────────────────────

DELIMITER ;;

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
-- orders id=1 exists from boot; the traffic generator mutates it
-- deterministically every cycle, so the README's time-travel query
-- always has history to return.

INSERT INTO customers (name, email, tier) VALUES
    ('Alice Andersen',   'alice@example.com',   'gold'),
    ('Bob Bakker',       'bob@example.com',      'silver'),
    ('Carol Chen',       'carol@example.com',    'platinum'),
    ('David Diaz',       'david@example.com',    'bronze'),
    ('Eve Eriksson',     'eve@example.com',      'silver');

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
