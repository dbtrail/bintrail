-- Seed for the e2e/shim Docker stack.
--
-- Creates two schemas in one MySQL container:
--
--   appdb           — the simulated "production" database the customer's
--                     app would target. Holds the live `orders` table
--                     so passthrough queries (no virtual schema) have
--                     a real backend to land on.
--
--   bintrail_index  — the bintrail row-event index. binlog_events is
--                     hand-seeded with a synthetic sequence for
--                     appdb.orders id=42 (INSERT → UPDATE → DELETE).
--                     This bypasses parser+indexer on purpose: the
--                     shim e2e is about the wire-protocol + ProxySQL
--                     routing path, not the binlog ingestion path
--                     which the integration suite already covers.
--
-- All event timestamps are anchored on 2026-05-04 (UTC) so the
-- expected results are deterministic regardless of when the test
-- runs. Partitions cover that day's relevant hours plus p_future.

CREATE DATABASE IF NOT EXISTS appdb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE appdb;

CREATE TABLE orders (
    id    INT PRIMARY KEY,
    sku   VARCHAR(64)  NOT NULL,
    qty   INT          NOT NULL,
    note  VARCHAR(255) DEFAULT NULL
) ENGINE=InnoDB;

-- A live row that the passthrough hostgroup returns. Its values
-- intentionally differ from any of the binlog images below so the
-- e2e test can distinguish "ProxySQL routed to passthrough"
-- (returns this row) from "ProxySQL routed to shim" (returns a
-- reconstructed historical image).
INSERT INTO orders (id, sku, qty, note) VALUES
    (42, 'LIVE-SKU', 999, 'live-row-from-passthrough');

-- testuser is the credential the wire-protocol client uses when it
-- talks to ProxySQL. ProxySQL forwards the same username/password
-- to backends — so for the passthrough hostgroup to actually reach
-- appdb.orders, MySQL has to recognise testuser too.
-- The shim's own backend connection uses root (--index-dsn), not
-- testuser, so this grant is scoped to appdb only.
CREATE USER 'testuser'@'%' IDENTIFIED WITH mysql_native_password BY 'testpw';
GRANT SELECT ON appdb.* TO 'testuser'@'%';

CREATE DATABASE IF NOT EXISTS bintrail_index CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE bintrail_index;

-- DDL mirrors `bintrail init`'s output for binlog_events. The shim's
-- planner reads information_schema.PARTITIONS scoped to this schema,
-- so the partition list must cover every hour we insert into.
CREATE TABLE binlog_events (
    event_id        BIGINT UNSIGNED AUTO_INCREMENT,
    binlog_file     VARCHAR(255)     NOT NULL,
    start_pos       BIGINT UNSIGNED  NOT NULL,
    end_pos         BIGINT UNSIGNED  NOT NULL,
    event_timestamp DATETIME         NOT NULL,
    gtid            VARCHAR(255)     DEFAULT NULL,
    connection_id   INT UNSIGNED     DEFAULT NULL,
    schema_name     VARCHAR(64)      NOT NULL,
    table_name      VARCHAR(64)      NOT NULL,
    event_type      TINYINT UNSIGNED NOT NULL,
    pk_values       VARCHAR(512)     NOT NULL,
    pk_hash         VARCHAR(64)      AS (SHA2(pk_values, 256)) STORED,
    changed_columns JSON             DEFAULT NULL,
    row_before      JSON             DEFAULT NULL,
    row_after       JSON             DEFAULT NULL,
    schema_version  INT UNSIGNED     NOT NULL DEFAULT 0,
    PRIMARY KEY (event_id, event_timestamp),
    INDEX idx_row_lookup (schema_name, table_name, event_timestamp),
    INDEX idx_pk_hash    (schema_name, table_name, pk_hash, event_timestamp),
    INDEX idx_gtid       (gtid)
) ENGINE=InnoDB
  PARTITION BY RANGE (TO_SECONDS(event_timestamp)) (
    PARTITION p_2026050409 VALUES LESS THAN (TO_SECONDS('2026-05-04 10:00:00')),
    PARTITION p_2026050410 VALUES LESS THAN (TO_SECONDS('2026-05-04 11:00:00')),
    PARTITION p_2026050411 VALUES LESS THAN (TO_SECONDS('2026-05-04 12:00:00')),
    PARTITION p_2026050412 VALUES LESS THAN (TO_SECONDS('2026-05-04 13:00:00')),
    PARTITION p_2026050413 VALUES LESS THAN (TO_SECONDS('2026-05-04 14:00:00')),
    PARTITION p_2026050414 VALUES LESS THAN (TO_SECONDS('2026-05-04 15:00:00')),
    PARTITION p_2026050415 VALUES LESS THAN (TO_SECONDS('2026-05-04 16:00:00')),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- schema_snapshots tells the shim what the source table's column
-- ordinal_position is, so SELECT * against _flashback / _snapshot
-- returns columns in DDL order (id, sku, qty, note) instead of
-- the alphabetical fallback (id, note, qty, sku).
--
-- Without a snapshot the shim degrades silently to alphabetical
-- key order — wire output is still valid SQL, just doesn't match
-- what `SELECT * FROM appdb.orders` directly would return.
CREATE TABLE schema_snapshots (
    id               INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    snapshot_id      INT UNSIGNED NOT NULL,
    snapshot_time    DATETIME     NOT NULL,
    schema_name      VARCHAR(64)  NOT NULL,
    table_name       VARCHAR(64)  NOT NULL,
    column_name      VARCHAR(64)  NOT NULL,
    ordinal_position INT UNSIGNED NOT NULL,
    column_key       VARCHAR(3)   NOT NULL,
    data_type        VARCHAR(64)  NOT NULL,
    column_type      VARCHAR(128) NOT NULL DEFAULT '',
    is_nullable      VARCHAR(3)   NOT NULL,
    column_default   TEXT         DEFAULT NULL,
    is_generated     TINYINT(1)   NOT NULL DEFAULT 0,
    INDEX idx_snapshot_id    (snapshot_id),
    INDEX idx_snapshot_table (snapshot_id, schema_name, table_name)
) ENGINE=InnoDB;

INSERT INTO schema_snapshots
    (snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, column_type, is_nullable)
VALUES
    (1, '2026-05-04 09:00:00', 'appdb', 'orders', 'id',   1, 'PRI', 'int',     'int',         'NO'),
    (1, '2026-05-04 09:00:00', 'appdb', 'orders', 'sku',  2, '',    'varchar', 'varchar(64)', 'NO'),
    (1, '2026-05-04 09:00:00', 'appdb', 'orders', 'qty',  3, '',    'int',     'int',         'NO'),
    (1, '2026-05-04 09:00:00', 'appdb', 'orders', 'note', 4, '',    'varchar', 'varchar(255)','YES');

-- archive_state is read by query.Plan and query.ResolveArchiveSources.
-- Empty: the live partitions above already cover every event hour,
-- so the planner finds no gap. --allow-gaps on the shim command is
-- defensive belt-and-braces in case a future seed change widens
-- the time range without extending the partition list.
CREATE TABLE archive_state (
    id              INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    partition_name  VARCHAR(20) NOT NULL,
    bintrail_id     VARCHAR(36),
    local_path      VARCHAR(1024),
    file_size_bytes BIGINT UNSIGNED,
    row_count       BIGINT UNSIGNED,
    s3_bucket       VARCHAR(255),
    s3_key          VARCHAR(1024),
    s3_uploaded_at  DATETIME,
    archived_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_partition (partition_name, bintrail_id)
) ENGINE=InnoDB;

-- Synthetic event sequence for appdb.orders id=42:
--
--   10:00 → INSERT  (sku=ABC-1, qty=1)
--   12:00 → UPDATE  (qty 1 → 2)
--   14:00 → DELETE
--
-- pk_hash is omitted (STORED generated column derives it from
-- pk_values). row_before is empty for INSERT, row_after is empty
-- for DELETE — matches the parser's output shape.
INSERT INTO binlog_events
    (binlog_file, start_pos, end_pos, event_timestamp, gtid, schema_name, table_name, event_type, pk_values, changed_columns, row_before, row_after)
VALUES
    ('mysql-bin.000001',  100,  200, '2026-05-04 10:00:00', 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1', 'appdb', 'orders', 1, '42',
        NULL,
        NULL,
        JSON_OBJECT('id', 42, 'sku', 'ABC-1', 'qty', 1, 'note', 'initial')),
    ('mysql-bin.000001',  300,  400, '2026-05-04 12:00:00', 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:2', 'appdb', 'orders', 2, '42',
        JSON_ARRAY('qty'),
        JSON_OBJECT('id', 42, 'sku', 'ABC-1', 'qty', 1, 'note', 'initial'),
        JSON_OBJECT('id', 42, 'sku', 'ABC-1', 'qty', 2, 'note', 'initial')),
    ('mysql-bin.000001',  500,  600, '2026-05-04 14:00:00', 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:3', 'appdb', 'orders', 3, '42',
        NULL,
        JSON_OBJECT('id', 42, 'sku', 'ABC-1', 'qty', 2, 'note', 'initial'),
        NULL);
