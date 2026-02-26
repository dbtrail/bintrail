-- Bintrail index database DDL
-- Reference schema: tables are created by `bintrail init`, not by executing
-- this file directly. The binlog_events partition list is generated dynamically
-- by the init command based on the --partitions flag.
--
-- Database: binlog_index (configurable via --index-dsn)

-- ─────────────────────────────────────────────────────────────────────────────
-- Main events table (range-partitioned by event_timestamp)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS binlog_events (
    event_id        BIGINT UNSIGNED  AUTO_INCREMENT,
    binlog_file     VARCHAR(255)     NOT NULL,
    start_pos       BIGINT UNSIGNED  NOT NULL,
    end_pos         BIGINT UNSIGNED  NOT NULL,
    event_timestamp DATETIME         NOT NULL,
    gtid            VARCHAR(255)     DEFAULT NULL,
    schema_name     VARCHAR(64)      NOT NULL,
    table_name      VARCHAR(64)      NOT NULL,
    event_type      TINYINT UNSIGNED NOT NULL COMMENT '1=INSERT, 2=UPDATE, 3=DELETE',
    pk_values       VARCHAR(512)     NOT NULL  COMMENT 'PK values in ordinal order, pipe-delimited. e.g. 12345 or 12345|2',
    pk_hash         VARCHAR(64)      AS (SHA2(pk_values, 256)) STORED,
    changed_columns JSON             DEFAULT NULL COMMENT 'list of columns that changed (UPDATEs only)',
    row_before      JSON             DEFAULT NULL COMMENT 'full row before image (UPDATE, DELETE)',
    row_after       JSON             DEFAULT NULL COMMENT 'full row after image (INSERT, UPDATE)',
    PRIMARY KEY (event_id, event_timestamp),
    INDEX idx_row_lookup (schema_name, table_name, event_timestamp),
    INDEX idx_pk_hash    (schema_name, table_name, pk_hash, event_timestamp),
    INDEX idx_gtid       (gtid),
    INDEX idx_file_pos   (binlog_file, start_pos)
) ENGINE=InnoDB
  PARTITION BY RANGE (TO_DAYS(event_timestamp)) (
    -- Daily partitions are created by `bintrail init --partitions N`.
    -- The naming convention is p_YYYYMMDD; each partition holds events
    -- with event_timestamp < the following day. TO_DAYS() is timezone-independent.
    -- Example (7 days from 2026-02-19):
    --   PARTITION p_20260219 VALUES LESS THAN (TO_DAYS('2026-02-20')),
    --   PARTITION p_20260220 VALUES LESS THAN (TO_DAYS('2026-02-21')),
    --   ...
    --   PARTITION p_20260225 VALUES LESS THAN (TO_DAYS('2026-02-26')),
    PARTITION p_future VALUES LESS THAN MAXVALUE
  );

-- Query pattern for PK lookup (always use both columns to guard hash collisions):
--   SELECT * FROM binlog_events
--   WHERE schema_name = 'mydb'
--     AND table_name  = 'orders'
--     AND pk_hash     = SHA2('12345', 256)
--     AND pk_values   = '12345'
--     AND event_timestamp BETWEEN ... AND ...;

-- ─────────────────────────────────────────────────────────────────────────────
-- Schema snapshot table
-- Stores table/column metadata from information_schema at snapshot time.
-- Used by the indexer to map binlog column ordinals to column names.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS schema_snapshots (
    id               INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    snapshot_id      INT UNSIGNED NOT NULL,          -- groups all rows of one snapshot
    snapshot_time    DATETIME     NOT NULL,           -- time the snapshot was taken (set by Go)
    schema_name      VARCHAR(64)  NOT NULL,
    table_name       VARCHAR(64)  NOT NULL,
    column_name      VARCHAR(64)  NOT NULL,
    ordinal_position INT UNSIGNED NOT NULL,
    column_key       VARCHAR(3)   NOT NULL COMMENT 'PRI, UNI, MUL, or empty',
    data_type        VARCHAR(64)  NOT NULL,
    is_nullable      VARCHAR(3)   NOT NULL,
    column_default   TEXT         DEFAULT NULL,
    is_generated     TINYINT(1)   NOT NULL DEFAULT 0 COMMENT '1 if STORED or VIRTUAL generated column',
    INDEX idx_snapshot_id    (snapshot_id),
    INDEX idx_snapshot_table (snapshot_id, schema_name, table_name)
) ENGINE=InnoDB;
-- Note: snapshot_id is NOT the auto-increment row ID. It is allocated by the
-- snapshot command via MAX(snapshot_id)+1 and shared by all rows of a snapshot.
-- NewResolver(db, 0) loads the latest snapshot; NewResolver(db, N) loads snapshot N.

-- ─────────────────────────────────────────────────────────────────────────────
-- Index state table
-- Tracks which binlog files have been indexed to prevent re-processing.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS index_state (
    binlog_file    VARCHAR(255) PRIMARY KEY,
    file_size      BIGINT UNSIGNED NOT NULL,
    last_position  BIGINT UNSIGNED NOT NULL COMMENT 'last parsed byte position in the file',
    events_indexed BIGINT UNSIGNED NOT NULL DEFAULT 0,
    status         ENUM('in_progress','completed','failed') NOT NULL,
    started_at     DATETIME NOT NULL,
    completed_at   DATETIME DEFAULT NULL,
    error_message  TEXT     DEFAULT NULL
) ENGINE=InnoDB;
