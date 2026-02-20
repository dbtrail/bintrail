package main

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Create index tables in the target MySQL database",
	Long: `Creates the binlog_events (partitioned), schema_snapshots, and index_state
tables in the target MySQL database. The database is created if it does not exist.`,
	RunE: runInit,
}

var (
	initIndexDSN  string
	initPartitions int
)

func init() {
	initCmd.Flags().StringVar(&initIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	initCmd.Flags().IntVar(&initPartitions, "partitions", 7, "Number of daily partitions to create starting from today")
	_ = initCmd.MarkFlagRequired("index-dsn")

	rootCmd.AddCommand(initCmd)
}

func runInit(cmd *cobra.Command, args []string) error {
	cfg, err := mysql.ParseDSN(initIndexDSN)
	if err != nil {
		return fmt.Errorf("invalid --index-dsn: %w", err)
	}

	dbName := cfg.DBName
	if dbName == "" {
		return fmt.Errorf("--index-dsn must include a database name (e.g. user:pass@tcp(host:3306)/binlog_index)")
	}

	// Step 1: Connect to the server without a database to CREATE DATABASE IF NOT EXISTS.
	// We avoid using the pooled connection for USE statements — reconnect with the DB
	// name baked into the DSN instead.
	if err := ensureDatabase(cfg, dbName); err != nil {
		return err
	}

	// Step 2: Reconnect with the database name in the DSN.
	db, err := sql.Open("mysql", initIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to open index database connection: %w", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to connect to index database %q: %w", dbName, err)
	}

	// Create binlog_events with dynamic daily partitions.
	if err := createBinlogEventsTable(db, initPartitions); err != nil {
		return fmt.Errorf("failed to create binlog_events: %w", err)
	}
	fmt.Println("  ✓ binlog_events")

	if _, err := db.Exec(ddlSchemaSnapshots); err != nil {
		return fmt.Errorf("failed to create schema_snapshots: %w", err)
	}
	fmt.Println("  ✓ schema_snapshots")

	if _, err := db.Exec(ddlIndexState); err != nil {
		return fmt.Errorf("failed to create index_state: %w", err)
	}
	fmt.Println("  ✓ index_state")

	if _, err := db.Exec(ddlStreamState); err != nil {
		return fmt.Errorf("failed to create stream_state: %w", err)
	}
	fmt.Println("  ✓ stream_state")

	fmt.Printf("\nInitialization complete. Index database: %s\n", dbName)
	return nil
}

// ensureDatabase creates the target database if it does not already exist.
// It connects without a database name so the CREATE DATABASE always succeeds.
func ensureDatabase(cfg *mysql.Config, dbName string) error {
	serverCfg := *cfg
	serverCfg.DBName = ""

	db, err := sql.Open("mysql", serverCfg.FormatDSN())
	if err != nil {
		return fmt.Errorf("failed to open server connection: %w", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to connect to MySQL server: %w", err)
	}

	q := fmt.Sprintf(
		"CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
		dbName,
	)
	if _, err := db.Exec(q); err != nil {
		return fmt.Errorf("failed to create database %q: %w", dbName, err)
	}

	fmt.Printf("Database %q ready.\n", dbName)
	return nil
}

// createBinlogEventsTable generates the CREATE TABLE with N daily partitions
// starting from today (UTC), plus a p_future catch-all partition.
//
// Each partition p_YYYYMMDD holds events where TO_DAYS(event_timestamp)
// is less than TO_DAYS of the following day (timezone-independent boundary).
// The p_future partition catches any events beyond the last pre-created partition.
func createBinlogEventsTable(db *sql.DB, numPartitions int) error {
	today := time.Now().UTC().Truncate(24 * time.Hour)

	parts := make([]string, 0, numPartitions+1)
	for i := range numPartitions {
		day := today.AddDate(0, 0, i)
		nextDay := day.AddDate(0, 0, 1)
		parts = append(parts, fmt.Sprintf(
			"    PARTITION p_%s VALUES LESS THAN (TO_DAYS('%s'))",
			day.Format("20060102"),
			nextDay.UTC().Format("2006-01-02"),
		))
	}
	parts = append(parts, "    PARTITION p_future VALUES LESS THAN MAXVALUE")

	ddl := `CREATE TABLE IF NOT EXISTS binlog_events (
    event_id        BIGINT UNSIGNED AUTO_INCREMENT,
    binlog_file     VARCHAR(255)     NOT NULL,
    start_pos       BIGINT UNSIGNED  NOT NULL,
    end_pos         BIGINT UNSIGNED  NOT NULL,
    event_timestamp DATETIME         NOT NULL,
    gtid            VARCHAR(255)     DEFAULT NULL,
    schema_name     VARCHAR(64)      NOT NULL,
    table_name      VARCHAR(64)      NOT NULL,
    event_type      TINYINT UNSIGNED NOT NULL COMMENT '1=INSERT, 2=UPDATE, 3=DELETE',
    pk_values       VARCHAR(512)     NOT NULL COMMENT 'PK values in ordinal order, pipe-delimited. e.g. 12345 or 12345|2',
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
` + strings.Join(parts, ",\n") + `
)`

	_, err := db.Exec(ddl)
	return err
}

// ---------------------------------------------------------------------------
// Static DDL — no partition logic needed for these tables.
// ---------------------------------------------------------------------------

const ddlSchemaSnapshots = `CREATE TABLE IF NOT EXISTS schema_snapshots (
    id               INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    snapshot_id      INT UNSIGNED NOT NULL,
    snapshot_time    DATETIME     NOT NULL,
    schema_name      VARCHAR(64)  NOT NULL,
    table_name       VARCHAR(64)  NOT NULL,
    column_name      VARCHAR(64)  NOT NULL,
    ordinal_position INT UNSIGNED NOT NULL,
    column_key       VARCHAR(3)   NOT NULL COMMENT 'PRI, UNI, MUL, or empty',
    data_type        VARCHAR(64)  NOT NULL,
    is_nullable      VARCHAR(3)   NOT NULL,
    column_default   TEXT         DEFAULT NULL,
    INDEX idx_snapshot_id    (snapshot_id),
    INDEX idx_snapshot_table (snapshot_id, schema_name, table_name)
) ENGINE=InnoDB`

const ddlStreamState = `CREATE TABLE IF NOT EXISTS stream_state (
    id               INT UNSIGNED    PRIMARY KEY DEFAULT 1,
    mode             ENUM('position','gtid') NOT NULL,
    binlog_file      VARCHAR(255)    NOT NULL DEFAULT '',
    binlog_position  BIGINT UNSIGNED NOT NULL DEFAULT 0,
    gtid_set         TEXT            DEFAULT NULL,
    events_indexed   BIGINT UNSIGNED NOT NULL DEFAULT 0,
    last_event_time  DATETIME        DEFAULT NULL,
    last_checkpoint  DATETIME        NOT NULL,
    server_id        INT UNSIGNED    NOT NULL,
    CONSTRAINT single_row CHECK (id = 1)
) ENGINE=InnoDB`

const ddlIndexState = `CREATE TABLE IF NOT EXISTS index_state (
    binlog_file    VARCHAR(255) PRIMARY KEY,
    file_size      BIGINT UNSIGNED NOT NULL,
    last_position  BIGINT UNSIGNED NOT NULL COMMENT 'last parsed position',
    events_indexed BIGINT UNSIGNED NOT NULL DEFAULT 0,
    status         ENUM('in_progress','completed','failed') NOT NULL,
    started_at     DATETIME NOT NULL,
    completed_at   DATETIME DEFAULT NULL,
    error_message  TEXT     DEFAULT NULL
) ENGINE=InnoDB`
