package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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
	initIndexDSN   string
	initPartitions int
	initS3Bucket   string
	initS3Region   string
)

func init() {
	initCmd.Flags().StringVar(&initIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	initCmd.Flags().IntVar(&initPartitions, "partitions", 48, "Number of hourly partitions to create starting from the current hour")
	initCmd.Flags().StringVar(&initS3Bucket, "s3-bucket", "", "S3 bucket name to create for archiving (optional)")
	initCmd.Flags().StringVar(&initS3Region, "s3-region", "us-east-1", "AWS region for the S3 bucket")
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

	// Create binlog_events with dynamic hourly partitions.
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

	if initS3Bucket != "" {
		fmt.Printf("\nSetting up S3 bucket...\n")
		if err := setupS3Bucket(cmd.Context(), initS3Bucket, initS3Region); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: could not create S3 bucket %q: %v\n\n", initS3Bucket, err)
			fmt.Fprint(os.Stderr, s3Instructions(initS3Bucket, initS3Region))
		} else {
			fmt.Printf("  ✓ S3 bucket: %s (region: %s)\n", initS3Bucket, initS3Region)
		}
	}

	fmt.Printf("\nInitialization complete. Index database: %s\n", dbName)
	return nil
}

// setupS3Bucket creates a private S3 bucket with a 1-year lifecycle expiry policy.
// It loads AWS credentials from the environment, ~/.aws/credentials, or the EC2 instance
// metadata service — whichever is available. If creation fails, the caller should print
// s3Instructions so the user can set up the bucket manually.
func setupS3Bucket(ctx context.Context, bucket, region string) error {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	client := s3.NewFromConfig(awsCfg)

	// Create the bucket. us-east-1 rejects CreateBucketConfiguration; all other
	// regions require it.
	createInput := &s3.CreateBucketInput{Bucket: aws.String(bucket)}
	if region != "us-east-1" {
		createInput.CreateBucketConfiguration = &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(region),
		}
	}
	if _, err := client.CreateBucket(ctx, createInput); err != nil {
		return fmt.Errorf("create bucket: %w", err)
	}

	// Block all public access.
	if _, err := client.PutPublicAccessBlock(ctx, &s3.PutPublicAccessBlockInput{
		Bucket: aws.String(bucket),
		PublicAccessBlockConfiguration: &types.PublicAccessBlockConfiguration{
			BlockPublicAcls:       aws.Bool(true),
			IgnorePublicAcls:      aws.Bool(true),
			BlockPublicPolicy:     aws.Bool(true),
			RestrictPublicBuckets: aws.Bool(true),
		},
	}); err != nil {
		return fmt.Errorf("block public access: %w", err)
	}

	// Add a lifecycle rule: expire all objects after 365 days.
	if _, err := client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: []types.LifecycleRule{
				{
					ID:     aws.String("bintrail-1yr-expiry"),
					Status: types.ExpirationStatusEnabled,
					Filter: &types.LifecycleRuleFilter{Prefix: aws.String("")},
					Expiration: &types.LifecycleExpiration{
						Days: aws.Int32(365),
					},
				},
			},
		},
	}); err != nil {
		return fmt.Errorf("set lifecycle policy: %w", err)
	}

	return nil
}

// s3Instructions returns a human-readable string with AWS CLI and console steps
// to manually create and configure the S3 bucket.
func s3Instructions(bucket, region string) string {
	var sb strings.Builder

	sb.WriteString("To create the bucket manually, run the following AWS CLI commands:\n\n")

	// Create bucket.
	sb.WriteString("  aws s3api create-bucket \\\n")
	fmt.Fprintf(&sb, "    --bucket %s \\\n", bucket)
	fmt.Fprintf(&sb, "    --region %s", region)
	if region != "us-east-1" {
		fmt.Fprintf(&sb, " \\\n    --create-bucket-configuration LocationConstraint=%s", region)
	}
	sb.WriteString("\n\n")

	// Block public access.
	sb.WriteString("  aws s3api put-public-access-block \\\n")
	fmt.Fprintf(&sb, "    --bucket %s \\\n", bucket)
	sb.WriteString("    --public-access-block-configuration \"BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true\"\n\n")

	// Lifecycle policy.
	sb.WriteString("  aws s3api put-bucket-lifecycle-configuration \\\n")
	fmt.Fprintf(&sb, "    --bucket %s \\\n", bucket)
	sb.WriteString("    --lifecycle-configuration '{\"Rules\":[{\"ID\":\"bintrail-1yr-expiry\",\"Status\":\"Enabled\",\"Filter\":{\"Prefix\":\"\"},\"Expiration\":{\"Days\":365}}]}'\n\n")

	// Console instructions.
	sb.WriteString("Or via the AWS Console (https://s3.console.aws.amazon.com/s3/bucket/create):\n")
	fmt.Fprintf(&sb, "  Bucket name             : %s\n", bucket)
	fmt.Fprintf(&sb, "  Region                  : %s\n", region)
	sb.WriteString("  Block all public access : enabled\n")
	sb.WriteString("  Lifecycle rule          : expire all objects after 365 days\n\n")

	return sb.String()
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

// createBinlogEventsTable generates the CREATE TABLE with N hourly partitions
// starting from the current hour (UTC), plus a p_future catch-all partition.
//
// Each partition p_YYYYMMDDHH holds events where TO_SECONDS(event_timestamp)
// is less than TO_SECONDS of the following hour (timezone-independent boundary).
// The p_future partition catches any events beyond the last pre-created partition.
func createBinlogEventsTable(db *sql.DB, numPartitions int) error {
	today := time.Now().UTC().Truncate(time.Hour)

	parts := make([]string, 0, numPartitions+1)
	for i := range numPartitions {
		hour := today.Add(time.Duration(i) * time.Hour)
		nextHour := hour.Add(time.Hour)
		parts = append(parts, fmt.Sprintf(
			"    PARTITION p_%s VALUES LESS THAN (TO_SECONDS('%s'))",
			hour.Format("2006010215"),
			nextHour.UTC().Format("2006-01-02 15:04:05"),
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
  PARTITION BY RANGE (TO_SECONDS(event_timestamp)) (
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
    is_generated     TINYINT(1)   NOT NULL DEFAULT 0 COMMENT '1 if STORED or VIRTUAL generated column',
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
