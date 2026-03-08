package main

import (
	"context"
	"database/sql"
	"errors"
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

	"github.com/bintrail/bintrail/internal/cliutil"
	"github.com/bintrail/bintrail/internal/serverid"
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
	initEncrypt    bool
	initS3Bucket   string
	initS3Region   string
	initS3ARN      string
	initFormat     string
)

func init() {
	initCmd.Flags().StringVar(&initIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	initCmd.Flags().IntVar(&initPartitions, "partitions", 48, "Number of hourly partitions to create; partitions span from (N-1) hours ago to the current hour so historical binlog events are properly distributed")
	initCmd.Flags().BoolVar(&initEncrypt, "encrypt", false, "Enable InnoDB tablespace encryption on binlog_events (requires a keyring plugin on the MySQL server; adds ENCRYPTION='Y' to the table DDL)")
	initCmd.Flags().StringVar(&initS3Bucket, "s3-bucket", "", "S3 bucket name to create for archiving (optional; mutually exclusive with --s3-arn)")
	initCmd.Flags().StringVar(&initS3Region, "s3-region", "us-east-1", "AWS region for the S3 bucket (required with --s3-bucket; ignored with --s3-arn since the SDK resolves the bucket region automatically)")
	initCmd.Flags().StringVar(&initS3ARN, "s3-arn", "", "ARN of an existing S3 bucket to use for archiving (optional; mutually exclusive with --s3-bucket)")
	initCmd.Flags().StringVar(&initFormat, "format", "text", "Output format: text or json")
	_ = initCmd.MarkFlagRequired("index-dsn")
	bindCommandEnv(initCmd)

	rootCmd.AddCommand(initCmd)
}

func runInit(cmd *cobra.Command, args []string) error {
	if !cliutil.IsValidOutputFormat(initFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", initFormat)
	}
	if initS3Bucket != "" && initS3ARN != "" {
		return fmt.Errorf("--s3-bucket and --s3-arn are mutually exclusive: use --s3-bucket to create a new bucket, or --s3-arn to use an existing one")
	}

	// Validate the ARN format before doing any database work so that a typo
	// does not leave the user with a half-initialized state and a non-zero exit.
	var s3ARNBucket, s3ARNPartition string
	if initS3ARN != "" {
		var err error
		s3ARNBucket, s3ARNPartition, err = parseS3ARN(initS3ARN)
		if err != nil {
			return fmt.Errorf("invalid --s3-arn: %w", err)
		}
	}

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

	// Track created tables for JSON output.
	var tablesCreated []string
	logTable := func(name string) {
		tablesCreated = append(tablesCreated, name)
		if initFormat != "json" {
			fmt.Printf("  \u2713 %s\n", name)
		}
	}

	// Create binlog_events with dynamic hourly partitions.
	if err := createBinlogEventsTable(db, initPartitions, initEncrypt); err != nil {
		return fmt.Errorf("failed to create binlog_events: %w", err)
	}
	logTable("binlog_events")

	// If --encrypt was requested, verify that the table actually has encryption
	// enabled. CREATE TABLE IF NOT EXISTS is a no-op when the table already
	// exists, so a pre-existing unencrypted table will silently remain
	// unencrypted. Warn the operator so they can encrypt it manually.
	if initEncrypt {
		var createOpts string
		row := db.QueryRowContext(cmd.Context(),
			`SELECT COALESCE(CREATE_OPTIONS, '') FROM information_schema.TABLES
			 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'binlog_events'`)
		if err := row.Scan(&createOpts); err == nil &&
			!strings.Contains(strings.ToUpper(createOpts), "ENCRYPTION=Y") {
			if initFormat != "json" {
				fmt.Fprintf(os.Stderr, "Warning: binlog_events already exists without encryption.\n"+
					"To encrypt it, run: ALTER TABLE binlog_events ENCRYPTION='Y'\n")
			}
		}
	}

	if _, err := db.Exec(ddlSchemaSnapshots); err != nil {
		return fmt.Errorf("failed to create schema_snapshots: %w", err)
	}
	logTable("schema_snapshots")

	if _, err := db.Exec(ddlIndexState); err != nil {
		return fmt.Errorf("failed to create index_state: %w", err)
	}
	logTable("index_state")

	if _, err := db.Exec(ddlStreamState); err != nil {
		return fmt.Errorf("failed to create stream_state: %w", err)
	}
	logTable("stream_state")

	if _, err := db.Exec(ddlBintrailServers); err != nil {
		return fmt.Errorf("failed to create bintrail_servers: %w", err)
	}
	logTable("bintrail_servers")

	if _, err := db.Exec(ddlBintrailServerChanges); err != nil {
		return fmt.Errorf("failed to create bintrail_server_changes: %w", err)
	}
	logTable("bintrail_server_changes")

	if _, err := db.Exec(ddlTableFlags); err != nil {
		return fmt.Errorf("failed to create table_flags: %w", err)
	}
	logTable("table_flags")

	if _, err := db.Exec(ddlProfiles); err != nil {
		return fmt.Errorf("failed to create profiles: %w", err)
	}
	logTable("profiles")

	if _, err := db.Exec(ddlAccessRules); err != nil {
		return fmt.Errorf("failed to create access_rules: %w", err)
	}
	logTable("access_rules")

	if _, err := db.Exec(ddlArchiveState); err != nil {
		return fmt.Errorf("failed to create archive_state: %w", err)
	}
	logTable("archive_state")

	if _, err := db.Exec(ddlSchemaChanges); err != nil {
		return fmt.Errorf("failed to create schema_changes: %w", err)
	}
	logTable("schema_changes")

	var s3Result *string
	if initS3Bucket != "" {
		if initFormat != "json" {
			fmt.Printf("\nSetting up S3 bucket...\n")
		}
		if err := setupS3Bucket(cmd.Context(), initS3Bucket, initS3Region); err != nil {
			if initFormat != "json" {
				fmt.Fprintf(os.Stderr, "Warning: could not create S3 bucket %q: %v\n\n", initS3Bucket, err)
				fmt.Fprint(os.Stderr, s3Instructions(initS3Bucket, initS3Region))
			}
		} else {
			s := fmt.Sprintf("%s (region: %s)", initS3Bucket, initS3Region)
			s3Result = &s
			if initFormat != "json" {
				fmt.Printf("  \u2713 S3 bucket: %s\n", s)
			}
		}
	}

	if initS3ARN != "" {
		if initFormat != "json" {
			fmt.Printf("\nVerifying existing S3 bucket...\n")
		}
		if err := verifyS3Bucket(cmd.Context(), s3ARNBucket, initS3Region); err != nil {
			if initFormat != "json" {
				fmt.Fprintf(os.Stderr, "Warning: could not verify S3 bucket %q: %v\n\n", s3ARNBucket, err)
				fmt.Fprint(os.Stderr, s3IAMInstructions(s3ARNBucket, s3ARNPartition))
			}
		} else {
			s := fmt.Sprintf("%s (ARN: %s)", s3ARNBucket, initS3ARN)
			s3Result = &s
			if initFormat != "json" {
				fmt.Printf("  \u2713 S3 bucket: %s\n", s)
			}
		}
	}

	if initFormat == "json" {
		result := struct {
			Database      string   `json:"database"`
			TablesCreated []string `json:"tables_created"`
			S3Bucket      *string  `json:"s3_bucket,omitempty"`
		}{
			Database:      dbName,
			TablesCreated: tablesCreated,
			S3Bucket:      s3Result,
		}
		return outputJSON(result)
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
	// regions require it. BucketAlreadyOwnedByYou is treated as success so that
	// re-running bintrail init is idempotent (mirrors CREATE TABLE IF NOT EXISTS).
	createInput := &s3.CreateBucketInput{Bucket: aws.String(bucket)}
	if region != "us-east-1" {
		createInput.CreateBucketConfiguration = &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(region),
		}
	}
	if _, err := client.CreateBucket(ctx, createInput); err != nil {
		var alreadyOwned *types.BucketAlreadyOwnedByYou
		if !errors.As(err, &alreadyOwned) {
			return fmt.Errorf("create bucket: %w", err)
		}
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

// parseS3ARN extracts the bucket name and partition from an S3 bucket ARN.
// S3 bucket ARNs have the form arn:partition:s3:::bucket-name where partition
// is "aws", "aws-cn", or "aws-us-gov". Both region (parts[3]) and account-id
// (parts[4]) are empty for bucket-level ARNs. Object ARNs contain a "/" in the
// resource field and are rejected to avoid a confusing HeadBucket error.
func parseS3ARN(arn string) (bucket, partition string, err error) {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) != 6 {
		return "", "", fmt.Errorf("expected 6 colon-separated fields, got %d (want arn:partition:s3:::bucket-name)", len(parts))
	}
	if parts[0] != "arn" {
		return "", "", fmt.Errorf("must start with \"arn:\", got %q", parts[0])
	}
	if parts[2] != "s3" {
		return "", "", fmt.Errorf("service must be \"s3\", got %q", parts[2])
	}
	if parts[3] != "" || parts[4] != "" {
		return "", "", fmt.Errorf("S3 bucket ARNs have empty region and account fields (got %q and %q); this looks like an object or access-point ARN", parts[3], parts[4])
	}
	bucket = parts[5]
	if bucket == "" {
		return "", "", fmt.Errorf("bucket name is empty in ARN %q", arn)
	}
	if strings.Contains(bucket, "/") {
		return "", "", fmt.Errorf("bucket name %q contains \"/\"; pass a bucket ARN (arn:aws:s3:::bucket-name), not an object ARN", bucket)
	}
	return bucket, parts[1], nil
}

// verifyS3Bucket checks that an S3 bucket exists and is accessible by the
// current AWS credentials using a HeadBucket request.
func verifyS3Bucket(ctx context.Context, bucket, region string) error {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	client := s3.NewFromConfig(awsCfg)
	if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)}); err != nil {
		return fmt.Errorf("head bucket: %w", err)
	}
	return nil
}

// s3IAMInstructions returns a human-readable IAM policy snippet the caller
// needs to attach to the IAM role or user that runs bintrail, so it can read
// and write Parquet archives to the bucket. partition must be the ARN partition
// string ("aws", "aws-cn", or "aws-us-gov") so the resource ARNs are correct.
func s3IAMInstructions(bucket, partition string) string {
	return fmt.Sprintf(`To allow bintrail to read and write archives in this bucket, attach the
following IAM policy to the role or user running bintrail:

  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Sid": "BintrailS3Access",
        "Effect": "Allow",
        "Action": [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ],
        "Resource": [
          "arn:%s:s3:::%s",
          "arn:%s:s3:::%s/*"
        ]
      }
    ]
  }

Minimum permissions explained:
  s3:PutObject    — write Parquet archive files
  s3:GetObject    — read back archive files
  s3:ListBucket   — enumerate archived partitions
  s3:DeleteObject — remove superseded archives (optional but recommended)

`, partition, bucket, partition, bucket)
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

// buildPartitionDefs returns numPartitions hourly partition clauses ending at
// the current hour (truncated from now), followed by a p_future catch-all.
//
// Partitions span forward from the current hour so that incoming events
// land in named partitions rather than accumulating in p_future.
// With numPartitions=48 (the
// default), the range covers the current hour through the next 47 hours.
// New events arriving beyond that range fall into p_future until rotate adds
// more named partitions.
func buildPartitionDefs(now time.Time, numPartitions int) []string {
	now = now.UTC().Truncate(time.Hour)
	start := now

	parts := make([]string, 0, numPartitions+1)
	for i := range numPartitions {
		hour := start.Add(time.Duration(i) * time.Hour)
		nextHour := hour.Add(time.Hour)
		parts = append(parts, fmt.Sprintf(
			"    PARTITION p_%s VALUES LESS THAN (TO_SECONDS('%s'))",
			hour.Format("2006010215"),
			nextHour.UTC().Format("2006-01-02 15:04:05"),
		))
	}
	parts = append(parts, "    PARTITION p_future VALUES LESS THAN MAXVALUE")
	return parts
}

// buildBinlogEventsDDL assembles the full CREATE TABLE statement for
// binlog_events. When encrypt is true, ENCRYPTION='Y' is added to the table
// options so MySQL uses InnoDB tablespace encryption (requires a keyring
// plugin on the server).
func buildBinlogEventsDDL(parts []string, encrypt bool) string {
	encryptClause := ""
	if encrypt {
		encryptClause = " ENCRYPTION='Y'"
	}
	return `CREATE TABLE IF NOT EXISTS binlog_events (
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
    schema_version  INT UNSIGNED     NOT NULL DEFAULT 0 COMMENT 'snapshot_id from schema_snapshots at index time; enables per-row resolver lookup for recovery',
    PRIMARY KEY (event_id, event_timestamp),
    INDEX idx_row_lookup (schema_name, table_name, event_timestamp),
    INDEX idx_pk_hash    (schema_name, table_name, pk_hash, event_timestamp),
    INDEX idx_gtid       (gtid),
    INDEX idx_file_pos   (binlog_file, start_pos)
) ENGINE=InnoDB` + encryptClause + `
  PARTITION BY RANGE (TO_SECONDS(event_timestamp)) (
` + strings.Join(parts, ",\n") + `
)`
}

// createBinlogEventsTable generates the CREATE TABLE with N hourly partitions
// spanning from (N-1) hours ago to the current hour (UTC), plus a p_future
// catch-all partition for any events arriving in subsequent hours.
//
// Each partition p_YYYYMMDDHH covers events where TO_SECONDS(event_timestamp)
// is less than TO_SECONDS of the following hour (timezone-independent).
// When encrypt is true, ENCRYPTION='Y' is added to enable InnoDB tablespace
// encryption (requires a keyring plugin on the MySQL server).
func createBinlogEventsTable(db *sql.DB, numPartitions int, encrypt bool) error {
	parts := buildPartitionDefs(time.Now(), numPartitions)
	ddl := buildBinlogEventsDDL(parts, encrypt)
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
    bintrail_id      CHAR(36)        NULL DEFAULT NULL,
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
    error_message  TEXT     DEFAULT NULL,
    bintrail_id    CHAR(36) NULL DEFAULT NULL,
    INDEX idx_bintrail_id (bintrail_id)
) ENGINE=InnoDB`

// ddlBintrailServers and ddlBintrailServerChanges are the canonical DDL
// statements for the server identity tables. They live in internal/serverid
// so that testutil and init share a single source of truth.
var (
	ddlBintrailServers       = serverid.DDLBintrailServers
	ddlBintrailServerChanges = serverid.DDLBintrailServerChanges
)

// ─── Archive tracking ─────────────────────────────────────────────────────────

const ddlArchiveState = `CREATE TABLE IF NOT EXISTS archive_state (
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
) ENGINE=InnoDB`

// ─── RBAC tables ─────────────────────────────────────────────────────────────

// ddlTableFlags stores named flags on tables or individual columns.
// column_name = '' means the flag applies to the whole table; a non-empty
// value names the specific column that carries the flag.
// This two-level design lets access_rules express both "deny the billing
// table" (table-level flag) and "redact the amount column" (column-level flag)
// using the same flag name with different column_name values.
const ddlTableFlags = `CREATE TABLE IF NOT EXISTS table_flags (
    id          INT UNSIGNED  AUTO_INCREMENT PRIMARY KEY,
    schema_name VARCHAR(64)   NOT NULL,
    table_name  VARCHAR(64)   NOT NULL,
    column_name VARCHAR(64)   NOT NULL DEFAULT '' COMMENT 'empty = table-level flag; non-empty = column-level flag',
    flag        VARCHAR(255)  NOT NULL,
    created_at  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_unique (schema_name, table_name, column_name, flag),
    INDEX idx_flag (flag)
) ENGINE=InnoDB`

// ddlProfiles stores named access profiles (e.g. "dev", "marketing").
// Profiles are referenced by access_rules to define what flags each profile
// may or may not access. Management is typically done from the web panel;
// the CLI provides the 'bintrail flag' command for DBA use.
const ddlProfiles = `CREATE TABLE IF NOT EXISTS profiles (
    id          INT UNSIGNED  AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(255)  NOT NULL,
    description TEXT          DEFAULT NULL,
    created_at  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_name (name)
) ENGINE=InnoDB`

// ddlAccessRules maps a profile to a flag with an allow/deny permission.
// Combined with table_flags this enables RBAC:
//   - profile "dev" DENY flag "billing"  → dev users cannot see billing table events
//   - profile "marketing" DENY flag "pii" → marketing users see events but pii columns are redacted
const ddlAccessRules = `CREATE TABLE IF NOT EXISTS access_rules (
    id          INT UNSIGNED  AUTO_INCREMENT PRIMARY KEY,
    profile_id  INT UNSIGNED  NOT NULL,
    flag        VARCHAR(255)  NOT NULL,
    permission  ENUM('allow','deny') NOT NULL,
    created_at  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_profile_flag (profile_id, flag),
    CONSTRAINT fk_access_rules_profile FOREIGN KEY (profile_id) REFERENCES profiles (id) ON DELETE CASCADE
) ENGINE=InnoDB`

// ─── DDL tracking ─────────────────────────────────────────────────────────────

const ddlSchemaChanges = `CREATE TABLE IF NOT EXISTS schema_changes (
    id              INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    detected_at     DATETIME NOT NULL,
    binlog_file     VARCHAR(255) NOT NULL,
    binlog_pos      BIGINT UNSIGNED NOT NULL,
    gtid            VARCHAR(255) DEFAULT NULL,
    schema_name     VARCHAR(64) NOT NULL,
    table_name      VARCHAR(64) NOT NULL,
    ddl_type        VARCHAR(50) NOT NULL,
    ddl_query       TEXT NOT NULL,
    snapshot_id     INT UNSIGNED DEFAULT NULL COMMENT 'auto-snapshot after DDL; NULL when not taken (file mode or snapshot failure)',
    INDEX idx_detected_at (detected_at)
) ENGINE=InnoDB`
