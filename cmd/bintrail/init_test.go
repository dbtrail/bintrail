package main

import (
	"strings"
	"testing"
	"time"
)

// ─── Cobra command wiring ─────────────────────────────────────────────────────

func TestInitCmd_registered(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "init" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'init' command to be registered under rootCmd")
	}
}

func TestInitCmd_allFlagsRegistered(t *testing.T) {
	for _, name := range []string{"index-dsn", "partitions", "encrypt", "s3-bucket", "s3-region", "s3-arn"} {
		if initCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on initCmd", name)
		}
	}
}

func TestInitCmd_indexDSNRequired(t *testing.T) {
	f := initCmd.Flag("index-dsn")
	if f == nil {
		t.Fatal("flag --index-dsn not registered")
	}
	if f.Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
		t.Error("--index-dsn should be marked required")
	}
}

func TestInitCmd_s3BucketOptional(t *testing.T) {
	f := initCmd.Flag("s3-bucket")
	if f == nil {
		t.Fatal("flag --s3-bucket not registered")
	}
	if f.Annotations["cobra_annotation_bash_completion_one_required_flag"] != nil {
		t.Error("--s3-bucket should not be marked required")
	}
}

func TestInitCmd_s3ARNOptional(t *testing.T) {
	f := initCmd.Flag("s3-arn")
	if f == nil {
		t.Fatal("flag --s3-arn not registered")
	}
	if f.Annotations["cobra_annotation_bash_completion_one_required_flag"] != nil {
		t.Error("--s3-arn should not be marked required")
	}
}

func TestInitCmd_encryptOptional(t *testing.T) {
	f := initCmd.Flag("encrypt")
	if f == nil {
		t.Fatal("flag --encrypt not registered")
	}
	if f.Annotations["cobra_annotation_bash_completion_one_required_flag"] != nil {
		t.Error("--encrypt should not be marked required")
	}
}

func TestInitCmd_defaults(t *testing.T) {
	cases := []struct {
		flag string
		want string
	}{
		{"partitions", "48"},
		{"encrypt", "false"},
		{"s3-region", "us-east-1"},
		{"s3-bucket", ""},
		{"s3-arn", ""},
	}
	for _, tc := range cases {
		f := initCmd.Flag(tc.flag)
		if f == nil {
			t.Errorf("flag --%s not registered", tc.flag)
			continue
		}
		if f.DefValue != tc.want {
			t.Errorf("flag --%s default: expected %q, got %q", tc.flag, tc.want, f.DefValue)
		}
	}
}

// ─── buildBinlogEventsDDL ───────────────────────────────────────────────────

func TestBuildBinlogEventsDDL_noEncrypt(t *testing.T) {
	parts := []string{"    PARTITION p_2026022814 VALUES LESS THAN (TO_SECONDS('2026-02-28 15:00:00'))",
		"    PARTITION p_future VALUES LESS THAN MAXVALUE"}
	ddl := buildBinlogEventsDDL(parts, false)

	if strings.Contains(ddl, "ENCRYPTION") {
		t.Error("expected no ENCRYPTION clause when encrypt=false")
	}
	if !strings.Contains(ddl, "ENGINE=InnoDB") {
		t.Error("expected ENGINE=InnoDB in DDL")
	}
	if !strings.Contains(ddl, "p_future") {
		t.Error("expected p_future partition in DDL")
	}
	if !strings.Contains(ddl, "schema_version") {
		t.Error("expected schema_version column in DDL")
	}
}

func TestBuildBinlogEventsDDL_withEncrypt(t *testing.T) {
	parts := []string{"    PARTITION p_2026022814 VALUES LESS THAN (TO_SECONDS('2026-02-28 15:00:00'))",
		"    PARTITION p_future VALUES LESS THAN MAXVALUE"}
	ddl := buildBinlogEventsDDL(parts, true)

	if !strings.Contains(ddl, "ENCRYPTION='Y'") {
		t.Error("expected ENCRYPTION='Y' in DDL when encrypt=true")
	}
	if !strings.Contains(ddl, "p_future") {
		t.Error("expected p_future partition in DDL")
	}
	if !strings.Contains(ddl, "schema_version") {
		t.Error("expected schema_version column in DDL")
	}
	// Encryption clause must appear after ENGINE=InnoDB and before PARTITION BY.
	engineIdx := strings.Index(ddl, "ENGINE=InnoDB")
	encryptIdx := strings.Index(ddl, "ENCRYPTION='Y'")
	partitionIdx := strings.Index(ddl, "PARTITION BY RANGE")
	if engineIdx < 0 || encryptIdx < 0 || partitionIdx < 0 {
		t.Fatal("DDL missing ENGINE=InnoDB, ENCRYPTION='Y', or PARTITION BY RANGE")
	}
	if !(engineIdx < encryptIdx && encryptIdx < partitionIdx) {
		t.Errorf("expected ENGINE < ENCRYPTION < PARTITION BY in DDL, got positions %d, %d, %d",
			engineIdx, encryptIdx, partitionIdx)
	}
}

// ─── parseS3ARN ──────────────────────────────────────────────────────────────

func TestParseS3ARN_validARNs(t *testing.T) {
	cases := []struct {
		arn       string
		bucket    string
		partition string
	}{
		{"arn:aws:s3:::my-bucket", "my-bucket", "aws"},
		{"arn:aws:s3:::acme-bintrail-archives", "acme-bintrail-archives", "aws"},
		{"arn:aws-cn:s3:::china-bucket", "china-bucket", "aws-cn"},
		{"arn:aws-us-gov:s3:::gov-bucket", "gov-bucket", "aws-us-gov"},
	}
	for _, tc := range cases {
		gotBucket, gotPartition, err := parseS3ARN(tc.arn)
		if err != nil {
			t.Errorf("parseS3ARN(%q) unexpected error: %v", tc.arn, err)
			continue
		}
		if gotBucket != tc.bucket {
			t.Errorf("parseS3ARN(%q) bucket = %q, want %q", tc.arn, gotBucket, tc.bucket)
		}
		if gotPartition != tc.partition {
			t.Errorf("parseS3ARN(%q) partition = %q, want %q", tc.arn, gotPartition, tc.partition)
		}
	}
}

func TestParseS3ARN_invalidARNs(t *testing.T) {
	cases := []struct {
		arn     string
		wantErr string
	}{
		{"not-an-arn", `expected 6 colon-separated fields`},
		{"xyz:aws:s3:::my-bucket", `must start with "arn:"`},
		{"arn:aws:ec2:::my-bucket", `service must be "s3"`},
		{"arn:aws:s3:::", `bucket name is empty`},
		{"arn:aws:s3:::my-bucket/key", `contains "/"`},
		{"arn:aws:s3:us-east-1:123456789012:my-bucket", `object or access-point ARN`},
	}
	for _, tc := range cases {
		_, _, err := parseS3ARN(tc.arn)
		if err == nil {
			t.Errorf("parseS3ARN(%q) expected error containing %q, got nil", tc.arn, tc.wantErr)
			continue
		}
		if !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("parseS3ARN(%q) error = %q, want it to contain %q", tc.arn, err.Error(), tc.wantErr)
		}
	}
}

// ─── s3IAMInstructions ───────────────────────────────────────────────────────────

func TestS3IAMInstructions_containsBucketName(t *testing.T) {
	out := s3IAMInstructions("my-archive-bucket", "aws")
	if !strings.Contains(out, "my-archive-bucket") {
		t.Error("expected bucket name in IAM instructions output")
	}
	if !strings.Contains(out, "s3:PutObject") {
		t.Error("expected s3:PutObject in IAM instructions output")
	}
	if !strings.Contains(out, "s3:GetObject") {
		t.Error("expected s3:GetObject in IAM instructions output")
	}
	if !strings.Contains(out, "s3:ListBucket") {
		t.Error("expected s3:ListBucket in IAM instructions output")
	}
}

func TestS3IAMInstructions_usesCorrectPartition(t *testing.T) {
	out := s3IAMInstructions("china-bucket", "aws-cn")
	if !strings.Contains(out, "arn:aws-cn:s3:::china-bucket") {
		t.Error("expected aws-cn partition in resource ARN")
	}
	if strings.Contains(out, "arn:aws:s3:::china-bucket") {
		t.Error("should not contain standard aws partition for aws-cn bucket")
	}
}

// ─── buildPartitionDefs ───────────────────────────────────────────────────────────

func TestBuildPartitionDefs_countAndFuture(t *testing.T) {
	now := time.Date(2026, 2, 28, 14, 30, 0, 0, time.UTC)
	parts := buildPartitionDefs(now, 48)

	// 48 named hourly partitions + p_future = 49
	if len(parts) != 49 {
		t.Fatalf("expected 49 partition defs, got %d", len(parts))
	}
	if !strings.Contains(parts[48], "p_future") {
		t.Errorf("last def should be p_future, got %q", parts[48])
	}
}

func TestBuildPartitionDefs_spansFromCurrentHourForward(t *testing.T) {
	// now truncated to 14:00; with 48 partitions: start = 14:00, end = 14:00 + 47h = 2026-03-02 13:00 UTC
	now := time.Date(2026, 2, 28, 14, 30, 0, 0, time.UTC)
	parts := buildPartitionDefs(now, 48)

	// First partition starts at the current hour
	if !strings.Contains(parts[0], "p_2026022814") {
		t.Errorf("expected first partition p_2026022814 (current hour), got %q", parts[0])
	}
	// Last named partition covers 47 hours from now
	if !strings.Contains(parts[47], "p_2026030213") {
		t.Errorf("expected last named partition p_2026030213 (+47h), got %q", parts[47])
	}
}

func TestBuildPartitionDefs_boundaryValues(t *testing.T) {
	now := time.Date(2026, 2, 28, 14, 30, 0, 0, time.UTC)
	parts := buildPartitionDefs(now, 3)

	// 3 partitions: p_2026022814, p_2026022815, p_2026022816, p_future
	if !strings.Contains(parts[0], "p_2026022814") {
		t.Errorf("expected p_2026022814 (current hour), got %q", parts[0])
	}
	if !strings.Contains(parts[1], "p_2026022815") {
		t.Errorf("expected p_2026022815, got %q", parts[1])
	}
	if !strings.Contains(parts[2], "p_2026022816") {
		t.Errorf("expected p_2026022816, got %q", parts[2])
	}
	// p_2026022814 boundary: LESS THAN TO_SECONDS('2026-02-28 15:00:00')
	if !strings.Contains(parts[0], "2026-02-28 15:00:00") {
		t.Errorf("expected boundary at 2026-02-28 15:00:00, got %q", parts[0])
	}
}

func TestBuildPartitionDefs_singlePartition(t *testing.T) {
	now := time.Date(2026, 2, 28, 10, 0, 0, 0, time.UTC)
	parts := buildPartitionDefs(now, 1)

	// 1 named + p_future = 2
	if len(parts) != 2 {
		t.Fatalf("expected 2 defs, got %d", len(parts))
	}
	if !strings.Contains(parts[0], "p_2026022810") {
		t.Errorf("expected p_2026022810 (current hour), got %q", parts[0])
	}
}

// ─── s3Instructions ───────────────────────────────────────────────────────────────

func TestS3Instructions_usEast1_noLocationConstraint(t *testing.T) {
	out := s3Instructions("my-bucket", "us-east-1")

	if !strings.Contains(out, "create-bucket") {
		t.Error("expected create-bucket command in output")
	}
	if strings.Contains(out, "LocationConstraint") {
		t.Error("us-east-1 should not include LocationConstraint")
	}
	if !strings.Contains(out, "my-bucket") {
		t.Error("expected bucket name in output")
	}
	if !strings.Contains(out, "us-east-1") {
		t.Error("expected region in output")
	}
}

func TestS3Instructions_otherRegion_hasLocationConstraint(t *testing.T) {
	out := s3Instructions("acme-bintrail", "eu-west-1")

	if !strings.Contains(out, "LocationConstraint=eu-west-1") {
		t.Error("expected LocationConstraint=eu-west-1 in output")
	}
	if !strings.Contains(out, "acme-bintrail") {
		t.Error("expected bucket name in output")
	}
}

func TestS3Instructions_containsPublicAccessBlock(t *testing.T) {
	out := s3Instructions("test-bucket", "us-east-1")

	if !strings.Contains(out, "put-public-access-block") {
		t.Error("expected put-public-access-block command in output")
	}
	if !strings.Contains(out, "BlockPublicAcls=true") {
		t.Error("expected BlockPublicAcls=true in output")
	}
}

func TestS3Instructions_containsLifecyclePolicy(t *testing.T) {
	out := s3Instructions("test-bucket", "us-east-1")

	if !strings.Contains(out, "put-bucket-lifecycle-configuration") {
		t.Error("expected lifecycle configuration command in output")
	}
	if !strings.Contains(out, "365") {
		t.Error("expected 365 days in lifecycle output")
	}
	if !strings.Contains(out, "bintrail-1yr-expiry") {
		t.Error("expected rule ID in lifecycle output")
	}
}

func TestS3Instructions_containsConsoleURL(t *testing.T) {
	out := s3Instructions("test-bucket", "ap-southeast-1")

	if !strings.Contains(out, "s3.console.aws.amazon.com") {
		t.Error("expected AWS Console URL in output")
	}
	if !strings.Contains(out, "365 days") {
		t.Error("expected 365 days mention in console instructions")
	}
}

// ─── runInit validation (no DB required) ─────────────────────────────────────

func TestRunInit_s3BucketAndARNMutuallyExclusive(t *testing.T) {
	savedBucket, savedARN := initS3Bucket, initS3ARN
	t.Cleanup(func() { initS3Bucket = savedBucket; initS3ARN = savedARN })

	initS3Bucket = "my-bucket"
	initS3ARN = "arn:aws:s3:::other-bucket"

	err := runInit(initCmd, nil)
	if err == nil {
		t.Fatal("expected error when both --s3-bucket and --s3-arn are set, got nil")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunInit_invalidS3ARN(t *testing.T) {
	savedBucket, savedARN := initS3Bucket, initS3ARN
	t.Cleanup(func() { initS3Bucket = savedBucket; initS3ARN = savedARN })

	initS3Bucket = ""
	initS3ARN = "not-an-arn"

	err := runInit(initCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid --s3-arn, got nil")
	}
	if !strings.Contains(err.Error(), "invalid --s3-arn") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunInit_missingDBName(t *testing.T) {
	savedDSN, savedBucket, savedARN := initIndexDSN, initS3Bucket, initS3ARN
	t.Cleanup(func() { initIndexDSN = savedDSN; initS3Bucket = savedBucket; initS3ARN = savedARN })

	initS3Bucket = ""
	initS3ARN = ""
	initIndexDSN = "user:pass@tcp(localhost:3306)/" // valid syntax, no database name

	err := runInit(initCmd, nil)
	if err == nil {
		t.Fatal("expected error when DSN has no database name, got nil")
	}
	if !strings.Contains(err.Error(), "--index-dsn must include a database name") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ─── DDL content: bintrail_id columns ─────────────────────────────────────────────

func TestDDLIndexState_hasBintrailID(t *testing.T) {
	if !strings.Contains(ddlIndexState, "bintrail_id") {
		t.Error("ddlIndexState must contain bintrail_id column")
	}
	if !strings.Contains(ddlIndexState, "idx_bintrail_id") {
		t.Error("ddlIndexState must contain idx_bintrail_id index")
	}
	if !strings.Contains(ddlIndexState, "CHAR(36)") {
		t.Error("ddlIndexState bintrail_id must be CHAR(36)")
	}
	if !strings.Contains(ddlIndexState, "NULL DEFAULT NULL") {
		t.Error("ddlIndexState bintrail_id must be nullable (NULL DEFAULT NULL)")
	}
}

func TestDDLStreamState_hasBintrailID(t *testing.T) {
	if !strings.Contains(ddlStreamState, "bintrail_id") {
		t.Error("ddlStreamState must contain bintrail_id column")
	}
	if !strings.Contains(ddlStreamState, "CHAR(36)") {
		t.Error("ddlStreamState bintrail_id must be CHAR(36)")
	}
	if !strings.Contains(ddlStreamState, "NULL DEFAULT NULL") {
		t.Error("ddlStreamState bintrail_id must be nullable (NULL DEFAULT NULL)")
	}
}

// ─── DDL content: archive_state ───────────────────────────────────────────────

func TestDDLArchiveState_hasExpectedColumns(t *testing.T) {
	for _, col := range []string{
		"partition_name", "bintrail_id", "local_path",
		"file_size_bytes", "row_count",
		"s3_bucket", "s3_key", "s3_uploaded_at", "archived_at",
	} {
		if !strings.Contains(ddlArchiveState, col) {
			t.Errorf("ddlArchiveState must contain %s column", col)
		}
	}
}

func TestDDLArchiveState_hasUniqueKey(t *testing.T) {
	if !strings.Contains(ddlArchiveState, "uq_partition") {
		t.Error("ddlArchiveState must contain uq_partition unique key")
	}
	if !strings.Contains(ddlArchiveState, "partition_name, bintrail_id") {
		t.Error("uq_partition must be on (partition_name, bintrail_id)")
	}
}

// ─── ddlSchemaChanges ───────────────────────────────────────────────────────

func TestDDLSchemaChanges_hasRequiredColumns(t *testing.T) {
	for _, col := range []string{
		"id", "detected_at", "binlog_file", "binlog_pos",
		"gtid", "schema_name", "table_name", "ddl_type",
		"ddl_query", "snapshot_id",
	} {
		if !strings.Contains(ddlSchemaChanges, col) {
			t.Errorf("ddlSchemaChanges must contain %s column", col)
		}
	}
}

func TestDDLSchemaChanges_hasIndex(t *testing.T) {
	if !strings.Contains(ddlSchemaChanges, "idx_detected_at") {
		t.Error("ddlSchemaChanges must contain idx_detected_at index")
	}
}
