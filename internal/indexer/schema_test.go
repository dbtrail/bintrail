package indexer

import (
	"strings"
	"testing"
	"time"
)

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
	if !strings.Contains(ddl, "query_text") || !strings.Contains(ddl, "query_hash") {
		t.Error("expected query_text and query_hash columns in DDL (#699)")
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

func TestDDLConstants_noUTCTimestampDefault(t *testing.T) {
	ddls := map[string]string{
		"ddlSchemaSnapshots": ddlSchemaSnapshots,
		"ddlStreamState":     ddlStreamState,
		"ddlIndexState":      ddlIndexState,
		"ddlArchiveState":    ddlArchiveState,
		"ddlTableFlags":      ddlTableFlags,
		"ddlProfiles":        ddlProfiles,
		"ddlAccessRules":     ddlAccessRules,
		"ddlSchemaChanges":   ddlSchemaChanges,
	}
	for name, ddl := range ddls {
		if strings.Contains(strings.ToUpper(ddl), "DEFAULT UTC_TIMESTAMP") {
			t.Errorf("%s must not use UTC_TIMESTAMP() as a DEFAULT — MySQL rejects it in DDL; use CURRENT_TIMESTAMP instead", name)
		}
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
	if !strings.Contains(ddlSchemaChanges, "idx_schema_table") {
		t.Error("ddlSchemaChanges must contain idx_schema_table index")
	}
}
