package status

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"
)

// ─── DescriptionToHuman ───────────────────────────────────────────────────────────────────

func TestDescriptionToHuman_maxvalue(t *testing.T) {
	for _, input := range []string{"MAXVALUE", "maxvalue", "MaxValue", ""} {
		got := DescriptionToHuman(input)
		if got != "MAXVALUE" {
			t.Errorf("DescriptionToHuman(%q) = %q, want MAXVALUE", input, got)
		}
	}
}

func TestDescriptionToHuman_toSecondsValue(t *testing.T) {
	// Compute a known TO_SECONDS value at runtime to avoid hardcoding date-sensitive values.
	// TO_SECONDS('1970-01-01 00:00:00') = 62167219200; any UTC time t has TO_SECONDS = 62167219200 + t.Unix().
	knownTime := time.Date(2026, 2, 20, 14, 0, 0, 0, time.UTC)
	toSecondsVal := int64(62167219200) + knownTime.Unix()
	got := DescriptionToHuman(strconv.FormatInt(toSecondsVal, 10))
	if !strings.Contains(got, "2026-02-20") {
		t.Errorf("expected 2026-02-20 in %q", got)
	}
	if !strings.Contains(got, "14:00") {
		t.Errorf("expected 14:00 in %q", got)
	}
	if !strings.Contains(got, "UTC") {
		t.Errorf("expected UTC suffix in %q", got)
	}
}

func TestDescriptionToHuman_unknownString(t *testing.T) {
	// A non-integer, non-MAXVALUE value should pass through unchanged.
	got := DescriptionToHuman("SOME_EXPR")
	if got != "SOME_EXPR" {
		t.Errorf("expected SOME_EXPR, got %q", got)
	}
}

func TestDescriptionToHuman_unixEpoch(t *testing.T) {
	// TO_SECONDS('1970-01-01 00:00:00') = 62167219200 — verify the Unix epoch converts correctly.
	got := DescriptionToHuman("62167219200")
	if !strings.Contains(got, "1970") {
		t.Errorf("expected 1970 in %q", got)
	}
}

// ─── Truncate ─────────────────────────────────────────────────────────────────────────────

func TestTruncate_shortString(t *testing.T) {
	got := Truncate("hello", 10)
	if got != "hello" {
		t.Errorf("expected no truncation, got %q", got)
	}
}

func TestTruncate_exactLength(t *testing.T) {
	got := Truncate("hello", 5)
	if got != "hello" {
		t.Errorf("expected no truncation at exact length, got %q", got)
	}
}

func TestTruncate_longString(t *testing.T) {
	long := strings.Repeat("x", 100)
	got := Truncate(long, 20)
	if !strings.HasSuffix(got, "\u2026") {
		t.Errorf("expected ellipsis suffix, got %q", got)
	}
	if len(got) != 21 { // 20 chars + "\u2026" (3 bytes, but counted as 1 rune display)
		// Note: "\u2026" is a multi-byte UTF-8 rune but len() counts bytes.
		// Our Truncate uses byte indexing, so the suffix adds 3 bytes.
		// Just check the prefix is preserved and suffix is "\u2026".
		if !strings.HasPrefix(got, strings.Repeat("x", 20)) {
			t.Errorf("expected 20 x's before ellipsis, got %q", got)
		}
	}
}

// ─── WriteStatus output structure ─────────────────────────────────────────────────────

func TestWriteStatus_noFiles_noPartitions(t *testing.T) {
	var buf bytes.Buffer
	WriteStatus(&buf, nil, nil, nil, nil)
	out := buf.String()

	assertContains(t, out, "=== Indexed Files ===")
	assertContains(t, out, "no files indexed yet")
	assertContains(t, out, "=== Partitions ===")
	assertContains(t, out, "no partitions found")
	// Summary section is omitted when there are no files.
	if strings.Contains(out, "=== Summary ===") {
		t.Error("expected no Summary section when no files exist")
	}
}

func TestWriteStatus_withFiles(t *testing.T) {
	ts := time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC)
	files := []IndexStateRow{
		{
			BinlogFile:    "binlog.000042",
			Status:        "completed",
			EventsIndexed: 1234,
			FileSize:      1024 * 1024,
			LastPosition:  1024 * 1024,
			StartedAt:     ts,
			CompletedAt:   sql.NullTime{Valid: true, Time: ts.Add(5 * 60 * 1e9)},
			ErrorMessage:  sql.NullString{},
		},
		{
			BinlogFile:    "binlog.000043",
			Status:        "failed",
			EventsIndexed: 0,
			FileSize:      512,
			LastPosition:  0,
			StartedAt:     ts.Add(10 * 60 * 1e9),
			CompletedAt:   sql.NullTime{Valid: false},
			ErrorMessage:  sql.NullString{Valid: true, String: "connection refused"},
		},
	}

	var buf bytes.Buffer
	WriteStatus(&buf, files, nil, nil, nil)
	out := buf.String()

	assertContains(t, out, "binlog.000042")
	assertContains(t, out, "completed")
	assertContains(t, out, "1234")
	assertContains(t, out, "binlog.000043")
	assertContains(t, out, "failed")
	assertContains(t, out, "connection refused")
	assertContains(t, out, "=== Summary ===")
	assertContains(t, out, "1 completed")
	assertContains(t, out, "1 failed")
	// Files without bintrail_id (NULL) must be grouped under "(unknown)".
	assertContains(t, out, "Server (unknown)")
}

func TestWriteStatus_withPartitions(t *testing.T) {
	// Compute TO_SECONDS values dynamically: TO_SECONDS('1970-01-01 00:00:00') = 62167219200.
	toSeconds := func(d time.Time) string {
		return strconv.FormatInt(int64(62167219200)+d.Unix(), 10)
	}
	parts := []PartitionStat{
		{Name: "p_2026021800", Description: toSeconds(time.Date(2026, 2, 18, 1, 0, 0, 0, time.UTC)), TableRows: 45000, Ordinal: 1},
		{Name: "p_2026021900", Description: toSeconds(time.Date(2026, 2, 19, 0, 0, 0, 0, time.UTC)), TableRows: 3401, Ordinal: 2},
		{Name: "p_future", Description: "MAXVALUE", TableRows: 0, Ordinal: 3},
	}

	var buf bytes.Buffer
	WriteStatus(&buf, nil, parts, nil, nil)
	out := buf.String()

	assertContains(t, out, "=== Partitions ===")
	assertContains(t, out, "p_2026021800")
	assertContains(t, out, "p_future")
	assertContains(t, out, "MAXVALUE")
	assertContains(t, out, "45000")
	// Total should be 48401
	assertContains(t, out, "48401")
}

func TestWriteStatus_errorTruncation(t *testing.T) {
	long := strings.Repeat("e", 100)
	files := []IndexStateRow{{
		BinlogFile:   "binlog.000001",
		Status:       "failed",
		StartedAt:    time.Now(),
		ErrorMessage: sql.NullString{Valid: true, String: long},
	}}

	var buf bytes.Buffer
	WriteStatus(&buf, files, nil, nil, nil)
	out := buf.String()

	// The error should be truncated — full 100-char string should not appear.
	if strings.Contains(out, long) {
		t.Error("expected long error to be truncated, but found it in full")
	}
	assertContains(t, out, "\u2026")
}

// ─── WriteStatus: bintrail_id column and per-server summary ─────────────────────────────

func TestWriteStatus_bintrailIDColumn(t *testing.T) {
	ts := time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC)
	files := []IndexStateRow{
		{
			BinlogFile:    "binlog.000001",
			Status:        "completed",
			EventsIndexed: 500,
			StartedAt:     ts,
			BintrailID:    sql.NullString{Valid: true, String: "abc123de-0000-0000-0000-000000000001"},
		},
		{
			BinlogFile:    "binlog.000002",
			Status:        "completed",
			EventsIndexed: 300,
			StartedAt:     ts.Add(time.Hour),
			BintrailID:    sql.NullString{Valid: false}, // NULL — old row
		},
	}

	var buf bytes.Buffer
	WriteStatus(&buf, files, nil, nil, nil)
	out := buf.String()

	// BINTRAIL_ID column header must appear.
	assertContains(t, out, "BINTRAIL_ID")
	// Known UUID must appear in the file row.
	assertContains(t, out, "abc123de-0000-0000-0000-000000000001")
	// NULL bintrail_id must be grouped as "(unknown)" in the per-server summary.
	assertContains(t, out, "Server (unknown)")
}

func TestWriteStatus_perServerSummary_multipleServers(t *testing.T) {
	ts := time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC)
	serverA := "aaaaaaaa-0000-0000-0000-000000000001"
	serverB := "bbbbbbbb-0000-0000-0000-000000000002"
	files := []IndexStateRow{
		{BinlogFile: "binlog.000001", Status: "completed", EventsIndexed: 1000, StartedAt: ts, BintrailID: sql.NullString{Valid: true, String: serverA}},
		{BinlogFile: "binlog.000002", Status: "completed", EventsIndexed: 500, StartedAt: ts.Add(time.Hour), BintrailID: sql.NullString{Valid: true, String: serverA}},
		{BinlogFile: "binlog.000010", Status: "completed", EventsIndexed: 200, StartedAt: ts, BintrailID: sql.NullString{Valid: true, String: serverB}},
		{BinlogFile: "binlog.000011", Status: "failed", EventsIndexed: 0, StartedAt: ts.Add(time.Hour), BintrailID: sql.NullString{Valid: true, String: serverB}},
	}

	var buf bytes.Buffer
	WriteStatus(&buf, files, nil, nil, nil)
	out := buf.String()

	assertContains(t, out, "=== Summary ===")
	// Both server IDs must appear as section headers.
	assertContains(t, out, "Server "+serverA)
	assertContains(t, out, "Server "+serverB)
	// Server A: 2 completed, 1500 events.
	assertContains(t, out, "2 completed")
	assertContains(t, out, "1500")
	// Server B: 1 completed, 1 failed, 200 events.
	assertContains(t, out, "1 failed")
	assertContains(t, out, "200")
}

func TestWriteStatus_perServerSummary_unknownID(t *testing.T) {
	ts := time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC)
	files := []IndexStateRow{
		{BinlogFile: "binlog.000001", Status: "completed", EventsIndexed: 42, StartedAt: ts, BintrailID: sql.NullString{Valid: false}},
	}

	var buf bytes.Buffer
	WriteStatus(&buf, files, nil, nil, nil)
	out := buf.String()

	// Null bintrail_id must be grouped under "(unknown)".
	assertContains(t, out, "Server (unknown)")
	assertContains(t, out, "1 completed")
}

// ─── Helper ────────────────────────────────────────────────────────────────────────────

func assertContains(t *testing.T, s, want string) {
	t.Helper()
	if !strings.Contains(s, want) {
		t.Errorf("expected %q in output:\n%s", want, s)
	}
}

// ─── WriteStatusJSON ────────────────────────────────────────────────────────

func TestWriteStatusJSON_empty(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteStatusJSON(&buf, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var result struct {
		Files      []any `json:"files"`
		Partitions []any `json:"partitions"`
		Total      int64 `json:"total_events_estimate"`
	}
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, buf.String())
	}
	if len(result.Files) != 0 {
		t.Errorf("expected 0 files, got %d", len(result.Files))
	}
	if len(result.Partitions) != 0 {
		t.Errorf("expected 0 partitions, got %d", len(result.Partitions))
	}
}

func TestWriteStatusJSON_withData(t *testing.T) {
	ts := time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC)
	files := []IndexStateRow{{
		BinlogFile:    "binlog.000042",
		Status:        "completed",
		EventsIndexed: 1234,
		FileSize:      1048576,
		LastPosition:  1048576,
		StartedAt:     ts,
		CompletedAt:   sql.NullTime{Valid: true, Time: ts.Add(5 * time.Minute)},
		BintrailID:    sql.NullString{Valid: true, String: "test-uuid"},
	}}
	parts := []PartitionStat{{
		Name:        "p_2026021914",
		Description: strconv.FormatInt(int64(62167219200)+ts.Add(time.Hour).Unix(), 10),
		TableRows:   5000,
		Ordinal:     1,
	}}

	var buf bytes.Buffer
	if err := WriteStatusJSON(&buf, files, parts, nil, nil); err != nil {
		t.Fatal(err)
	}

	var result struct {
		Files []struct {
			BinlogFile    string  `json:"binlog_file"`
			Status        string  `json:"status"`
			EventsIndexed int64   `json:"events_indexed"`
			BintrailID    *string `json:"bintrail_id"`
		} `json:"files"`
		Partitions []struct {
			Name      string `json:"name"`
			TableRows int64  `json:"table_rows"`
		} `json:"partitions"`
		Total int64 `json:"total_events_estimate"`
	}
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, buf.String())
	}

	if len(result.Files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(result.Files))
	}
	if result.Files[0].BinlogFile != "binlog.000042" {
		t.Errorf("wrong binlog_file: %s", result.Files[0].BinlogFile)
	}
	if result.Files[0].EventsIndexed != 1234 {
		t.Errorf("wrong events_indexed: %d", result.Files[0].EventsIndexed)
	}
	if result.Files[0].BintrailID == nil || *result.Files[0].BintrailID != "test-uuid" {
		t.Errorf("wrong bintrail_id: %v", result.Files[0].BintrailID)
	}

	if len(result.Partitions) != 1 {
		t.Fatalf("expected 1 partition, got %d", len(result.Partitions))
	}
	if result.Partitions[0].Name != "p_2026021914" {
		t.Errorf("wrong partition name: %s", result.Partitions[0].Name)
	}
	if result.Total != 5000 {
		t.Errorf("wrong total: %d", result.Total)
	}
}

func TestWriteStatusJSON_nullFields(t *testing.T) {
	files := []IndexStateRow{{
		BinlogFile: "binlog.000001",
		Status:     "in_progress",
		StartedAt:  time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC),
		// CompletedAt, BintrailID, ErrorMessage all null
	}}

	var buf bytes.Buffer
	if err := WriteStatusJSON(&buf, files, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	var result struct {
		Files []struct {
			CompletedAt  *string `json:"completed_at"`
			BintrailID   *string `json:"bintrail_id"`
			ErrorMessage *string `json:"error_message"`
		} `json:"files"`
	}
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, buf.String())
	}
	if result.Files[0].CompletedAt != nil {
		t.Error("expected null completed_at")
	}
	if result.Files[0].BintrailID != nil {
		t.Error("expected null bintrail_id")
	}
	if result.Files[0].ErrorMessage != nil {
		t.Error("expected null error_message")
	}
}

// ─── formatBytes ────────────────────────────────────────────────────────────

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.0 GB"},
		{1288490189, "1.2 GB"},
		{1099511627776, "1.0 TB"},
	}
	for _, tt := range tests {
		got := formatBytes(tt.input)
		if got != tt.want {
			t.Errorf("formatBytes(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// ─── WriteStatus: archives section ──────────────────────────────────────────

func TestWriteStatus_withArchives(t *testing.T) {
	archives := &ArchiveStats{
		TotalFiles:     42,
		TotalRows:      84000,
		TotalSizeBytes: 1288490189, // ~1.2 GB
		LocalFiles:     42,
		S3Files:        42,
		S3Buckets:      []string{"my-bintrail-archives"},
	}

	var buf bytes.Buffer
	WriteStatus(&buf, nil, nil, archives, nil)
	out := buf.String()

	assertContains(t, out, "=== Archives ===")
	assertContains(t, out, "42 files")
	assertContains(t, out, "1.2 GB")
	assertContains(t, out, "84000 rows")
	assertContains(t, out, "Local:  42")
	assertContains(t, out, "S3:     42")
	assertContains(t, out, "my-bintrail-archives")
}

func TestWriteStatus_nilArchives_omitsSection(t *testing.T) {
	var buf bytes.Buffer
	WriteStatus(&buf, nil, nil, nil, nil)
	out := buf.String()

	if strings.Contains(out, "=== Archives ===") {
		t.Error("expected no Archives section when archives is nil")
	}
}

func TestWriteStatus_zeroArchives_omitsSection(t *testing.T) {
	var buf bytes.Buffer
	WriteStatus(&buf, nil, nil, &ArchiveStats{}, nil)
	out := buf.String()

	if strings.Contains(out, "=== Archives ===") {
		t.Error("expected no Archives section when TotalFiles is 0")
	}
}

func TestWriteStatus_archives_noS3(t *testing.T) {
	archives := &ArchiveStats{
		TotalFiles:     5,
		TotalRows:      1000,
		TotalSizeBytes: 5242880,
		LocalFiles:     5,
		S3Files:        0,
	}

	var buf bytes.Buffer
	WriteStatus(&buf, nil, nil, archives, nil)
	out := buf.String()

	assertContains(t, out, "=== Archives ===")
	assertContains(t, out, "S3:     0")
	if strings.Contains(out, "bucket") {
		t.Error("expected no bucket info when S3Files is 0")
	}
}

// ─── WriteStatusJSON: archives ──────────────────────────────────────────────

func TestWriteStatusJSON_withArchives(t *testing.T) {
	archives := &ArchiveStats{
		TotalFiles:     10,
		TotalRows:      5000,
		TotalSizeBytes: 1048576,
		LocalFiles:     10,
		S3Files:        3,
		S3Buckets:      []string{"bucket-a"},
	}

	var buf bytes.Buffer
	if err := WriteStatusJSON(&buf, nil, nil, archives, nil); err != nil {
		t.Fatal(err)
	}

	var result struct {
		Archives *struct {
			TotalFiles     int      `json:"total_files"`
			TotalRows      int64    `json:"total_rows"`
			TotalSizeBytes int64    `json:"total_size_bytes"`
			TotalSizeHuman string   `json:"total_size_human"`
			LocalFiles     int      `json:"local_files"`
			S3Files        int      `json:"s3_files"`
			S3Buckets      []string `json:"s3_buckets"`
		} `json:"archives"`
	}
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, buf.String())
	}
	if result.Archives == nil {
		t.Fatal("expected archives key in JSON")
	}
	if result.Archives.TotalFiles != 10 {
		t.Errorf("wrong total_files: %d", result.Archives.TotalFiles)
	}
	if result.Archives.TotalRows != 5000 {
		t.Errorf("wrong total_rows: %d", result.Archives.TotalRows)
	}
	if result.Archives.TotalSizeHuman != "1.0 MB" {
		t.Errorf("wrong total_size_human: %s", result.Archives.TotalSizeHuman)
	}
	if result.Archives.S3Files != 3 {
		t.Errorf("wrong s3_files: %d", result.Archives.S3Files)
	}
	if len(result.Archives.S3Buckets) != 1 || result.Archives.S3Buckets[0] != "bucket-a" {
		t.Errorf("wrong s3_buckets: %v", result.Archives.S3Buckets)
	}
}

func TestWriteStatusJSON_nilArchives_omitsKey(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteStatusJSON(&buf, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	var raw map[string]any
	if err := json.Unmarshal(buf.Bytes(), &raw); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if _, ok := raw["archives"]; ok {
		t.Error("expected no 'archives' key when archives is nil")
	}
}

// ─── WriteStatus: coverage section ──────────────────────────────────────────

func TestWriteStatus_withCoverage(t *testing.T) {
	earliest := time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC)
	latest := time.Date(2026, 2, 20, 14, 30, 0, 0, time.UTC)
	coverage := &CoverageInfo{
		EarliestEvent: sql.NullTime{Valid: true, Time: earliest},
		LatestEvent:   sql.NullTime{Valid: true, Time: latest},
		TotalEvents:   42000,
		SchemaChanges: 3,
		UncoveredDDLs: 1,
	}

	var buf bytes.Buffer
	WriteStatus(&buf, nil, nil, nil, coverage)
	out := buf.String()

	assertContains(t, out, "=== Restore Coverage ===")
	assertContains(t, out, "2026-02-18")
	assertContains(t, out, "2026-02-20")
	assertContains(t, out, "42000")
	assertContains(t, out, "3")
	assertContains(t, out, "Warning")
}

func TestWriteStatus_nilCoverage_omitsSection(t *testing.T) {
	var buf bytes.Buffer
	WriteStatus(&buf, nil, nil, nil, nil)
	out := buf.String()

	if strings.Contains(out, "=== Restore Coverage ===") {
		t.Error("expected no coverage section when coverage is nil")
	}
}

func TestWriteStatus_zeroCoverage_noWarning(t *testing.T) {
	coverage := &CoverageInfo{
		TotalEvents:   100,
		SchemaChanges: 2,
		UncoveredDDLs: 0,
	}

	var buf bytes.Buffer
	WriteStatus(&buf, nil, nil, nil, coverage)
	out := buf.String()

	assertContains(t, out, "=== Restore Coverage ===")
	if strings.Contains(out, "Warning") {
		t.Error("expected no warning when UncoveredDDLs is 0")
	}
}

func TestWriteStatusJSON_withCoverage(t *testing.T) {
	earliest := time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC)
	latest := time.Date(2026, 2, 20, 14, 30, 0, 0, time.UTC)
	coverage := &CoverageInfo{
		EarliestEvent: sql.NullTime{Valid: true, Time: earliest},
		LatestEvent:   sql.NullTime{Valid: true, Time: latest},
		TotalEvents:   42000,
		SchemaChanges: 3,
		UncoveredDDLs: 1,
	}

	var buf bytes.Buffer
	if err := WriteStatusJSON(&buf, nil, nil, nil, coverage); err != nil {
		t.Fatal(err)
	}

	var result struct {
		Coverage *struct {
			EarliestEvent string `json:"earliest_event"`
			LatestEvent   string `json:"latest_event"`
			TotalEvents   int64  `json:"total_events"`
			SchemaChanges int    `json:"schema_changes"`
			UncoveredDDLs int    `json:"uncovered_ddls"`
		} `json:"coverage"`
	}
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, buf.String())
	}
	if result.Coverage == nil {
		t.Fatal("expected coverage key in JSON")
	}
	if result.Coverage.TotalEvents != 42000 {
		t.Errorf("wrong total_events: %d", result.Coverage.TotalEvents)
	}
	if result.Coverage.SchemaChanges != 3 {
		t.Errorf("wrong schema_changes: %d", result.Coverage.SchemaChanges)
	}
	if result.Coverage.UncoveredDDLs != 1 {
		t.Errorf("wrong uncovered_ddls: %d", result.Coverage.UncoveredDDLs)
	}
}

func TestWriteStatusJSON_nilCoverage_omitsKey(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteStatusJSON(&buf, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	var raw map[string]any
	if err := json.Unmarshal(buf.Bytes(), &raw); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if _, ok := raw["coverage"]; ok {
		t.Error("expected no 'coverage' key when coverage is nil")
	}
}
