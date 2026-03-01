package main

import (
	"strings"
	"testing"
	"time"

	"github.com/bintrail/bintrail/internal/query"
)

// ─── cobra command wiring ─────────────────────────────────────────────────────

func TestQueryCmd_registered(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "query" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'query' command to be registered under rootCmd")
	}
}

func TestQueryCmd_indexDSN_required(t *testing.T) {
	flag := queryCmd.Flag("index-dsn")
	if flag == nil {
		t.Fatal("flag --index-dsn not registered")
	}
	if flag.Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
		t.Error("flag --index-dsn is not marked required")
	}
}

func TestQueryCmd_defaults(t *testing.T) {
	cases := []struct {
		flag string
		want string
	}{
		{"format", "table"},
		{"limit", "100"},
	}
	for _, tc := range cases {
		f := queryCmd.Flag(tc.flag)
		if f == nil {
			t.Errorf("flag --%s not registered", tc.flag)
			continue
		}
		if f.DefValue != tc.want {
			t.Errorf("flag --%s: expected default %q, got %q", tc.flag, tc.want, f.DefValue)
		}
	}
}

func TestQueryCmd_emptyStringDefaults(t *testing.T) {
	for _, name := range []string{
		"schema", "table", "pk", "event-type", "gtid", "since", "until", "changed-column",
	} {
		f := queryCmd.Flag(name)
		if f == nil {
			t.Errorf("flag --%s not registered", name)
			continue
		}
		if f.DefValue != "" {
			t.Errorf("flag --%s: expected empty default, got %q", name, f.DefValue)
		}
	}
}

func TestQueryCmd_allFlagsRegistered(t *testing.T) {
	for _, name := range []string{
		"index-dsn", "schema", "table", "pk", "event-type",
		"gtid", "since", "until", "changed-column", "format", "limit",
	} {
		if queryCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on queryCmd", name)
		}
	}
}

// ─── runQuery validation (no DB required) ─────────────────────────────────────

func TestRunQuery_pkRequiresSchemaTable(t *testing.T) {
	saved, savedS, savedT := qPK, qSchema, qTable
	t.Cleanup(func() { qPK = saved; qSchema = savedS; qTable = savedT })

	qPK = "42"
	qSchema = ""
	qTable = ""

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error when --pk used without --schema/--table, got nil")
	}
	if !strings.Contains(err.Error(), "--pk requires") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRunQuery_changedColRequiresSchemaTable(t *testing.T) {
	saved, savedS, savedT := qChangedCol, qSchema, qTable
	t.Cleanup(func() { qChangedCol = saved; qSchema = savedS; qTable = savedT })

	qChangedCol = "status"
	qSchema = ""
	qTable = ""

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error when --changed-column used without --schema/--table, got nil")
	}
	if !strings.Contains(err.Error(), "--changed-column requires") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRunQuery_invalidFormat(t *testing.T) {
	savedFmt, savedPK, savedCol := qFormat, qPK, qChangedCol
	t.Cleanup(func() { qFormat = savedFmt; qPK = savedPK; qChangedCol = savedCol })

	qPK = ""
	qChangedCol = ""
	qFormat = "xml"

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid --format, got nil")
	}
	if !strings.Contains(err.Error(), "invalid --format") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRunQuery_invalidEventType(t *testing.T) {
	savedET, savedPK, savedCol, savedFmt := qEventType, qPK, qChangedCol, qFormat
	t.Cleanup(func() {
		qEventType = savedET
		qPK = savedPK
		qChangedCol = savedCol
		qFormat = savedFmt
	})

	qPK = ""
	qChangedCol = ""
	qFormat = "table"
	qEventType = "UPSERT"

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid --event-type, got nil")
	}
	if !strings.Contains(err.Error(), "UPSERT") {
		t.Errorf("expected 'UPSERT' in error, got: %v", err)
	}
}

func TestRunQuery_invalidSince(t *testing.T) {
	savedSince, savedPK, savedCol, savedFmt, savedET := qSince, qPK, qChangedCol, qFormat, qEventType
	t.Cleanup(func() {
		qSince = savedSince
		qPK = savedPK
		qChangedCol = savedCol
		qFormat = savedFmt
		qEventType = savedET
	})

	qPK = ""
	qChangedCol = ""
	qFormat = "table"
	qEventType = ""
	qSince = "not-a-date"

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid --since, got nil")
	}
	if !strings.Contains(err.Error(), "--since") {
		t.Errorf("expected '--since' in error, got: %v", err)
	}
}

func TestRunQuery_invalidUntil(t *testing.T) {
	savedUntil, savedPK, savedCol, savedFmt, savedET, savedSince :=
		qUntil, qPK, qChangedCol, qFormat, qEventType, qSince
	t.Cleanup(func() {
		qUntil = savedUntil
		qPK = savedPK
		qChangedCol = savedCol
		qFormat = savedFmt
		qEventType = savedET
		qSince = savedSince
	})

	qPK = ""
	qChangedCol = ""
	qFormat = "table"
	qEventType = ""
	qSince = ""
	qUntil = "not-a-date"

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid --until, got nil")
	}
	if !strings.Contains(err.Error(), "--until") {
		t.Errorf("expected '--until' in error, got: %v", err)
	}
}

// ─── --pk partial flag combinations ──────────────────────────────────────────

// TestRunQuery_pkWithSchemaOnly verifies that --pk + --schema (no --table)
// is rejected — the guard uses OR, so having only one of schema/table fails.
func TestRunQuery_pkWithSchemaOnly(t *testing.T) {
	savedPK, savedS, savedT := qPK, qSchema, qTable
	t.Cleanup(func() { qPK = savedPK; qSchema = savedS; qTable = savedT })

	qPK = "42"
	qSchema = "mydb"
	qTable = ""

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error when --pk used with --schema but no --table, got nil")
	}
	if !strings.Contains(err.Error(), "--pk requires") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestRunQuery_pkWithTableOnly verifies that --pk + --table (no --schema) is
// also rejected — the symmetric case to pkWithSchemaOnly.
func TestRunQuery_pkWithTableOnly(t *testing.T) {
	savedPK, savedS, savedT := qPK, qSchema, qTable
	t.Cleanup(func() { qPK = savedPK; qSchema = savedS; qTable = savedT })

	qPK = "42"
	qSchema = ""
	qTable = "orders"

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error when --pk used with --table but no --schema, got nil")
	}
	if !strings.Contains(err.Error(), "--pk requires") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestRunQuery_changedColWithSchemaOnly verifies that --changed-column + --schema
// (no --table) is rejected — the OR guard applies here too.
func TestRunQuery_changedColWithSchemaOnly(t *testing.T) {
	savedCol, savedS, savedT := qChangedCol, qSchema, qTable
	t.Cleanup(func() { qChangedCol = savedCol; qSchema = savedS; qTable = savedT })

	qChangedCol = "status"
	qSchema = "mydb"
	qTable = ""

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error when --changed-column used with --schema but no --table, got nil")
	}
	if !strings.Contains(err.Error(), "--changed-column requires") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// ─── archive flag wiring ──────────────────────────────────────────────────────

func TestQueryCmd_archiveFlagsRegistered(t *testing.T) {
	for _, name := range []string{"archive-dir", "archive-s3"} {
		if queryCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on queryCmd", name)
		}
	}
}

func TestQueryCmd_archiveFlagDefaults(t *testing.T) {
	for _, name := range []string{"archive-dir", "archive-s3"} {
		f := queryCmd.Flag(name)
		if f == nil {
			t.Fatalf("flag --%s not registered", name)
		}
		if f.DefValue != "" {
			t.Errorf("flag --%s: expected empty default, got %q", name, f.DefValue)
		}
	}
}

// ─── archiveSources ──────────────────────────────────────────────────────────

func TestArchiveSources_both(t *testing.T) {
	savedDir, savedS3 := qArchiveDir, qArchiveS3
	t.Cleanup(func() { qArchiveDir = savedDir; qArchiveS3 = savedS3 })

	qArchiveDir = "/data/archives"
	qArchiveS3 = "s3://bucket/prefix"

	srcs := archiveSources()
	if len(srcs) != 2 {
		t.Fatalf("expected 2 sources, got %d: %v", len(srcs), srcs)
	}
	if srcs[0] != "/data/archives" || srcs[1] != "s3://bucket/prefix" {
		t.Errorf("unexpected sources: %v", srcs)
	}
}

func TestArchiveSources_dirOnly(t *testing.T) {
	savedDir, savedS3 := qArchiveDir, qArchiveS3
	t.Cleanup(func() { qArchiveDir = savedDir; qArchiveS3 = savedS3 })

	qArchiveDir = "/data/archives"
	qArchiveS3 = ""

	srcs := archiveSources()
	if len(srcs) != 1 || srcs[0] != "/data/archives" {
		t.Errorf("expected [/data/archives], got %v", srcs)
	}
}

func TestArchiveSources_none(t *testing.T) {
	savedDir, savedS3 := qArchiveDir, qArchiveS3
	t.Cleanup(func() { qArchiveDir = savedDir; qArchiveS3 = savedS3 })

	qArchiveDir = ""
	qArchiveS3 = ""

	if srcs := archiveSources(); len(srcs) != 0 {
		t.Errorf("expected empty sources, got %v", srcs)
	}
}

// ─── mergeResults ─────────────────────────────────────────────────────────────

func TestMergeResults_deduplicatesByEventID(t *testing.T) {
	t0 := time.Date(2026, 2, 28, 10, 0, 0, 0, time.UTC)
	rows := []query.ResultRow{
		{EventID: 1, EventTimestamp: t0, SchemaName: "db", TableName: "t"},
		{EventID: 1, EventTimestamp: t0, SchemaName: "db", TableName: "t"}, // duplicate
		{EventID: 2, EventTimestamp: t0.Add(time.Second), SchemaName: "db", TableName: "t"},
	}
	got := mergeResults(rows, 0)
	if len(got) != 2 {
		t.Fatalf("expected 2 unique rows, got %d", len(got))
	}
}

func TestMergeResults_sortsByTimestampThenEventID(t *testing.T) {
	t0 := time.Date(2026, 2, 28, 10, 0, 0, 0, time.UTC)
	rows := []query.ResultRow{
		{EventID: 3, EventTimestamp: t0.Add(2 * time.Second)},
		{EventID: 1, EventTimestamp: t0},
		{EventID: 2, EventTimestamp: t0.Add(time.Second)},
	}
	got := mergeResults(rows, 0)
	if got[0].EventID != 1 || got[1].EventID != 2 || got[2].EventID != 3 {
		t.Errorf("expected sorted by timestamp, got event IDs %d %d %d",
			got[0].EventID, got[1].EventID, got[2].EventID)
	}
}

// TestMergeResults_sameTimestampSortsByEventID verifies the secondary sort key
// when two rows share the same event_timestamp.
func TestMergeResults_sameTimestampSortsByEventID(t *testing.T) {
	t0 := time.Date(2026, 2, 28, 10, 0, 0, 0, time.UTC)
	rows := []query.ResultRow{
		{EventID: 5, EventTimestamp: t0},
		{EventID: 2, EventTimestamp: t0},
		{EventID: 8, EventTimestamp: t0},
	}
	got := mergeResults(rows, 0)
	if got[0].EventID != 2 || got[1].EventID != 5 || got[2].EventID != 8 {
		t.Errorf("expected sorted by event_id at same timestamp, got %d %d %d",
			got[0].EventID, got[1].EventID, got[2].EventID)
	}
}

func TestMergeResults_appliesLimit(t *testing.T) {
	t0 := time.Date(2026, 2, 28, 10, 0, 0, 0, time.UTC)
	rows := []query.ResultRow{
		{EventID: 1, EventTimestamp: t0},
		{EventID: 2, EventTimestamp: t0.Add(time.Second)},
		{EventID: 3, EventTimestamp: t0.Add(2 * time.Second)},
	}
	got := mergeResults(rows, 2)
	if len(got) != 2 {
		t.Fatalf("expected limit 2, got %d rows", len(got))
	}
	if got[0].EventID != 1 || got[1].EventID != 2 {
		t.Errorf("expected first two rows, got event IDs %d %d", got[0].EventID, got[1].EventID)
	}
}

func TestMergeResults_zeroLimitNoTruncation(t *testing.T) {
	t0 := time.Date(2026, 2, 28, 10, 0, 0, 0, time.UTC)
	rows := []query.ResultRow{
		{EventID: 1, EventTimestamp: t0},
		{EventID: 2, EventTimestamp: t0.Add(time.Second)},
		{EventID: 3, EventTimestamp: t0.Add(2 * time.Second)},
	}
	got := mergeResults(rows, 0)
	if len(got) != 3 {
		t.Errorf("expected all 3 rows when limit=0, got %d", len(got))
	}
}

func TestMergeResults_empty(t *testing.T) {
	if got := mergeResults(nil, 10); len(got) != 0 {
		t.Errorf("expected empty result for nil input, got %v", got)
	}
}
