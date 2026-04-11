package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/query"
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
		"schema", "table", "pk", "event-type", "gtid", "since", "until", "changed-column", "flag",
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
		"gtid", "since", "until", "changed-column", "flag", "format", "limit",
		"no-archive",
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
	for _, name := range []string{"archive-dir", "archive-s3", "bintrail-id"} {
		if queryCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on queryCmd", name)
		}
	}
}

func TestQueryCmd_archiveFlagDefaults(t *testing.T) {
	for _, name := range []string{"archive-dir", "archive-s3", "bintrail-id"} {
		f := queryCmd.Flag(name)
		if f == nil {
			t.Fatalf("flag --%s not registered", name)
		}
		if f.DefValue != "" {
			t.Errorf("flag --%s: expected empty default, got %q", name, f.DefValue)
		}
	}
}

func TestQueryCmd_noArchiveDefault(t *testing.T) {
	f := queryCmd.Flag("no-archive")
	if f == nil {
		t.Fatal("flag --no-archive not registered")
	}
	if f.DefValue != "false" {
		t.Errorf("expected --no-archive default %q, got %q", "false", f.DefValue)
	}
}

func TestRunQuery_noArchiveConflictsWithArchiveDir(t *testing.T) {
	saved := struct {
		na          bool
		ad, as3, pk, cc, fmt, bid string
	}{qNoArchive, qArchiveDir, qArchiveS3, qPK, qChangedCol, qFormat, qBintrailID}
	t.Cleanup(func() {
		qNoArchive = saved.na; qArchiveDir = saved.ad; qArchiveS3 = saved.as3
		qPK = saved.pk; qChangedCol = saved.cc; qFormat = saved.fmt; qBintrailID = saved.bid
	})

	qPK = ""
	qChangedCol = ""
	qFormat = "table"
	qNoArchive = true
	qArchiveDir = "/some/dir"
	qArchiveS3 = ""
	qBintrailID = "abc"

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error for --no-archive + --archive-dir")
	}
	if !strings.Contains(err.Error(), "--no-archive cannot be combined") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ─── archiveSources ──────────────────────────────────────────────────────────

func TestArchiveSources_both(t *testing.T) {
	savedDir, savedS3, savedID := qArchiveDir, qArchiveS3, qBintrailID
	t.Cleanup(func() { qArchiveDir = savedDir; qArchiveS3 = savedS3; qBintrailID = savedID })

	qArchiveDir = "/data/archives"
	qArchiveS3 = "s3://bucket/prefix"
	qBintrailID = "abc-123"

	srcs := archiveSources()
	if len(srcs) != 2 {
		t.Fatalf("expected 2 sources, got %d: %v", len(srcs), srcs)
	}
	if srcs[0] != "/data/archives/bintrail_id=abc-123" {
		t.Errorf("unexpected dir source: %q", srcs[0])
	}
	if srcs[1] != "s3://bucket/prefix/bintrail_id=abc-123" {
		t.Errorf("unexpected s3 source: %q", srcs[1])
	}
}

func TestArchiveSources_dirOnly(t *testing.T) {
	savedDir, savedS3, savedID := qArchiveDir, qArchiveS3, qBintrailID
	t.Cleanup(func() { qArchiveDir = savedDir; qArchiveS3 = savedS3; qBintrailID = savedID })

	qArchiveDir = "/data/archives"
	qArchiveS3 = ""
	qBintrailID = "abc-123"

	srcs := archiveSources()
	if len(srcs) != 1 || srcs[0] != "/data/archives/bintrail_id=abc-123" {
		t.Errorf("expected [/data/archives/bintrail_id=abc-123], got %v", srcs)
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

// ─── query.MergeResults ─────────────────────────────────────────────────────────────

func TestMergeResults_deduplicatesByEventID(t *testing.T) {
	t0 := time.Date(2026, 2, 28, 10, 0, 0, 0, time.UTC)
	rows := []query.ResultRow{
		{EventID: 1, EventTimestamp: t0, SchemaName: "db", TableName: "t"},
		{EventID: 1, EventTimestamp: t0, SchemaName: "db", TableName: "t"}, // duplicate
		{EventID: 2, EventTimestamp: t0.Add(time.Second), SchemaName: "db", TableName: "t"},
	}
	got := query.MergeResults(rows, 0)
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
	got := query.MergeResults(rows, 0)
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
	got := query.MergeResults(rows, 0)
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
	got := query.MergeResults(rows, 2)
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
	got := query.MergeResults(rows, 0)
	if len(got) != 3 {
		t.Errorf("expected all 3 rows when limit=0, got %d", len(got))
	}
}

func TestMergeResults_empty(t *testing.T) {
	if got := query.MergeResults(nil, 10); len(got) != 0 {
		t.Errorf("expected empty result for nil input, got %v", got)
	}
}

// ─── handleArchiveFetchError (issue #203) ────────────────────────────────────
//
// These tests pin down the fix for issue #203: archive fetch failures in
// `bintrail query` were previously swallowed by slog.Warn + continue, producing
// empty or partial results with exit 0 and no visible stderr signal. The new
// helper must (a) always emit a stderr line for real errors regardless of log
// level, (b) short-circuit the loop when the context is canceled or its
// deadline has exceeded, and (c) leave the per-source "keep going" semantics
// untouched for non-cancellation errors so one broken archive doesn't kill the
// whole query.

func TestHandleArchiveFetchError_plainErrorWritesStderr(t *testing.T) {
	var buf bytes.Buffer
	err := handleArchiveFetchError(
		context.Background(),
		"s3://bucket/prefix/bintrail_id=abc",
		errors.New("DuckDB Binder Error: column connection_id not found"),
		&buf,
	)
	if err != nil {
		t.Fatalf("expected nil (keep going) for plain error, got %v", err)
	}
	out := buf.String()
	// The message must name the source AND include the underlying error so
	// operators can diagnose without turning on --log-level debug. This is
	// the visibility guarantee #203 asks for — assert it verbatim so a
	// regression is caught on the next change.
	if !strings.Contains(out, "Warning: archive query failed") {
		t.Errorf("stderr missing 'Warning: archive query failed' prefix: %q", out)
	}
	if !strings.Contains(out, "s3://bucket/prefix/bintrail_id=abc") {
		t.Errorf("stderr missing source path: %q", out)
	}
	if !strings.Contains(out, "Binder Error") {
		t.Errorf("stderr missing underlying error detail: %q", out)
	}
	if !strings.HasSuffix(out, "\n") {
		t.Errorf("stderr line must end with newline for clean CLI output: %q", out)
	}
}

func TestHandleArchiveFetchError_canceledContextShortCircuits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // canceled before the call
	var buf bytes.Buffer
	err := handleArchiveFetchError(
		ctx,
		"/local/archive/bintrail_id=abc",
		// parquetquery.Fetch typically wraps the ctx error, but the helper
		// only looks at ctx.Err(), so any fetchErr is fine here.
		errors.New("context canceled"),
		&buf,
	)
	if err == nil {
		t.Fatal("expected non-nil error for canceled context (short-circuit), got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected errors.Is(err, context.Canceled), got %v", err)
	}
	if !strings.Contains(err.Error(), "query canceled") {
		t.Errorf("expected wrapped error to mention 'query canceled', got %v", err)
	}
	// On cancellation we must NOT print an "archive failed" warning — the
	// user pressed Ctrl-C, they don't want per-source noise.
	if buf.Len() != 0 {
		t.Errorf("expected no stderr output on cancellation, got %q", buf.String())
	}
}

func TestHandleArchiveFetchError_deadlineExceededShortCircuits(t *testing.T) {
	// Use a deadline in the past so ctx.Err() already reports
	// DeadlineExceeded at the first check inside the helper.
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	var buf bytes.Buffer
	err := handleArchiveFetchError(
		ctx,
		"s3://bucket/prefix/bintrail_id=abc",
		errors.New("operation took too long"),
		&buf,
	)
	if err == nil {
		t.Fatal("expected non-nil error for expired deadline, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected errors.Is(err, context.DeadlineExceeded), got %v", err)
	}
	if buf.Len() != 0 {
		t.Errorf("expected no stderr output on deadline expiry, got %q", buf.String())
	}
}

func TestHandleArchiveFetchError_multipleSourcesKeepGoing(t *testing.T) {
	// Simulates the common case: two broken archive sources, context still
	// live. Both failures must be reported on stderr, helper must return
	// nil for each, matching the "one bad source doesn't kill the query"
	// behavior operators rely on.
	var buf bytes.Buffer
	ctx := context.Background()

	if err := handleArchiveFetchError(ctx, "src1", errors.New("AccessDenied"), &buf); err != nil {
		t.Fatalf("src1 unexpectedly returned error: %v", err)
	}
	if err := handleArchiveFetchError(ctx, "src2", errors.New("memory_limit exceeded"), &buf); err != nil {
		t.Fatalf("src2 unexpectedly returned error: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "src1") || !strings.Contains(out, "AccessDenied") {
		t.Errorf("stderr missing src1 failure: %q", out)
	}
	if !strings.Contains(out, "src2") || !strings.Contains(out, "memory_limit") {
		t.Errorf("stderr missing src2 failure: %q", out)
	}
	if strings.Count(out, "Warning: archive query failed") != 2 {
		t.Errorf("expected exactly 2 warning lines, got: %q", out)
	}
}
