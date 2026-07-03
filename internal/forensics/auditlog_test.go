package forensics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// ---------------------------------------------------------------------------
// Filter tests
// ---------------------------------------------------------------------------

func TestAuditLogFilter_TimeRange(t *testing.T) {
	since := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	until := time.Date(2024, 6, 15, 10, 35, 0, 0, time.UTC)

	f := auditLogFilter{since: since, until: until}

	tests := []struct {
		ts   string
		want bool
	}{
		{"2024-06-15T10:29:00Z", false}, // before since
		{"2024-06-15T10:30:00Z", true},  // exactly since
		{"2024-06-15T10:32:00Z", true},  // within range
		{"2024-06-15T10:35:00Z", false}, // exactly until (exclusive)
		{"2024-06-15T10:36:00Z", false}, // after until
	}

	for _, tt := range tests {
		ev := &AuditEvent{Timestamp: tt.ts}
		if got := f.matches(ev); got != tt.want {
			t.Errorf("filter(%s) = %v, want %v", tt.ts, got, tt.want)
		}
	}
}

func TestAuditLogFilter_UnparseableTimestampExcluded(t *testing.T) {
	f := auditLogFilter{since: time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)}

	// Events with unparseable timestamps should be excluded when time
	// bounds are set, not silently included.
	ev := &AuditEvent{Timestamp: "not-a-date", User: "root"}
	if f.matches(ev) {
		t.Error("unparseable timestamp should be excluded when since is set")
	}
}

func TestAuditLogFilter_NoTimeBoundsAcceptsAny(t *testing.T) {
	f := auditLogFilter{} // no time bounds
	ev := &AuditEvent{Timestamp: "not-a-date", User: "root"}
	if !f.matches(ev) {
		t.Error("with no time bounds, any timestamp should be accepted")
	}
}

func TestAuditLogFilter_User(t *testing.T) {
	f := auditLogFilter{user: "root"}

	if !f.matches(&AuditEvent{Timestamp: "2024-06-15T10:30:00Z", User: "root"}) {
		t.Error("should match user 'root'")
	}
	if !f.matches(&AuditEvent{Timestamp: "2024-06-15T10:30:00Z", User: "ROOT"}) {
		t.Error("should match case-insensitively")
	}
	if f.matches(&AuditEvent{Timestamp: "2024-06-15T10:30:00Z", User: "app"}) {
		t.Error("should not match user 'app'")
	}
}

func TestAuditLogFilter_EventType(t *testing.T) {
	f := auditLogFilter{eventType: "Query"}

	if !f.matches(&AuditEvent{Timestamp: "2024-06-15T10:30:00Z", EventType: "Query"}) {
		t.Error("should match event_type 'Query'")
	}
	if !f.matches(&AuditEvent{Timestamp: "2024-06-15T10:30:00Z", EventType: "query"}) {
		t.Error("should match case-insensitively")
	}
	if f.matches(&AuditEvent{Timestamp: "2024-06-15T10:30:00Z", EventType: "Connect"}) {
		t.Error("should not match event_type 'Connect'")
	}
}

func TestParseFlexTimestamp(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"2024-06-15T10:30:00Z", "2024-06-15T10:30:00Z"},
		{"2024-06-15T10:30:00", "2024-06-15T10:30:00Z"},
		{"2024-06-15 10:30:00", "2024-06-15T10:30:00Z"},
		{"20240615 10:30:00", "2024-06-15T10:30:00Z"},
	}
	for _, tt := range tests {
		got, err := parseFlexTimestamp(tt.input)
		if err != nil {
			t.Errorf("parseFlexTimestamp(%q) error: %v", tt.input, err)
			continue
		}
		if got.UTC().Format(time.RFC3339) != tt.want {
			t.Errorf("parseFlexTimestamp(%q) = %v, want %v", tt.input, got.UTC().Format(time.RFC3339), tt.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Tail-mode
// ---------------------------------------------------------------------------

// TestResolveTailLines protects the auto-default from silently reverting:
// when Since is set but TailLines is 0, tail-mode must kick in (a regression
// here re-introduces full-file scans on time-filtered queries). Negative
// TailLines is the explicit full-scan escape hatch.
func TestResolveTailLines(t *testing.T) {
	since := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name      string
		since     time.Time
		tailLines int
		want      int
	}{
		{"since set, tail_lines omitted -> default applied", since, 0, defaultTailLines},
		{"since set, explicit tail_lines kept", since, 500, 500},
		{"since set, negative forces full scan", since, -1, 0},
		{"no since, no tail_lines -> full scan", time.Time{}, 0, 0},
		{"no since, explicit tail_lines kept", time.Time{}, 2000, 2000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveTailLines(tc.tailLines, tc.since); got != tc.want {
				t.Errorf("resolveTailLines(%d, since=%v) = %d, want %d", tc.tailLines, !tc.since.IsZero(), got, tc.want)
			}
		})
	}
}

// TestTailReader_LocalFile exercises the tailReader helper that powers the
// on-disk tail path: below-threshold files pass through unchanged, large
// files seek to the tail and drop the partial leading line, and tailBytes=0
// disables the behaviour.
func TestTailReader_LocalFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.log")
	var sb strings.Builder
	for i := range 100 {
		fmt.Fprintf(&sb, "line-%03d,padding-padding-padding-padding\n", i)
	}
	if err := os.WriteFile(path, []byte(sb.String()), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	t.Run("tailBytes=0 returns file unchanged", func(t *testing.T) {
		f, _ := os.Open(path)
		defer f.Close()
		r, seeked, err := tailReader(f, 0)
		if err != nil {
			t.Fatalf("tailReader: %v", err)
		}
		if seeked {
			t.Error("seeked = true for tailBytes=0, want false")
		}
		got, _ := io.ReadAll(r)
		if string(got) != sb.String() {
			t.Errorf("expected full contents, got %d bytes (want %d)", len(got), sb.Len())
		}
	})

	t.Run("file smaller than tailBytes returns unchanged", func(t *testing.T) {
		f, _ := os.Open(path)
		defer f.Close()
		r, seeked, err := tailReader(f, 1<<20) // 1 MB, bigger than ~5 KB file
		if err != nil {
			t.Fatalf("tailReader: %v", err)
		}
		if seeked {
			t.Error("seeked = true for small file, want false (remediation copy would mislead the caller)")
		}
		got, _ := io.ReadAll(r)
		if len(got) != sb.Len() {
			t.Errorf("expected file unchanged, got %d bytes (want %d)", len(got), sb.Len())
		}
	})

	t.Run("large file seeks and drops partial first line", func(t *testing.T) {
		f, _ := os.Open(path)
		defer f.Close()
		r, seeked, err := tailReader(f, 500) // seek back ~500 B from ~5 KB file
		if err != nil {
			t.Fatalf("tailReader: %v", err)
		}
		if !seeked {
			t.Error("seeked = false for large file, want true")
		}
		got, _ := io.ReadAll(r)
		// First byte must be the start of a well-formed line.
		if !strings.HasPrefix(string(got), "line-") {
			t.Errorf("tail did not start at a line boundary; first 40 bytes: %q", string(got[:min(40, len(got))]))
		}
		// The last line must be the file's final line (boundary-safe).
		if !strings.HasSuffix(string(got), "line-099,padding-padding-padding-padding\n") {
			t.Errorf("tail missing final line; last 80 bytes: %q", string(got[max(0, len(got)-80):]))
		}
	})
}

// ---------------------------------------------------------------------------
// Format detection
// ---------------------------------------------------------------------------

func TestDetectAuditLogFormat_ContentBased(t *testing.T) {
	tests := []struct {
		name    string
		content string
		variant AuditVariant
		want    AuditFormat
	}{
		{"json_object", `{"timestamp":"..."}`, "", AuditFormatJSON},
		{"json_array", `[{"timestamp":"..."}]`, "", AuditFormatJSON},
		{"xml", `<?xml version="1.0"?>`, "", AuditFormatXML},
		{"mariadb_hint", `20240615 10:30:00,server,root`, AuditVariantMariaDB, AuditFormatMariaDB},
		{"aurora_epoch_with_mariadb_hint", `1520091734997155,server,root`, AuditVariantMariaDB, AuditFormatMariaDB},
		{"percona_csv", `"2024-06-15","root","localhost"`, "", AuditFormatCSV},
		{"undetectable", `garbage content`, "", AuditFormatUnknown},
		// A valid-but-empty file falls back to the discovered variant so it is
		// parsed (to zero events), not rejected as "unknown format" (#5).
		{"empty_mariadb_variant", "", AuditVariantMariaDB, AuditFormatMariaDB},
		{"empty_whitespace_mariadb", "   \n\t\n", AuditVariantMariaDB, AuditFormatMariaDB},
		{"empty_no_variant", "", "", AuditFormatUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpFile := filepath.Join(t.TempDir(), "audit.log")
			if err := os.WriteFile(tmpFile, []byte(tt.content), 0o644); err != nil {
				t.Fatal(err)
			}
			if got := detectAuditLogFormat(tmpFile, tt.variant); got != tt.want {
				t.Errorf("detectAuditLogFormat() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDetectAuditLogFormat_ByExtension(t *testing.T) {
	tests := []struct {
		ext     string
		variant AuditVariant
		want    AuditFormat
	}{
		{".json", "", AuditFormatJSON},
		{".xml", "", AuditFormatXML},
		{".csv", "", AuditFormatCSV},
		{".csv", AuditVariantMariaDB, AuditFormatMariaDB},
	}

	for _, tt := range tests {
		t.Run(tt.ext+"_"+string(tt.variant), func(t *testing.T) {
			tmpFile := filepath.Join(t.TempDir(), "audit"+tt.ext)
			if err := os.WriteFile(tmpFile, []byte("some content"), 0o644); err != nil {
				t.Fatal(err)
			}
			if got := detectAuditLogFormat(tmpFile, tt.variant); got != tt.want {
				t.Errorf("detectAuditLogFormat() = %q, want %q", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Rotated file collection
// ---------------------------------------------------------------------------

func TestCollectAuditLogFiles(t *testing.T) {
	t.Run("primary only without includeRotated", func(t *testing.T) {
		dir := t.TempDir()
		primary := filepath.Join(dir, "audit.log")
		mustWrite(t, primary, "x")
		mustWrite(t, filepath.Join(dir, "audit.log.1"), "x")

		files, warns := collectAuditLogFiles(primary, false)
		if len(warns) != 0 {
			t.Errorf("unexpected warnings: %v", warns)
		}
		if len(files) != 1 || files[0] != primary {
			t.Errorf("files = %v, want just the primary", files)
		}
	})

	t.Run("missing primary yields empty set", func(t *testing.T) {
		files, _ := collectAuditLogFiles(filepath.Join(t.TempDir(), "absent.log"), false)
		if len(files) != 0 {
			t.Errorf("files = %v, want empty", files)
		}
	})

	t.Run("rotated files sorted newest-first after primary", func(t *testing.T) {
		dir := t.TempDir()
		primary := filepath.Join(dir, "audit.log")
		older := filepath.Join(dir, "audit.log.2")
		newer := filepath.Join(dir, "audit.log.1")
		dateSuffixed := filepath.Join(dir, "audit.log-20240101")
		unrelated := filepath.Join(dir, "other.log")
		mustWrite(t, primary, "p")
		mustWrite(t, older, "o")
		mustWrite(t, newer, "n")
		mustWrite(t, dateSuffixed, "d")
		mustWrite(t, unrelated, "u")
		// Also a directory whose name matches the rotation prefix — must be
		// skipped.
		if err := os.Mkdir(filepath.Join(dir, "audit.log.d"), 0o755); err != nil {
			t.Fatal(err)
		}

		now := time.Now()
		for i, p := range []string{dateSuffixed, older, newer} { // oldest → newest
			mt := now.Add(time.Duration(i-3) * time.Hour)
			if err := os.Chtimes(p, mt, mt); err != nil {
				t.Fatal(err)
			}
		}

		files, warns := collectAuditLogFiles(primary, true)
		if len(warns) != 0 {
			t.Errorf("unexpected warnings: %v", warns)
		}
		want := []string{primary, newer, older, dateSuffixed}
		if len(files) != len(want) {
			t.Fatalf("files = %v, want %v", files, want)
		}
		for i := range want {
			if files[i] != want[i] {
				t.Errorf("files[%d] = %s, want %s", i, files[i], want[i])
			}
		}
	})

	t.Run("cap keeps the NEWEST rotated files and warns on the drop", func(t *testing.T) {
		dir := t.TempDir()
		primary := filepath.Join(dir, "audit.log")
		mustWrite(t, primary, "p")
		// Create more rotated files than the cap, with mtimes ascending in
		// index order (file .0 oldest, .(N+4) newest). Filenames are in a fixed
		// zero-padded order so os.ReadDir returns them oldest-first — the order
		// the pre-fix code capped in, which would keep the oldest.
		total := maxRotatedFiles + 5
		now := time.Now()
		for i := range total {
			p := filepath.Join(dir, fmt.Sprintf("audit.log.%03d", i))
			mustWrite(t, p, "r")
			mt := now.Add(time.Duration(i-total) * time.Minute)
			if err := os.Chtimes(p, mt, mt); err != nil {
				t.Fatal(err)
			}
		}

		files, warns := collectAuditLogFiles(primary, true)
		if len(files) != 1+maxRotatedFiles {
			t.Fatalf("len(files) = %d, want %d (primary + cap)", len(files), 1+maxRotatedFiles)
		}
		if !warningsContain(warns, "older history was not read") {
			t.Errorf("expected a truncation warning, got %v", warns)
		}
		// The newest rotated file (.(total-1)) must be kept; the oldest (.000)
		// must be dropped — the pre-fix behaviour was the reverse.
		newest := filepath.Join(dir, fmt.Sprintf("audit.log.%03d", total-1))
		oldest := filepath.Join(dir, "audit.log.000")
		var haveNewest, haveOldest bool
		for _, f := range files {
			if f == newest {
				haveNewest = true
			}
			if f == oldest {
				haveOldest = true
			}
		}
		if !haveNewest {
			t.Error("cap dropped the NEWEST rotated file; must keep the most recent history")
		}
		if haveOldest {
			t.Error("cap kept the OLDEST rotated file; the newest should win")
		}
	})
}

// ---------------------------------------------------------------------------
// Orchestrator: paging, warnings, non-fatal errors, tail
// ---------------------------------------------------------------------------

func TestParseAuditLogFiles_OffsetLimitAcrossFiles(t *testing.T) {
	dir := t.TempDir()
	file1 := filepath.Join(dir, "audit.json")
	file2 := filepath.Join(dir, "audit.json.1")
	var sb1, sb2 strings.Builder
	for i := range 3 {
		fmt.Fprintf(&sb1, `{"timestamp":"2024-06-15T10:3%d:00Z","name":"Query","user":"root","sqltext":"F1-%d"}`+"\n", i, i)
		fmt.Fprintf(&sb2, `{"timestamp":"2024-06-15T11:3%d:00Z","name":"Query","user":"root","sqltext":"F2-%d"}`+"\n", i, i)
	}
	mustWrite(t, file1, sb1.String())
	mustWrite(t, file2, sb2.String())

	res, err := parseAuditLogFiles([]string{file1, file2}, AuditFormatJSON, auditLogFilter{}, 2, 3, 0)
	if err != nil {
		t.Fatalf("parseAuditLogFiles: %v", err)
	}
	if res.totalScanned != 6 {
		t.Errorf("totalScanned = %d, want 6", res.totalScanned)
	}
	want := []string{"F1-2", "F2-0", "F2-1"}
	if len(res.events) != len(want) {
		t.Fatalf("events = %d, want %d: %+v", len(res.events), len(want), res.events)
	}
	for i, w := range want {
		if res.events[i].SQLText != w {
			t.Errorf("events[%d].SQLText = %q, want %q", i, res.events[i].SQLText, w)
		}
	}
}

func TestParseAuditLogFiles_LimitStopsBeforeLaterFiles(t *testing.T) {
	dir := t.TempDir()
	file1 := filepath.Join(dir, "audit.json")
	file2 := filepath.Join(dir, "audit.json.1")
	mustWrite(t, file1, `{"timestamp":"2024-06-15T10:30:00Z","name":"Query","user":"root"}`+"\n"+
		`{"timestamp":"2024-06-15T10:31:00Z","name":"Query","user":"root"}`+"\n")
	mustWrite(t, file2, `{"timestamp":"2024-06-15T11:30:00Z","name":"Query","user":"root"}`+"\n")

	res, err := parseAuditLogFiles([]string{file1, file2}, AuditFormatJSON, auditLogFilter{}, 0, 2, 0)
	if err != nil {
		t.Fatalf("parseAuditLogFiles: %v", err)
	}
	if len(res.events) != 2 {
		t.Fatalf("events = %d, want 2", len(res.events))
	}
	// The second file must not have been scanned at all.
	if res.totalScanned != 2 {
		t.Errorf("totalScanned = %d, want 2 (limit reached before file 2)", res.totalScanned)
	}
}

func TestParseAuditLogFiles_UnreadableFileWarnsAndContinues(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "gone.json")
	real := filepath.Join(dir, "audit.json")
	mustWrite(t, real, `{"timestamp":"2024-06-15T10:30:00Z","name":"Query","user":"root"}`+"\n")

	res, err := parseAuditLogFiles([]string{missing, real}, AuditFormatJSON, auditLogFilter{}, 0, 10, 0)
	if err != nil {
		t.Fatalf("parseAuditLogFiles: %v", err)
	}
	if len(res.events) != 1 {
		t.Errorf("events = %d, want 1 (real file still parsed)", len(res.events))
	}
	if !warningsContain(res.warnings, "could not open") {
		t.Errorf("warnings = %v, want a 'could not open' entry", res.warnings)
	}
}

// TestParseAuditLogFiles_OversizedRecordSkippedNotFatal: a single record larger
// than maxAuditLineBytes must be skipped and counted, NOT abort the scan —
// records after it still parse. The old bufio.Scanner returned ErrTooLong here
// and dropped the rest of the file; the whole point of the fix is that a valid
// record following an oversized one is still returned (#7).
func TestParseAuditLogFiles_OversizedRecordSkippedNotFatal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.json")
	mustWrite(t, path,
		`{"timestamp":"2024-06-15T10:30:00Z","name":"Query","user":"root"}`+"\n"+
			`{"timestamp":"2024-06-15T10:31:00Z","name":"Query","pad":"`+strings.Repeat("x", 2<<20)+`"}`+"\n"+
			`{"timestamp":"2024-06-15T10:32:00Z","name":"Query","user":"dave"}`+"\n")

	res, err := parseAuditLogFiles([]string{path}, AuditFormatJSON, auditLogFilter{}, 0, 10, 0)
	if err != nil {
		t.Fatalf("parseAuditLogFiles returned fatal error: %v", err)
	}
	// Both bracketing records parse — proving the scan continued past the
	// oversized middle record instead of aborting on it.
	if len(res.events) != 2 {
		t.Fatalf("events = %d, want 2 (records before AND after the oversized one)", len(res.events))
	}
	if got := res.events[0].User; got != "root" {
		t.Errorf("first event user = %q, want root", got)
	}
	if got := res.events[1].User; got != "dave" {
		t.Errorf("second event user = %q, want dave — the record after the oversized one was lost", got)
	}
	if res.skippedLines < 1 {
		t.Errorf("skippedLines = %d, want >= 1 (the oversized record counted as skipped)", res.skippedLines)
	}
}

// TestParseAuditLogFiles_TailMissWarning: a tail-seek that found scanned
// lines but zero in-window events must tell the caller how to widen the
// search; a file read in full must not (nothing to enlarge).
func TestParseAuditLogFiles_TailMissWarning(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.log")
	base := time.Date(2026, 4, 13, 0, 0, 0, 0, time.UTC)
	var sb strings.Builder
	for i := range 100 {
		ts := base.Add(time.Duration(i) * time.Second)
		sb.WriteString(ts.Format("20060102 15:04:05"))
		sb.WriteString(",host,admin,10.0.0.1,42,1,QUERY,mydb,'SELECT 1',0\n")
	}
	mustWrite(t, path, sb.String())

	// Window entirely after the log's events.
	filter := auditLogFilter{since: base.Add(24 * time.Hour)}

	t.Run("tail seek with zero in-window events warns", func(t *testing.T) {
		res, err := parseAuditLogFiles([]string{path}, AuditFormatMariaDB, filter, 0, 10, 512)
		if err != nil {
			t.Fatalf("parseAuditLogFiles: %v", err)
		}
		if len(res.events) != 0 {
			t.Fatalf("events = %d, want 0", len(res.events))
		}
		if !warningsContain(res.warnings, "tail seek of") {
			t.Errorf("warnings = %v, want a tail-miss remediation entry", res.warnings)
		}
	})

	t.Run("full read (file smaller than tail) does not warn", func(t *testing.T) {
		res, err := parseAuditLogFiles([]string{path}, AuditFormatMariaDB, filter, 0, 10, 1<<20)
		if err != nil {
			t.Fatalf("parseAuditLogFiles: %v", err)
		}
		if warningsContain(res.warnings, "tail seek of") {
			t.Errorf("warnings = %v, tail-miss warning must not fire when the file was read in full", res.warnings)
		}
	})
}

// TestParseAuditLogFiles_TailDiscardsPartialLine: after a tail seek the
// first (partial) line is discarded by tailReader, so the parser only ever
// sees whole records — skipped must stay 0.
func TestParseAuditLogFiles_TailDiscardsPartialLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.log")
	base := time.Date(2026, 4, 13, 0, 0, 0, 0, time.UTC)
	var sb strings.Builder
	const total = 200
	for i := range total {
		ts := base.Add(time.Duration(i) * time.Second)
		sb.WriteString(ts.Format("20060102 15:04:05"))
		sb.WriteString(",host,admin,10.0.0.1,42,1,QUERY,mydb,'SELECT 1',0\n")
	}
	mustWrite(t, path, sb.String())

	res, err := parseAuditLogFiles([]string{path}, AuditFormatMariaDB, auditLogFilter{}, 0, total, 1000)
	if err != nil {
		t.Fatalf("parseAuditLogFiles: %v", err)
	}
	if res.skippedLines != 0 {
		t.Errorf("skippedLines = %d, want 0 — the partial first line must be discarded by the tail reader, not hit the parser", res.skippedLines)
	}
	if len(res.events) == 0 || len(res.events) >= total {
		t.Errorf("events = %d, want a strict subset of %d (tail only)", len(res.events), total)
	}
}

// ---------------------------------------------------------------------------
// Fixture files: realistic per-format samples end-to-end through detection
// and parsing.
// ---------------------------------------------------------------------------

func TestParseAuditLogFiles_Fixtures(t *testing.T) {
	cases := []struct {
		file       string
		variant    AuditVariant
		wantFormat AuditFormat
		wantEvents int
		check      func(t *testing.T, events []AuditEvent)
	}{
		{
			file: "mariadb_upstream.log", variant: AuditVariantMariaDB,
			wantFormat: AuditFormatMariaDB, wantEvents: 5,
			check: func(t *testing.T, events []AuditEvent) {
				if events[0].SQLText != "SELECT id, name FROM users WHERE id = 1" {
					t.Errorf("commas inside quoted query mis-split: %q", events[0].SQLText)
				}
				if events[1].SQLText != "INSERT INTO t1 VALUES (1, 'a,b', 2)" {
					t.Errorf("escaped quotes mis-parsed: %q", events[1].SQLText)
				}
				if events[4].EventType != "CREATE_TABLE" || events[4].SQLText != "orders" {
					t.Errorf("table op mis-parsed: %+v", events[4])
				}
			},
		},
		{
			file: "rds_mysql.log", variant: AuditVariantMariaDB,
			wantFormat: AuditFormatMariaDB, wantEvents: 4,
			check: func(t *testing.T, events []AuditEvent) {
				if events[0].SQLText != "SELECT 1, 2, 3" {
					t.Errorf("RDS 12-field QUERY row mis-parsed: %q", events[0].SQLText)
				}
				if events[2].EventType != "FAILED_CONNECT" || events[2].Status != 1045 {
					t.Errorf("RDS FAILED_CONNECT row mis-parsed: %+v", events[2])
				}
			},
		},
		{
			file: "percona.csv", variant: AuditVariantPercona,
			wantFormat: AuditFormatCSV, wantEvents: 3,
			check: func(t *testing.T, events []AuditEvent) {
				if events[2].SQLText != `INSERT INTO t1 VALUES(1, "x,y")` {
					t.Errorf("Percona quoted comma mis-parsed: %q", events[2].SQLText)
				}
			},
		},
		{
			file: "enterprise.json", variant: AuditVariantMySQLEnterprise,
			wantFormat: AuditFormatJSON, wantEvents: 2,
			check: func(t *testing.T, events []AuditEvent) {
				if events[0].EventType != "general/status" || events[0].SQLText != "SELECT 1" {
					t.Errorf("enterprise JSON mis-parsed: %+v", events[0])
				}
			},
		},
		{
			file: "enterprise.xml", variant: AuditVariantMySQLEnterprise,
			wantFormat: AuditFormatXML, wantEvents: 2,
			check: func(t *testing.T, events []AuditEvent) {
				if events[0].SQLText != "SELECT * FROM users" || events[0].ConnectionID != 42 {
					t.Errorf("enterprise XML mis-parsed: %+v", events[0])
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.file, func(t *testing.T) {
			path := filepath.Join("testdata", tc.file)
			format := detectAuditLogFormat(path, tc.variant)
			if format != tc.wantFormat {
				t.Fatalf("detectAuditLogFormat = %q, want %q", format, tc.wantFormat)
			}
			res, err := parseAuditLogFiles([]string{path}, format, auditLogFilter{}, 0, 100, 0)
			if err != nil {
				t.Fatalf("parseAuditLogFiles: %v", err)
			}
			if res.skippedLines != 0 {
				t.Errorf("skippedLines = %d, want 0", res.skippedLines)
			}
			if len(res.events) != tc.wantEvents {
				t.Fatalf("events = %d, want %d: %+v", len(res.events), tc.wantEvents, res.events)
			}
			tc.check(t, res.events)
		})
	}
}

// ---------------------------------------------------------------------------
// ReadAuditLog: discovery, hardening, and end-to-end via sqlmock
// ---------------------------------------------------------------------------

const (
	showAuditLogFileSQL    = "SHOW GLOBAL VARIABLES LIKE 'audit_log_file'"
	showServerAuditPathSQL = "SHOW GLOBAL VARIABLES LIKE 'server_audit_file_path'"
	showDataDirSQL         = "SHOW GLOBAL VARIABLES LIKE 'datadir'"
	pluginsSQL             = "SELECT PLUGIN_NAME, PLUGIN_DESCRIPTION FROM information_schema.PLUGINS"
)

func variableRows(name, value string) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow(name, value)
}

func noVariableRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"Variable_name", "Value"})
}

func pluginRows(rows ...[2]string) *sqlmock.Rows {
	r := sqlmock.NewRows([]string{"PLUGIN_NAME", "PLUGIN_DESCRIPTION"})
	for _, row := range rows {
		r.AddRow(row[0], row[1])
	}
	return r
}

func TestReadAuditLog_NotConfigured(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(showAuditLogFileSQL).WillReturnRows(noVariableRows())
	mock.ExpectQuery(showServerAuditPathSQL).WillReturnRows(noVariableRows())

	_, err = ReadAuditLog(context.Background(), db, AuditReadOptions{})
	if !errors.Is(err, ErrAuditNotConfigured) {
		t.Fatalf("err = %v, want ErrAuditNotConfigured", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestReadAuditLog_DiscoveryQueryFailureIsNotNotConfigured: a server we
// could not even ask must not masquerade as "no audit plugin".
func TestReadAuditLog_DiscoveryQueryFailureIsNotNotConfigured(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	connErr := errors.New("connection refused")
	mock.ExpectQuery(showAuditLogFileSQL).WillReturnError(connErr)
	mock.ExpectQuery(showServerAuditPathSQL).WillReturnError(connErr)

	_, err = ReadAuditLog(context.Background(), db, AuditReadOptions{})
	if err == nil || errors.Is(err, ErrAuditNotConfigured) {
		t.Fatalf("err = %v, want a query failure distinct from ErrAuditNotConfigured", err)
	}
	if !errors.Is(err, connErr) {
		t.Fatalf("err = %v, want wrapped %v", err, connErr)
	}
}

func TestReadAuditLog_PerconaJSONEndToEnd(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "audit.json")
	mustWrite(t, logPath,
		`{"timestamp":"2024-06-15T10:30:00Z","name":"Query","user":"app_user","host":"10.0.0.1","sqltext":"INSERT INTO t1 VALUES(1)","db":"mydb","connection_id":99,"status":0}`+"\n"+
			`{"timestamp":"2024-06-15T10:31:00Z","name":"Connect","user":"root","host":"localhost","connection_id":100,"status":0}`+"\n")

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(showAuditLogFileSQL).WillReturnRows(variableRows("audit_log_file", logPath))
	mock.ExpectQuery(pluginsSQL).WillReturnRows(pluginRows([2]string{"audit_log", "Percona Audit Log"}))

	res, err := ReadAuditLog(context.Background(), db, AuditReadOptions{})
	if err != nil {
		t.Fatalf("ReadAuditLog: %v", err)
	}
	if res.Variant != AuditVariantPercona {
		t.Errorf("Variant = %q, want percona", res.Variant)
	}
	if res.FormatDetected != AuditFormatJSON {
		t.Errorf("FormatDetected = %q, want json", res.FormatDetected)
	}
	if res.FilePath != logPath {
		t.Errorf("FilePath = %q, want %q", res.FilePath, logPath)
	}
	if res.FilesRead != 1 {
		t.Errorf("FilesRead = %d, want 1", res.FilesRead)
	}
	if len(res.Events) != 2 || res.Events[0].User != "app_user" {
		t.Errorf("Events = %+v, want 2 events with app_user first", res.Events)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestReadAuditLog_MariaDBFallbackDiscovery(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "server_audit.log")
	mustWrite(t, logPath, "20240615 10:30:00,server1,root,localhost,42,1,QUERY,test,'SELECT 1, 2',0\n")

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(showAuditLogFileSQL).WillReturnRows(noVariableRows())
	mock.ExpectQuery(showServerAuditPathSQL).WillReturnRows(variableRows("server_audit_file_path", logPath))

	res, err := ReadAuditLog(context.Background(), db, AuditReadOptions{})
	if err != nil {
		t.Fatalf("ReadAuditLog: %v", err)
	}
	if res.Variant != AuditVariantMariaDB {
		t.Errorf("Variant = %q, want mariadb", res.Variant)
	}
	if res.FormatDetected != AuditFormatMariaDB {
		t.Errorf("FormatDetected = %q, want mariadb", res.FormatDetected)
	}
	if len(res.Events) != 1 || res.Events[0].SQLText != "SELECT 1, 2" {
		t.Errorf("Events = %+v, want the quote-aware parsed query", res.Events)
	}
	if !warningsContain(res.Warnings, "server-local time") {
		t.Errorf("Warnings = %v, want the local-time caveat", res.Warnings)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestReadAuditLog_RelativePathResolvedAgainstDatadir(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "audit.json"),
		`{"timestamp":"2024-06-15T10:30:00Z","name":"Query","user":"root"}`+"\n")

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(showAuditLogFileSQL).WillReturnRows(variableRows("audit_log_file", "audit.json"))
	mock.ExpectQuery(pluginsSQL).WillReturnRows(pluginRows()) // none matched → enterprise default
	mock.ExpectQuery(showDataDirSQL).WillReturnRows(variableRows("datadir", dir))

	res, err := ReadAuditLog(context.Background(), db, AuditReadOptions{})
	if err != nil {
		t.Fatalf("ReadAuditLog: %v", err)
	}
	if res.Variant != AuditVariantMySQLEnterprise {
		t.Errorf("Variant = %q, want mysql_enterprise default", res.Variant)
	}
	if res.FilePath != filepath.Join(dir, "audit.json") {
		t.Errorf("FilePath = %q, want datadir-joined path", res.FilePath)
	}
	if len(res.Events) != 1 {
		t.Errorf("Events = %d, want 1", len(res.Events))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestReadAuditLog_PathHardening(t *testing.T) {
	t.Run("absolute path with traversal rejected", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		mock.ExpectQuery(showAuditLogFileSQL).WillReturnRows(variableRows("audit_log_file", "/var/lib/mysql/../../etc/passwd"))
		mock.ExpectQuery(pluginsSQL).WillReturnRows(pluginRows())

		_, err = ReadAuditLog(context.Background(), db, AuditReadOptions{})
		if err == nil || !strings.Contains(err.Error(), "path traversal") {
			t.Fatalf("err = %v, want path traversal rejection", err)
		}
	})

	t.Run("relative path with traversal rejected before datadir join", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		mock.ExpectQuery(showAuditLogFileSQL).WillReturnRows(variableRows("audit_log_file", "logs/../../../etc/passwd"))
		mock.ExpectQuery(pluginsSQL).WillReturnRows(pluginRows())
		// No datadir expectation: the rejection must fire BEFORE the join
		// would have cleaned the traversal away.

		_, err = ReadAuditLog(context.Background(), db, AuditReadOptions{})
		if err == nil || !strings.Contains(err.Error(), "path traversal") {
			t.Fatalf("err = %v, want path traversal rejection", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unmet expectations (datadir must not be queried): %v", err)
		}
	})

	t.Run("unresolvable relative path must not be parsed", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		mock.ExpectQuery(showAuditLogFileSQL).WillReturnRows(variableRows("audit_log_file", "audit.log"))
		mock.ExpectQuery(pluginsSQL).WillReturnRows(pluginRows())
		mock.ExpectQuery(showDataDirSQL).WillReturnError(errors.New("boom"))

		res, err := ReadAuditLog(context.Background(), db, AuditReadOptions{})
		if err == nil || !strings.Contains(err.Error(), "must be absolute") {
			t.Fatalf("err = %v, want absolute-path requirement", err)
		}
		if !warningsContain(res.Warnings, "could not query datadir") {
			t.Errorf("Warnings = %v, want datadir failure surfaced", res.Warnings)
		}
	})
}

func TestReadAuditLog_FileNotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	missing := filepath.Join(t.TempDir(), "absent.log")
	mock.ExpectQuery(showAuditLogFileSQL).WillReturnRows(variableRows("audit_log_file", missing))
	mock.ExpectQuery(pluginsSQL).WillReturnRows(pluginRows())

	_, err = ReadAuditLog(context.Background(), db, AuditReadOptions{})
	if !errors.Is(err, ErrAuditFileNotFound) {
		t.Fatalf("err = %v, want ErrAuditFileNotFound", err)
	}
}

func TestReadAuditLog_UnknownFormat(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "audit.log")
	mustWrite(t, logPath, "garbage that matches no known format\n")

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(showAuditLogFileSQL).WillReturnRows(variableRows("audit_log_file", logPath))
	mock.ExpectQuery(pluginsSQL).WillReturnRows(pluginRows())

	_, err = ReadAuditLog(context.Background(), db, AuditReadOptions{})
	if !errors.Is(err, ErrAuditUnknownFormat) {
		t.Fatalf("err = %v, want ErrAuditUnknownFormat", err)
	}
}

// TestReadAuditLog_EmptyFileNoError: a validly-configured but empty audit log
// (post-rotation / freshly enabled / quiet server) resolves to its discovered
// plugin variant's format and parses to zero events, rather than hard-failing
// with ErrAuditUnknownFormat as it did before detectAuditLogFormat fell back to
// the variant on empty content (#5).
func TestReadAuditLog_EmptyFileNoError(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "audit.log")
	mustWrite(t, logPath, "") // zero bytes

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	// An empty plugins row makes detectVariantFromPlugin return its
	// MySQLEnterprise default (→ JSON), so detection resolves a concrete format
	// and the empty file parses to zero events via the #5 variant fallback.
	mock.ExpectQuery(showAuditLogFileSQL).WillReturnRows(variableRows("audit_log_file", logPath))
	mock.ExpectQuery(pluginsSQL).WillReturnRows(pluginRows())

	res, err := ReadAuditLog(context.Background(), db, AuditReadOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil — an empty (validly-configured) audit log must not be a hard error", err)
	}
	if len(res.Events) != 0 {
		t.Errorf("events = %d, want 0", len(res.Events))
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}

func warningsContain(warnings []string, substr string) bool {
	for _, w := range warnings {
		if strings.Contains(w, substr) {
			return true
		}
	}
	return false
}
