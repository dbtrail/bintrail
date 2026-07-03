package forensics

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 3-dialect matrix: upstream MariaDB / AWS RDS MySQL fork / Aurora Advanced
// Auditing. The query field is single-quoted with backslash escapes and may
// contain unescaped commas — the naive-split failure mode these cases guard
// against.
// ---------------------------------------------------------------------------

func TestParseMariaDBLine_DialectMatrix(t *testing.T) {
	auroraTime := time.Date(2018, 3, 3, 15, 42, 14, 997155000, time.UTC)
	auroraEpochMicros := auroraTime.UnixMicro()

	cases := []struct {
		name      string
		line      string
		want      AuditEvent
		wantLocal bool
		wantOK    bool
	}{
		{
			name: "upstream QUERY, 10 fields",
			line: `20240615 10:30:00,server1,root,localhost,42,1,QUERY,test,'SELECT 1',0`,
			want: AuditEvent{
				Timestamp: "20240615 10:30:00", User: "root", Host: "localhost",
				ConnectionID: 42, EventType: "QUERY", DB: "test", SQLText: "SELECT 1",
			},
			wantLocal: true, wantOK: true,
		},
		{
			name: "upstream CONNECT, empty object",
			line: `20240615 10:31:00,server1,app,10.0.0.5,43,2,CONNECT,mydb,,0`,
			want: AuditEvent{
				Timestamp: "20240615 10:31:00", User: "app", Host: "10.0.0.5",
				ConnectionID: 43, EventType: "CONNECT", DB: "mydb",
			},
			wantLocal: true, wantOK: true,
		},
		{
			name: "unescaped commas inside quoted query",
			line: `20240615 10:30:00,server1,root,localhost,42,1,QUERY,test,'INSERT INTO t1 VALUES (1, 2, 3)',0`,
			want: AuditEvent{
				Timestamp: "20240615 10:30:00", User: "root", Host: "localhost",
				ConnectionID: 42, EventType: "QUERY", DB: "test",
				SQLText: "INSERT INTO t1 VALUES (1, 2, 3)",
			},
			wantLocal: true, wantOK: true,
		},
		{
			name: "escaped quote inside query (comma after it must not split)",
			line: `20240615 10:30:00,server1,root,localhost,42,1,QUERY,test,'SELECT \'a,b\' FROM t',0`,
			want: AuditEvent{
				Timestamp: "20240615 10:30:00", User: "root", Host: "localhost",
				ConnectionID: 42, EventType: "QUERY", DB: "test",
				SQLText: `SELECT 'a,b' FROM t`,
			},
			wantLocal: true, wantOK: true,
		},
		{
			name: "escaped newline and tab inside query",
			line: `20240615 10:30:00,server1,root,localhost,42,1,QUERY,test,'line1\nline2\tend',0`,
			want: AuditEvent{
				Timestamp: "20240615 10:30:00", User: "root", Host: "localhost",
				ConnectionID: 42, EventType: "QUERY", DB: "test",
				SQLText: "line1\nline2\tend",
			},
			wantLocal: true, wantOK: true,
		},
		{
			name: "escaped backslash and escaped quotes inside query",
			line: `20240615 10:30:00,server1,root,localhost,42,1,QUERY,test,'LOAD DATA INFILE \'C:\\tmp\'',0`,
			want: AuditEvent{
				Timestamp: "20240615 10:30:00", User: "root", Host: "localhost",
				ConnectionID: 42, EventType: "QUERY", DB: "test",
				SQLText: `LOAD DATA INFILE 'C:\tmp'`,
			},
			wantLocal: true, wantOK: true,
		},
		{
			name: "unknown escape preserved verbatim",
			line: `20240615 10:30:00,server1,root,localhost,42,1,QUERY,test,'a\qb',0`,
			want: AuditEvent{
				Timestamp: "20240615 10:30:00", User: "root", Host: "localhost",
				ConnectionID: 42, EventType: "QUERY", DB: "test",
				SQLText: `a\qb`,
			},
			wantLocal: true, wantOK: true,
		},
		{
			name: "masked password passthrough",
			line: `20240615 10:30:00,server1,root,localhost,42,1,QUERY,mysql,'CREATE USER u1@localhost IDENTIFIED BY *****',0`,
			want: AuditEvent{
				Timestamp: "20240615 10:30:00", User: "root", Host: "localhost",
				ConnectionID: 42, EventType: "QUERY", DB: "mysql",
				SQLText: "CREATE USER u1@localhost IDENTIFIED BY *****",
			},
			wantLocal: true, wantOK: true,
		},
		{
			name: "truncated by SERVER_AUDIT_QUERY_LOG_LIMIT (quote still closed)",
			line: `20240615 10:30:00,server1,root,localhost,42,1,QUERY,test,'SELECT very_long_a, very_long_b FROM some_tabl',0`,
			want: AuditEvent{
				Timestamp: "20240615 10:30:00", User: "root", Host: "localhost",
				ConnectionID: 42, EventType: "QUERY", DB: "test",
				SQLText: "SELECT very_long_a, very_long_b FROM some_tabl",
			},
			wantLocal: true, wantOK: true,
		},
		{
			name: "unterminated quote (corrupt record) keeps text to EOL",
			line: `20240615 10:30:00,server1,root,localhost,42,1,QUERY,test,'SELECT 1, 2`,
			want: AuditEvent{
				Timestamp: "20240615 10:30:00", User: "root", Host: "localhost",
				ConnectionID: 42, EventType: "QUERY", DB: "test",
				SQLText: "SELECT 1, 2",
			},
			wantLocal: true, wantOK: true,
		},
		{
			name: "trailing lone backslash at EOL preserved",
			line: `20240615 10:30:00,server1,root,localhost,42,1,QUERY,test,'abc\`,
			want: AuditEvent{
				Timestamp: "20240615 10:30:00", User: "root", Host: "localhost",
				ConnectionID: 42, EventType: "QUERY", DB: "test",
				SQLText: `abc\`,
			},
			wantLocal: true, wantOK: true,
		},
		{
			name: "table operation, unquoted object",
			line: `20240615 10:33:00,server1,root,localhost,42,5,CREATE_TABLE,test,orders,0`,
			want: AuditEvent{
				Timestamp: "20240615 10:33:00", User: "root", Host: "localhost",
				ConnectionID: 42, EventType: "CREATE_TABLE", DB: "test", SQLText: "orders",
			},
			wantLocal: true, wantOK: true,
		},
		{
			name: "RDS fork QUERY, 12 naive fields (two trailing empties)",
			line: `20240615 10:30:00,ip-10-0-0-1,root,localhost,42,1,QUERY,test,'SELECT 1, 2',0,,`,
			want: AuditEvent{
				Timestamp: "20240615 10:30:00", User: "root", Host: "localhost",
				ConnectionID: 42, EventType: "QUERY", DB: "test", SQLText: "SELECT 1, 2",
			},
			wantLocal: true, wantOK: true,
		},
		{
			name: "RDS fork CONNECT, 11 naive fields (connection_type)",
			line: `20240615 10:31:00,ip-10-0-0-1,app,10.0.0.5,43,2,CONNECT,mydb,,0,SSL`,
			want: AuditEvent{
				Timestamp: "20240615 10:31:00", User: "app", Host: "10.0.0.5",
				ConnectionID: 43, EventType: "CONNECT", DB: "mydb",
			},
			wantLocal: true, wantOK: true,
		},
		{
			name: "RDS fork FAILED_CONNECT, nonzero retcode before connection_type",
			line: `20240615 10:32:00,ip-10-0-0-1,baduser,10.0.0.6,44,0,FAILED_CONNECT,,,1045,SOCKET`,
			want: AuditEvent{
				Timestamp: "20240615 10:32:00", User: "baduser", Host: "10.0.0.6",
				ConnectionID: 44, EventType: "FAILED_CONNECT", Status: 1045,
			},
			wantLocal: true, wantOK: true,
		},
		{
			name: "Aurora epoch-microseconds normalised to RFC 3339 UTC",
			line: fmt.Sprintf(`%d,ip-10-0-0-97,admin,10.1.2.3,34,1234,QUERY,test_db,'SELECT scientist_id, name FROM scientists',0`, auroraEpochMicros),
			want: AuditEvent{
				Timestamp: auroraTime.Format(time.RFC3339Nano), User: "admin", Host: "10.1.2.3",
				ConnectionID: 34, EventType: "QUERY", DB: "test_db",
				SQLText: "SELECT scientist_id, name FROM scientists",
			},
			wantLocal: false, wantOK: true,
		},
		{
			name: "Aurora CONNECT",
			line: fmt.Sprintf(`%d,ip-10-0-0-97,admin,10.1.2.3,34,0,CONNECT,,,0`, auroraEpochMicros),
			want: AuditEvent{
				Timestamp: auroraTime.Format(time.RFC3339Nano), User: "admin", Host: "10.1.2.3",
				ConnectionID: 34, EventType: "CONNECT",
			},
			wantLocal: false, wantOK: true,
		},
		{
			name:   "too few fields rejected",
			line:   `20240615 10:30:00,server1,root`,
			wantOK: false,
		},
		{
			name:   "unrecognisable timestamp rejected",
			line:   `garbage,server1,root,localhost,42,1,QUERY,test,'x',0`,
			wantOK: false,
		},
		{
			name:   "all-digits but too short for epoch-micros rejected",
			line:   `20240615,server1,root,localhost,42,1,QUERY,test,'x',0`,
			wantOK: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, local, ok := parseMariaDBLine(tc.line)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (event: %+v)", ok, tc.wantOK, got)
			}
			if !tc.wantOK {
				return
			}
			if local != tc.wantLocal {
				t.Errorf("localTime = %v, want %v", local, tc.wantLocal)
			}
			if got != tc.want {
				t.Errorf("event mismatch:\n got  %+v\n want %+v", got, tc.want)
			}
		})
	}
}

// TestParseMariaDBFile is the ported SaaS baseline: a two-line upstream log.
func TestParseMariaDBFile(t *testing.T) {
	input := `20240615 10:30:00,server1,root,localhost,42,1,QUERY,test,'SELECT 1',0
20240615 10:31:00,server1,app,10.0.0.5,43,2,CONNECT,,,0`

	events, _, _, _, err := parseMariaDBFile(strings.NewReader(input), auditLogFilter{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}

	ev := events[0]
	if ev.User != "root" {
		t.Errorf("expected user 'root', got %q", ev.User)
	}
	if ev.Host != "localhost" {
		t.Errorf("expected host 'localhost', got %q", ev.Host)
	}
	if ev.EventType != "QUERY" {
		t.Errorf("expected event_type 'QUERY', got %q", ev.EventType)
	}
	if ev.SQLText != "SELECT 1" {
		t.Errorf("expected sql_text 'SELECT 1', got %q", ev.SQLText)
	}
	if ev.ConnectionID != 42 {
		t.Errorf("expected connection_id 42, got %d", ev.ConnectionID)
	}
}

func TestParseMariaDBFile_SkipsMalformedAndCounts(t *testing.T) {
	input := `20240615 10:30:00,server1,root,localhost,42,1,QUERY,test,'SELECT 1',0
this line is not an audit record
20240615 10:31:00,server1,app,10.0.0.5,43,2,CONNECT,,,0`

	events, scanned, skipped, _, err := parseMariaDBFile(strings.NewReader(input), auditLogFilter{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 2 {
		t.Errorf("expected 2 events, got %d", len(events))
	}
	if scanned != 2 {
		t.Errorf("expected 2 scanned, got %d", scanned)
	}
	if skipped != 1 {
		t.Errorf("expected 1 skipped line, got %d", skipped)
	}
}

// TestParseMariaDBFile_LocalTimeNote verifies the "assumed UTC" caveat: the
// local-time dialects (upstream + RDS fork) must attach it exactly once, the
// Aurora epoch-microseconds dialect (unambiguously UTC) must not.
func TestParseMariaDBFile_LocalTimeNote(t *testing.T) {
	epoch := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC).UnixMicro()

	t.Run("local-time lines attach the note once", func(t *testing.T) {
		input := `20240615 10:30:00,server1,root,localhost,42,1,QUERY,test,'SELECT 1',0
20240615 10:31:00,server1,root,localhost,42,2,QUERY,test,'SELECT 2',0`
		_, _, _, notes, err := parseMariaDBFile(strings.NewReader(input), auditLogFilter{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(notes) != 1 || notes[0] != mariadbLocalTimeNote {
			t.Errorf("notes = %v, want exactly [%q]", notes, mariadbLocalTimeNote)
		}
	})

	t.Run("Aurora-only file attaches no note", func(t *testing.T) {
		input := fmt.Sprintf("%d,ip-1,admin,10.1.2.3,34,1,QUERY,db,'SELECT 1',0\n", epoch)
		_, _, _, notes, err := parseMariaDBFile(strings.NewReader(input), auditLogFilter{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(notes) != 0 {
			t.Errorf("notes = %v, want none for epoch-microsecond timestamps", notes)
		}
	})
}

// TestParseMariaDBFile_EpochMicrosFiltering proves that time filters work on
// Aurora lines end-to-end: the epoch is normalised to RFC 3339 before the
// filter runs.
func TestParseMariaDBFile_EpochMicrosFiltering(t *testing.T) {
	base := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	var sb strings.Builder
	for i := range 10 {
		ts := base.Add(time.Duration(i) * time.Minute)
		fmt.Fprintf(&sb, "%d,ip-1,admin,10.1.2.3,34,%d,QUERY,db,'SELECT %d',0\n", ts.UnixMicro(), i, i)
	}

	filter := auditLogFilter{
		since: base.Add(3 * time.Minute),
		until: base.Add(6 * time.Minute), // exclusive
	}
	events, scanned, skipped, _, err := parseMariaDBFile(strings.NewReader(sb.String()), filter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if skipped != 0 {
		t.Errorf("skipped = %d, want 0", skipped)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events in [3m,6m), got %d (scanned %d)", len(events), scanned)
	}
	if events[0].SQLText != "SELECT 3" || events[2].SQLText != "SELECT 5" {
		t.Errorf("unexpected window contents: first=%q last=%q", events[0].SQLText, events[2].SQLText)
	}
	// until early-exit: the scan must not have processed all 10 lines.
	if scanned >= 10 {
		t.Errorf("scanned = %d; expected early exit before the end of the file", scanned)
	}
}

// ---------------------------------------------------------------------------
// Ported SaaS regression tests (#1285 in the SaaS repo): filter-in-parser
// must let tight time windows reach the tail of huge files, and until must
// early-exit.
// ---------------------------------------------------------------------------

// TestParseMariaDB_FilterReachesTail: before the filter-in-parser change,
// the parser accumulated the first maxEventsPerFile (100K) events and
// stopped — time-filtered matches near EOF of a large audit log were never
// reached. With the filter applied inline, the cap bounds *matched* events,
// so large files scan to EOF even with tight time windows.
func TestParseMariaDB_FilterReachesTail(t *testing.T) {
	// 150K synthetic RDS-dialect events at 1-second spacing (MariaDB audit
	// timestamps are second-precision). Only the last 10 fall inside the
	// [since, until) window at the very end of the log.
	var sb strings.Builder
	base := time.Date(2026, 4, 13, 0, 0, 0, 0, time.UTC)
	const total = 150_000
	for i := range total {
		ts := base.Add(time.Duration(i) * time.Second)
		sb.WriteString(ts.Format("20060102 15:04:05"))
		sb.WriteString(",host,admin,10.0.0.1,42,100,QUERY,mydb,'SELECT 1',0,,\n")
	}

	cutoff := base.Add(time.Duration(total-10) * time.Second)
	end := base.Add(time.Duration(total) * time.Second)

	filter := auditLogFilter{since: cutoff, until: end}
	events, totalScanned, _, _, err := parseMariaDBFile(strings.NewReader(sb.String()), filter)
	if err != nil {
		t.Fatalf("parseMariaDBFile: %v", err)
	}

	if len(events) != 10 {
		t.Fatalf("matched events = %d, want 10 — filter did not reach tail of %d-event log", len(events), total)
	}
	if events[0].SQLText != "SELECT 1" {
		t.Errorf("SQLText = %q, want %q (quotes must be stripped)", events[0].SQLText, "SELECT 1")
	}
	if totalScanned < total-10 {
		t.Errorf("totalScanned = %d, want >= %d — scan terminated prematurely", totalScanned, total-10)
	}
}

// TestParseMariaDB_EarlyExitUntil verifies that the parser stops once it
// observes a timestamp at or past filter.until: audit logs are append-only
// time-ordered, so nothing useful follows that boundary.
func TestParseMariaDB_EarlyExitUntil(t *testing.T) {
	var sb strings.Builder
	base := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	const total = 1_000
	for i := range total {
		ts := base.Add(time.Duration(i) * time.Second)
		sb.WriteString(ts.Format("20060102 15:04:05"))
		sb.WriteString(",host,admin,10.0.0.1,42,100,QUERY,mydb,'SELECT 1',0,,\n")
	}

	until := base.Add(100 * time.Second)
	filter := auditLogFilter{until: until}
	events, totalScanned, _, _, err := parseMariaDBFile(strings.NewReader(sb.String()), filter)
	if err != nil {
		t.Fatalf("parseMariaDBFile: %v", err)
	}
	if len(events) != 100 {
		t.Errorf("matched events = %d, want 100", len(events))
	}
	if totalScanned >= total {
		t.Errorf("totalScanned = %d; expected early exit at ~100, scanned entire %d-event log", totalScanned, total)
	}
}

func TestScanSingleQuoted(t *testing.T) {
	cases := []struct {
		in             string
		wantContent    string
		wantRest       string
		wantTerminated bool
	}{
		{`'SELECT 1',0`, "SELECT 1", ",0", true},
		{`'a,b,c',0,,`, "a,b,c", ",0,,", true},
		{`'esc \' quote',7`, "esc ' quote", ",7", true},
		{`'back\\slash',0`, `back\slash`, ",0", true},
		{`'nl\nnl',0`, "nl\nnl", ",0", true},
		{`''`, "", "", true},
		{`'unterminated`, "unterminated", "", false},
		{`'trailing\`, `trailing\`, "", false},
	}
	for _, tc := range cases {
		content, rest, terminated := scanSingleQuoted(tc.in)
		if content != tc.wantContent || rest != tc.wantRest || terminated != tc.wantTerminated {
			t.Errorf("scanSingleQuoted(%q) = (%q, %q, %v), want (%q, %q, %v)",
				tc.in, content, rest, terminated, tc.wantContent, tc.wantRest, tc.wantTerminated)
		}
	}
}
