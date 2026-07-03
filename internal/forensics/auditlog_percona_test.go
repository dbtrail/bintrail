package forensics

import (
	"strings"
	"testing"
)

func TestParsePerconaCSV(t *testing.T) {
	input := `"2024-06-15T10:30:00Z","root","localhost","42","1","Query","test","SELECT 1","0"
"2024-06-15T10:31:00Z","app","10.0.0.5","43","2","Connect","","","0"`

	events, _, _, err := parsePerconaCSV(strings.NewReader(input), auditLogFilter{})
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
	if ev.SQLText != "SELECT 1" {
		t.Errorf("expected sql_text 'SELECT 1', got %q", ev.SQLText)
	}
	if ev.ConnectionID != 42 {
		t.Errorf("expected connection_id 42, got %d", ev.ConnectionID)
	}
	if ev.EventType != "Query" {
		t.Errorf("expected event_type 'Query', got %q", ev.EventType)
	}
}

func TestParsePerconaCSV_QuotedCommas(t *testing.T) {
	input := `"2024-06-15T10:30:00Z","root","localhost","42","1","Query","test","INSERT INTO t1 VALUES(1, ""hello, world"")","0"`

	events, _, _, err := parsePerconaCSV(strings.NewReader(input), auditLogFilter{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	if events[0].SQLText != `INSERT INTO t1 VALUES(1, "hello, world")` {
		t.Errorf("unexpected sql_text: %q", events[0].SQLText)
	}
}

func TestSplitCSVLine(t *testing.T) {
	line := `"hello","world, ok","escaped ""quote""",plain`
	fields := splitCSVLine(line)

	expected := []string{`"hello"`, `"world, ok"`, `"escaped ""quote"""`, `plain`}
	if len(fields) != len(expected) {
		t.Fatalf("expected %d fields, got %d: %v", len(expected), len(fields), fields)
	}
	for i, e := range expected {
		if fields[i] != e {
			t.Errorf("field[%d] = %q, want %q", i, fields[i], e)
		}
	}
}

func TestUnquote(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{`"hello"`, "hello"},
		{`"hello ""world"""`, `hello "world"`},
		{`plain`, "plain"},
		{`""`, ""},
	}
	for _, tt := range tests {
		if got := unquote(tt.in); got != tt.want {
			t.Errorf("unquote(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
