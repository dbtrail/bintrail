package forensics

import (
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// JSON parser (MySQL Enterprise + Percona)
// ---------------------------------------------------------------------------

func TestParseAuditJSON_MySQLEnterprise(t *testing.T) {
	input := `[
{"timestamp":"2024-06-15T10:30:00Z","class":"general","event":"status","login":{"user":"root","host":"localhost"},"general_data":{"query":"SELECT 1"},"connection_id":42,"status":0},
{"timestamp":"2024-06-15T10:31:00Z","class":"connection","event":"connect","login":{"user":"app","host":"10.0.0.5"},"connection_id":43,"status":0}
]`
	events, _, _, err := parseAuditJSON(strings.NewReader(input), auditLogFilter{})
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
	if ev.EventType != "general/status" {
		t.Errorf("expected event_type 'general/status', got %q", ev.EventType)
	}
	if ev.ConnectionID != 42 {
		t.Errorf("expected connection_id 42, got %d", ev.ConnectionID)
	}
}

func TestParseAuditJSON_PerconaFlat(t *testing.T) {
	input := `{"timestamp":"2024-06-15T10:30:00Z","name":"Query","user":"app_user","host":"10.0.0.1","sqltext":"INSERT INTO t1 VALUES(1)","db":"mydb","connection_id":99,"status":0}
{"timestamp":"2024-06-15T10:31:00Z","name":"Connect","user":"root","host":"localhost","connection_id":100,"status":0}`

	events, _, _, err := parseAuditJSON(strings.NewReader(input), auditLogFilter{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}

	ev := events[0]
	if ev.User != "app_user" {
		t.Errorf("expected user 'app_user', got %q", ev.User)
	}
	if ev.SQLText != "INSERT INTO t1 VALUES(1)" {
		t.Errorf("expected sql_text, got %q", ev.SQLText)
	}
	if ev.DB != "mydb" {
		t.Errorf("expected db 'mydb', got %q", ev.DB)
	}
	if ev.EventType != "Query" {
		t.Errorf("expected event_type 'Query', got %q", ev.EventType)
	}
}

func TestParseAuditJSON_SkipsMalformedLines(t *testing.T) {
	input := `{"timestamp":"2024-06-15T10:30:00Z","name":"Query","user":"root"}
not valid json
{"timestamp":"2024-06-15T10:31:00Z","name":"Connect","user":"app"}`

	events, _, _, err := parseAuditJSON(strings.NewReader(input), auditLogFilter{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 2 {
		t.Errorf("expected 2 events (skipping malformed), got %d", len(events))
	}
}

func TestParseAuditJSON_ReportsSkippedLines(t *testing.T) {
	input := `{"timestamp":"2024-06-15T10:30:00Z","name":"Query","user":"root"}
not valid json
also not json
{"timestamp":"2024-06-15T10:31:00Z","name":"Connect","user":"app"}`

	events, _, skipped, err := parseAuditJSON(strings.NewReader(input), auditLogFilter{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 2 {
		t.Errorf("expected 2 events, got %d", len(events))
	}
	if skipped != 2 {
		t.Errorf("expected 2 skipped lines, got %d", skipped)
	}
}

// ---------------------------------------------------------------------------
// XML parser (MySQL Enterprise legacy)
// ---------------------------------------------------------------------------

func TestParseAuditXML(t *testing.T) {
	input := `<?xml version="1.0" encoding="UTF-8"?>
<AUDIT>
  <AUDIT_RECORD>
    <TIMESTAMP>2024-06-15T10:30:00Z</TIMESTAMP>
    <NAME>Query</NAME>
    <USER>root</USER>
    <HOST>localhost</HOST>
    <CONNECTION_ID>42</CONNECTION_ID>
    <DB>test</DB>
    <SQLTEXT>SELECT * FROM users</SQLTEXT>
    <STATUS>0</STATUS>
  </AUDIT_RECORD>
  <AUDIT_RECORD>
    <TIMESTAMP>2024-06-15T10:31:00Z</TIMESTAMP>
    <NAME>Connect</NAME>
    <USER>app</USER>
    <HOST>10.0.0.5</HOST>
    <CONNECTION_ID>43</CONNECTION_ID>
    <STATUS>0</STATUS>
  </AUDIT_RECORD>
</AUDIT>`

	events, _, _, err := parseAuditXML(strings.NewReader(input), auditLogFilter{})
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
	if ev.SQLText != "SELECT * FROM users" {
		t.Errorf("expected sql_text, got %q", ev.SQLText)
	}
	if ev.ConnectionID != 42 {
		t.Errorf("expected connection_id 42, got %d", ev.ConnectionID)
	}
	if ev.EventType != "Query" {
		t.Errorf("expected event_type 'Query', got %q", ev.EventType)
	}
	if ev.DB != "test" {
		t.Errorf("expected db 'test', got %q", ev.DB)
	}
}

// TestParseAuditXML_TruncatedFileReturnsPartial: an abruptly-ended XML file
// (rotation mid-write) must return the records parsed so far with the error,
// which the orchestrator downgrades to a warning.
func TestParseAuditXML_TruncatedFileReturnsPartial(t *testing.T) {
	input := `<AUDIT>
  <AUDIT_RECORD>
    <TIMESTAMP>2024-06-15T10:30:00Z</TIMESTAMP>
    <NAME>Query</NAME>
    <USER>root</USER>
  </AUDIT_RECORD>
  <AUDIT_RECORD>
    <TIMESTAMP>2024-06-15T10:31:00`

	events, _, _, err := parseAuditXML(strings.NewReader(input), auditLogFilter{})
	if err == nil {
		t.Fatal("expected an error for truncated XML")
	}
	if len(events) != 1 {
		t.Errorf("expected 1 partial event, got %d", len(events))
	}
}
