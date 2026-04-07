package byos

import (
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/parser"
)

func TestPKHash(t *testing.T) {
	// Verify Go SHA-256 matches MySQL SHA2(val, 256) output.
	// MySQL: SELECT SHA2('12345', 256)
	//   → 5994471abb01112afcc18159f6cc74b4f511b99806da59b3caf5a9c173cacfc5
	input := "12345"
	want := "5994471abb01112afcc18159f6cc74b4f511b99806da59b3caf5a9c173cacfc5"
	got := PKHash(input)
	if got != want {
		t.Errorf("PKHash(%q) = %q, want %q", input, got, want)
	}
}

func TestPKHashPipeDelimited(t *testing.T) {
	// Composite PK: pipe-delimited values. Verify it produces a
	// 64-char lowercase hex string (valid SHA-256 digest).
	got := PKHash("42|hello")
	if len(got) != 64 {
		t.Errorf("PKHash length = %d, want 64", len(got))
	}
	for _, c := range got {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Errorf("PKHash contains non-hex char %q", c)
			break
		}
	}
}

func TestSplitEventInsert(t *testing.T) {
	ev := parser.Event{
		BinlogFile:    "mysql-bin.000001",
		StartPos:      100,
		EndPos:        200,
		Timestamp:     time.Date(2026, 3, 31, 12, 0, 0, 0, time.UTC),
		GTID:          "aaa-bbb:1",
		ConnectionID:  7890,
		Schema:        "mydb",
		Table:         "users",
		EventType:     parser.EventInsert,
		PKValues:      "42",
		RowAfter:      map[string]any{"id": 42, "name": "Alice"},
		SchemaVersion: 5,
	}

	ident := SourceIdentity{ServerUUID: "uuid-1", Host: "10.0.0.5", Port: 3306, User: "repl"}
	meta, payload, err := SplitEvent(ev, "server-1", ident)
	if err != nil {
		t.Fatalf("SplitEvent: %v", err)
	}

	// Metadata checks.
	wantHash := PKHash("42")
	if meta.PKHash != wantHash {
		t.Errorf("meta.PKHash = %q, want %q", meta.PKHash, wantHash)
	}
	if meta.SchemaName != "mydb" {
		t.Errorf("meta.SchemaName = %q, want %q", meta.SchemaName, "mydb")
	}
	if meta.TableName != "users" {
		t.Errorf("meta.TableName = %q, want %q", meta.TableName, "users")
	}
	if meta.EventType != "INSERT" {
		t.Errorf("meta.EventType = %q, want %q", meta.EventType, "INSERT")
	}
	if meta.ServerID != "server-1" {
		t.Errorf("meta.ServerID = %q, want %q", meta.ServerID, "server-1")
	}
	if meta.GTID != "aaa-bbb:1" {
		t.Errorf("meta.GTID = %q, want %q", meta.GTID, "aaa-bbb:1")
	}
	if meta.RowCount != 1 {
		t.Errorf("meta.RowCount = %d, want 1", meta.RowCount)
	}
	if meta.ConnectionID != 7890 {
		t.Errorf("meta.ConnectionID = %d, want 7890", meta.ConnectionID)
	}
	if meta.ChangedColumns != nil {
		t.Errorf("meta.ChangedColumns = %v, want nil for INSERT", meta.ChangedColumns)
	}

	// Source identity propagation (architecture §22.11).
	if meta.ServerUUID != "uuid-1" {
		t.Errorf("meta.ServerUUID = %q, want %q", meta.ServerUUID, "uuid-1")
	}
	if meta.SourceHost != "10.0.0.5" {
		t.Errorf("meta.SourceHost = %q, want %q", meta.SourceHost, "10.0.0.5")
	}
	if meta.SourcePort != 3306 {
		t.Errorf("meta.SourcePort = %d, want 3306", meta.SourcePort)
	}
	if meta.SourceUser != "repl" {
		t.Errorf("meta.SourceUser = %q, want %q", meta.SourceUser, "repl")
	}

	// Payload checks — must contain row data.
	if payload.PKHash != wantHash {
		t.Errorf("payload.PKHash = %q, want %q", payload.PKHash, wantHash)
	}
	if payload.PKValues != "42" {
		t.Errorf("payload.PKValues = %q, want %q", payload.PKValues, "42")
	}
	if payload.RowBefore != nil {
		t.Error("payload.RowBefore should be nil for INSERT")
	}
	if payload.RowAfter == nil {
		t.Fatal("payload.RowAfter should not be nil for INSERT")
	}
	if payload.RowAfter["name"] != "Alice" {
		t.Errorf("payload.RowAfter[name] = %v, want Alice", payload.RowAfter["name"])
	}
	if payload.SchemaVersion != 5 {
		t.Errorf("payload.SchemaVersion = %d, want 5", payload.SchemaVersion)
	}
}

func TestSplitEventDelete(t *testing.T) {
	ev := parser.Event{
		Timestamp: time.Date(2026, 3, 31, 12, 0, 0, 0, time.UTC),
		Schema:    "mydb",
		Table:     "users",
		EventType: parser.EventDelete,
		PKValues:  "42",
		RowBefore: map[string]any{"id": 42, "name": "Alice"},
	}

	meta, payload, err := SplitEvent(ev, "srv", SourceIdentity{})
	if err != nil {
		t.Fatalf("SplitEvent: %v", err)
	}

	if meta.EventType != "DELETE" {
		t.Errorf("meta.EventType = %q, want DELETE", meta.EventType)
	}
	if meta.ChangedColumns != nil {
		t.Errorf("meta.ChangedColumns = %v, want nil for DELETE", meta.ChangedColumns)
	}
	if payload.RowAfter != nil {
		t.Error("payload.RowAfter should be nil for DELETE")
	}
	if payload.RowBefore == nil {
		t.Fatal("payload.RowBefore should not be nil for DELETE")
	}
}

func TestSplitEventUpdate(t *testing.T) {
	ev := parser.Event{
		Timestamp: time.Date(2026, 3, 31, 12, 0, 0, 0, time.UTC),
		Schema:    "mydb",
		Table:     "users",
		EventType: parser.EventUpdate,
		PKValues:  "42",
		RowBefore: map[string]any{"id": 42, "name": "Alice", "email": "a@old.com"},
		RowAfter:  map[string]any{"id": 42, "name": "Alice", "email": "a@new.com"},
	}

	meta, payload, err := SplitEvent(ev, "srv", SourceIdentity{})
	if err != nil {
		t.Fatalf("SplitEvent: %v", err)
	}

	if meta.EventType != "UPDATE" {
		t.Errorf("meta.EventType = %q, want UPDATE", meta.EventType)
	}

	// changed_columns should contain only "email" — "id" and "name" are unchanged.
	if len(meta.ChangedColumns) != 1 || meta.ChangedColumns[0] != "email" {
		t.Errorf("meta.ChangedColumns = %v, want [email]", meta.ChangedColumns)
	}

	// Payload should have both before and after images.
	if payload.RowBefore == nil || payload.RowAfter == nil {
		t.Fatal("payload must have both RowBefore and RowAfter for UPDATE")
	}
	if len(payload.ChangedColumns) != 1 || payload.ChangedColumns[0] != "email" {
		t.Errorf("payload.ChangedColumns = %v, want [email]", payload.ChangedColumns)
	}
}

func TestSplitEventUpdateMultipleChanges(t *testing.T) {
	ev := parser.Event{
		Timestamp: time.Date(2026, 3, 31, 12, 0, 0, 0, time.UTC),
		Schema:    "mydb",
		Table:     "users",
		EventType: parser.EventUpdate,
		PKValues:  "42",
		RowBefore: map[string]any{"id": 42, "email": "a@old.com", "updated_at": "2026-01-01"},
		RowAfter:  map[string]any{"id": 42, "email": "a@new.com", "updated_at": "2026-03-31"},
	}

	meta, _, err := SplitEvent(ev, "srv", SourceIdentity{})
	if err != nil {
		t.Fatalf("SplitEvent: %v", err)
	}

	// Should have exactly email and updated_at (sorted).
	if len(meta.ChangedColumns) != 2 {
		t.Fatalf("want 2 changed columns, got %d: %v", len(meta.ChangedColumns), meta.ChangedColumns)
	}
	if meta.ChangedColumns[0] != "email" || meta.ChangedColumns[1] != "updated_at" {
		t.Errorf("changed columns = %v, want [email updated_at]", meta.ChangedColumns)
	}
}

func TestSplitEventUnsupportedType(t *testing.T) {
	ev := parser.Event{
		EventType: parser.EventDDL,
	}
	_, _, err := SplitEvent(ev, "srv", SourceIdentity{})
	if err == nil {
		t.Fatal("expected error for DDL event type")
	}
}

func TestSplitEventMapIsolation(t *testing.T) {
	// Verify that mutating the source event's maps after SplitEvent
	// does not affect the payload record (maps are cloned).
	after := map[string]any{"id": 1, "name": "Alice"}
	ev := parser.Event{
		Timestamp: time.Date(2026, 3, 31, 12, 0, 0, 0, time.UTC),
		Schema:    "mydb",
		Table:     "users",
		EventType: parser.EventInsert,
		PKValues:  "1",
		RowAfter:  after,
	}

	_, payload, err := SplitEvent(ev, "srv", SourceIdentity{})
	if err != nil {
		t.Fatalf("SplitEvent: %v", err)
	}

	// Mutate the source map after splitting.
	after["name"] = "MUTATED"

	// Payload should still have the original value.
	if payload.RowAfter["name"] != "Alice" {
		t.Errorf("payload.RowAfter[name] = %v, want Alice (map was not cloned)", payload.RowAfter["name"])
	}
}

func TestEventTypeName(t *testing.T) {
	tests := []struct {
		et      parser.EventType
		want    string
		wantErr bool
	}{
		{parser.EventInsert, "INSERT", false},
		{parser.EventUpdate, "UPDATE", false},
		{parser.EventDelete, "DELETE", false},
		{parser.EventDDL, "", true},
		{parser.EventGTID, "", true},
	}
	for _, tt := range tests {
		got, err := eventTypeName(tt.et)
		if tt.wantErr {
			if err == nil {
				t.Errorf("eventTypeName(%d) = %q, want error", tt.et, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("eventTypeName(%d) unexpected error: %v", tt.et, err)
			continue
		}
		if got != tt.want {
			t.Errorf("eventTypeName(%d) = %q, want %q", tt.et, got, tt.want)
		}
	}
}
