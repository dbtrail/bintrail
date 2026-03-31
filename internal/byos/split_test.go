package byos

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/parser"
)

func TestPKHash(t *testing.T) {
	// Verify Go SHA-256 matches MySQL SHA2(val, 256) output.
	// MySQL: SELECT SHA2('12345', 256)
	//   → 5994471abb01112afcc18159f6cc74b4f511b99806da59b3caf5a9c173cacfc5
	input := "12345"
	want := sha256Hex(input)
	got := PKHash(input)
	if got != want {
		t.Errorf("PKHash(%q) = %q, want %q", input, got, want)
	}
}

func TestPKHashPipeDelimited(t *testing.T) {
	// Composite PK: pipe-delimited values.
	input := "42|hello"
	want := sha256Hex(input)
	got := PKHash(input)
	if got != want {
		t.Errorf("PKHash(%q) = %q, want %q", input, got, want)
	}
}

func TestSplitEventInsert(t *testing.T) {
	ev := parser.Event{
		BinlogFile: "mysql-bin.000001",
		StartPos:   100,
		EndPos:     200,
		Timestamp:  time.Date(2026, 3, 31, 12, 0, 0, 0, time.UTC),
		GTID:       "aaa-bbb:1",
		Schema:     "mydb",
		Table:      "users",
		EventType:  parser.EventInsert,
		PKValues:   "42",
		RowAfter:   map[string]any{"id": 42, "name": "Alice"},
	}

	meta, payload := SplitEvent(ev, "server-1")

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
	if meta.ChangedColumns != nil {
		t.Errorf("meta.ChangedColumns = %v, want nil for INSERT", meta.ChangedColumns)
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

	meta, payload := SplitEvent(ev, "srv")

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

	meta, payload := SplitEvent(ev, "srv")

	if meta.EventType != "UPDATE" {
		t.Errorf("meta.EventType = %q, want UPDATE", meta.EventType)
	}

	// changed_columns should contain only "email" — "id" and "name" are unchanged.
	if len(meta.ChangedColumns) != 1 || meta.ChangedColumns[0] != "email" {
		t.Errorf("meta.ChangedColumns = %v, want [email]", meta.ChangedColumns)
	}

	// Metadata must NOT contain any values — only column names.
	// (This is enforced by the type: ChangedColumns is []string, not map.)

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

	meta, _ := SplitEvent(ev, "srv")

	// Should have exactly email and updated_at (sorted).
	if len(meta.ChangedColumns) != 2 {
		t.Fatalf("want 2 changed columns, got %d: %v", len(meta.ChangedColumns), meta.ChangedColumns)
	}
	if meta.ChangedColumns[0] != "email" || meta.ChangedColumns[1] != "updated_at" {
		t.Errorf("changed columns = %v, want [email updated_at]", meta.ChangedColumns)
	}
}

func TestEventTypeName(t *testing.T) {
	tests := []struct {
		et   parser.EventType
		want string
	}{
		{parser.EventInsert, "INSERT"},
		{parser.EventUpdate, "UPDATE"},
		{parser.EventDelete, "DELETE"},
		{parser.EventDDL, "UNKNOWN"},
		{parser.EventGTID, "UNKNOWN"},
	}
	for _, tt := range tests {
		if got := eventTypeName(tt.et); got != tt.want {
			t.Errorf("eventTypeName(%d) = %q, want %q", tt.et, got, tt.want)
		}
	}
}

// sha256Hex is a test helper that computes SHA-256 hex directly.
func sha256Hex(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}
