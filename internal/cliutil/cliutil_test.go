package cliutil

import (
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/parser"
)

// ─── ParseEventType ──────────────────────────────────────────────────────────

func TestParseEventType_empty(t *testing.T) {
	got, err := ParseEventType("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for empty string, got %v", *got)
	}
}

func TestParseEventType_insert(t *testing.T) {
	got, err := ParseEventType("INSERT")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil || *got != parser.EventInsert {
		t.Errorf("expected EventInsert, got %v", got)
	}
}

func TestParseEventType_update(t *testing.T) {
	got, err := ParseEventType("UPDATE")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil || *got != parser.EventUpdate {
		t.Errorf("expected EventUpdate, got %v", got)
	}
}

func TestParseEventType_delete(t *testing.T) {
	got, err := ParseEventType("DELETE")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil || *got != parser.EventDelete {
		t.Errorf("expected EventDelete, got %v", got)
	}
}

func TestParseEventType_snapshot(t *testing.T) {
	got, err := ParseEventType("SNAPSHOT")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil || *got != parser.EventSnapshot {
		t.Errorf("expected EventSnapshot, got %v", got)
	}
}

func TestParseEventType_invalidMentionsSnapshot(t *testing.T) {
	_, err := ParseEventType("PICK")
	if err == nil {
		t.Fatal("expected error for unknown event type")
	}
	// The error message lists valid types; SNAPSHOT must appear so future
	// contributors who add a type remember to update the message.
	if !contains(err.Error(), "SNAPSHOT") {
		t.Errorf("error message should list SNAPSHOT as a valid type; got %q", err.Error())
	}
}

// contains is a tiny stdlib-avoiding helper kept local to this test to match
// the existing style in this file (no strings import).
func contains(haystack, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

func TestParseEventType_caseInsensitive(t *testing.T) {
	for _, input := range []string{"insert", "Insert", "iNsErT"} {
		got, err := ParseEventType(input)
		if err != nil {
			t.Fatalf("ParseEventType(%q) unexpected error: %v", input, err)
		}
		if got == nil || *got != parser.EventInsert {
			t.Errorf("ParseEventType(%q) expected EventInsert, got %v", input, got)
		}
	}
}

func TestParseEventType_invalid(t *testing.T) {
	_, err := ParseEventType("UPSERT")
	if err == nil {
		t.Error("expected error for invalid event type, got nil")
	}
}

// ─── ParseTime ───────────────────────────────────────────────────────────────

func TestParseTime_empty(t *testing.T) {
	got, err := ParseTime("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for empty string, got %v", got)
	}
}

func TestParseTime_mysqlDatetime(t *testing.T) {
	got, err := ParseTime("2026-02-19 14:30:00")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil time")
	}
	if got.Year() != 2026 || got.Month() != 2 || got.Day() != 19 {
		t.Errorf("wrong date: %v", got)
	}
	if got.Hour() != 14 || got.Minute() != 30 {
		t.Errorf("wrong time: %v", got)
	}
}

func TestParseTime_rfc3339(t *testing.T) {
	got, err := ParseTime("2026-02-19T14:30:00Z")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil time")
	}
	if got.Year() != 2026 || got.Month() != 2 || got.Day() != 19 {
		t.Errorf("wrong date: %v", got)
	}
	if got.Hour() != 14 || got.Minute() != 30 {
		t.Errorf("wrong time: %v", got)
	}
}

func TestParseTime_dateOnly(t *testing.T) {
	// Date-only input is now accepted — parsed as midnight UTC.
	got, err := ParseTime("2026-02-19")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil time")
	}
	if got.Year() != 2026 || got.Month() != 2 || got.Day() != 19 {
		t.Errorf("wrong date: %v", got)
	}
	if got.Hour() != 0 || got.Minute() != 0 || got.Second() != 0 {
		t.Errorf("expected midnight, got %v", got)
	}
}

func TestParseTime_mysqlDatetime_isUTC(t *testing.T) {
	// MySQL datetime format must be anchored to UTC, not local time,
	// because binlog_events stores timestamps in UTC.
	got, err := ParseTime("2026-02-19 14:30:00")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Location() != time.UTC {
		t.Errorf("expected UTC location, got %v", got.Location())
	}
}

func TestParseTime_dateOnly_isUTC(t *testing.T) {
	got, err := ParseTime("2026-02-19")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Location() != time.UTC {
		t.Errorf("expected UTC location, got %v", got.Location())
	}
}

func TestParseTime_invalidFormat(t *testing.T) {
	_, err := ParseTime("02/19/2026")
	if err == nil {
		t.Error("expected error for bad format, got nil")
	}
}

// ─── IsValidFormat ───────────────────────────────────────────────────────────

func TestIsValidFormat_valid(t *testing.T) {
	for _, f := range []string{"table", "json", "csv", "TABLE", "Json", "CSV"} {
		if !IsValidFormat(f) {
			t.Errorf("IsValidFormat(%q) = false, want true", f)
		}
	}
}

func TestIsValidFormat_invalid(t *testing.T) {
	for _, f := range []string{"xml", "yaml", "", "tsv"} {
		if IsValidFormat(f) {
			t.Errorf("IsValidFormat(%q) = true, want false", f)
		}
	}
}

// ─── IsValidOutputFormat ─────────────────────────────────────────────────────

func TestIsValidOutputFormat_valid(t *testing.T) {
	for _, f := range []string{"text", "json", "TEXT", "Json", "JSON"} {
		if !IsValidOutputFormat(f) {
			t.Errorf("IsValidOutputFormat(%q) = false, want true", f)
		}
	}
}

func TestIsValidOutputFormat_invalid(t *testing.T) {
	for _, f := range []string{"table", "csv", "", "xml", "yaml"} {
		if IsValidOutputFormat(f) {
			t.Errorf("IsValidOutputFormat(%q) = true, want false", f)
		}
	}
}

// ─── ParseSchemaList ──────────────────────────────────────────────────────────

func TestParseSchemaList_empty(t *testing.T) {
	got := ParseSchemaList("")
	if got != nil {
		t.Errorf("expected nil for empty string, got %v", got)
	}
}

func TestParseSchemaList_single(t *testing.T) {
	got := ParseSchemaList("mydb")
	if len(got) != 1 || got[0] != "mydb" {
		t.Errorf("expected [mydb], got %v", got)
	}
}

func TestParseSchemaList_multiple(t *testing.T) {
	got := ParseSchemaList("db1,db2,db3")
	if len(got) != 3 || got[0] != "db1" || got[1] != "db2" || got[2] != "db3" {
		t.Errorf("expected [db1 db2 db3], got %v", got)
	}
}

func TestParseSchemaList_trims(t *testing.T) {
	got := ParseSchemaList(" db1 , db2 , db3 ")
	if len(got) != 3 || got[0] != "db1" || got[1] != "db2" || got[2] != "db3" {
		t.Errorf("expected trimmed [db1 db2 db3], got %v", got)
	}
}

func TestParseSchemaList_dropsEmpty(t *testing.T) {
	got := ParseSchemaList("db1,,db2,")
	if len(got) != 2 || got[0] != "db1" || got[1] != "db2" {
		t.Errorf("expected [db1 db2] with empty entries dropped, got %v", got)
	}
}

func TestParseSchemaList_allEmpty(t *testing.T) {
	got := ParseSchemaList(",,,")
	if len(got) != 0 {
		t.Errorf("expected empty result for all-empty entries, got %v", got)
	}
}

// TestParseSchemaList_whitespaceOnly verifies that a non-empty string containing
// only whitespace returns nil — the early "" guard doesn't fire, the loop runs,
// trims the single part to "", skips it, and returns nil.
func TestParseSchemaList_whitespaceOnly(t *testing.T) {
	if got := ParseSchemaList("   "); got != nil {
		t.Errorf("expected nil for whitespace-only input, got %v", got)
	}
}

// ─── BuildIndexFilters ────────────────────────────────────────────────────────

func TestBuildIndexFilters_empty(t *testing.T) {
	f := BuildIndexFilters("", "")
	if f.Schemas != nil {
		t.Errorf("expected nil Schemas map, got %v", f.Schemas)
	}
	if f.Tables != nil {
		t.Errorf("expected nil Tables map, got %v", f.Tables)
	}
}

func TestBuildIndexFilters_schemasOnly(t *testing.T) {
	f := BuildIndexFilters("mydb,other", "")
	if f.Schemas == nil || !f.Schemas["mydb"] || !f.Schemas["other"] {
		t.Errorf("expected Schemas {mydb:true, other:true}, got %v", f.Schemas)
	}
	if f.Tables != nil {
		t.Errorf("expected nil Tables, got %v", f.Tables)
	}
}

func TestBuildIndexFilters_tablesOnly(t *testing.T) {
	f := BuildIndexFilters("", "mydb.orders,mydb.items")
	if f.Schemas != nil {
		t.Errorf("expected nil Schemas, got %v", f.Schemas)
	}
	if f.Tables == nil || !f.Tables["mydb.orders"] || !f.Tables["mydb.items"] {
		t.Errorf("expected Tables {mydb.orders:true, mydb.items:true}, got %v", f.Tables)
	}
}

func TestBuildIndexFilters_both(t *testing.T) {
	f := BuildIndexFilters("mydb", "mydb.orders")
	if f.Schemas == nil || !f.Schemas["mydb"] {
		t.Error("expected Schemas with mydb")
	}
	if f.Tables == nil || !f.Tables["mydb.orders"] {
		t.Error("expected Tables with mydb.orders")
	}
}

func TestBuildIndexFilters_trimming(t *testing.T) {
	f := BuildIndexFilters(" mydb , other ", " mydb.orders , mydb.items ")
	if !f.Schemas["mydb"] || !f.Schemas["other"] {
		t.Errorf("expected trimmed schemas, got %v", f.Schemas)
	}
	if !f.Tables["mydb.orders"] || !f.Tables["mydb.items"] {
		t.Errorf("expected trimmed tables, got %v", f.Tables)
	}
}
