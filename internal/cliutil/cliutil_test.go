package cliutil

import (
	"testing"
	"time"

	"github.com/bintrail/bintrail/internal/parser"
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
