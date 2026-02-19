package main

import (
	"testing"

	"github.com/bintrail/bintrail/internal/parser"
)

// ─── parseEventType ──────────────────────────────────────────────────────────

func TestParseEventType_empty(t *testing.T) {
	got, err := parseEventType("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for empty string, got %v", *got)
	}
}

func TestParseEventType_insert(t *testing.T) {
	got, err := parseEventType("INSERT")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil || *got != parser.EventInsert {
		t.Errorf("expected EventInsert, got %v", got)
	}
}

func TestParseEventType_update(t *testing.T) {
	got, err := parseEventType("UPDATE")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil || *got != parser.EventUpdate {
		t.Errorf("expected EventUpdate, got %v", got)
	}
}

func TestParseEventType_delete(t *testing.T) {
	got, err := parseEventType("DELETE")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil || *got != parser.EventDelete {
		t.Errorf("expected EventDelete, got %v", got)
	}
}

func TestParseEventType_caseInsensitive(t *testing.T) {
	for _, input := range []string{"insert", "Insert", "iNsErT"} {
		got, err := parseEventType(input)
		if err != nil {
			t.Fatalf("parseEventType(%q) unexpected error: %v", input, err)
		}
		if got == nil || *got != parser.EventInsert {
			t.Errorf("parseEventType(%q) expected EventInsert, got %v", input, got)
		}
	}
}

func TestParseEventType_invalid(t *testing.T) {
	_, err := parseEventType("UPSERT")
	if err == nil {
		t.Error("expected error for invalid event type, got nil")
	}
}

// ─── parseQueryTime ──────────────────────────────────────────────────────────

func TestParseQueryTime_empty(t *testing.T) {
	got, err := parseQueryTime("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for empty string, got %v", got)
	}
}

func TestParseQueryTime_valid(t *testing.T) {
	got, err := parseQueryTime("2026-02-19 14:30:00")
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

func TestParseQueryTime_invalidFormat(t *testing.T) {
	_, err := parseQueryTime("02/19/2026")
	if err == nil {
		t.Error("expected error for bad format, got nil")
	}
}

func TestParseQueryTime_partialDate(t *testing.T) {
	_, err := parseQueryTime("2026-02-19")
	if err == nil {
		t.Error("expected error for date-only format (missing time), got nil")
	}
}

// ─── isValidFormat ───────────────────────────────────────────────────────────

func TestIsValidFormat_valid(t *testing.T) {
	for _, f := range []string{"table", "json", "csv", "TABLE", "Json", "CSV"} {
		if !isValidFormat(f) {
			t.Errorf("isValidFormat(%q) = false, want true", f)
		}
	}
}

func TestIsValidFormat_invalid(t *testing.T) {
	for _, f := range []string{"xml", "yaml", "", "tsv"} {
		if isValidFormat(f) {
			t.Errorf("isValidFormat(%q) = true, want false", f)
		}
	}
}
