package main

import (
	"strings"
	"testing"

	"github.com/bintrail/bintrail/internal/parser"
)

const defaultLimit = 100

// ─── buildQueryOptions ───────────────────────────────────────────────────────

func TestBuildQueryOptions_empty(t *testing.T) {
	opts, err := buildQueryOptions("", "", "", "", "", "", "", "", 0, defaultLimit)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.Limit != defaultLimit {
		t.Errorf("expected limit %d, got %d", defaultLimit, opts.Limit)
	}
	if opts.EventType != nil {
		t.Errorf("expected nil EventType, got %v", opts.EventType)
	}
	if opts.Since != nil {
		t.Errorf("expected nil Since, got %v", opts.Since)
	}
	if opts.Until != nil {
		t.Errorf("expected nil Until, got %v", opts.Until)
	}
}

func TestBuildQueryOptions_allFields(t *testing.T) {
	opts, err := buildQueryOptions(
		"mydb", "orders", "12345", "INSERT",
		"abc:1", "2026-02-19 14:00:00", "2026-02-19 15:00:00", "status",
		50, defaultLimit,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.Schema != "mydb" {
		t.Errorf("expected schema mydb, got %q", opts.Schema)
	}
	if opts.Table != "orders" {
		t.Errorf("expected table orders, got %q", opts.Table)
	}
	if opts.PKValues != "12345" {
		t.Errorf("expected pk 12345, got %q", opts.PKValues)
	}
	if opts.EventType == nil || *opts.EventType != parser.EventInsert {
		t.Errorf("expected EventInsert, got %v", opts.EventType)
	}
	if opts.GTID != "abc:1" {
		t.Errorf("expected gtid abc:1, got %q", opts.GTID)
	}
	if opts.Since == nil {
		t.Error("expected non-nil Since")
	}
	if opts.Until == nil {
		t.Error("expected non-nil Until")
	}
	if opts.ChangedColumn != "status" {
		t.Errorf("expected changed_column status, got %q", opts.ChangedColumn)
	}
	if opts.Limit != 50 {
		t.Errorf("expected limit 50, got %d", opts.Limit)
	}
}

func TestBuildQueryOptions_pkWithoutSchemaTable(t *testing.T) {
	_, err := buildQueryOptions("", "", "12345", "", "", "", "", "", 0, defaultLimit)
	if err == nil {
		t.Error("expected error when pk is set without schema/table")
	}
	if !strings.Contains(err.Error(), "schema") {
		t.Errorf("expected schema mention in error, got: %v", err)
	}
}

func TestBuildQueryOptions_pkWithSchemaOnly(t *testing.T) {
	_, err := buildQueryOptions("mydb", "", "12345", "", "", "", "", "", 0, defaultLimit)
	if err == nil {
		t.Error("expected error when pk is set with schema but no table")
	}
}

func TestBuildQueryOptions_changedColumnWithoutSchemaTable(t *testing.T) {
	_, err := buildQueryOptions("", "", "", "", "", "", "", "status", 0, defaultLimit)
	if err == nil {
		t.Error("expected error when changed_column is set without schema/table")
	}
	if !strings.Contains(err.Error(), "schema") {
		t.Errorf("expected schema mention in error, got: %v", err)
	}
}

func TestBuildQueryOptions_invalidEventType(t *testing.T) {
	_, err := buildQueryOptions("mydb", "orders", "", "UPSERT", "", "", "", "", 0, defaultLimit)
	if err == nil {
		t.Error("expected error for invalid event_type")
	}
}

func TestBuildQueryOptions_invalidSince(t *testing.T) {
	_, err := buildQueryOptions("", "", "", "", "", "not-a-date", "", "", 0, defaultLimit)
	if err == nil {
		t.Error("expected error for invalid since")
	}
	if !strings.Contains(err.Error(), "since") {
		t.Errorf("expected 'since' in error, got: %v", err)
	}
}

func TestBuildQueryOptions_invalidUntil(t *testing.T) {
	_, err := buildQueryOptions("", "", "", "", "", "", "not-a-date", "", 0, defaultLimit)
	if err == nil {
		t.Error("expected error for invalid until")
	}
	if !strings.Contains(err.Error(), "until") {
		t.Errorf("expected 'until' in error, got: %v", err)
	}
}

func TestBuildQueryOptions_limitZeroUsesDefault(t *testing.T) {
	opts, err := buildQueryOptions("", "", "", "", "", "", "", "", 0, defaultLimit)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.Limit != defaultLimit {
		t.Errorf("expected default limit %d, got %d", defaultLimit, opts.Limit)
	}
}

func TestBuildQueryOptions_negativeLimitUsesDefault(t *testing.T) {
	opts, err := buildQueryOptions("", "", "", "", "", "", "", "", -5, defaultLimit)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.Limit != defaultLimit {
		t.Errorf("expected default limit %d, got %d", defaultLimit, opts.Limit)
	}
}
