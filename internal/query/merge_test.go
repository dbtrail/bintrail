package query

import (
	"testing"
	"time"
)

func TestMergeResults_dedup(t *testing.T) {
	ts := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	rows := []ResultRow{
		{EventID: 1, EventTimestamp: ts, SchemaName: "a"},
		{EventID: 1, EventTimestamp: ts, SchemaName: "b"}, // duplicate
		{EventID: 2, EventTimestamp: ts.Add(time.Second)},
	}
	got := MergeResults(rows, 0)
	if len(got) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(got))
	}
	// First occurrence wins (MySQL row kept over archive).
	if got[0].SchemaName != "a" {
		t.Errorf("expected first occurrence kept, got schema=%q", got[0].SchemaName)
	}
}

func TestMergeResults_sort(t *testing.T) {
	ts1 := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	ts2 := time.Date(2026, 3, 1, 11, 0, 0, 0, time.UTC)
	rows := []ResultRow{
		{EventID: 2, EventTimestamp: ts1},
		{EventID: 1, EventTimestamp: ts2},
	}
	got := MergeResults(rows, 0)
	if got[0].EventID != 1 {
		t.Errorf("expected event 1 first (earlier timestamp), got %d", got[0].EventID)
	}
}

func TestMergeResults_sortByEventID(t *testing.T) {
	ts := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	rows := []ResultRow{
		{EventID: 5, EventTimestamp: ts},
		{EventID: 3, EventTimestamp: ts},
	}
	got := MergeResults(rows, 0)
	if got[0].EventID != 3 {
		t.Errorf("expected event 3 first (same timestamp, lower ID), got %d", got[0].EventID)
	}
}

func TestMergeResults_limit(t *testing.T) {
	ts := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	rows := []ResultRow{
		{EventID: 1, EventTimestamp: ts},
		{EventID: 2, EventTimestamp: ts.Add(time.Second)},
		{EventID: 3, EventTimestamp: ts.Add(2 * time.Second)},
	}
	got := MergeResults(rows, 2)
	if len(got) != 2 {
		t.Fatalf("expected 2 rows after limit, got %d", len(got))
	}
}

func TestMergeResults_zeroLimit(t *testing.T) {
	ts := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	rows := []ResultRow{
		{EventID: 1, EventTimestamp: ts},
		{EventID: 2, EventTimestamp: ts.Add(time.Second)},
	}
	got := MergeResults(rows, 0)
	if len(got) != 2 {
		t.Fatalf("expected all rows with limit=0, got %d", len(got))
	}
}

func TestMergeResults_empty(t *testing.T) {
	got := MergeResults(nil, 10)
	if len(got) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(got))
	}
}
