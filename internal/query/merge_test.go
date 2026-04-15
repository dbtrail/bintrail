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

// TestMergeAndTrim_perPKBeforeGlobal pins the load-bearing ordering invariant
// between LimitPerPK and Limit. With three PKs each having three ASC-sorted
// events, a naive "MergeResults(..., Limit) then LimitPerPK" ordering would
// truncate the union to Limit=6 rows (rows for "c" lost entirely) and then
// trim per-PK to 1 each — final: only 2 rows for "a" and "b", 0 for "c".
// The correct ordering (per-PK first, global last) keeps 1 row per PK for all
// three PKs — final: 3 rows total. If someone swaps the sequence in a future
// refactor this test fails immediately.
func TestMergeAndTrim_perPKBeforeGlobal(t *testing.T) {
	ts := func(m int) time.Time {
		return time.Date(2026, 3, 1, 12, m, 0, 0, time.UTC)
	}
	// Events ordered so that all of "a"'s events come before all of "b"'s,
	// and all of "b"'s before all of "c"'s — a worst-case for the ordering bug.
	rows := []ResultRow{
		{EventID: 1, EventTimestamp: ts(1), PKValues: "a"},
		{EventID: 2, EventTimestamp: ts(2), PKValues: "a"},
		{EventID: 3, EventTimestamp: ts(3), PKValues: "a"},
		{EventID: 4, EventTimestamp: ts(4), PKValues: "b"},
		{EventID: 5, EventTimestamp: ts(5), PKValues: "b"},
		{EventID: 6, EventTimestamp: ts(6), PKValues: "b"},
		{EventID: 7, EventTimestamp: ts(7), PKValues: "c"},
		{EventID: 8, EventTimestamp: ts(8), PKValues: "c"},
		{EventID: 9, EventTimestamp: ts(9), PKValues: "c"},
	}
	got := MergeAndTrim(rows, 6, 1)
	if len(got) != 3 {
		t.Fatalf("expected 3 rows (1 per PK) after correct trim ordering, got %d: %+v", len(got), got)
	}
	seen := map[string]bool{}
	for _, r := range got {
		if seen[r.PKValues] {
			t.Errorf("duplicate PK %q in output — LimitPerPK not enforced", r.PKValues)
		}
		seen[r.PKValues] = true
	}
	if !seen["a"] || !seen["b"] || !seen["c"] {
		t.Errorf("expected all three PKs represented, got %v", seen)
	}
}

func TestMergeAndTrim_crossSourceDedup(t *testing.T) {
	ts := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	// Same PK appears in both sources with the same event_id (MySQL + archive
	// overlap). After MergeAndTrim with LimitPerPK=2 we expect the duplicate
	// to be collapsed so the PK has at most 2 events, not 4.
	rows := []ResultRow{
		{EventID: 1, EventTimestamp: ts, PKValues: "a"},             // MySQL
		{EventID: 2, EventTimestamp: ts.Add(time.Second), PKValues: "a"}, // MySQL
		{EventID: 1, EventTimestamp: ts, PKValues: "a"},             // archive dup of 1
		{EventID: 2, EventTimestamp: ts.Add(time.Second), PKValues: "a"}, // archive dup of 2
	}
	got := MergeAndTrim(rows, 0, 2)
	if len(got) != 2 {
		t.Fatalf("expected 2 rows after dedup + per-PK trim, got %d", len(got))
	}
}
