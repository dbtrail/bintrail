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

// TestMergeAndTrim_crossSourceDedup exercises the two-step contract: dedup
// collapses cross-source duplicates, and LimitPerPK then trims the deduped
// set to the cap. The scenario is specifically constructed so that dedup
// alone is NOT sufficient — PK "a" has 3 distinct events after dedup
// (event IDs 1, 2, 3) plus duplicates of each from the archive source, and
// LimitPerPK=2 must still fire to trim the deduped result down to 2. A
// regression that drops LimitPerPK would leave len(got)==3 and fail this
// test, whereas a regression that keeps the duplicates without running
// LimitPerPK would leave len(got)==6.
func TestMergeAndTrim_crossSourceDedup(t *testing.T) {
	ts := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	rows := []ResultRow{
		// MySQL side — three distinct events for PK "a".
		{EventID: 1, EventTimestamp: ts, PKValues: "a"},
		{EventID: 2, EventTimestamp: ts.Add(time.Second), PKValues: "a"},
		{EventID: 3, EventTimestamp: ts.Add(2 * time.Second), PKValues: "a"},
		// Archive side — duplicates of the same three events (overlap window).
		{EventID: 1, EventTimestamp: ts, PKValues: "a"},
		{EventID: 2, EventTimestamp: ts.Add(time.Second), PKValues: "a"},
		{EventID: 3, EventTimestamp: ts.Add(2 * time.Second), PKValues: "a"},
	}
	got := MergeAndTrim(rows, 0, 2)
	if len(got) != 2 {
		t.Fatalf("expected 2 rows after dedup (3 unique) + LimitPerPK=2 trim, got %d", len(got))
	}
	// Latest two events kept: IDs 2 and 3 in ASC order.
	if got[0].EventID != 2 || got[1].EventID != 3 {
		t.Errorf("expected events [2,3] (latest 2 in ASC order), got %v", []uint64{got[0].EventID, got[1].EventID})
	}
}
