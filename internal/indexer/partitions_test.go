package indexer

import (
	"testing"
	"time"
)

// ─── PartitionDate ──────────────────────────────────────────────────────────────

func TestPartitionDate_valid(t *testing.T) {
	d, ok := PartitionDate("p_2026021900")
	if !ok {
		t.Fatal("expected ok=true for p_2026021900")
	}
	if d.Year() != 2026 || d.Month() != 2 || d.Day() != 19 || d.Hour() != 0 {
		t.Errorf("unexpected time: %v", d)
	}
}

func TestPartitionDate_firstOfMonth(t *testing.T) {
	d, ok := PartitionDate("p_2026020114")
	if !ok {
		t.Fatal("expected ok=true for p_2026020114")
	}
	if d.Year() != 2026 || d.Month() != 2 || d.Day() != 1 || d.Hour() != 14 {
		t.Errorf("unexpected time: %v", d)
	}
}

func TestPartitionDate_invalid(t *testing.T) {
	cases := []string{
		"p_future",      // MAXVALUE catch-all
		"p_",            // too short
		"p_202602",      // incomplete
		"p_20260219",    // missing hour (10 chars, old daily format)
		"p_20260219000", // one digit too many (13 chars)
		"binlog_events",
		"",
	}
	for _, c := range cases {
		if _, ok := PartitionDate(c); ok {
			t.Errorf("expected ok=false for %q", c)
		}
	}
}

// ─── PartitionName ──────────────────────────────────────────────────────────────

func TestPartitionName(t *testing.T) {
	d := time.Date(2026, 2, 19, 0, 0, 0, 0, time.UTC)
	if got := PartitionName(d); got != "p_2026021900" {
		t.Errorf("expected p_2026021900, got %s", got)
	}
}

func TestPartitionName_roundTrip(t *testing.T) {
	// PartitionName and PartitionDate must round-trip correctly.
	original := time.Date(2026, 12, 31, 14, 30, 0, 0, time.UTC)
	name := PartitionName(original)
	got, ok := PartitionDate(name)
	if !ok {
		t.Fatalf("PartitionDate(%q) returned ok=false", name)
	}
	// PartitionDate parses to the top of the hour; year/month/day/hour must match.
	if got.Year() != original.Year() || got.Month() != original.Month() || got.Day() != original.Day() || got.Hour() != original.Hour() {
		t.Errorf("round-trip mismatch: original=%v, got=%v", original, got)
	}
}
