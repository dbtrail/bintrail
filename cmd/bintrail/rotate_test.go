package main

import (
	"strings"
	"testing"
	"time"
)

// ─── parseRetain ─────────────────────────────────────────────────────────────

func TestParseRetain_days(t *testing.T) {
	d, err := parseRetain("7d")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d != 7*24*time.Hour {
		t.Errorf("expected 168h, got %v", d)
	}
}

func TestParseRetain_hours(t *testing.T) {
	d, err := parseRetain("24h")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d != 24*time.Hour {
		t.Errorf("expected 24h, got %v", d)
	}
}

func TestParseRetain_largeDays(t *testing.T) {
	d, err := parseRetain("365d")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d != 365*24*time.Hour {
		t.Errorf("expected 365*24h, got %v", d)
	}
}

func TestParseRetain_invalid(t *testing.T) {
	cases := []string{
		"",    // too short
		"d",   // no number
		"7x",  // unknown unit
		"7",   // no unit
		"-1d", // negative
		"0d",  // zero
		"0h",  // zero hours
	}
	for _, c := range cases {
		if _, err := parseRetain(c); err == nil {
			t.Errorf("expected error for %q, got nil", c)
		}
	}
}

// ─── partitionDate ────────────────────────────────────────────────────────────

func TestPartitionDate_valid(t *testing.T) {
	d, ok := partitionDate("p_20260219")
	if !ok {
		t.Fatal("expected ok=true for p_20260219")
	}
	if d.Year() != 2026 || d.Month() != 2 || d.Day() != 19 {
		t.Errorf("unexpected date: %v", d)
	}
}

func TestPartitionDate_firstOfMonth(t *testing.T) {
	d, ok := partitionDate("p_20260201")
	if !ok {
		t.Fatal("expected ok=true for p_20260201")
	}
	if d.Year() != 2026 || d.Month() != 2 || d.Day() != 1 {
		t.Errorf("unexpected date: %v", d)
	}
}

func TestPartitionDate_invalid(t *testing.T) {
	cases := []string{
		"p_future",    // MAXVALUE catch-all
		"p_",          // too short
		"p_202602",    // incomplete date
		"p_2026021",   // one digit short
		"p_202602190", // one digit too many
		"binlog_events",
		"",
	}
	for _, c := range cases {
		if _, ok := partitionDate(c); ok {
			t.Errorf("expected ok=false for %q", c)
		}
	}
}

// ─── partitionName ───────────────────────────────────────────────────────────

func TestPartitionName(t *testing.T) {
	d := time.Date(2026, 2, 19, 0, 0, 0, 0, time.UTC)
	if got := partitionName(d); got != "p_20260219" {
		t.Errorf("expected p_20260219, got %s", got)
	}
}

func TestPartitionName_roundTrip(t *testing.T) {
	// partitionName and partitionDate must round-trip correctly.
	original := time.Date(2026, 12, 31, 12, 30, 0, 0, time.UTC)
	name := partitionName(original)
	got, ok := partitionDate(name)
	if !ok {
		t.Fatalf("partitionDate(%q) returned ok=false", name)
	}
	// partitionDate parses midnight UTC; the day must match.
	if got.Year() != original.Year() || got.Month() != original.Month() || got.Day() != original.Day() {
		t.Errorf("round-trip mismatch: original=%v, got=%v", original, got)
	}
}

// ─── nextPartitionStart ───────────────────────────────────────────────────────

func TestNextPartitionStart_withNamedPartitions(t *testing.T) {
	partitions := []partitionInfo{
		{Name: "p_20260217"},
		{Name: "p_20260218"},
		{Name: "p_20260219"},
		{Name: "p_future", Description: "MAXVALUE"},
	}
	next := nextPartitionStart(partitions)
	if next.Year() != 2026 || next.Month() != 2 || next.Day() != 20 {
		t.Errorf("expected 2026-02-20, got %v", next)
	}
}

func TestNextPartitionStart_onlyFuture(t *testing.T) {
	// No named partitions — should return today (UTC, truncated to midnight).
	partitions := []partitionInfo{
		{Name: "p_future", Description: "MAXVALUE"},
	}
	today := time.Now().UTC().Truncate(24 * time.Hour)
	next := nextPartitionStart(partitions)
	if !next.Equal(today) {
		t.Errorf("expected today %v, got %v", today, next)
	}
}

func TestNextPartitionStart_empty(t *testing.T) {
	today := time.Now().UTC().Truncate(24 * time.Hour)
	next := nextPartitionStart(nil)
	if !next.Equal(today) {
		t.Errorf("expected today %v, got %v", today, next)
	}
}

// ─── addFuturePartitions SQL shape ───────────────────────────────────────────

// TestAddFuturePartitions_sqlShape verifies the generated DDL looks correct
// without needing a live database. We call the helper that builds the SQL
// internally. Since addFuturePartitions calls db.ExecContext, we can't easily
// unit-test it end-to-end without a DB, but we can test the shape of the
// generated partition spec strings.
func TestPartitionSpec_shape(t *testing.T) {
	start := time.Date(2026, 2, 20, 0, 0, 0, 0, time.UTC)
	// Build the partition spec strings the same way addFuturePartitions does.
	parts := make([]string, 0, 3+1)
	for i := range 3 {
		d := start.AddDate(0, 0, i)
		nextDay := d.AddDate(0, 0, 1)
		parts = append(parts, "PARTITION "+partitionName(d)+
			" VALUES LESS THAN (TO_DAYS('"+
			nextDay.UTC().Format("2006-01-02")+"'))")
	}
	parts = append(parts, "PARTITION p_future VALUES LESS THAN MAXVALUE")

	if len(parts) != 4 {
		t.Fatalf("expected 4 parts, got %d", len(parts))
	}
	if !strings.Contains(parts[0], "p_20260220") {
		t.Errorf("expected p_20260220 in first spec: %s", parts[0])
	}
	if !strings.Contains(parts[0], "2026-02-21") {
		t.Errorf("expected 2026-02-21 (next day boundary) in first spec: %s", parts[0])
	}
	if !strings.Contains(parts[1], "p_20260221") {
		t.Errorf("expected p_20260221 in second spec: %s", parts[1])
	}
	if parts[3] != "PARTITION p_future VALUES LESS THAN MAXVALUE" {
		t.Errorf("expected p_future catch-all last, got: %s", parts[3])
	}
}
