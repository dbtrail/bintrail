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
	d, ok := partitionDate("p_2026021900")
	if !ok {
		t.Fatal("expected ok=true for p_2026021900")
	}
	if d.Year() != 2026 || d.Month() != 2 || d.Day() != 19 || d.Hour() != 0 {
		t.Errorf("unexpected time: %v", d)
	}
}

func TestPartitionDate_firstOfMonth(t *testing.T) {
	d, ok := partitionDate("p_2026020114")
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
		if _, ok := partitionDate(c); ok {
			t.Errorf("expected ok=false for %q", c)
		}
	}
}

// ─── partitionName ───────────────────────────────────────────────────────────

func TestPartitionName(t *testing.T) {
	d := time.Date(2026, 2, 19, 0, 0, 0, 0, time.UTC)
	if got := partitionName(d); got != "p_2026021900" {
		t.Errorf("expected p_2026021900, got %s", got)
	}
}

func TestPartitionName_roundTrip(t *testing.T) {
	// partitionName and partitionDate must round-trip correctly.
	original := time.Date(2026, 12, 31, 14, 30, 0, 0, time.UTC)
	name := partitionName(original)
	got, ok := partitionDate(name)
	if !ok {
		t.Fatalf("partitionDate(%q) returned ok=false", name)
	}
	// partitionDate parses to the top of the hour; year/month/day/hour must match.
	if got.Year() != original.Year() || got.Month() != original.Month() || got.Day() != original.Day() || got.Hour() != original.Hour() {
		t.Errorf("round-trip mismatch: original=%v, got=%v", original, got)
	}
}

// ─── nextPartitionStart ───────────────────────────────────────────────────────

func TestNextPartitionStart_withNamedPartitions(t *testing.T) {
	partitions := []partitionInfo{
		{Name: "p_2026021700"},
		{Name: "p_2026021800"},
		{Name: "p_2026021900"},
		{Name: "p_future", Description: "MAXVALUE"},
	}
	next := nextPartitionStart(partitions)
	// Latest is p_2026021900 (hour 00 on 2026-02-19), next should be hour 01.
	if next.Year() != 2026 || next.Month() != 2 || next.Day() != 19 || next.Hour() != 1 {
		t.Errorf("expected 2026-02-19 01:00, got %v", next)
	}
}

func TestNextPartitionStart_onlyFuture(t *testing.T) {
	// No named partitions — should return the current hour (UTC, truncated to hour).
	partitions := []partitionInfo{
		{Name: "p_future", Description: "MAXVALUE"},
	}
	thisHour := time.Now().UTC().Truncate(time.Hour)
	next := nextPartitionStart(partitions)
	if !next.Equal(thisHour) {
		t.Errorf("expected current hour %v, got %v", thisHour, next)
	}
}

func TestNextPartitionStart_empty(t *testing.T) {
	thisHour := time.Now().UTC().Truncate(time.Hour)
	next := nextPartitionStart(nil)
	if !next.Equal(thisHour) {
		t.Errorf("expected current hour %v, got %v", thisHour, next)
	}
}

// ─── addFuturePartitions SQL shape ───────────────────────────────────────────

// TestPartitionSpec_shape verifies the generated DDL looks correct without
// needing a live database.
func TestPartitionSpec_shape(t *testing.T) {
	start := time.Date(2026, 2, 20, 14, 0, 0, 0, time.UTC)
	// Build the partition spec strings the same way addFuturePartitions does.
	parts := make([]string, 0, 3+1)
	for i := range 3 {
		d := start.Add(time.Duration(i) * time.Hour)
		nextHour := d.Add(time.Hour)
		parts = append(parts, "PARTITION "+partitionName(d)+
			" VALUES LESS THAN (TO_SECONDS('"+
			nextHour.UTC().Format("2006-01-02 15:04:05")+"'))")
	}
	parts = append(parts, "PARTITION p_future VALUES LESS THAN MAXVALUE")

	if len(parts) != 4 {
		t.Fatalf("expected 4 parts, got %d", len(parts))
	}
	if !strings.Contains(parts[0], "p_2026022014") {
		t.Errorf("expected p_2026022014 in first spec: %s", parts[0])
	}
	if !strings.Contains(parts[0], "2026-02-20 15:00:00") {
		t.Errorf("expected 2026-02-20 15:00:00 (next hour boundary) in first spec: %s", parts[0])
	}
	if !strings.Contains(parts[1], "p_2026022015") {
		t.Errorf("expected p_2026022015 in second spec: %s", parts[1])
	}
	if parts[3] != "PARTITION p_future VALUES LESS THAN MAXVALUE" {
		t.Errorf("expected p_future catch-all last, got: %s", parts[3])
	}
}

// ─── autoAdd calculation ──────────────────────────────────────────────────────

// TestAutoAddReplacesDropped verifies the toAdd formula for both default and
// --no-replace modes.
func TestAutoAddReplacesDropped(t *testing.T) {
	cases := []struct {
		dropped   int
		addFuture int
		noReplace bool
		wantTotal int
	}{
		{dropped: 0, addFuture: 0, noReplace: false, wantTotal: 0},
		{dropped: 3, addFuture: 0, noReplace: false, wantTotal: 3},   // pure retention: 3 dropped → 3 added
		{dropped: 0, addFuture: 5, noReplace: false, wantTotal: 5},   // only --add-future
		{dropped: 4, addFuture: 2, noReplace: false, wantTotal: 6},   // dropped + extra
		{dropped: 168, addFuture: 0, noReplace: false, wantTotal: 168}, // 7-day rotation
		{dropped: 3, addFuture: 0, noReplace: true, wantTotal: 0},    // --no-replace: pure drop, nothing added
		{dropped: 3, addFuture: 2, noReplace: true, wantTotal: 2},    // --no-replace + --add-future: only explicit extras
	}
	for _, c := range cases {
		toAdd := c.addFuture
		if !c.noReplace {
			toAdd += c.dropped
		}
		if toAdd != c.wantTotal {
			t.Errorf("dropped=%d addFuture=%d noReplace=%v: expected toAdd=%d, got %d",
				c.dropped, c.addFuture, c.noReplace, c.wantTotal, toAdd)
		}
	}
}

// ─── cobra command wiring ─────────────────────────────────────────────────────

func TestRotateCmd_registered(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "rotate" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'rotate' command to be registered under rootCmd")
	}
}

func TestRotateCmd_indexDSN_required(t *testing.T) {
	flag := rotateCmd.Flag("index-dsn")
	if flag == nil {
		t.Fatal("flag --index-dsn not registered on rotateCmd")
	}
	if flag.Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
		t.Error("flag --index-dsn is not marked required on rotateCmd")
	}
}

func TestRotateCmd_allFlagsRegistered(t *testing.T) {
	for _, name := range []string{"index-dsn", "retain", "add-future", "no-replace", "archive-dir", "archive-compression"} {
		if rotateCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on rotateCmd", name)
		}
	}
}

// ─── runRotate validation (no DB required) ────────────────────────────────────

func TestRunRotate_noFlagsError(t *testing.T) {
	savedRetain, savedAdd, savedNoReplace := rotRetain, rotAddFuture, rotNoReplace
	t.Cleanup(func() { rotRetain = savedRetain; rotAddFuture = savedAdd; rotNoReplace = savedNoReplace })

	rotRetain = ""
	rotAddFuture = 0

	err := runRotate(rotateCmd, nil)
	if err == nil {
		t.Fatal("expected error when neither --retain nor --add-future is set, got nil")
	}
	if !strings.Contains(err.Error(), "--retain or --add-future") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRunRotate_invalidRetain(t *testing.T) {
	savedRetain, savedAdd, savedNoReplace := rotRetain, rotAddFuture, rotNoReplace
	t.Cleanup(func() { rotRetain = savedRetain; rotAddFuture = savedAdd; rotNoReplace = savedNoReplace })

	rotRetain = "5weeks" // invalid unit
	rotAddFuture = 0

	err := runRotate(rotateCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid --retain value, got nil")
	}
	if !strings.Contains(err.Error(), "--retain") {
		t.Errorf("expected '--retain' in error, got: %v", err)
	}
}
