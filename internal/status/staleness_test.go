package status

import (
	"bytes"
	"database/sql"
	"strings"
	"testing"
	"time"
)

func TestBaselineStalenessFor(t *testing.T) {
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	oldest := now.Add(-100 * time.Hour) // coverage span = 100h; aging floor = 80h ago
	cases := []struct {
		name     string
		snapshot time.Time
		oldest   time.Time
		want     BaselineStalenessVerdict
	}{
		{"fresh snapshot", now.Add(-1 * time.Hour), oldest, BaselineOK},
		{"just inside the aging floor", now.Add(-79 * time.Hour), oldest, BaselineOK},
		{"exactly at 80% of the span", now.Add(-80 * time.Hour), oldest, BaselineAging},
		{"old but still covered", now.Add(-99 * time.Hour), oldest, BaselineAging},
		{"anchor equals the floor", oldest, oldest, BaselineAging},
		{"anchor predates coverage", oldest.Add(-time.Minute), oldest, BaselineBroken},
		{"no evaluable floor", now.Add(-1 * time.Hour), time.Time{}, BaselineUnknown},
		{"zero snapshot time", time.Time{}, oldest, BaselineUnknown},
		// Degenerate: coverage starting now (span <= 0) must not divide by it.
		{"coverage starts now", now, now, BaselineOK},
	}
	for _, tc := range cases {
		if got := BaselineStalenessFor(tc.snapshot, tc.oldest, now); got != tc.want {
			t.Errorf("%s: got %s, want %s", tc.name, got, tc.want)
		}
	}
}

func TestCoverageOldestDelta(t *testing.T) {
	at := func(h int) sql.NullTime {
		return sql.NullTime{Time: time.Date(2026, 8, 1, h, 0, 0, 0, time.UTC), Valid: true}
	}
	var nilCov *CoverageInfo
	if !nilCov.OldestDelta().IsZero() {
		t.Fatal("nil coverage must be unknown")
	}
	if got := (&CoverageInfo{}).OldestDelta(); !got.IsZero() {
		t.Fatalf("empty coverage must be unknown, got %v", got)
	}
	// Archives extend live coverage backwards; the earlier of the two wins.
	c := &CoverageInfo{EarliestEvent: at(10), ArchiveEarliestHour: at(3)}
	if got := c.OldestDelta(); got != at(3).Time {
		t.Fatalf("archive floor must win when earlier: %v", got)
	}
	c = &CoverageInfo{EarliestEvent: at(2), ArchiveEarliestHour: at(5)}
	if got := c.OldestDelta(); got != at(2).Time {
		t.Fatalf("live floor must win when earlier: %v", got)
	}
	c = &CoverageInfo{ArchiveEarliestHour: at(7)}
	if got := c.OldestDelta(); got != at(7).Time {
		t.Fatalf("archive-only coverage must count: %v", got)
	}
}

func TestOverallBaselineStaleness_newestPerTable(t *testing.T) {
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	oldest := now.Add(-100 * time.Hour)
	baselines := []BaselineInfo{
		// A superseded broken snapshot must NOT drive the headline…
		{Database: "shop", Table: "orders", SnapshotTime: oldest.Add(-24 * time.Hour)},
		{Database: "shop", Table: "orders", SnapshotTime: now.Add(-2 * time.Hour)},
		// …but a table whose NEWEST snapshot is broken must.
		{Database: "shop", Table: "legacy", SnapshotTime: oldest.Add(-time.Hour)},
	}
	AnnotateBaselineStaleness(baselines, oldest, now)
	if baselines[0].Staleness != BaselineBroken || baselines[1].Staleness != BaselineOK {
		t.Fatalf("per-entry annotation wrong: %+v", baselines)
	}
	if got := OverallBaselineStaleness(baselines); got != BaselineBroken {
		t.Fatalf("overall = %s, want broken (legacy's newest is broken)", got)
	}

	// Without the broken table, the superseded broken snapshot is ignored.
	if got := OverallBaselineStaleness(baselines[:2]); got != BaselineOK {
		t.Fatalf("overall = %s, want ok (orders' newest is fresh)", got)
	}
	if got := OverallBaselineStaleness(nil); got != "" {
		t.Fatalf("empty list must have no verdict, got %q", got)
	}
}

func TestWriteBaselines_stalenessColumnAndBanner(t *testing.T) {
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	oldest := now.Add(-100 * time.Hour)
	baselines := []BaselineInfo{
		{Database: "shop", Table: "orders", SnapshotTime: now.Add(-2 * time.Hour)},
		{Database: "shop", Table: "legacy", SnapshotTime: oldest.Add(-time.Hour)},
	}
	AnnotateBaselineStaleness(baselines, oldest, now)
	var buf bytes.Buffer
	writeBaselines(&buf, baselines)
	out := buf.String()
	if !strings.Contains(out, "STALENESS") || !strings.Contains(out, "⚠ broken") {
		t.Fatalf("staleness column missing:\n%s", out)
	}
	if !strings.Contains(out, "BASELINE STALE — FULL-TABLE RESTORE BROKEN") {
		t.Fatalf("broken banner missing:\n%s", out)
	}

	// All fresh: no banner, quiet "ok" verdicts.
	fresh := []BaselineInfo{{Database: "shop", Table: "orders", SnapshotTime: now.Add(-2 * time.Hour)}}
	AnnotateBaselineStaleness(fresh, oldest, now)
	buf.Reset()
	writeBaselines(&buf, fresh)
	if strings.Contains(buf.String(), "BASELINE STALE") {
		t.Fatalf("banner must not fire without a broken newest snapshot:\n%s", buf.String())
	}
}
