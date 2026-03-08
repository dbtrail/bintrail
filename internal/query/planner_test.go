package query

import (
	"testing"
	"time"
)

func TestParsePartitionName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantOK  bool
		wantUTC time.Time
	}{
		{"valid", "p_2026021914", true, time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC)},
		{"valid midnight", "p_2026010100", true, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"p_future", "p_future", false, time.Time{}},
		{"too short", "p_20260219", false, time.Time{}},
		{"no prefix", "x_2026021914", false, time.Time{}},
		{"empty", "", false, time.Time{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ParsePartitionName(tc.input)
			if ok != tc.wantOK {
				t.Fatalf("ParsePartitionName(%q) ok=%v, want %v", tc.input, ok, tc.wantOK)
			}
			if ok && !got.Equal(tc.wantUTC) {
				t.Errorf("ParsePartitionName(%q) = %v, want %v", tc.input, got, tc.wantUTC)
			}
		})
	}
}

func TestBuildContiguousRanges(t *testing.T) {
	h := func(day, hour int) time.Time {
		return time.Date(2026, 1, day, hour, 0, 0, 0, time.UTC)
	}

	hours := []time.Time{h(5, 10), h(5, 11), h(5, 12), h(5, 15), h(5, 16)}
	start := h(5, 10)
	end := h(5, 17)

	ranges := buildContiguousRanges(hours, start, end)
	if len(ranges) != 2 {
		t.Fatalf("expected 2 contiguous ranges, got %d: %+v", len(ranges), ranges)
	}
	if !ranges[0].Start.Equal(h(5, 10)) || !ranges[0].End.Equal(h(5, 13)) {
		t.Errorf("range[0] = %v – %v, want 10:00 – 13:00", ranges[0].Start, ranges[0].End)
	}
	if !ranges[1].Start.Equal(h(5, 15)) || !ranges[1].End.Equal(h(5, 17)) {
		t.Errorf("range[1] = %v – %v, want 15:00 – 17:00", ranges[1].Start, ranges[1].End)
	}
}

func TestBuildContiguousRanges_filtersOutOfRange(t *testing.T) {
	h := func(hour int) time.Time {
		return time.Date(2026, 1, 5, hour, 0, 0, 0, time.UTC)
	}

	hours := []time.Time{h(8), h(9), h(10), h(11), h(12)}
	start := h(10)
	end := h(12)

	ranges := buildContiguousRanges(hours, start, end)
	if len(ranges) != 1 {
		t.Fatalf("expected 1 range, got %d", len(ranges))
	}
	if !ranges[0].Start.Equal(h(10)) || !ranges[0].End.Equal(h(12)) {
		t.Errorf("range = %v – %v, want 10:00 – 12:00", ranges[0].Start, ranges[0].End)
	}
}

func TestBuildContiguousRanges_singleHour(t *testing.T) {
	h := time.Date(2026, 1, 5, 10, 0, 0, 0, time.UTC)
	ranges := buildContiguousRanges([]time.Time{h}, h, h.Add(time.Hour))
	if len(ranges) != 1 {
		t.Fatalf("expected 1 range, got %d", len(ranges))
	}
	if !ranges[0].Start.Equal(h) || !ranges[0].End.Equal(h.Add(time.Hour)) {
		t.Errorf("range = %v – %v, want single hour", ranges[0].Start, ranges[0].End)
	}
}

func TestBuildContiguousRanges_empty(t *testing.T) {
	start := time.Date(2026, 1, 5, 10, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	ranges := buildContiguousRanges(nil, start, end)
	if len(ranges) != 0 {
		t.Errorf("expected empty ranges for nil hours, got %+v", ranges)
	}
}

func TestFormatGapWarning_noGaps(t *testing.T) {
	if w := FormatGapWarning(nil); w != "" {
		t.Errorf("expected empty warning for no gaps, got %q", w)
	}
}

func TestFormatGapWarning_withGaps(t *testing.T) {
	gaps := []time.Time{
		time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 5, 1, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 9, 23, 0, 0, 0, time.UTC),
	}
	w := FormatGapWarning(gaps)
	want := "query covers hours with no data (rotated and not archived): 2026-01-05 00:00 – 2026-01-09 23:00"
	if w != want {
		t.Errorf("got:\n  %q\nwant:\n  %q", w, want)
	}
}

func TestFormatGapWarning_singleHour(t *testing.T) {
	gaps := []time.Time{
		time.Date(2026, 3, 1, 14, 0, 0, 0, time.UTC),
	}
	w := FormatGapWarning(gaps)
	want := "query covers hours with no data (rotated and not archived): 2026-03-01 14:00 – 2026-03-01 14:00"
	if w != want {
		t.Errorf("got:\n  %q\nwant:\n  %q", w, want)
	}
}

func TestQueryPlan_SkipMySQL(t *testing.T) {
	// A nil plan (fallback) should NOT skip MySQL — safety default.
	if (*QueryPlan)(nil).SkipMySQL() {
		t.Error("expected SkipMySQL=false for nil plan")
	}

	// A plan with no MySQL ranges and no gaps means MySQL can be skipped.
	p := &QueryPlan{}
	if !p.SkipMySQL() {
		t.Error("expected SkipMySQL=true for empty (fully-archived) plan")
	}

	// If there are MySQL ranges, don't skip.
	p2 := &QueryPlan{
		MySQLRanges: []TimeRange{{
			Start: time.Date(2026, 1, 5, 10, 0, 0, 0, time.UTC),
			End:   time.Date(2026, 1, 5, 11, 0, 0, 0, time.UTC),
		}},
	}
	if p2.SkipMySQL() {
		t.Error("expected SkipMySQL=false when MySQL ranges exist")
	}

	// If there are gaps, don't skip (data might be there but not rotated yet).
	p3 := &QueryPlan{
		GapHours: []time.Time{time.Date(2026, 1, 5, 10, 0, 0, 0, time.UTC)},
	}
	if p3.SkipMySQL() {
		t.Error("expected SkipMySQL=false when gaps exist")
	}
}

func TestPlan_nilDB(t *testing.T) {
	since := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	p, err := Plan(t.Context(), nil, "testdb", &since, nil)
	if err != nil {
		t.Fatal(err)
	}
	if p != nil {
		t.Errorf("expected nil plan for nil DB, got %+v", p)
	}
}

func TestPlan_noTimeRange(t *testing.T) {
	// Nil since and until should produce nil plan (no routing possible).
	p, err := Plan(t.Context(), nil, "testdb", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if p != nil {
		t.Errorf("expected nil plan for no time range, got %+v", p)
	}
}

// ─── buildPlan tests (core routing logic) ─────────────────────────────────────

func h(day, hour int) time.Time {
	return time.Date(2026, 1, day, hour, 0, 0, 0, time.UTC)
}

func TestBuildPlan_allLiveNoneArchived(t *testing.T) {
	live := []time.Time{h(5, 10), h(5, 11), h(5, 12)}
	plan := buildPlan(live, nil, h(5, 10), h(5, 13))

	if plan == nil {
		t.Fatal("expected non-nil plan")
	}
	if plan.SkipMySQL() {
		t.Error("should not skip MySQL when all hours are live")
	}
	if len(plan.GapHours) != 0 {
		t.Errorf("expected no gaps, got %d", len(plan.GapHours))
	}
	if len(plan.MySQLRanges) != 1 {
		t.Fatalf("expected 1 MySQL range, got %d", len(plan.MySQLRanges))
	}
}

func TestBuildPlan_allArchivedNoneLive(t *testing.T) {
	archived := []time.Time{h(5, 10), h(5, 11), h(5, 12)}
	plan := buildPlan(nil, archived, h(5, 10), h(5, 13))

	if plan == nil {
		t.Fatal("expected non-nil plan")
	}
	if !plan.SkipMySQL() {
		t.Error("should skip MySQL when all hours are archived")
	}
	if len(plan.GapHours) != 0 {
		t.Errorf("expected no gaps, got %d", len(plan.GapHours))
	}
	if len(plan.MySQLRanges) != 0 {
		t.Errorf("expected no MySQL ranges, got %d", len(plan.MySQLRanges))
	}
}

func TestBuildPlan_mixedLiveAndArchived(t *testing.T) {
	live := []time.Time{h(5, 12), h(5, 13)}
	archived := []time.Time{h(5, 10), h(5, 11)}
	plan := buildPlan(live, archived, h(5, 10), h(5, 14))

	if plan == nil {
		t.Fatal("expected non-nil plan")
	}
	if plan.SkipMySQL() {
		t.Error("should not skip MySQL in mixed scenario")
	}
	if len(plan.GapHours) != 0 {
		t.Errorf("expected no gaps, got %d", len(plan.GapHours))
	}
	if len(plan.MySQLRanges) != 1 {
		t.Fatalf("expected 1 MySQL range, got %d", len(plan.MySQLRanges))
	}
	if !plan.MySQLRanges[0].Start.Equal(h(5, 12)) || !plan.MySQLRanges[0].End.Equal(h(5, 14)) {
		t.Errorf("MySQL range = %v – %v, want 12:00 – 14:00",
			plan.MySQLRanges[0].Start, plan.MySQLRanges[0].End)
	}
}

func TestBuildPlan_withGaps(t *testing.T) {
	live := []time.Time{h(5, 10)}
	archived := []time.Time{h(5, 13)}
	// Range 10-14, live at 10, gap at 11-12, archived at 13
	plan := buildPlan(live, archived, h(5, 10), h(5, 14))

	if plan == nil {
		t.Fatal("expected non-nil plan")
	}
	if plan.SkipMySQL() {
		t.Error("should not skip MySQL when there are gaps")
	}
	if len(plan.GapHours) != 2 {
		t.Fatalf("expected 2 gap hours, got %d: %v", len(plan.GapHours), plan.GapHours)
	}
	if !plan.GapHours[0].Equal(h(5, 11)) || !plan.GapHours[1].Equal(h(5, 12)) {
		t.Errorf("gaps = %v, want [11:00, 12:00]", plan.GapHours)
	}
}

func TestBuildPlan_overlappingLiveAndArchived(t *testing.T) {
	// An hour exists in both live and archive — should not be a gap.
	live := []time.Time{h(5, 10), h(5, 11)}
	archived := []time.Time{h(5, 10), h(5, 11)}
	plan := buildPlan(live, archived, h(5, 10), h(5, 12))

	if plan == nil {
		t.Fatal("expected non-nil plan")
	}
	if len(plan.GapHours) != 0 {
		t.Errorf("expected no gaps for overlapping hours, got %d", len(plan.GapHours))
	}
	// MySQL is still needed because hours are live.
	if plan.SkipMySQL() {
		t.Error("should not skip MySQL when live hours exist")
	}
}

func TestBuildPlan_allGaps(t *testing.T) {
	// No live, no archived — everything is a gap.
	plan := buildPlan(nil, nil, h(5, 10), h(5, 13))

	if plan == nil {
		t.Fatal("expected non-nil plan")
	}
	if len(plan.GapHours) != 3 {
		t.Fatalf("expected 3 gap hours, got %d", len(plan.GapHours))
	}
	// SkipMySQL should be false because there are gaps.
	if plan.SkipMySQL() {
		t.Error("should not skip MySQL when there are gaps")
	}
}

func TestBuildPlan_noBoundsReturnsNil(t *testing.T) {
	plan := buildPlan(nil, nil, time.Time{}, time.Time{})
	if plan != nil {
		t.Errorf("expected nil for zero-value bounds, got %+v", plan)
	}
}
