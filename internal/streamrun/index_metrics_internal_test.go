package streamrun

import (
	"database/sql"
	"testing"
	"time"
)

// TestGapScrapeRange pins the gap window: a CONCRETE archive-aware range for
// query.Plan (it short-circuits to a nil plan on a nil range — the cause of an
// earlier nil-deref panic), since = earliest data, until = one hour past the
// newest EXPLICIT partition (so the not-yet-rotated p_future tail is excluded),
// and ok=false when there is no rotated span to measure.
func TestGapScrapeRange(t *testing.T) {
	oldest := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	archive := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	newest := time.Date(2026, 1, 20, 0, 0, 0, 0, time.UTC) // newest explicit partition hour

	tests := []struct {
		name           string
		oldest         time.Time
		archive        sql.NullTime
		newestExplicit time.Time
		wantOK         bool
		wantSince      time.Time
	}{
		{"empty index → no range", time.Time{}, sql.NullTime{}, time.Time{}, false, time.Time{}},
		{"data but only p_future (no explicit partition) → no rotated span", oldest, sql.NullTime{}, time.Time{}, false, time.Time{}},
		{"live only → since=oldest", oldest, sql.NullTime{}, newest, true, oldest},
		{"archive earlier than live → since=archive", oldest, sql.NullTime{Time: archive, Valid: true}, newest, true, archive},
		{"archive only, no live → since=archive", time.Time{}, sql.NullTime{Time: archive, Valid: true}, newest, true, archive},
		{"newest explicit before since → empty span", oldest, sql.NullTime{}, oldest.Add(-time.Hour), false, time.Time{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			since, until, ok := gapScrapeRange(tt.oldest, tt.archive, tt.newestExplicit)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if !ok {
				return // no rotated span — scraper skips Plan entirely
			}
			if !since.Equal(tt.wantSince) {
				t.Errorf("since = %v, want %v", since, tt.wantSince)
			}
			// until excludes the p_future tail: one hour past the newest explicit partition.
			if want := tt.newestExplicit.Add(time.Hour); !until.Equal(want) {
				t.Errorf("until = %v, want %v (newest explicit + 1h)", until, want)
			}
		})
	}
}
