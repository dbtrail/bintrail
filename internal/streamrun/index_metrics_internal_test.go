package streamrun

import (
	"database/sql"
	"testing"
	"time"
)

// TestGapScrapeRange pins that the scraper computes a CONCRETE archive-aware
// window for query.Plan (it short-circuits to a nil plan on a nil range, which
// caused a nil-deref panic), and skips entirely on an empty index.
func TestGapScrapeRange(t *testing.T) {
	now := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	oldest := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	archive := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		oldest    time.Time
		archive   sql.NullTime
		wantOK    bool
		wantSince time.Time
	}{
		{"empty index → no range", time.Time{}, sql.NullTime{}, false, time.Time{}},
		{"live only → since=oldest", oldest, sql.NullTime{}, true, oldest},
		{"archive earlier than live → since=archive", oldest, sql.NullTime{Time: archive, Valid: true}, true, archive},
		{"archive only, no live → since=archive", time.Time{}, sql.NullTime{Time: archive, Valid: true}, true, archive},
		{"archive later than live → since=oldest", oldest, sql.NullTime{Time: now, Valid: true}, true, oldest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			since, until, ok := gapScrapeRange(tt.oldest, tt.archive, now)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if !ok {
				return // no range — scraper skips Plan entirely
			}
			if !since.Equal(tt.wantSince) {
				t.Errorf("since = %v, want %v", since, tt.wantSince)
			}
			if !until.Equal(now) {
				t.Errorf("until = %v, want now %v", until, now)
			}
		})
	}
}
