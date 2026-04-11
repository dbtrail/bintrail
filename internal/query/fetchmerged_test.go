package query

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestFetchMerged_nilArchiveFetcherRejected verifies the programming-error
// guard: calling FetchMerged without an ArchiveFetcher when archives are
// enabled must fail loudly instead of silently skipping archives. This is the
// exact class of bug #209 is fixing.
func TestFetchMerged_nilArchiveFetcherRejected(t *testing.T) {
	_, _, err := FetchMerged(context.Background(), nil, nil, FetchMergedOptions{
		NoArchive:      false,
		ArchiveFetcher: nil,
	})
	if err == nil {
		t.Fatal("expected error when ArchiveFetcher is nil and NoArchive is false, got nil")
	}
	if !strings.Contains(err.Error(), "ArchiveFetcher") {
		t.Errorf("expected error to mention ArchiveFetcher, got: %v", err)
	}
}

// TestFetchMerged_strictModeRequiresDBName verifies that AllowGaps=false with
// a time range set but an empty DBName is rejected at the validation stage.
// The combination is unrepresentable — strict mode promises "abort on gap"
// but the planner cannot detect gaps without a DBName, so the promise could
// only be kept by silently degrading to "no gap detection," which is exactly
// the silent-failure class this PR (#209) exists to prevent.
func TestFetchMerged_strictModeRequiresDBName(t *testing.T) {
	since := time.Now().UTC().Add(-24 * time.Hour)
	until := time.Now().UTC()
	_, _, err := FetchMerged(context.Background(), nil, nil, FetchMergedOptions{
		Opts: Options{
			Since: &since,
			Until: &until,
		},
		DBName:         "", // missing — strict mode cannot honor its contract
		AllowGaps:      false,
		ArchiveFetcher: func(_ context.Context, _ Options, _ string) ([]ResultRow, error) { return nil, nil },
	})
	if err == nil {
		t.Fatal("expected error for empty DBName under AllowGaps=false with a time range, got nil")
	}
	if !strings.Contains(err.Error(), "DBName") || !strings.Contains(err.Error(), "AllowGaps") {
		t.Errorf("expected error to mention DBName and AllowGaps, got: %v", err)
	}
}

// TestFetchMerged_strictModeNoTimeRangeOK verifies that strict mode with an
// empty DBName is NOT rejected when there is no time range — gap detection
// is moot without a range, so the validation should not fire.
func TestFetchMerged_strictModeNoTimeRangeOK(t *testing.T) {
	// We pass nil db/engine — the call will panic on engine.Fetch once it
	// gets past validation. The test only asserts that validation does not
	// reject this combination, via a deferred recover.
	defer func() {
		// Any panic here came from engine.Fetch — proves we passed validation.
		_ = recover()
	}()
	_, _, err := FetchMerged(context.Background(), nil, nil, FetchMergedOptions{
		DBName:         "",
		AllowGaps:      false,
		NoArchive:      true, // skip archive path entirely
		ArchiveFetcher: nil,
	})
	// If we reached here without a panic AND without an error, that's also
	// fine — validation simply didn't trip.
	if err != nil && (strings.Contains(err.Error(), "DBName") || strings.Contains(err.Error(), "ArchiveFetcher")) {
		t.Errorf("validation should not fire when no time range is set: %v", err)
	}
}

// TestGapError_errorsAs verifies that a GapError can be unwrapped with
// errors.As so programmatic callers (full-table reconstruct #187, MCP
// tools) can inspect the gap hours without string-matching.
func TestGapError_errorsAs(t *testing.T) {
	hours := []time.Time{
		time.Date(2026, 4, 9, 14, 0, 0, 0, time.UTC),
		time.Date(2026, 4, 9, 15, 0, 0, 0, time.UTC),
	}
	var err error = &GapError{GapHours: hours}

	var gapErr *GapError
	if !errors.As(err, &gapErr) {
		t.Fatal("errors.As failed to unwrap GapError")
	}
	if len(gapErr.GapHours) != 2 {
		t.Errorf("expected 2 gap hours, got %d", len(gapErr.GapHours))
	}
	if !strings.Contains(gapErr.Error(), "allow-gaps") {
		t.Errorf("expected Error() to mention --allow-gaps, got: %s", gapErr.Error())
	}
}
