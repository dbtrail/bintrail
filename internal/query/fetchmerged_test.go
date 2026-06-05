package query

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
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

// TestFetchMerged_partialArchiveFailureAbortsStrictUnit mirrors the
// integration test of the same name (cmd/bintrail) one tier down: the
// any-source-fails strict contract (#377) would otherwise be pinned only
// behind the Docker-gated integration tier, so a revert to the old
// all-sources-must-fail guard would keep `go test ./...` green. sqlmock
// stands in for MySQL: one query feeds ResolveArchiveSources two sources
// (local base dirs must exist on disk — the resolver os.Stat's them),
// one query feeds engine.Fetch zero live rows. No DBName and no time
// range keeps the planner out of the way (validation allows that).
func TestFetchMerged_partialArchiveFailureAbortsStrictUnit(t *testing.T) {
	dir := t.TempDir()
	healthyBase := filepath.Join(dir, "bintrail_id=healthy")
	brokenBase := filepath.Join(dir, "bintrail_id=broken")
	for _, d := range []string{healthyBase, brokenBase} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
	}

	brokenErr := errors.New("stub: broken archive (intentional)")
	stubFetcher := func(_ context.Context, _ Options, src string) ([]ResultRow, error) {
		if strings.Contains(src, "broken") {
			return nil, brokenErr
		}
		return nil, nil
	}

	// expectQueries arms one round of FetchMerged's two DB touches:
	// the archive_state resolution and the live binlog_events fetch.
	expectQueries := func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("FROM archive_state").WillReturnRows(
			sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
				AddRow("healthy", filepath.Join(healthyBase, "events.parquet"), nil, nil).
				AddRow("broken", filepath.Join(brokenBase, "events.parquet"), nil, nil))
		mock.ExpectQuery("FROM binlog_events").WillReturnRows(
			sqlmock.NewRows([]string{"event_id"}))
	}

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// Strict mode: the broken source aborts the fetch even though the
	// healthy source succeeded.
	expectQueries(mock)
	_, _, err = FetchMerged(context.Background(), db, New(db), FetchMergedOptions{
		AllowGaps:      false,
		ArchiveFetcher: stubFetcher,
	})
	if err == nil {
		t.Fatal("partial archive failure under strict mode: expected error, got nil")
	}
	if !errors.Is(err, brokenErr) {
		t.Errorf("expected wrapped broken-source error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "archive source") || !strings.Contains(err.Error(), "broken") {
		t.Errorf("expected error to name the broken archive source, got: %v", err)
	}

	// Permissive mode: same partial failure stays warn-and-continue.
	expectQueries(mock)
	if _, _, err = FetchMerged(context.Background(), db, New(db), FetchMergedOptions{
		AllowGaps:      true,
		ArchiveFetcher: stubFetcher,
	}); err != nil {
		t.Fatalf("partial archive failure under permissive mode: expected success, got: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
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
	// The Error() string is library-neutral (no CLI flag name leaked into
	// the internal/query package). It should still mention the gap hours.
	if !strings.Contains(gapErr.Error(), "no data") {
		t.Errorf("expected Error() to describe the gap, got: %s", gapErr.Error())
	}
	if strings.Contains(gapErr.Error(), "--") {
		t.Errorf("GapError.Error() must not leak CLI flag names, got: %s", gapErr.Error())
	}
}
