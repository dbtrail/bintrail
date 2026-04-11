package query

import (
	"context"
	"strings"
	"testing"
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

// TestFetchMerged_nilArchiveFetcherOKWhenNoArchive verifies the opposite:
// callers that explicitly opt out of archives do not need to supply a fetcher.
// We can't actually run a full query here without a *sql.DB, but we can
// confirm the nil-check does not fire.
func TestFetchMerged_nilArchiveFetcherOKWhenNoArchive(t *testing.T) {
	// We pass nil db/engine so the call will fail after the nil-fetcher check
	// but before any real DB work. The only thing we're asserting is that the
	// ArchiveFetcher guard doesn't fire.
	defer func() {
		if r := recover(); r != nil {
			// Panic is also acceptable — it proves we got past the nil
			// fetcher guard. A nil engine.Fetch will naturally panic.
			t.Logf("recovered from expected downstream panic: %v", r)
		}
	}()
	_, _, err := FetchMerged(context.Background(), nil, nil, FetchMergedOptions{
		NoArchive:      true,
		ArchiveFetcher: nil,
	})
	if err != nil && strings.Contains(err.Error(), "ArchiveFetcher") {
		t.Errorf("unexpected ArchiveFetcher error with NoArchive=true: %v", err)
	}
}
