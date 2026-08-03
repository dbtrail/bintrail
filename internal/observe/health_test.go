package observe

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestContinuityGauge_SetAndClear(t *testing.T) {
	SetContinuityGapLost("srv-a", true)
	if got := testutil.ToFloat64(continuityGapLost.WithLabelValues("srv-a")); got != 1 {
		t.Fatalf("gap_lost after Set(true) = %v, want 1", got)
	}
	SetContinuityGapLost("srv-a", false)
	if got := testutil.ToFloat64(continuityGapLost.WithLabelValues("srv-a")); got != 0 {
		t.Fatalf("gap_lost after Set(false) = %v, want 0", got)
	}

	// Clear unpublishes the series: unknown must never read as a healthy 0.
	ClearContinuity("srv-a")
	if n := testutil.CollectAndCount(continuityGapLost); n != 0 {
		t.Fatalf("after Clear the series must be absent, found %d", n)
	}
}

func TestVerifyGauges(t *testing.T) {
	at := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	SetVerifyOutcome("wp", at, 12, 1, 2, 0)
	if got := testutil.ToFloat64(verifyLastRun.WithLabelValues("wp")); got != float64(at.Unix()) {
		t.Fatalf("last_run = %v, want %v", got, at.Unix())
	}
	for _, tc := range []struct {
		status string
		want   float64
	}{{"match", 12}, {"mismatch", 1}, {"inconclusive", 2}, {"error", 0}} {
		if got := testutil.ToFloat64(verifyTables.WithLabelValues("wp", tc.status)); got != tc.want {
			t.Fatalf("tables{status=%q} = %v, want %v", tc.status, got, tc.want)
		}
	}
}

func TestDeleteVerifyOutcome_unpublishes(t *testing.T) {
	// Count-delta against the shared default registry: sibling tests may own
	// other labels, but Delete must remove exactly this label's series —
	// ABSENCE, not a zeroed value, is the invariant the alert rules and the
	// Help text promise.
	before := testutil.CollectAndCount(verifyLastRun)
	beforeTables := testutil.CollectAndCount(verifyTables)
	SetVerifyOutcome("gone", time.Now(), 1, 2, 3, 4)
	if n := testutil.CollectAndCount(verifyLastRun); n != before+1 {
		t.Fatalf("Set did not add a last_run series: %d -> %d", before, n)
	}
	DeleteVerifyOutcome("gone")
	if n := testutil.CollectAndCount(verifyLastRun); n != before {
		t.Fatalf("Delete left last_run series behind: want %d, got %d", before, n)
	}
	if n := testutil.CollectAndCount(verifyTables); n != beforeTables {
		t.Fatalf("Delete left tables series behind: want %d, got %d", beforeTables, n)
	}
}

func TestRotationGauges(t *testing.T) {
	SetRotationHealth(false, 0)
	if got := testutil.ToFloat64(rotationHealthy.WithLabelValues()); got != 1 {
		t.Fatalf("healthy cycle: gauge = %v, want 1", got)
	}
	SetRotationHealth(false, 3)
	if got := testutil.ToFloat64(rotationHealthy.WithLabelValues()); got != 0 {
		t.Fatalf("deferring cycle must read unhealthy, got %v", got)
	}
	if got := testutil.ToFloat64(rotationDeferred.WithLabelValues()); got != 3 {
		t.Fatalf("deferred = %v, want 3", got)
	}
	SetRotationHealth(true, 0)
	if got := testutil.ToFloat64(rotationHealthy.WithLabelValues()); got != 0 {
		t.Fatalf("failed cycle must read unhealthy, got %v", got)
	}
}
