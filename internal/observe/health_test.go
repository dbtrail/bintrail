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
	SetVerifyOutcome("gone", time.Now(), 1, 2, 3, 4)
	DeleteVerifyOutcome("gone")
	if n := testutil.CollectAndCount(verifyLastRun); n != 0 {
		// Other tests publish under different labels; count only after
		// isolating — reset by deleting the known label and asserting the
		// specific series is gone via a fresh set/delete cycle.
		t.Logf("registry still has %d verify_last_run series from sibling tests", n)
	}
	// The concrete assertion: re-reading the deleted label creates a NEW
	// zero-valued child, proving the old values did not linger.
	if got := testutil.ToFloat64(verifyLastRun.WithLabelValues("gone")); got != 0 {
		t.Fatalf("deleted series lingered with value %v", got)
	}
	verifyLastRun.DeleteLabelValues("gone")
	if got := testutil.ToFloat64(verifyTables.WithLabelValues("gone", "mismatch")); got != 0 {
		t.Fatalf("deleted tables series lingered with value %v", got)
	}
	verifyTables.DeleteLabelValues("gone", "mismatch")
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
