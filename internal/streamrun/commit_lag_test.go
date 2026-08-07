package streamrun

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/parser"
)

// histCount reads a histogram child's sample count. testutil.ToFloat64 refuses
// histograms, so go through the Metric interface the observer also implements.
func histCount(t *testing.T, o prometheus.Observer) uint64 {
	t.Helper()
	c, ok := o.(prometheus.Metric)
	if !ok {
		t.Fatalf("observer %T is not a prometheus.Metric", o)
	}
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("Write: %v", err)
	}
	return m.GetHistogram().GetSampleCount()
}

func TestObserveCommitLag_observesPerEventAndTakesTheWorstLag(t *testing.T) {
	m := observe.ForSource("commit-lag-basic")
	now := time.Date(2026, 8, 7, 12, 0, 30, 0, time.UTC)
	read := now.Add(-2 * time.Second)

	batch := []parser.Event{
		// Committed at the source 10s before T2.
		{ReadAt: read, Timestamp: now.Add(-10 * time.Second)},
		// The OLDEST source commit in the batch — this is the one the gauge must
		// report, not the newest and not the last.
		{ReadAt: read, Timestamp: now.Add(-45 * time.Second)},
		{ReadAt: read, Timestamp: now.Add(-3 * time.Second)},
	}
	before := histCount(t, m.IndexCommitLatency)
	observeCommitLag(m, batch, now)

	if got := histCount(t, m.IndexCommitLatency) - before; got != 3 {
		t.Errorf("histogram observations = %d, want 3 (one per event, not one per flush)", got)
	}
	if got := testutil.ToFloat64(m.AvailabilityLag); got != 45 {
		t.Errorf("availability_lag = %v, want 45 (the batch maximum)", got)
	}
	if got := testutil.ToFloat64(m.LastFlushTimestamp); got != float64(now.Unix()) {
		t.Errorf("last_flush_timestamp = %v, want %v", got, now.Unix())
	}
}

// A zero ReadAt must be skipped, not observed as a 0-second latency: file-mode
// re-indexing of old binlogs would otherwise publish the most convincing
// possible "everything is perfectly fresh".
func TestObserveCommitLag_skipsZeroReadAtPerEvent(t *testing.T) {
	m := observe.ForSource("commit-lag-zero")
	now := time.Date(2026, 8, 7, 12, 0, 30, 0, time.UTC)

	batch := []parser.Event{
		{Timestamp: now.Add(-90 * time.Second)},                               // zero ReadAt: skipped entirely
		{ReadAt: now.Add(-time.Second), Timestamp: now.Add(-5 * time.Second)}, // the only observable event
	}
	before := histCount(t, m.IndexCommitLatency)
	observeCommitLag(m, batch, now)

	if got := histCount(t, m.IndexCommitLatency) - before; got != 1 {
		t.Errorf("histogram observations = %d, want 1 — a zero ReadAt must not be observed", got)
	}
	// The skipped event is 90s old at the source; folding it in would report 90.
	if got := testutil.ToFloat64(m.AvailabilityLag); got != 5 {
		t.Errorf("availability_lag = %v, want 5 — a zero-ReadAt event must not reach the gauge", got)
	}
}

// An all-zero batch (every event from a non-replication producer) must leave the
// lag gauges untouched rather than resetting them to 0 — but the flush DID make
// data queryable, so the flush timestamp still advances.
func TestObserveCommitLag_allZeroLeavesLagGaugesButStampsFlush(t *testing.T) {
	m := observe.ForSource("commit-lag-allzero")
	m.AvailabilityLag.Set(123)
	now := time.Date(2026, 8, 7, 12, 0, 30, 0, time.UTC)

	before := histCount(t, m.IndexCommitLatency)
	observeCommitLag(m, []parser.Event{{Timestamp: now}, {Timestamp: now}}, now)

	if got := histCount(t, m.IndexCommitLatency) - before; got != 0 {
		t.Errorf("histogram observations = %d, want 0", got)
	}
	if got := testutil.ToFloat64(m.AvailabilityLag); got != 123 {
		t.Errorf("availability_lag = %v, want the untouched 123 — never reset to a fabricated 0", got)
	}
	if got := testutil.ToFloat64(m.LastFlushTimestamp); got != float64(now.Unix()) {
		t.Errorf("last_flush_timestamp = %v, want %v — the flush happened regardless", got, now.Unix())
	}
}

// A source clock running ahead of ours produces a negative T2−T0. Publishing it
// would drag any average or rate panel below zero for a state that does not
// exist.
func TestObserveCommitLag_clampsNegativeLagToZero(t *testing.T) {
	m := observe.ForSource("commit-lag-skew")
	now := time.Date(2026, 8, 7, 12, 0, 30, 0, time.UTC)

	observeCommitLag(m, []parser.Event{
		{ReadAt: now.Add(-time.Second), Timestamp: now.Add(30 * time.Second)},
	}, now)

	if got := testutil.ToFloat64(m.AvailabilityLag); got != 0 {
		t.Errorf("availability_lag = %v, want 0 (clamped)", got)
	}
}

func TestObserveCommitLag_emptyBatchIsANoOp(t *testing.T) {
	m := observe.ForSource("commit-lag-empty")
	m.LastFlushTimestamp.Set(7)

	observeCommitLag(m, nil, time.Now())

	if got := testutil.ToFloat64(m.LastFlushTimestamp); got != 7 {
		t.Errorf("last_flush_timestamp = %v, want the untouched 7", got)
	}
}
