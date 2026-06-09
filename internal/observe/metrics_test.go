package observe_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/dbtrail/dbtrail/internal/observe"
)

func TestForSource_registration(t *testing.T) {
	// promauto registers the vectors in the default registry at init time,
	// but a labeled vector only emits families once a child exists — touch
	// one source first.
	m := observe.ForSource("reg-test")
	m.EventsReceived.Inc()

	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	var found int
	for _, mf := range mfs {
		if len(mf.GetName()) >= 8 && mf.GetName()[:8] == "bintrail" {
			found++
		}
	}
	if found == 0 {
		t.Error("expected at least one bintrail_* metric family in default registry")
	}
}

func TestForSource_isolatesSources(t *testing.T) {
	a := observe.ForSource("src-a")
	b := observe.ForSource("src-b")

	a.EventsReceived.Add(3)
	b.EventsReceived.Add(7)

	if got := testutil.ToFloat64(a.EventsReceived); got != 3 {
		t.Errorf("src-a events_received = %v, want 3", got)
	}
	if got := testutil.ToFloat64(b.EventsReceived); got != 7 {
		t.Errorf("src-b events_received = %v, want 7", got)
	}

	// Gauges must not clobber across sources — the whole point of the label.
	a.ReplicationLag.Set(1)
	b.ReplicationLag.Set(99)
	if got := testutil.ToFloat64(a.ReplicationLag); got != 1 {
		t.Errorf("src-a replication_lag = %v, want 1", got)
	}
}

func TestForSource_sameSourceAccumulates(t *testing.T) {
	first := observe.ForSource("src-acc")
	second := observe.ForSource("src-acc")

	first.EventsIndexed.Add(2)
	second.EventsIndexed.Add(5)

	if got := testutil.ToFloat64(second.EventsIndexed); got != 7 {
		t.Errorf("events_indexed = %v, want 7 (same source must share the child)", got)
	}
}

func TestForSource_emptyFallsBackToDefault(t *testing.T) {
	anon := observe.ForSource("")
	named := observe.ForSource("default")

	before := testutil.ToFloat64(named.BatchFlushes)
	anon.BatchFlushes.Inc()
	if got := testutil.ToFloat64(named.BatchFlushes); got != before+1 {
		t.Errorf(`ForSource("") must alias ForSource("default"): got %v, want %v`, got, before+1)
	}
}

func TestForSource_errorsKeepTypeLabel(t *testing.T) {
	m := observe.ForSource("src-err")
	// Incrementing with the remaining "type" label must not panic.
	m.Errors.WithLabelValues("batch_flush").Inc()
	m.Errors.WithLabelValues("checkpoint").Inc()
	m.Errors.WithLabelValues("gtid_update").Inc()

	if got := testutil.ToFloat64(m.Errors.WithLabelValues("checkpoint")); got != 1 {
		t.Errorf("errors{type=checkpoint} = %v, want 1", got)
	}
}

func TestForSource_batchSizeObserve(t *testing.T) {
	m := observe.ForSource("src-hist")
	// Observing histogram values should not panic.
	m.BatchSize.Observe(10)
	m.BatchSize.Observe(500)
	m.BatchSize.Observe(1000)
}
