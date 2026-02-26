package observe_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bintrail/bintrail/internal/observe"
)

func TestStreamMetrics_registration(t *testing.T) {
	// promauto registers metrics in the default registry at init time.
	// Gathering should produce at least one metric family for our namespace.
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

func TestStreamEventsReceived_increment(t *testing.T) {
	before := testutil.ToFloat64(observe.StreamEventsReceived)
	observe.StreamEventsReceived.Add(3)
	after := testutil.ToFloat64(observe.StreamEventsReceived)
	if after-before != 3 {
		t.Errorf("expected counter to increase by 3, got %v", after-before)
	}
}

func TestStreamErrors_labelValues(t *testing.T) {
	// Incrementing with label values should not panic.
	observe.StreamErrors.WithLabelValues("batch_flush").Inc()
	observe.StreamErrors.WithLabelValues("checkpoint").Inc()
	observe.StreamErrors.WithLabelValues("gtid_update").Inc()
}

func TestStreamBatchSize_observe(t *testing.T) {
	// Observing histogram values should not panic.
	observe.StreamBatchSize.Observe(10)
	observe.StreamBatchSize.Observe(500)
	observe.StreamBatchSize.Observe(1000)
}
