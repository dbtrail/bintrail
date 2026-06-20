//go:build integration

package streamrun

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestScrapeIndexMetrics_endToEnd drives the real scrape path (CollectStatus +
// query.Plan + Set) against a live index — the path the unit tests don't reach.
// It guards the regression where scrapeIndexMetrics called query.Plan(nil,nil)
// (which returns a nil plan) and dereferenced it: with a real event present,
// OldestEvent is non-zero so Plan IS invoked with a concrete range. Beyond not
// panicking, it asserts a coverage-derived gauge was actually published, so a
// future early-return before Set can't keep the test green while silently
// un-exercising the path.
func TestScrapeIndexMetrics_endToEnd(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	ts := time.Now().UTC().Add(-1 * time.Hour).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, nil,
		"shop", "orders", 1, "1", nil, nil, []byte(`{"id":1}`))

	m := observe.IndexForSource("e2e-src")
	scrapeIndexMetrics(context.Background(), db, dbName, m)

	// The one inserted event must surface in events_total{source="e2e-src"} —
	// proving CollectStatus + Set ran end-to-end (read via the public gatherer so
	// the test doesn't reach into observe's unexported vectors).
	if got, ok := indexGaugeValue(t, "bintrail_index_events_total", "e2e-src"); !ok || got != 1 {
		t.Fatalf("events_total{source=e2e-src} = %v (present=%v), want 1", got, ok)
	}
}

// indexGaugeValue reads a single gauge value by metric name and source label
// from the default Prometheus registry.
func indexGaugeValue(t *testing.T, name, source string) (float64, bool) {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "source" && l.GetValue() == source {
					return m.GetGauge().GetValue(), true
				}
			}
		}
	}
	return 0, false
}
