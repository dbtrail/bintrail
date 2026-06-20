package observe

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestIndexMetricsSet(t *testing.T) {
	m := IndexForSource("test-src")
	oldest := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	newest := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)

	m.Set(IndexSnapshot{
		OldestEvent:      oldest,
		NewestEvent:      newest,
		Events:           1234,
		ActivePartitions: 5,
		FuturePartitions: 1,
		GapHours:         2,
		MySQLBytes:       1000,
		ParquetBytes:     2000,
	}, now)

	checks := []struct {
		name string
		got  float64
		want float64
	}{
		{"oldest", testutil.ToFloat64(indexOldestEvent.WithLabelValues("test-src")), float64(oldest.Unix())},
		{"newest", testutil.ToFloat64(indexNewestEvent.WithLabelValues("test-src")), float64(newest.Unix())},
		{"retention", testutil.ToFloat64(indexRetentionHorizon.WithLabelValues("test-src")), now.Sub(oldest).Seconds()},
		{"events", testutil.ToFloat64(indexEventsTotal.WithLabelValues("test-src")), 1234},
		{"active_partitions", testutil.ToFloat64(indexPartitions.WithLabelValues("test-src", "active")), 5},
		{"future_partitions", testutil.ToFloat64(indexPartitions.WithLabelValues("test-src", "future")), 1},
		{"gap_hours", testutil.ToFloat64(indexGapHours.WithLabelValues("test-src")), 2},
		{"mysql_bytes", testutil.ToFloat64(indexStorageBytes.WithLabelValues("test-src", "mysql")), 1000},
		{"parquet_bytes", testutil.ToFloat64(indexStorageBytes.WithLabelValues("test-src", "parquet")), 2000},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

// A zero OldestEvent/NewestEvent (empty index) must NOT publish a misleading
// 1970-epoch timestamp — the timestamp + retention gauges stay at their default.
func TestIndexMetricsSet_zeroTimestampsNotPublished(t *testing.T) {
	m := IndexForSource("empty-src")
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	m.Set(IndexSnapshot{Events: 0, ActivePartitions: 1}, now)

	if got := testutil.ToFloat64(indexOldestEvent.WithLabelValues("empty-src")); got != 0 {
		t.Errorf("oldest timestamp should be unpublished (0) for an empty index, got %v", got)
	}
	if got := testutil.ToFloat64(indexRetentionHorizon.WithLabelValues("empty-src")); got != 0 {
		t.Errorf("retention horizon should be unpublished (0) for an empty index, got %v", got)
	}
	// Aggregates without a time dimension are still published.
	if got := testutil.ToFloat64(indexPartitions.WithLabelValues("empty-src", "active")); got != 1 {
		t.Errorf("active partitions = %v, want 1", got)
	}
}
