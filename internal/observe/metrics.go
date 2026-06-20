package observe

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Stream metrics — all in the bintrail_stream namespace.
//
// Every metric carries a "source" label so N concurrent streams in one
// process (the `bintrail-console watch` control plane) stay distinguishable: without
// it the counters of different sources conflate and the gauges clobber each
// other last-writer-wins. The label value is the supervisor's entry ID for
// monitored streams and the resolved bintrail_id (or "default") for a
// standalone `bintrail stream`. Call ForSource to obtain handles curried to
// one source; the underlying vectors are deliberately unexported.
var (
	streamEventsReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bintrail",
		Subsystem: "stream",
		Name:      "events_received_total",
		Help:      "Total number of binlog row events received from the replication stream.",
	}, []string{"source"})

	streamEventsIndexed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bintrail",
		Subsystem: "stream",
		Name:      "events_indexed_total",
		Help:      "Total number of binlog row events written to binlog_events.",
	}, []string{"source"})

	streamBatchFlushes = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bintrail",
		Subsystem: "stream",
		Name:      "batch_flushes_total",
		Help:      "Total number of batch INSERT operations executed.",
	}, []string{"source"})

	streamCheckpointSaves = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bintrail",
		Subsystem: "stream",
		Name:      "checkpoint_saves_total",
		Help:      "Total number of successful checkpoint saves to stream_state.",
	}, []string{"source"})

	streamLastEventTimestamp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail",
		Subsystem: "stream",
		Name:      "last_event_timestamp_seconds",
		Help:      "Unix timestamp of the last binlog event processed.",
	}, []string{"source"})

	streamReplicationLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail",
		Subsystem: "stream",
		Name:      "replication_lag_seconds",
		Help:      "Seconds between now and the timestamp of the last processed binlog event.",
	}, []string{"source"})

	streamErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bintrail",
		Subsystem: "stream",
		Name:      "errors_total",
		Help:      "Total number of errors encountered, partitioned by type.",
	}, []string{"source", "type"})

	streamBatchSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "bintrail",
		Subsystem: "stream",
		Name:      "batch_size",
		Help:      "Distribution of batch sizes (events per INSERT batch).",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 11), // 1, 2, 4, ..., 1024
	}, []string{"source"})
)

// StreamMetrics is the full set of stream metrics curried to one source
// label — call sites keep the ergonomics of plain counters/gauges.
type StreamMetrics struct {
	EventsReceived     prometheus.Counter
	EventsIndexed      prometheus.Counter
	BatchFlushes       prometheus.Counter
	CheckpointSaves    prometheus.Counter
	LastEventTimestamp prometheus.Gauge
	ReplicationLag     prometheus.Gauge
	// Errors keeps its remaining "type" dimension (batch_flush, checkpoint,
	// gtid_update).
	Errors *prometheus.CounterVec
	// BatchSize is the per-source histogram handle.
	BatchSize prometheus.Observer
}

// ForSource returns the stream metrics curried to the given source label.
// An empty source falls back to "default" so a standalone stream without a
// resolved identity still produces well-formed series.
func ForSource(source string) *StreamMetrics {
	if source == "" {
		source = "default"
	}
	return &StreamMetrics{
		EventsReceived:     streamEventsReceived.WithLabelValues(source),
		EventsIndexed:      streamEventsIndexed.WithLabelValues(source),
		BatchFlushes:       streamBatchFlushes.WithLabelValues(source),
		CheckpointSaves:    streamCheckpointSaves.WithLabelValues(source),
		LastEventTimestamp: streamLastEventTimestamp.WithLabelValues(source),
		ReplicationLag:     streamReplicationLag.WithLabelValues(source),
		Errors:             streamErrors.MustCurryWith(prometheus.Labels{"source": source}),
		BatchSize:          streamBatchSize.WithLabelValues(source),
	}
}

// Index metrics — the bintrail_index namespace (#351).
//
// Where the stream metrics describe the live replication pipeline, these
// describe the STATE of the index a DBA cares about for forensic readiness:
// how far back recovery reaches, whether there are coverage gaps, and how much
// disk the index + archives occupy. They are GAUGES set periodically by a
// scraper (see streamrun) from a status snapshot — there is no per-event hot
// path here. Every series carries "source" for the same multi-stream reason as
// the stream metrics.
//
// CARDINALITY: index_events_total is a single aggregate per source — NOT
// labelled per schema/table, which would grow unbounded as tables come and go.
var (
	indexOldestEvent = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail", Subsystem: "index", Name: "oldest_event_timestamp_seconds",
		Help: "Unix timestamp of the oldest event still in the index (recovery floor).",
	}, []string{"source"})

	indexNewestEvent = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail", Subsystem: "index", Name: "newest_event_timestamp_seconds",
		Help: "Unix timestamp of the newest event in the index.",
	}, []string{"source"})

	indexRetentionHorizon = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail", Subsystem: "index", Name: "retention_horizon_seconds",
		Help: "How far back the index reaches: now minus the oldest event timestamp.",
	}, []string{"source"})

	indexEventsTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail", Subsystem: "index", Name: "events_total",
		Help: "Approximate number of row events in the index (information_schema estimate).",
	}, []string{"source"})

	indexPartitions = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail", Subsystem: "index", Name: "partitions_total",
		Help: "Number of binlog_events partitions, by kind (active hourly vs the p_future catch-all).",
	}, []string{"source", "kind"})

	indexGapHours = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail", Subsystem: "index", Name: "gap_hours",
		Help: "Hours that were rotated out of MySQL but not archived to Parquet — a hole in recovery coverage.",
	}, []string{"source"})

	indexStorageBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail", Subsystem: "index", Name: "storage_bytes",
		Help: "Bytes occupied by the index, by location (mysql = binlog_events table, parquet = archived partitions).",
	}, []string{"source", "location"})
)

// IndexSnapshot is a flat, status-package-free view of the index state that the
// scraper passes to IndexMetrics.Set. Keeping it plain avoids an observe→status
// import (status already depends on lower layers; a cycle would be one edit
// away). Zero OldestEvent/NewestEvent mean "unknown" (empty index) and are not
// published, so the timestamp gauges never report a misleading 1970 epoch.
type IndexSnapshot struct {
	OldestEvent      time.Time
	NewestEvent      time.Time
	Events           int64
	ActivePartitions int
	FuturePartitions int
	GapHours         int
	MySQLBytes       int64
	ParquetBytes     int64
	// HaveCoverage reports whether the coverage load succeeded. Events and
	// MySQLBytes come from it; when it degraded (false) they are NOT published,
	// so a swallowed partial-load failure can't mask the real values with a 0 —
	// the same misleading-zero defense as the OldestEvent/NewestEvent guards.
	HaveCoverage bool
}

// IndexMetrics is the index gauge set curried to one source.
type IndexMetrics struct{ source string }

// IndexForSource returns an index-metrics handle for the given source label
// (empty → "default", matching ForSource).
func IndexForSource(source string) *IndexMetrics {
	if source == "" {
		source = "default"
	}
	return &IndexMetrics{source: source}
}

// Set publishes one snapshot to the gauges. now is taken explicitly so the
// retention-horizon computation is deterministic under test.
func (m *IndexMetrics) Set(snap IndexSnapshot, now time.Time) {
	s := m.source
	if !snap.OldestEvent.IsZero() {
		indexOldestEvent.WithLabelValues(s).Set(float64(snap.OldestEvent.Unix()))
		indexRetentionHorizon.WithLabelValues(s).Set(now.Sub(snap.OldestEvent).Seconds())
	}
	if !snap.NewestEvent.IsZero() {
		indexNewestEvent.WithLabelValues(s).Set(float64(snap.NewestEvent.Unix()))
	}
	// events_total and storage_bytes{mysql} are coverage-derived — skip them on a
	// degraded load so a swallowed failure can't publish a misleading 0.
	if snap.HaveCoverage {
		indexEventsTotal.WithLabelValues(s).Set(float64(snap.Events))
		indexStorageBytes.WithLabelValues(s, "mysql").Set(float64(snap.MySQLBytes))
	}
	indexPartitions.WithLabelValues(s, "active").Set(float64(snap.ActivePartitions))
	indexPartitions.WithLabelValues(s, "future").Set(float64(snap.FuturePartitions))
	indexGapHours.WithLabelValues(s).Set(float64(snap.GapHours))
	indexStorageBytes.WithLabelValues(s, "parquet").Set(float64(snap.ParquetBytes))
}
