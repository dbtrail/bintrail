package observe

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Stream metrics — all in the bintrail_stream namespace.
//
// Every metric carries a "source" label so N concurrent streams in one
// process (the `up --console` control plane) stay distinguishable: without
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
