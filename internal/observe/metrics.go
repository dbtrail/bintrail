package observe

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Stream metrics — all in the bintrail_stream namespace.
// These are only meaningful for the long-running `bintrail stream` command.
var (
	// StreamEventsReceived counts every binlog row event received from the source.
	StreamEventsReceived = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "bintrail",
		Subsystem: "stream",
		Name:      "events_received_total",
		Help:      "Total number of binlog row events received from the replication stream.",
	})

	// StreamEventsIndexed counts events successfully written to the index database.
	StreamEventsIndexed = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "bintrail",
		Subsystem: "stream",
		Name:      "events_indexed_total",
		Help:      "Total number of binlog row events written to binlog_events.",
	})

	// StreamBatchFlushes counts batch INSERT operations.
	StreamBatchFlushes = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "bintrail",
		Subsystem: "stream",
		Name:      "batch_flushes_total",
		Help:      "Total number of batch INSERT operations executed.",
	})

	// StreamCheckpointSaves counts successful checkpoint writes.
	StreamCheckpointSaves = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "bintrail",
		Subsystem: "stream",
		Name:      "checkpoint_saves_total",
		Help:      "Total number of successful checkpoint saves to stream_state.",
	})

	// StreamLastEventTimestamp is a Unix timestamp of the last processed event.
	StreamLastEventTimestamp = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "bintrail",
		Subsystem: "stream",
		Name:      "last_event_timestamp_seconds",
		Help:      "Unix timestamp of the last binlog event processed.",
	})

	// StreamReplicationLag is the wall-clock age of the last processed event in seconds.
	StreamReplicationLag = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "bintrail",
		Subsystem: "stream",
		Name:      "replication_lag_seconds",
		Help:      "Seconds between now and the timestamp of the last processed binlog event.",
	})

	// StreamErrors counts errors by type.
	StreamErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bintrail",
		Subsystem: "stream",
		Name:      "errors_total",
		Help:      "Total number of errors encountered, partitioned by type.",
	}, []string{"type"})

	// StreamBatchSize observes the number of events in each batch flush.
	StreamBatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "bintrail",
		Subsystem: "stream",
		Name:      "batch_size",
		Help:      "Distribution of batch sizes (events per INSERT batch).",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 11), // 1, 2, 4, ..., 1024
	})
)
