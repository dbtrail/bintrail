package observe

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Watch-daemon health gauges (#1203) — the pull-based siblings of the webhook
// notification channel (#1192): the same three "your safety net has a hole"
// conditions, exported for Prometheus/Alertmanager.
//
// All are *Vec collectors — including the label-less rotation pair — so
// nothing is published until the daemon actually evaluates the condition:
// an absent series means "not evaluated", never a misleading healthy zero
// (the same rule the index timestamp gauges follow for an empty index).
// The server label is the display name — the same identity the webhook's
// `server` field and the console UI use.
var (
	continuityGapLost = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail",
		Subsystem: "continuity",
		Name:      "gap_lost",
		Help:      "1 when the stream stamped a permanent capture gap (events in it are unrecoverable); 0 when continuity is intact. Absent while the index cannot be evaluated.",
	}, []string{"server"})

	verifyLastRun = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail",
		Subsystem: "verify",
		Name:      "last_run_timestamp_seconds",
		Help:      "Unix time of the newest finished verify run (manual or scheduled) for this server.",
	}, []string{"server"})

	verifyTables = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail",
		Subsystem: "verify",
		Name:      "tables",
		Help:      "Per-status table counts of the newest finished verify run.",
	}, []string{"server", "status"})

	rotationHealthy = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail",
		Subsystem: "rotation",
		Name:      "healthy",
		Help:      "1 when the last built-in rotation cycle neither failed nor deferred unarchived partitions; 0 otherwise.",
	}, nil)

	rotationDeferred = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail",
		Subsystem: "rotation",
		Name:      "deferred_partitions",
		Help:      "Unarchived partitions the last built-in rotation cycle declined to drop.",
	}, nil)
)

// SetContinuityGapLost publishes one index's continuity verdict.
func SetContinuityGapLost(server string, lost bool) {
	continuityGapLost.WithLabelValues(server).Set(boolGauge(lost))
}

// ClearContinuity unpublishes a server's continuity gauge — the unknowable
// case (unreachable index, legacy schema without the gap columns). Unknown is
// never "no gap", so it must never read as 0.
func ClearContinuity(server string) {
	continuityGapLost.DeleteLabelValues(server)
}

// SetVerifyOutcome publishes the newest finished verify run for a server.
func SetVerifyOutcome(server string, finishedAt time.Time, match, mismatch, inconclusive, errorCount int) {
	verifyLastRun.WithLabelValues(server).Set(float64(finishedAt.Unix()))
	verifyTables.WithLabelValues(server, "match").Set(float64(match))
	verifyTables.WithLabelValues(server, "mismatch").Set(float64(mismatch))
	verifyTables.WithLabelValues(server, "inconclusive").Set(float64(inconclusive))
	verifyTables.WithLabelValues(server, "error").Set(float64(errorCount))
}

// SetRotationHealth publishes the last rotation cycle's health — the same
// (failed, deferred) pair rotation.StartLoop's onCycle callbacks observe.
func SetRotationHealth(failed bool, deferred int) {
	rotationHealthy.WithLabelValues().Set(boolGauge(!failed && deferred == 0))
	rotationDeferred.WithLabelValues().Set(float64(deferred))
}

func boolGauge(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
