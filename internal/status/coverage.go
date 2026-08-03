package status

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"
)

// ContinuityStatus is the ONE continuity-verdict rule — the status JSON and
// GET /api/coverage (#1194) both call it, so the two surfaces can never
// disagree on what a gap state means (the Report.ExitError() discipline).
//
//	"ok"          — no gap in the captured range (NOT a liveness assertion)
//	"gap_lost"    — an unfillable gap was stamped: events permanently lost
//	"unknown"     — legacy index without the gap columns; never a false "ok"
//	"unavailable" — stream_state could not be read; never a false "ok"
//	"none"        — no stream row (file-mode index): no capture ran, so no
//	                continuity could break — a genuine no-claim, not a hole
func ContinuityStatus(stream *StreamStateInfo, streamErr error) string {
	switch {
	case stream == nil && streamErr != nil:
		return "unavailable"
	case stream == nil:
		return "none"
	case stream.GapLostAt.Valid:
		return "gap_lost"
	case !stream.GapColumnsPresent:
		return "unknown"
	default:
		return "ok"
	}
}

// CoverageSummary is the lean live-RPO view behind the console's coverage
// card (#1194): the reconstructable delta window, capture lag, and the
// continuity verdict. Deliberately CHEAP — no COUNT(*), no index-size scan —
// it loads on every server switch; CollectStatus stays the full report.
type CoverageSummary struct {
	// DeltaFrom is the delta-coverage floor (OldestDeltaFromDB — the #1213
	// strict rule). Zero = unknown, never assumed.
	DeltaFrom time.Time
	// DeltaTo is the newest INDEXED event — never the wall clock: claiming
	// restorability "up to now" while the stream is down would be unearned
	// assurance. LagSeconds is what says how close to now the edge is.
	DeltaTo time.Time
	// LagSeconds = now − DeltaTo, present only when a stream row exists — a
	// file-mode index has no liveness to measure.
	LagSeconds *int64
	Continuity string
}

// CollectCoverageSummary computes the summary against one index. The floor
// degrades to unknown on error (warn-and-degrade, CollectStatus's stance);
// a failure to read the newest event is fatal — without the window's upper
// edge there is nothing to state.
func CollectCoverageSummary(ctx context.Context, db *sql.DB, dbName string, now time.Time) (*CoverageSummary, error) {
	sum := &CoverageSummary{}
	if floor, err := OldestDeltaFromDB(ctx, db, dbName); err != nil {
		slog.Warn("could not determine the delta-coverage floor; coverage window start is unknown", "error", err)
	} else {
		sum.DeltaFrom = floor
	}
	var latest sql.NullTime
	if err := db.QueryRowContext(ctx, `SELECT MAX(event_timestamp) FROM binlog_events`).Scan(&latest); err != nil {
		return nil, fmt.Errorf("read newest indexed event: %w", err)
	}
	if latest.Valid {
		sum.DeltaTo = latest.Time
	}
	stream, streamErr := LoadStreamState(ctx, db)
	if streamErr != nil {
		slog.Warn("could not load stream state for the coverage summary", "error", streamErr)
	}
	sum.Continuity = ContinuityStatus(stream, streamErr)
	if stream != nil && !sum.DeltaTo.IsZero() {
		lag := int64(now.Sub(sum.DeltaTo) / time.Second)
		if lag < 0 {
			lag = 0
		}
		sum.LagSeconds = &lag
	}
	return sum, nil
}
