package reconstruct

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/dbtrail/dbtrail/internal/status"
)

// gapInWindow reports whether a recorded capture-gap timestamp falls inside
// (since, until] — the reconstruction window a baseline+delta replay covers.
// Extracted so the boundary logic is unit-testable without a *sql.DB.
func gapInWindow(gapAt, since, until time.Time) bool {
	return gapAt.After(since) && !gapAt.After(until)
}

// CheckCaptureGap consults stream_state.gap_lost_at/gap_lost_detail (#765) —
// the durable record indexer/streamrun stamps when an unfillable binlog gap
// forces an auto-advance, permanently losing events (docs/rotation-and-status.md:
// "the index is valid only up to the gap"). This is a DIFFERENT check from
// GapDetected in gap.go (baseline-anchor-vs-first-event position gap) and from
// the query planner's coverage-gap check (query.GapError: an hour rotated out
// of MySQL with no archive, which is potentially fillable by re-archiving).
// gap_lost_at marks events that no longer exist ANYWHERE — not live MySQL, not
// an archive — so a reconstruction spanning it is silently incomplete
// regardless of archive coverage.
//
// If gap_lost_at falls inside (since, until], the loss is in scope for this
// reconstruction. Under strict mode (allowGaps=false, the reconstruct default)
// this returns an error; under --allow-gaps it logs a slog.Warn and returns
// nil so the caller proceeds with a known-incomplete result.
func CheckCaptureGap(ctx context.Context, db *sql.DB, schema, table string, since, until time.Time, allowGaps bool) error {
	ss, err := status.LoadStreamState(ctx, db)
	if err != nil {
		return fmt.Errorf("check stream_state capture gap: %w", err)
	}
	if ss == nil || !ss.GapLostAt.Valid {
		return nil
	}
	gapAt := ss.GapLostAt.Time
	if !gapInWindow(gapAt, since, until) {
		return nil
	}

	detail := ss.GapLostDetail.String
	if allowGaps {
		slog.Warn("reconstruct: stamped capture gap falls inside the reconstruction window — events after the gap are permanently lost, output will be incomplete; proceeding due to --allow-gaps",
			"schema", schema, "table", table,
			"gap_lost_at", gapAt.UTC().Format(time.RFC3339), "detail", detail,
			"window_since", since.UTC().Format(time.RFC3339), "window_until", until.UTC().Format(time.RFC3339))
		return nil
	}
	return fmt.Errorf("reconstruct: stamped capture gap at %s falls inside the reconstruction window (%s, %s] for %s.%s — the index permanently lost events after that point (%s); pass --allow-gaps to proceed with a known-incomplete reconstruction",
		gapAt.UTC().Format(time.RFC3339), since.UTC().Format(time.RFC3339), until.UTC().Format(time.RFC3339), schema, table, detail)
}
