package console

import (
	"log/slog"
	"net/http"
	"sort"
	"time"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/status"
)

// coverageResponse is GET /api/coverage — the live RPO statement behind the
// overview card (#1194): "any point between delta_from and delta_to is
// restorable", plus how far behind capture is and whether the range has
// holes. Metadata-only (timestamps and verdicts, no row data), so like
// /api/status it carries no profile gate — unlike /api/baselines, nothing
// here bypasses redaction.
type coverageResponse struct {
	// Delta window: any row/point in [delta_from, delta_to] is recoverable
	// from indexed deltas. delta_from omitted = unknown floor (never
	// assumed); delta_to omitted = empty index.
	DeltaFrom string `json:"delta_from,omitempty"`
	DeltaTo   string `json:"delta_to,omitempty"`
	// LagSeconds = now − delta_to, present only when a capture stream exists.
	// The window's upper edge is the last INDEXED event, never the wall
	// clock — the lag is what says how close to "now" that edge is.
	LagSeconds *int64 `json:"lag_seconds,omitempty"`
	// Continuity: ok | gap_lost | unknown | unavailable | none — the exact
	// status.ContinuityStatus rule, never recomputed here.
	Continuity         string `json:"continuity"`
	BaselineConfigured bool   `json:"baseline_configured"`
	// FullTableFrom: from this instant onwards EVERY table with a baseline
	// is fully reconstructable (the latest usable newest-per-table anchor).
	// Omitted when no baseline is configured, the floor is unknown, or no
	// table has a usable newest baseline.
	FullTableFrom string `json:"full_table_from,omitempty"`
	// BrokenTables: tables whose NEWEST baseline predates the delta floor —
	// full-table restore through that hole is impossible (#1193's verdict).
	BrokenTables []string `json:"broken_tables,omitempty"`
}

func (s *Server) handleCoverage(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	resp := coverageResponse{BaselineConfigured: b.baselineSrc != ""}
	if b.db == nil {
		// An unopened bundle connection degrades to an explicit
		// "unavailable" card, never a fabricated window.
		slog.Warn("console: coverage not evaluated — the server's index connection is not open")
		resp.Continuity = "unavailable"
		writeJSON(w, http.StatusOK, resp)
		return
	}
	now := time.Now().UTC()
	sum, err := status.CollectCoverageSummary(r.Context(), b.db, b.dbName, now)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	resp.Continuity = sum.Continuity
	resp.LagSeconds = sum.LagSeconds
	if !sum.DeltaFrom.IsZero() {
		resp.DeltaFrom = sum.DeltaFrom.Format(consoleTSFormat)
	}
	if !sum.DeltaTo.IsZero() {
		resp.DeltaTo = sum.DeltaTo.Format(consoleTSFormat)
	}
	// Full-table half: newest-per-table baseline anchors graded against the
	// floor — #1193's verdict via the status package, no second
	// implementation. Best-effort: a listing failure degrades to the delta
	// half, loudly. Skipped whole on an unknown floor — grading anchors
	// against an unknown floor can neither claim a window nor name a broken
	// table.
	if b.baselineSrc != "" && !sum.DeltaFrom.IsZero() {
		files, err := reconstruct.ListBaselines(r.Context(), b.baselineSrc)
		if err != nil {
			slog.Warn("console: coverage card could not list baselines", "source", b.baselineSrc, "error", err)
		} else {
			newest := make(map[string]time.Time, len(files))
			for _, f := range files {
				k := f.Schema + "." + f.Table
				if f.SnapshotTime.After(newest[k]) {
					newest[k] = f.SnapshotTime
				}
			}
			var from time.Time
			for k, ts := range newest {
				if status.BaselineStalenessFor(ts, sum.DeltaFrom, now) == status.BaselineBroken {
					resp.BrokenTables = append(resp.BrokenTables, k)
					continue
				}
				// The window covering ALL tables starts at the LATEST usable
				// anchor: table i is reconstructable for points >= its own
				// anchor, so "every table" holds only past the newest one.
				if ts.After(from) {
					from = ts
				}
			}
			sort.Strings(resp.BrokenTables)
			if !from.IsZero() {
				resp.FullTableFrom = from.Format(consoleTSFormat)
			}
		}
	}
	writeJSON(w, http.StatusOK, resp)
}
