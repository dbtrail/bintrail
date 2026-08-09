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
// holes. The DELTA half is metadata-only (timestamps and verdicts, no row
// data) and carries no profile gate, like /api/status. The FULL-TABLE half
// is derived from the baseline listing — the surface /api/baselines refuses
// to a profiled session (#1075: baseline reads aren't redacted, and
// broken_tables is a table-name inventory) — so it is gated by the SAME
// sessionRestricted rule. Capture-health drops (#1034) are deliberately not
// consulted — they have their own surface (status, --fail-on-gap).
type coverageResponse struct {
	// Delta window: any row/point in [delta_from, delta_to] is recoverable
	// from indexed deltas. delta_from omitted = unknown floor (never
	// assumed); delta_to omitted = empty index.
	DeltaFrom string `json:"delta_from,omitempty"`
	DeltaTo   string `json:"delta_to,omitempty"`
	// LagSeconds = now − delta_to, present only when a capture stream exists
	// AND at least one event is indexed. The window's upper edge is the last
	// INDEXED event, never the wall clock — the lag is what says how close
	// to "now" that edge is.
	LagSeconds *int64 `json:"lag_seconds,omitempty"`
	// Continuity: ok | gap_lost | unknown | unavailable | none — the exact
	// status.ContinuityStatus rule, never recomputed here.
	Continuity string `json:"continuity"`
	// Freshness: current | idle | stalled | unknown | unavailable | none — the
	// exact status.FreshnessStatus rule, never recomputed here either (#1227).
	// It is the LIVENESS half continuity is not, and it is what makes
	// lag_seconds readable: the same number means a dead daemon under
	// "stalled" and a quiet source under "idle". Note the offline limit
	// FreshnessStatus documents — "idle" cannot separate a quiet source from
	// one whose capture is far behind, and the card must not imply either.
	Freshness string `json:"freshness"`
	// CheckpointAgeSeconds is how long ago the daemon last wrote stream_state;
	// omitted (never 0) when there is no checkpoint to age. Under "stalled"
	// this is the number that says how long it has been down.
	CheckpointAgeSeconds *int64 `json:"checkpoint_age_seconds,omitempty"`
	// BaselineConfigured mirrors /api/baselines' "configured" — a baseline
	// SOURCE exists. It is NOT the reconstruct gate (/api/capabilities), the
	// same configured-vs-reconstruct split baselines_api documents.
	BaselineConfigured bool `json:"baseline_configured"`
	// FullTableStatus discriminates the full-table half when a source is
	// configured: "ok" = evaluated (fields below are meaningful, possibly
	// empty because no usable baseline exists yet), "unknown" = could NOT be
	// evaluated (unknown floor or a failed listing) — absent fields must
	// never read as "nothing broken". Omitted when no source is configured
	// or the session is profile-restricted.
	FullTableStatus string `json:"full_table_status,omitempty"`
	// FullTableFrom: from this instant onwards every table WITH A USABLE
	// baseline is fully reconstructable (the latest usable newest-per-table
	// anchor). Tables in broken_tables are excluded — they are not
	// reconstructable at all; never-baselined tables are invisible here.
	// Omitted when full_table_status != "ok" or no usable baseline exists.
	FullTableFrom string `json:"full_table_from,omitempty"`
	// BrokenTables: tables whose NEWEST baseline predates the delta floor —
	// full-table restore through that hole is impossible (#1193's verdict).
	BrokenTables []string `json:"broken_tables,omitempty"`
}

// tableAnchors holds the two baseline anchors of one table, which answer two
// different questions and must not be conflated.
//
// newest decides whether the table is BROKEN: if even its most recent baseline
// predates the delta floor, no point in time is fully reconstructable for it.
//
// earliestUsable decides where the table's restorable window STARTS.
// reconstruct.FindBaseline does not use a table's newest baseline — it picks
// the newest snapshot AT OR BEFORE the requested instant — so an instant older
// than the newest baseline is served from an older one, provided that one's
// own anchor is inside delta coverage. Taking the newest here understated the
// window by however long ago the last baseline ran, and got worse the more
// diligently an operator snapshotted: every new baseline pushed the reported
// floor forward (#1294).
type tableAnchors struct {
	newest         time.Time
	earliestUsable time.Time
}

// observe folds one baseline file's anchor in, with the verdict already graded
// against the delta floor. Usable means at or above the floor — ok and aging
// both qualify; aging says the window is shrinking, not that it is gone.
func (a *tableAnchors) observe(ts time.Time, v status.BaselineStalenessVerdict) {
	if ts.After(a.newest) {
		a.newest = ts
	}
	switch v {
	case status.BaselineOK, status.BaselineAging:
		if a.earliestUsable.IsZero() || ts.Before(a.earliestUsable) {
			a.earliestUsable = ts
		}
	}
}

// serverID labels log lines with the request's selected server — a
// multi-server console emitting unattributed warns is undebuggable.
func serverID(r *http.Request) string {
	if id := r.Header.Get(serverHeader); id != "" {
		return id
	}
	return "default"
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
		slog.Warn("console: coverage not evaluated — the server's index connection is not open", "server", serverID(r))
		resp.Continuity = "unavailable"
		resp.Freshness = status.FreshnessUnavailable
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
	resp.Freshness = sum.Freshness
	resp.CheckpointAgeSeconds = sum.CheckpointAgeSeconds
	resp.LagSeconds = sum.LagSeconds
	if !sum.Floor.Hour.IsZero() {
		resp.DeltaFrom = sum.Floor.Hour.Format(consoleTSFormat)
	}
	if !sum.DeltaTo.IsZero() {
		resp.DeltaTo = sum.DeltaTo.Format(consoleTSFormat)
	}
	// Full-table half: newest-per-table baseline anchors graded against the
	// floor — #1193's verdict via the status package, no second
	// implementation. Gated exactly like /api/baselines (#1075): the listing
	// this is derived from bypasses redaction, and broken_tables is a
	// table-name inventory a deny-profile withholds elsewhere. A failed
	// listing or an unknown floor is "unknown", never a silently-empty "ok"
	// — a broken-table warning must not vanish because of an error.
	switch {
	case b.baselineSrc == "":
	case sessionRestricted(r):
		recordProfileGateDeny(r, "coverage")
	case sum.Floor.Hour.IsZero():
		resp.FullTableStatus = "unknown"
	default:
		files, err := reconstruct.ListBaselines(r.Context(), b.baselineSrc)
		if err != nil {
			slog.Warn("console: coverage card could not list baselines", "server", serverID(r), "source", b.baselineSrc, "error", err)
			resp.FullTableStatus = "unknown"
			break
		}
		resp.FullTableStatus = "ok"
		anchors := make(map[string]*tableAnchors, len(files))
		for _, f := range files {
			k := f.Schema + "." + f.Table
			a := anchors[k]
			if a == nil {
				a = &tableAnchors{}
				anchors[k] = a
			}
			a.observe(f.SnapshotTime, sum.Floor.Grade(f.SnapshotTime, now))
		}
		// Graded THROUGH the floor, never against its hour alone: on a
		// multi-source index the hour is the live floor and an anchor below
		// it is unattributable, not broken (#1219). Grading with the bare
		// hour would name healthy tables in broken_tables — the false alarm
		// the floor's own narrowing exists to avoid.
		var from time.Time
		var unevaluable bool
		for k, a := range anchors {
			if !a.earliestUsable.IsZero() {
				// The window covering ALL usable tables starts at the LATEST
				// of their individual starts — and a table's own start is its
				// EARLIEST usable anchor, not its newest (see tableAnchors).
				if a.earliestUsable.After(from) {
					from = a.earliestUsable
				}
				continue
			}
			// No usable anchor at all: the newest one says which kind of
			// nothing this is.
			if sum.Floor.Grade(a.newest, now) == status.BaselineBroken {
				resp.BrokenTables = append(resp.BrokenTables, k)
				continue
			}
			// Neither broken nor usable: it must not join broken_tables AND
			// must not define the window, or "ok" would assert restorability
			// from an anchor whose coverage is unknown.
			unevaluable = true
		}
		sort.Strings(resp.BrokenTables)
		if unevaluable {
			resp.FullTableStatus = "unknown"
			break
		}
		if !from.IsZero() {
			resp.FullTableFrom = from.Format(consoleTSFormat)
		}
	}
	writeJSON(w, http.StatusOK, resp)
}
