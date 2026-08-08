package console

import "github.com/dbtrail/dbtrail/internal/query"

// coverageWarnings extends gapWarnings with the two allow_gaps blind spots
// (#1281) — conditions where partial state would otherwise render with no
// indication at all, which matters here because a console user sees only the
// response, never the server log:
//
//   - archive sources that FAILED and were skipped (including the
//     discovery-failure sentinel): the planner will normally have counted
//     their hours as covered (its archive_state read is best-effort, so not
//     guaranteed — the failure direction is a double warning, never a
//     missed one), keeping them out of GapHours;
//   - a nil plan under allow_gaps: coverage was not evaluated. Registry
//     bundles guarantee a database name (buildBundle rejects DSNs without
//     one) and this handler always sets Since/Until, so nil means the
//     planner errored — and on any exotic path where the planner simply
//     could not run, coverage is equally unverified, so the warning still
//     tells the truth.
//
// Planner-detected GapHours keep flowing through gapWarnings unchanged. The
// per-source wording follows query.warnSkippedSources' semantics: each source
// is a distinct bintrail_id whose deltas no other source carries, so its
// events ARE missing, not "may be".
func coverageWarnings(plan *query.QueryPlan, skippedSources []string, allowGaps bool) []string {
	w := gapWarnings(plan)
	for _, s := range skippedSources {
		if s == query.DiscoveryFailedSource {
			w = append(w, "archive source discovery failed — no archives were read, and archived hours may still be counted as covered; the result may be incomplete")
			continue
		}
		w = append(w, "archive source failed and was skipped — events held only by this source are missing from the result: "+s)
	}
	if allowGaps && plan == nil {
		w = append(w, "coverage could not be verified (query planner unavailable): gaps in the captured history may be undetected")
	}
	return w
}
