package console

import "github.com/dbtrail/dbtrail/internal/query"

// coverageWarnings extends gapWarnings with the two allow_gaps blind spots
// (#1281) — conditions where partial state would otherwise render with no
// indication at all, which matters here because a console user sees only the
// response, never the server log:
//
//   - archive sources that FAILED and were skipped: the planner counted their
//     hours as covered, so they are absent from GapHours too;
//   - a nil plan under allow_gaps: the planner errored, so coverage could not
//     be evaluated at all. In the reconstruct handler Since/Until are always
//     set, so nil never means the benign "planner didn't run" case.
//
// Planner-detected GapHours keep flowing through gapWarnings unchanged.
func coverageWarnings(plan *query.QueryPlan, skippedSources []string, allowGaps bool) []string {
	w := gapWarnings(plan)
	for _, s := range skippedSources {
		w = append(w, "archive source failed and was skipped — the result may be missing its events: "+s)
	}
	if allowGaps && plan == nil {
		w = append(w, "coverage could not be verified (query planner unavailable): gaps in the captured history may be undetected")
	}
	return w
}
