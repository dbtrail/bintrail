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

// restrictedFetchWarnings is gapWarnings for a fetch that may have excluded the
// archives (#1311). It closes two holes the plan alone cannot:
//
//  1. NO PLAN, NO WARNING. The planner only runs with a time range, so the
//     default browse — newest N events, no since/until — produced no plan and
//     therefore said nothing at all, even though the session was reading half
//     the index. The exclusion notice does not depend on the plan, so it is
//     emitted first and unconditionally.
//
//  2. THE PLAN MISATTRIBUTES. Under an exclusion, Plan deliberately classifies
//     archived-only hours as gaps (they will not be fetched), and
//     FormatGapWarning explains gaps as "rotated and not archived". For this
//     reader that is the wrong cause: the hours very likely ARE archived, and
//     the operator gets sent to audit a rotation that is working. The hours are
//     still worth naming, so they are reported with the cause left open.
func restrictedFetchWarnings(plan *query.QueryPlan, excl archiveExclusion) []string {
	var out []string
	// The two exclusions are announced ASYMMETRICALLY, on purpose.
	//
	// A session data profile is invisible to the person reading the screen:
	// they did not set it, the UI does not show it, and nothing else in the
	// response hints that half the index is out of scope. That one is always
	// announced.
	//
	// A console started with --no-archive is a property of the whole
	// deployment. Announcing it on every response would put a permanent
	// banner on every page of that console — and a banner that is always
	// there is read by nobody, including on the day it matters. It is
	// announced only when the plan actually found hours this read could not
	// see, which is when it stops being configuration and becomes an
	// incomplete answer.
	gaps := plan != nil && len(plan.GapHours) > 0
	if excl == archivesExcludedByProfile || (excl == archivesExcludedByServer && gaps) {
		if n := excl.notice(); n != "" {
			out = append(out, n)
		}
	}
	if !gaps {
		return out
	}
	if excl == archivesRead {
		return append(out, query.FormatGapWarning(plan.GapHours))
	}
	first, last := query.GapRange(plan.GapHours)
	return append(out, "Hours in this window returned no live-index data: "+first+" – "+last+
		". They are outside what this session reads, so this is NOT a finding that the changes are "+
		"missing — they may be sitting in archive storage.")
}
