package console

import (
	"fmt"

	"github.com/dbtrail/dbtrail/internal/query"
)

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

// divergenceWarning renders the merge-layer divergence finding (#1325) for the
// events and recover responses: two copies of one event_id — live index vs an
// archived partition — disagreed, and one was silently chosen. The merge layer
// already slog.Warns with per-event detail; this is the response-level echo for
// an operator who is in a browser, not in the daemon log. It matters most on
// the recover path, where the chosen copy's row images become the generated
// reversal SQL. The wording claims no winner: which copy is kept is a
// positional convention of the caller, not a contract of the merge (see the
// comment inside query.MergeResultsReport).
func divergenceWarning(n int) string {
	return fmt.Sprintf("%d duplicate event(s) disagreed between the live index and an archive copy; the first copy fetched (normally the live index) was used. An archived partition should be a byte-for-byte copy of the index rows — a mismatch means the index row changed after archiving, or two index generations wrote under the same bintrail_id. Details are in the server log.", n)
}

// appendDivergenceWarning appends divergenceWarning when the merge reported
// any diverging duplicates, else returns w unchanged.
func appendDivergenceWarning(w []string, diverged int) []string {
	if diverged > 0 {
		w = append(w, divergenceWarning(diverged))
	}
	return w
}

// archiveElisionNotice is the response-level record of the newest-first
// short-circuit (#1353): registered archives were deliberately not read
// because they provably could not change this page — every archived hour sits
// below the live rotation floor, and the live index filled the requested page
// with events newer than that floor. Unlike archiveExclusion's notices this
// describes a CORRECTNESS-PRESERVING optimization, not a scope reduction, and
// the wording must carry that distinction: the #1311/#1321 contract is that a
// result says what scope was read, and "we skipped the archives because they
// could not matter" is a different fact from "we skipped the archives and your
// result may be incomplete". It still must be said — silence here would make
// the fast path indistinguishable from a fetch that never knew archives
// existed.
func archiveElisionNotice() string {
	return "Archived (rotated) hours were not searched for this page because they could not change it: " +
		"the live index filled the page with the newest matching events, and every archived hour is older " +
		"than what the live index still holds. Nothing is missing from this page. Paging further back, or " +
		"filtering to a time range that reaches archived hours, does search the archives."
}

// appendArchiveElisionNotice appends archiveElisionNotice when the fetch
// reported the newest-first short-circuit, else returns w unchanged.
func appendArchiveElisionNotice(w []string, archivesElided bool) []string {
	if archivesElided {
		w = append(w, archiveElisionNotice())
	}
	return w
}

// restrictedFetchWarnings is gapWarnings for a fetch that may have excluded the
// archives (#1311). It closes two holes the plan alone cannot:
//
//  1. NO PLAN, NO WARNING. The planner only runs with a time range, so the
//     default browse — newest N events, no since/until — produced no plan and
//     therefore said nothing at all, even though the session was reading half
//     the index. A session profile is therefore announced without consulting
//     the plan at all (see archiveExclusion.announce).
//
//  2. THE PLAN MISATTRIBUTES. Under an exclusion, Plan deliberately classifies
//     archived-only hours as gaps (they will not be fetched), and
//     FormatGapWarning explains gaps as "rotated and not archived". For this
//     reader that is the wrong cause: the hours very likely ARE archived, and
//     the operator gets sent to audit a rotation that is working. The hours are
//     still worth naming, so they are reported with the cause left open.
//
// The notice is emitted FIRST here, but callers may prepend their own — the
// recover handler puts cascade caveats ahead of it — so "first" is this
// function's contract, not the response's.
func restrictedFetchWarnings(plan *query.QueryPlan, excl archiveExclusion) []string {
	var out []string
	gaps := plan != nil && len(plan.GapHours) > 0
	if excl.announce(gaps) {
		out = append(out, excl.notice())
	}
	if !gaps {
		return out
	}
	if !excl.any() {
		return append(out, query.FormatGapWarning(plan.GapHours))
	}
	first, last := query.GapRange(plan.GapHours)
	return append(out, "Hours in this window returned no live-index data: "+first+" – "+last+
		". They are outside what this session reads, so this is NOT a finding that the changes are "+
		"missing — they may be sitting in archive storage.")
}
