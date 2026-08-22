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

// archiveElisionNote is the response-level record of the newest-first
// short-circuit (#1353): registered archives were deliberately not read
// because they provably could not change this page. FOUR proofs reach here —
// the live index filled a newest-first page from a contiguous live range, every
// named PK already held its latest N live (#1403), the one event the request
// anchored on came back live (#1411), or a Since bound sat inside contiguous
// live coverage (#1414) — and the flag does not say which. The remedies below
// were re-read against the fourth proof: "widening the time range" is exactly
// its live remedy (a wider window reaches under the live floor and the proof
// declines), so the wording needs no per-proof branching.
//
// That is why the wording below states the FACT and not the reason, which it
// used to. Inferring the reason from the request is wrong: the newest-first
// proof is tried first and can fire on a request that also carries a per-row
// limit. The old text asserted the window reason ("every archived event is
// older than this page reaches") and offered a remedy that is inert under the
// per-PK proof, which never reads the limit at all. An audit note naming the
// wrong reason is worse than one naming none. Unlike archiveExclusion's notices this
// describes a CORRECTNESS-PRESERVING optimization, not a scope reduction, and
// since #1365 that distinction is carried on the wire: this is a NOTE (info),
// never a warning — a benign audit fact rendered in one alarm register with
// the coverage-gap and exclusion warnings read as an incident while its own
// text says nothing is missing. It still must be said — silence here would
// make the fast path indistinguishable from a fetch that never knew archives
// existed (the #1311/#1321 contract: a result says what scope was read). The
// wording is plain words on purpose, and flag-free: API and MCP-side clients
// read this string too.
func archiveElisionNote() string {
	return "This page was answered from the live index; the registered archives were not read. " +
		"Nothing they hold could have survived this request's filters, so nothing is missing here. " +
		"Widening the time range, clearing a per-row limit, or clearing a single-event " +
		"selection, reads them."
}

// The third remedy is not padding. anchorSatisfiedLive (#1411) is the
// short-circuit that fires on an ordinary Undo, and neither of the first two
// reaches it: the Undo prefill no longer sets a per-row limit, and widening the
// time range cannot make a one-event membership filter admit anything else. The
// only way to the archives from there is the banner's Clear, which is what
// "clearing a single-event selection" names. Review found the note listing two
// remedies that both did nothing on the path most likely to show it.

// recoverArchiveElisionNote is archiveElisionNote in the recover surface's own
// words: a reversal request has a window and a limit, not pages, so "this
// page" and "paging further back" would be nonsense to the operator reviewing
// an undo script. Same fact, same severity, surface-appropriate wording —
// both strings are pinned verbatim by TestArchiveElisionNote.
func recoverArchiveElisionNote() string {
	return "This reversal was built from the live index; the registered archives were not read. " +
		"Nothing they hold could have survived this request's filters, so nothing is missing here. " +
		"Widening the time range, clearing the per-row limit, or clearing a single-event " +
		"selection, reads them."
}

// reconstructArchiveElisionNote is the elision record in the reconstruct
// surface's own words (#1414): a state computation has an anchor and a
// window, not pages or per-row limits, so the events/recover remedies would
// be nonsense here — and no remedy is needed, since the skip is
// completeness-preserving by proof.
func reconstructArchiveElisionNote() string {
	// The FACT, not the reason: the flag does not say which proof fired
	// (archiveElisionNote's doc establishes why inferring it is wrong), so
	// this wording must stay true under any future fifth proof.
	return "This state was computed from the live index; the registered archives were not read " +
		"because they provably could not change this result — nothing is missing here."
}

// archiveElisionNotes returns the response Notes list for a fetch that
// reported the newest-first short-circuit — nil otherwise. elisionNote is the
// surface's own wording (archiveElisionNote / recoverArchiveElisionNote). It
// feeds the `notes` (info) list, never `warnings`: see responseAdvisories.
func archiveElisionNotes(archivesElided bool, elisionNote string) []string {
	if !archivesElided {
		return nil
	}
	return []string{elisionNote}
}

// responseAdvisories assembles the two severity lists the events and recover
// responses carry (#1365):
//
//   - warnings — cautionary facts: coverage gaps, a session's archive
//     exclusion (#1311/#1321), merge divergence findings (#1325). Things an
//     operator may need to act on, rendered in the alert register.
//   - notes — benign audit facts: the #1353 archive-elision record, which is
//     correct-by-construction ("nothing is missing") and exists for
//     auditability, not attention. Rendered muted, never as an alert.
//
// The split lives HERE, on the wire, because the API and the console UI share
// the shape: classifying UI-side would leave every other client (curl, an
// agent) with one undifferentiated list. `notes` is additive — consumers that
// ignore it see the same warnings contract as before.
//
// The lists' exclusivity is enforced by CONVENTION, not by type: this
// function and liveScopeAdvisories (its scope=live sibling) are the only
// places the events and recover handlers may assemble
// advisory lists. A handler that hand-appends a fact to either list bypasses
// the split and re-creates the one-register bug — add facts here, where the
// severity decision is visible and unit-tested
// (TestResponseAdvisoriesSeveritySplit).
//
// elisionNote is the surface's own wording for the elision record; pass
// archiveElisionNote() (events) or recoverArchiveElisionNote() (recover).
func responseAdvisories(plan *query.QueryPlan, excl archiveExclusion, skipped []string, diverged int, archivesElided bool, elisionNote string) (warnings, notes []string) {
	warnings = appendDivergenceWarning(restrictedFetchWarnings(plan, excl), diverged)
	// The incompleteness inventory (#1414 review pass 2): a failed archive
	// source or a failed discovery under AllowGaps used to be log-only here,
	// which turned the scope=live phase-1 promise ("a full read will report
	// it") into a false claim the moment phase 2 swept the marker. Same
	// wording contract as reconstruct's coverageWarnings: a skipped source's
	// events ARE missing, not "may be".
	for _, src := range skipped {
		if src == query.DiscoveryFailedSource {
			warnings = append(warnings, "archive source discovery failed — no archives were read, and archived hours may still be counted as covered; the result may be incomplete")
			continue
		}
		warnings = append(warnings, "archive source failed and was skipped — events held only by this source are missing from the result: "+src)
	}
	notes = archiveElisionNotes(archivesElided, elisionNote)
	return warnings, notes
}

// liveScopeAdvisories assembles the advisory lists for a scope=live (phase 1)
// read (#1414) — the sibling of responseAdvisories, and bound by the same
// convention: handlers assemble advisory lists ONLY through these two.
//
// pending is the number of registered archive sources the live-only read did
// NOT consult; negative means discovery itself failed, so whether archived
// history exists is UNKNOWN. The severity split is the issue's own
// requirement: a phase-1 render is the OPPOSITE of the elision note's
// situation — the archives were not read and were not provably redundant — so
// partiality is a WARNING, louder than the elision's info note, and it must
// stay up until a full read actually lands.
//
// The plan is threaded through, and WHEN it speaks is the subtle half
// (review pass 1 caught the first cut passing nil unconditionally, which
// silenced the coverage story exactly on the three shapes that never get a
// phase 2):
//
//   - archives excluded (profile / --no-archive console): pending is 0, no
//     phase 2 runs, and the exclusion notice plus the REAL plan's gap lines
//     are this response's only chance to say what was not read.
//   - nothing registered (pending == 0): no phase 2 runs; a gap hour the
//     planner found is a fact about THIS answer, and "complete answer"
//     beside it would be false — the note softens to point at the gaps.
//   - a phase 2 IS coming (pending != 0): plan-derived gap lines are
//     deliberately withheld. Under NoArchive the planner classifies
//     archived-only hours as "rotated and not archived" — a false claim
//     about hours the full read is about to serve — and phase 2's own
//     advisories replace this list wholesale with the true classification.
func liveScopeAdvisories(plan *query.QueryPlan, excl archiveExclusion, pending int) (warnings, notes []string) {
	switch {
	case excl.any():
		// The archives are excluded for this session/console regardless of
		// scope; the exclusion machinery owns the whole story, gaps included
		// — and "partial pending a full read" would promise a phase 2 that
		// cannot read them either.
		warnings = restrictedFetchWarnings(plan, excl)
	case pending < 0:
		warnings = append(restrictedFetchWarnings(nil, excl), "Live-index scope (scope=live): archive "+
			"discovery failed, so whether archived history exists is unknown. A full read (without "+
			"scope=live) will report it.")
	case pending > 0:
		warnings = append(restrictedFetchWarnings(nil, excl), fmt.Sprintf("Live-index scope (scope=live): "+
			"%d registered archive source(s) were NOT read. This list is PARTIAL wherever the window "+
			"reaches into archived history; a full read (without scope=live) completes it.", pending))
	default:
		warnings = restrictedFetchWarnings(plan, excl)
		note := "Live-index scope (scope=live): no archive sources are registered, so nothing " +
			"further exists to read"
		if len(warnings) == 0 {
			note += " — this is already the complete answer."
		} else {
			note += "; the hours warned about above are gaps nothing recorded, not unread archives."
		}
		notes = append(notes, note)
	}
	return warnings, notes
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
