package console

import (
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
)

// The two allow_gaps blind spots (#1281): each must produce a visible payload
// warning, and the quiet path must stay quiet.
func TestCoverageWarnings(t *testing.T) {
	t.Run("nil plan under allow_gaps → coverage-unverified warning", func(t *testing.T) {
		w := coverageWarnings(nil, nil, true)
		if len(w) != 1 || !strings.Contains(w[0], "coverage could not be verified") {
			t.Fatalf("want the unverified-coverage warning, got %v", w)
		}
	})

	t.Run("nil plan WITHOUT allow_gaps → no warning (strict mode already errored loud)", func(t *testing.T) {
		if w := coverageWarnings(nil, nil, false); len(w) != 0 {
			t.Fatalf("strict mode must not warn, got %v", w)
		}
	})

	t.Run("skipped sources → one warning each, naming the source", func(t *testing.T) {
		w := coverageWarnings(&query.QueryPlan{}, []string{"s3://bkt/a", "/var/archives"}, true)
		if len(w) != 2 || !strings.Contains(w[0], "s3://bkt/a") || !strings.Contains(w[1], "/var/archives") {
			t.Fatalf("want one warning per skipped source, got %v", w)
		}
	})

	t.Run("discovery-failure sentinel → its own warning, not the per-source one", func(t *testing.T) {
		w := coverageWarnings(&query.QueryPlan{}, []string{query.DiscoveryFailedSource}, true)
		if len(w) != 1 || !strings.Contains(w[0], "discovery failed") {
			t.Fatalf("want the discovery-failure warning, got %v", w)
		}
	})

	t.Run("healthy plan, nothing skipped → quiet", func(t *testing.T) {
		if w := coverageWarnings(&query.QueryPlan{}, nil, true); len(w) != 0 {
			t.Fatalf("healthy path must stay quiet, got %v", w)
		}
	})
}

// #1325: the merge divergence finding must render once, count-first, and the
// zero case must append nothing (cry-wolf rule — the normal
// archived-but-not-dropped overlap produces agreeing duplicates, count 0).
func TestAppendDivergenceWarning(t *testing.T) {
	if w := appendDivergenceWarning(nil, 0); len(w) != 0 {
		t.Errorf("count 0 must append nothing, got %#v", w)
	}
	base := []string{"existing"}
	w := appendDivergenceWarning(base, 3)
	if len(w) != 2 || w[0] != "existing" {
		t.Fatalf("expected existing warning + one divergence entry, got %#v", w)
	}
	for _, want := range []string{"3 duplicate event(s) disagreed", "archive copy", "byte-for-byte"} {
		if !strings.Contains(w[1], want) {
			t.Errorf("warning lacks %q: %s", want, w[1])
		}
	}
	// The console DTO boundary deliberately omits row internals
	// (connection_id/query_text); the warning must not smuggle any in — it
	// carries a COUNT, and per-event detail stays in the server log.
	if strings.Contains(w[1], "connection_id") || strings.Contains(w[1], "query_text") {
		t.Errorf("warning must not name row internals: %s", w[1])
	}
}

// #1365: the archive-elision record is an info NOTE — plain words, still
// carrying the audit fact (archives skipped, and why), and never worded or
// shaped like an incident. Both surface strings are pinned VERBATIM (PR #1367
// review): fragment checks alone let a rewording that keeps the fragments —
// e.g. one dropping the justification — ship green. Changing the copy is fine;
// changing it without looking here is not.
//
// The copy stopped naming WHY the archives were skipped when #1403 added a
// second short-circuit: the flag reaching this function is a bool, and the two
// proofs have different remedies (a wider window vs clearing the per-row
// limit). Stating one reason would be wrong half the time, so it states the
// fact and names both levers.
func TestArchiveElisionNote(t *testing.T) {
	if n := archiveElisionNotes(false, archiveElisionNote()); len(n) != 0 {
		t.Fatalf("no elision must produce no note, got %#v", n)
	}
	const wantEvents = "This page was answered from the live index; the registered archives were not read. " +
		"Nothing they hold could have survived this request's filters, so nothing is missing here. " +
		"Widening the time range, or clearing a per-row limit, reads them."
	const wantRecover = "This reversal was built from the live index; the registered archives were not read. " +
		"Nothing they hold could have survived this request's filters, so nothing is missing here. " +
		"Widening the time range, or clearing the per-row limit, reads them."
	if got := archiveElisionNote(); got != wantEvents {
		t.Errorf("events elision note drifted from the pinned copy:\ngot:  %s\nwant: %s", got, wantEvents)
	}
	if got := recoverArchiveElisionNote(); got != wantRecover {
		t.Errorf("recover elision note drifted from the pinned copy:\ngot:  %s\nwant: %s", got, wantRecover)
	}
	n := archiveElisionNotes(true, archiveElisionNote())
	if len(n) != 1 || n[0] != wantEvents {
		t.Fatalf("want exactly the pinned events note, got %#v", n)
	}
	for name, s := range map[string]string{"events": wantEvents, "recover": wantRecover} {
		// The audit fact survives the rewrite on BOTH surfaces: the note says
		// the archives went unread, why that loses nothing, and how to reach
		// them.
		for _, want := range []string{"live index", "were not read", "nothing is missing", "time range"} {
			if !strings.Contains(strings.ToLower(s), strings.ToLower(want)) {
				t.Errorf("%s note lost the audit fact %q: %s", name, want, s)
			}
		}
		// The jargon the issue was filed over is gone...
		if strings.Contains(s, "could not change it") {
			t.Errorf("%s note brings back the 'because they could not change it' jargon: %s", name, s)
		}
		// ...and the strings are read by API/MCP clients too, so they must
		// stay flag-free (no `--flag` a client cannot pass on its surface).
		if strings.Contains(s, "--") {
			t.Errorf("the %s wire note must not name CLI flags: %s", name, s)
		}
	}
}

// #1365: the severity split itself. Every cautionary fact — coverage gap,
// session archive-exclusion (#1311/#1321), divergence finding (#1325) — lands
// in warnings; the benign elision record (#1353) lands in notes. Routing the
// elision back into warnings, or dropping it, fails here.
func TestResponseAdvisoriesSeveritySplit(t *testing.T) {
	hour := time.Date(2026, 8, 1, 3, 0, 0, 0, time.UTC)
	planWithGap := &query.QueryPlan{GapHours: []time.Time{hour}}

	warnings, notes := responseAdvisories(planWithGap, archiveExclusion{profile: true}, 2, true, archiveElisionNote())

	joinedW := strings.Join(warnings, "\n")
	for name, want := range map[string]string{
		"session exclusion (#1321)": "LIVE INDEX ONLY",
		"gap hours":                 "2026-08-01 03:00",
		"divergence (#1325)":        "2 duplicate event(s) disagreed",
	} {
		if !strings.Contains(joinedW, want) {
			t.Errorf("%s missing from warnings: %#v", name, warnings)
		}
	}
	if len(notes) != 1 || notes[0] != archiveElisionNote() {
		t.Fatalf("the elision record must be the info note, got %#v", notes)
	}
	// The recover surface flows its own wording through the same seam.
	if _, rn := responseAdvisories(&query.QueryPlan{}, archiveExclusion{}, 0, true, recoverArchiveElisionNote()); len(rn) != 1 || rn[0] != recoverArchiveElisionNote() {
		t.Fatalf("the recover elision record must flow through as the info note, got %#v", rn)
	}
	// The lists never cross: the elision is not ALSO (or instead) a warning,
	// and no cautionary fact is quietly demoted to a note. The probes are the
	// elision strings' own openings — a bare "live index" would false-positive
	// on the divergence warning, which legitimately names the live index.
	if strings.Contains(joinedW, "answered from the live index") ||
		strings.Contains(joinedW, "built from the live index") ||
		strings.Contains(joinedW, "could not change it") {
		t.Errorf("the elision record leaked into warnings: %#v", warnings)
	}
	joinedN := strings.Join(notes, "\n")
	for _, caution := range []string{"LIVE INDEX ONLY", "disagreed", "2026-08-01"} {
		if strings.Contains(joinedN, caution) {
			t.Errorf("a cautionary fact was demoted to a note: %#v", notes)
		}
	}

	// The quiet path: nothing to say in either register.
	w, n := responseAdvisories(&query.QueryPlan{}, archiveExclusion{}, 0, false, archiveElisionNote())
	if len(w) != 0 || len(n) != 0 {
		t.Errorf("clean read must produce no warnings and no notes, got %#v / %#v", w, n)
	}
}
