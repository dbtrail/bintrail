package verify

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// The three inconclusive kinds (#1416), each driven through the REAL walk. One
// bucket carried three meanings and rendered them identically, so a healthy
// run over a server full of log tables read as a page of warnings. The split
// exists so a summary can be read; these pin which shape lands where.
func TestRecoverInconclusiveKinds(t *testing.T) {
	tests := []struct {
		name      string
		events    []query.ResultRow
		truncated bool
		wantKind  string
		why       string
	}{
		{
			name:     "no activity",
			events:   nil,
			wantKind: InconclusiveNoActivity,
			why:      "an empty window is the quiet-table case, not a finding",
		},
		{
			name: "true append-only: single INSERTs",
			events: []query.ResultRow{
				riEvent(1, event.EventInsert, "1", nil, riRow(1, "a", 1)),
				riEvent(2, event.EventInsert, "2", nil, riRow(2, "b", 1)),
			},
			wantKind: InconclusiveNothingToAssert,
			why: "every chain is a single INSERT — zero assertions is this shape's only " +
				"possible outcome, in every window, forever",
		},
		{
			name: "single mid-history UPDATE is NOT append-only",
			events: []query.ResultRow{
				riEvent(9, event.EventUpdate, "7", riRow(7, "w", 1), riRow(7, "w", 5)),
			},
			wantKind: InconclusiveUnproven,
			why: "the row has prior history the window cannot see; widening the lookback " +
				"makes it assertable, so 'does not apply' would over-claim benignity",
		},
		{
			name: "a truncated window is unproven even over a benign shape",
			events: []query.ResultRow{
				riEvent(1, event.EventInsert, "1", nil, riRow(1, "a", 1)),
			},
			truncated: true,
			wantKind:  InconclusiveUnproven,
			why: "the tail of the window was never loaded, so nothing about its shape is " +
				"known — rounding truncation toward 'nothing to check' hides exactly the " +
				"runs that most need a narrower window",
		},
		{
			name: "drift rows are activity, not quiet",
			events: []query.ResultRow{
				riEvent(1, event.EventUpdate, "", riRow(1, "a", 1), riRow(1, "a", 2)),
			},
			wantKind: InconclusiveUnproven,
			why: "Events counts what was WALKED, so a drift-only table also has Events==0 — " +
				"reporting it as 'no changes in the window' is false",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := riInput(tc.events)
			in.Truncated = tc.truncated
			out := checkRecoverChains(in)
			if out.Status != StatusInconclusive {
				t.Fatalf("Status = %s (%s), want inconclusive", out.Status, out.Detail)
			}
			if out.InconclusiveKind != tc.wantKind {
				t.Errorf("kind = %q, want %q — %s\ndetail: %s", out.InconclusiveKind, tc.wantKind, tc.why, out.Detail)
			}
		})
	}
}

// A proven or diverged table carries NO kind: the field subdivides
// inconclusive only, and a stray value on a match would let a summary count a
// verified table as "nothing to check".
func TestRecoverKindEmptyOffInconclusive(t *testing.T) {
	out := checkRecoverChains(riInput([]query.ResultRow{
		riEvent(1, event.EventInsert, "1", nil, riRow(1, "a", 1)),
		riEvent(2, event.EventUpdate, "1", riRow(1, "a", 1), riRow(1, "a", 2)),
	}))
	if out.Status != StatusMatch {
		t.Fatalf("Status = %s (%s), want match", out.Status, out.Detail)
	}
	if out.InconclusiveKind != "" {
		t.Errorf("kind = %q on a match, want empty", out.InconclusiveKind)
	}
}

// The summary split. The benign counter follows the KIND, and an inconclusive
// with no kind counts on the attention side — defaulting the unknown to benign
// is the direction a verify tool must never round.
func TestSummaryCountsBenignInconclusive(t *testing.T) {
	var s Summary
	s.CountWithKind(StatusInconclusive, InconclusiveNoActivity)
	s.CountWithKind(StatusInconclusive, InconclusiveNothingToAssert)
	s.CountWithKind(StatusInconclusive, InconclusiveUnproven)
	s.CountWithKind(StatusInconclusive, "") // unclassified: content modes, older producers
	s.CountWithKind(StatusMatch, "")
	// A benign kind riding on a NON-inconclusive status must not bump the
	// benign counter — the kind subdivides inconclusive only, and the wire
	// can carry any pairing (pass 2 proved the status conjunct deletable
	// with every test green).
	s.CountWithKind(StatusMatch, InconclusiveNoActivity)

	if s.Inconclusive != 4 {
		t.Errorf("Inconclusive = %d, want 4 — the split is a subdivision, not a fifth bucket", s.Inconclusive)
	}
	if s.Match != 2 {
		t.Errorf("Match = %d, want 2", s.Match)
	}
	if s.InconclusiveNothingToCheck != 2 {
		t.Errorf("InconclusiveNothingToCheck = %d, want 2 — unproven and unclassified must both "+
			"land on the attention side", s.InconclusiveNothingToCheck)
	}
	if s.Total != 6 {
		t.Errorf("Total = %d, want 6", s.Total)
	}
}

// The report carries the kind to JSON consumers and the exit message carries
// the split — a CI failure reading "20 inconclusive" with no hint that 18 had
// nothing to check sends someone debugging a healthy server.
func TestReportCarriesKindAndSplitExitMessage(t *testing.T) {
	rep := NewReport(ModeRecoverInputs, []TableResult{
		{Schema: "s", Table: "quiet", Status: StatusInconclusive, InconclusiveKind: InconclusiveNoActivity},
		{Schema: "s", Table: "log", Status: StatusInconclusive, InconclusiveKind: InconclusiveNothingToAssert},
		{Schema: "s", Table: "hard", Status: StatusInconclusive, InconclusiveKind: InconclusiveUnproven},
	})
	if rep.Summary.InconclusiveNothingToCheck != 2 {
		t.Fatalf("summary split = %d, want 2", rep.Summary.InconclusiveNothingToCheck)
	}
	if got := rep.Tables[2].InconclusiveKind; got != InconclusiveNoActivity {
		t.Errorf("table kind lost in NewReport: %q", got) // tables sorted by name: hard, log, quiet
	}
	err := rep.ExitError()
	if err == nil {
		t.Fatal("an all-inconclusive run must still exit non-zero — a CI gate must not read " +
			"'nothing proven' as success, however benign the reasons")
	}
	if !strings.Contains(err.Error(), "nothing to check") {
		t.Errorf("exit message = %q, want the benign split named", err.Error())
	}
}
