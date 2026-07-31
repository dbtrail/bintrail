package verify

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
)

// TestNewReportClassification pins the per-table shape and the summary buckets
// the JSON consumer branches on (#954).
func TestNewReportClassification(t *testing.T) {
	rep := NewReport(ModeBaselinePair, []TableResult{
		// Deliberately out of order: the report must sort by schema.table so a
		// scheduled diff of two runs is stable.
		{Schema: "mydb", Table: "orders", Status: StatusMismatch, SourceRows: 10, ReconstructRows: 9,
			SourceDigest: "v2:aaa", ReconstructDigest: "v2:bbb", Anchor: "binlog.000007:4711", Detail: "digest differs"},
		{Schema: "mydb", Table: "customers", Status: StatusMatch, SourceRows: 3, ReconstructRows: 3},
		{Schema: "adb", Table: "audit", Status: StatusInconclusive, Detail: "never baselined"},
	})

	if got := []string{rep.Tables[0].Table, rep.Tables[1].Table, rep.Tables[2].Table}; got[0] != "audit" ||
		got[1] != "customers" || got[2] != "orders" {
		t.Errorf("tables not sorted by schema.table: %v", got)
	}
	if rep.Summary != (Summary{Match: 1, Mismatch: 1, Inconclusive: 1, Total: 3}) {
		t.Errorf("summary = %+v", rep.Summary)
	}
	if rep.Verdict != VerdictMismatch {
		t.Errorf("verdict = %q, want %q", rep.Verdict, VerdictMismatch)
	}
	mm := rep.Tables[2]
	if mm.Reason != "digest differs" || mm.Anchor != "binlog.000007:4711" ||
		mm.SourceDigest != "v2:aaa" || mm.ReconstructDigest != "v2:bbb" {
		t.Errorf("mismatch row lost fields: %+v", mm)
	}
}

// TestNewReportUnknownStatus: an unrecognized (or zero) status must count as an
// error, never as the benign inconclusive bucket, and must not serialize a
// status string a consumer's switch would fall through — the JSON side of the
// no-false-assurance rule the text report already enforced.
func TestNewReportUnknownStatus(t *testing.T) {
	for _, raw := range []Status{"", "bogus"} {
		rep := NewReport(ModeLive, []TableResult{
			{Schema: "db", Table: "t", Status: StatusMatch},
			{Schema: "db", Table: "u", Status: raw, Detail: "who knows"},
		})
		if rep.Summary.Error != 1 || rep.Summary.Inconclusive != 0 {
			t.Errorf("status %q: summary = %+v, want 1 error / 0 inconclusive", raw, rep.Summary)
		}
		if rep.Tables[1].Status != StatusError {
			t.Errorf("status %q: serialized as %q, want %q", raw, rep.Tables[1].Status, StatusError)
		}
		if !strings.Contains(rep.Tables[1].Reason, "unrecognized verify status") ||
			!strings.Contains(rep.Tables[1].Reason, "who knows") {
			t.Errorf("status %q: reason lost the cause: %q", raw, rep.Tables[1].Reason)
		}
		if rep.Verdict != VerdictError {
			t.Errorf("status %q: verdict = %q", raw, rep.Verdict)
		}
		if err := rep.ExitError(); err == nil {
			t.Errorf("status %q: want a non-zero exit", raw)
		}
	}
}

// TestNormalizeStatusBuckets pins the single status→bucket decision (#1127)
// shared by the CLI report and the console wire path: every canonical status
// passes through unchanged, and anything else — including the zero value —
// lands in Error, never in a benign bucket.
func TestNormalizeStatusBuckets(t *testing.T) {
	for _, s := range []Status{StatusMatch, StatusMismatch, StatusInconclusive, StatusError} {
		got, reason := NormalizeStatus(s, "note")
		if got != s || reason != "note" {
			t.Errorf("NormalizeStatus(%q) = (%q, %q), want passed through unchanged", s, got, reason)
		}
	}
	for _, raw := range []Status{"", "bogus", "MATCH"} {
		got, reason := NormalizeStatus(raw, "ctx")
		if got != StatusError {
			t.Errorf("NormalizeStatus(%q) = %q, want %q", raw, got, StatusError)
		}
		if !strings.Contains(reason, "unrecognized verify status") || !strings.Contains(reason, "ctx") {
			t.Errorf("NormalizeStatus(%q) reason = %q, want the raw value and detail kept", raw, reason)
		}
	}
}

// TestSummaryCount: Count normalizes before tallying, so a caller feeding it
// raw wire strings (the console supervisor) can never file an unknown status
// outside Error, and Total always equals the number of Count calls.
func TestSummaryCount(t *testing.T) {
	var s Summary
	for _, st := range []Status{StatusMatch, StatusMismatch, StatusInconclusive, StatusError, "", "bogus"} {
		s.Count(st)
	}
	if s != (Summary{Match: 1, Mismatch: 1, Inconclusive: 1, Error: 3, Total: 6}) {
		t.Errorf("summary = %+v, want 1/1/1/3 with total 6", s)
	}
}

// TestReportExitError locks the exit contract and its precedence.
func TestReportExitError(t *testing.T) {
	r := func(s Status) TableResult { return TableResult{Schema: "db", Table: "t", Status: s} }
	cases := []struct {
		name        string
		results     []TableResult
		wantVerdict string
		wantErr     bool
	}{
		{"all match", []TableResult{r(StatusMatch)}, VerdictVerified, false},
		{"match + inconclusive", []TableResult{r(StatusMatch), r(StatusInconclusive)}, VerdictVerified, false},
		{"mismatch outranks error", []TableResult{r(StatusMismatch), r(StatusError)}, VerdictMismatch, true},
		{"error", []TableResult{r(StatusMatch), r(StatusError)}, VerdictError, true},
		{"all inconclusive", []TableResult{r(StatusInconclusive)}, VerdictUnproven, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rep := NewReport(ModeBaselinePair, tc.results)
			if rep.Verdict != tc.wantVerdict {
				t.Errorf("verdict = %q, want %q", rep.Verdict, tc.wantVerdict)
			}
			if err := rep.ExitError(); (err != nil) != tc.wantErr {
				t.Errorf("ExitError = %v, want error: %v", err, tc.wantErr)
			}
		})
	}
}

// TestNoPredecessorReportExitsZero: exactly one baseline is a legitimate first
// run — reported, not failed.
func TestNoPredecessorReportExitsZero(t *testing.T) {
	rep := NewNoPredecessorReport(ModeBaselinePair, "/data/baselines", "only one baseline")
	if rep.Verdict != VerdictNoPredecessor {
		t.Errorf("verdict = %q", rep.Verdict)
	}
	if err := rep.ExitError(); err != nil {
		t.Errorf("want exit 0, got %v", err)
	}
	// Tables must serialize as [] rather than null: a consumer ranging over it
	// should not have to special-case this path.
	b, err := json.Marshal(rep)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(b), `"tables":[]`) {
		t.Errorf("want an empty tables array, got %s", b)
	}
}

// TestExplainReportEntry: the JSON drill-down must carry what Write prints —
// including the deferred-type caveat and the per-kind overflow breakdown, both
// of which live in unexported fields that a direct marshal would drop.
func TestExplainReportEntry(t *testing.T) {
	ex := &MismatchExplanation{Schema: "mydb", Table: "orders", Anchor: "binlog.000007:4711"}
	ex.deferredSeen = true
	ex.add(RowDiff{PK: "1", Kind: diffChanged, Cells: []CellDiff{{Column: "total", Recovery: "5", Baseline: "6"}}})
	// Overflow the cap with rows of a different kind, so the breakdown has to
	// report the missing rows that never made it into Diffs.
	for i := range maxExplainRows + 10 {
		ex.add(RowDiff{PK: "m" + strconv.Itoa(i), Kind: diffMissing})
	}

	got := ex.ReportEntry()
	if got.Schema != "mydb" || got.Table != "orders" || got.Anchor != "binlog.000007:4711" {
		t.Errorf("identity fields lost: %+v", got)
	}
	if !got.DeferredTypeNote {
		t.Error("deferred-type caveat dropped")
	}
	if got.TotalDifferingRows != maxExplainRows+11 {
		t.Errorf("total_differing_rows = %d, want %d", got.TotalDifferingRows, maxExplainRows+11)
	}
	if len(got.Rows) != maxExplainRows {
		t.Errorf("rows = %d, want the %d-row cap", len(got.Rows), maxExplainRows)
	}
	if got.OverflowByKind[diffMissing] != 11 {
		t.Errorf("overflow_by_kind = %v, want 11 missing", got.OverflowByKind)
	}
	if len(got.Rows[0].Cells) != 1 || got.Rows[0].Cells[0].Column != "total" ||
		got.Rows[0].Cells[0].Recovery != "5" || got.Rows[0].Cells[0].Baseline != "6" {
		t.Errorf("cell diff lost: %+v", got.Rows[0])
	}
}

// TestExplainReportEntryNoOverflow: under the cap there is no overflow section
// to report.
func TestExplainReportEntryNoOverflow(t *testing.T) {
	ex := &MismatchExplanation{Schema: "mydb", Table: "orders"}
	ex.add(RowDiff{PK: "1", Kind: diffExtra})
	got := ex.ReportEntry()
	if got.OverflowByKind != nil {
		t.Errorf("overflow_by_kind = %v, want none", got.OverflowByKind)
	}
	if got.DeferredTypeNote {
		t.Error("deferred note set without a deferred-type column")
	}
	if got.TotalDifferingRows != 1 || len(got.Rows) != 1 || got.Rows[0].Kind != diffExtra {
		t.Errorf("entry = %+v", got)
	}
}
