package verify

import (
	"bytes"
	"strings"
	"testing"
)

// TestMismatchExplanation_Write pins the drill-down formatting: a changed row
// shows the column with recovery-vs-baseline values, missing/extra rows are
// labeled by side, and a capped run reports the remaining count.
func TestMismatchExplanation_Write(t *testing.T) {
	ex := &MismatchExplanation{
		Schema: "mydb", Table: "orders", Anchor: "binlog.000001:300",
		Diffs: []RowDiff{
			{PK: "id=2", Kind: diffChanged, Cells: []CellDiff{{Column: "status", Recovery: "wrong", Baseline: "shipped"}}},
			{PK: "id=5", Kind: diffExtra},
			{PK: "id=7", Kind: diffMissing},
		},
		Total: 5, // two more than shown
	}
	var buf bytes.Buffer
	ex.Write(&buf)
	out := buf.String()

	for _, want := range []string{
		"mydb.orders @ binlog.000001:300",
		"id=2", "status", "recovery=wrong", "baseline=shipped",
		"id=5", "absent from the new baseline",
		"id=7", "NOT reproduced by the recovery",
		"and 2 more differing row(s)",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("drill-down output missing %q\n--- output ---\n%s", want, out)
		}
	}
}

// TestMismatchExplanation_Write_rowCountOnly covers the edge where a mismatch was
// flagged on row count but every matched PK lines up cell-for-cell (Total == 0):
// the drill-down must say so, not print an empty section.
func TestMismatchExplanation_Write_rowCountOnly(t *testing.T) {
	ex := &MismatchExplanation{Schema: "mydb", Table: "orders", Anchor: "binlog.000001:300"}
	var buf bytes.Buffer
	ex.Write(&buf)
	if !strings.Contains(buf.String(), "row count, not row content") {
		t.Errorf("want the row-count-only note, got:\n%s", buf.String())
	}
}

// TestMismatchExplanation_add_caps locks the cap: detail is bounded at
// maxExplainRows while Total keeps counting, so a pathological all-differ table
// never dumps the whole table yet nothing is silently hidden.
func TestMismatchExplanation_add_caps(t *testing.T) {
	ex := &MismatchExplanation{}
	for i := 0; i < maxExplainRows+25; i++ {
		ex.add(RowDiff{PK: "x", Kind: diffExtra})
	}
	if len(ex.Diffs) != maxExplainRows {
		t.Errorf("Diffs len = %d, want capped at %d", len(ex.Diffs), maxExplainRows)
	}
	if ex.Total != maxExplainRows+25 {
		t.Errorf("Total = %d, want %d (counting continues past the cap)", ex.Total, maxExplainRows+25)
	}
}
