package cascade

import (
	"encoding/json"
	"testing"

	"github.com/dbtrail/dbtrail/internal/query"
)

// TestValToString covers every branch of the value renderer that backs the
// re-parented comparison. The index read path decodes numbers as json.Number
// (UseNumber, #496) — that is the production type, and a plain float64 would
// lose precision on a BIGINT > 2^53, so the json.Number branch is load-bearing.
func TestValToString(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want string
	}{
		{"nil", nil, ""},
		{"string", "abc", "abc"},
		{"json.Number int", json.Number("42"), "42"},
		{"json.Number bigint > 2^53", json.Number("9007199254740993"), "9007199254740993"},
		{"json.Number negative", json.Number("-7"), "-7"},
		{"float64 integral", float64(1), "1"},
		{"float64 fractional", 1.5, "1.5"},
		{"bool true", true, "true"},
		{"bool false", false, "false"},
		{"bytes", []byte("xy"), "xy"},
	}
	for _, c := range cases {
		if got := valToString(c.in); got != c.want {
			t.Errorf("%s: valToString(%#v) = %q, want %q", c.name, c.in, got, c.want)
		}
	}
}

// TestFKColumnAbsentFromAll covers the child-side DDL-skew detector (#832). When
// a cascade older than a child FK-column rename is recovered, the candidate scan
// (ColumnEq on the LATEST snapshot's column name) matches 0 rows against events
// keyed by the OLD name — an outcome indistinguishable from "no children
// existed". The probe samples the child images without the FK filter and flags
// skew only when the snapshot's column name is absent from every sampled image.
func TestFKColumnAbsentFromAll(t *testing.T) {
	rowAfter := func(m map[string]any) query.ResultRow { return query.ResultRow{RowAfter: m} }
	rowBefore := func(m map[string]any) query.ResultRow { return query.ResultRow{RowBefore: m} }

	cases := []struct {
		name   string
		col    string
		sample []query.ResultRow
		want   bool
	}{
		{
			name:   "empty sample is inconclusive (no children, not skew)",
			col:    "parent_id",
			sample: nil,
			want:   false,
		},
		{
			name: "renamed FK column absent from all after-images → skew",
			col:  "parent_id", // snapshot name; events use the old "pid"
			sample: []query.ResultRow{
				rowAfter(map[string]any{"id": json.Number("1"), "pid": json.Number("9")}),
				rowAfter(map[string]any{"id": json.Number("2"), "pid": json.Number("9")}),
			},
			want: true,
		},
		{
			name: "renamed FK column absent from delete before-images → skew",
			col:  "parent_id",
			sample: []query.ResultRow{
				rowBefore(map[string]any{"id": json.Number("1"), "pid": json.Number("9")}),
			},
			want: true,
		},
		{
			name: "column present in after-image → not skew",
			col:  "parent_id",
			sample: []query.ResultRow{
				rowAfter(map[string]any{"id": json.Number("1"), "parent_id": json.Number("9")}),
			},
			want: false,
		},
		{
			name: "column present in before-image → not skew",
			col:  "parent_id",
			sample: []query.ResultRow{
				rowBefore(map[string]any{"id": json.Number("1"), "parent_id": json.Number("9")}),
			},
			want: false,
		},
		{
			name: "mixed: at least one image carries the column → not skew",
			col:  "parent_id",
			sample: []query.ResultRow{
				rowAfter(map[string]any{"id": json.Number("1"), "pid": json.Number("9")}),
				rowAfter(map[string]any{"id": json.Number("2"), "parent_id": json.Number("9")}),
			},
			want: false,
		},
		{
			name: "column present but NULL-valued still counts as present (not skew)",
			col:  "parent_id",
			sample: []query.ResultRow{
				rowAfter(map[string]any{"id": json.Number("1"), "parent_id": nil}),
			},
			want: false,
		},
		{
			name:   "sample rows with no images at all is inconclusive",
			col:    "parent_id",
			sample: []query.ResultRow{{}, {}},
			want:   false,
		},
	}
	for _, c := range cases {
		if got := fkColumnAbsentFromAll(c.col, c.sample); got != c.want {
			t.Errorf("%s: fkColumnAbsentFromAll(%q, …) = %v, want %v", c.name, c.col, got, c.want)
		}
	}
}
