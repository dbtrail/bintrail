package cascade

import (
	"encoding/json"
	"testing"
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
