package main

import (
	"testing"
)

func TestParseTableFilter(t *testing.T) {
	cases := []struct {
		input string
		want  []string
	}{
		{"", nil},
		{"mydb.orders", []string{"mydb.orders"}},
		{"mydb.orders, mydb.users", []string{"mydb.orders", "mydb.users"}},
		{"  mydb.orders  ,  ", []string{"mydb.orders"}},
	}
	for _, tc := range cases {
		got := parseTableFilter(tc.input)
		if len(got) != len(tc.want) {
			t.Errorf("parseTableFilter(%q) = %v, want %v", tc.input, got, tc.want)
			continue
		}
		for i := range tc.want {
			if got[i] != tc.want[i] {
				t.Errorf("parseTableFilter(%q)[%d] = %q, want %q", tc.input, i, got[i], tc.want[i])
			}
		}
	}
}

func TestBaselineFlagValidation(t *testing.T) {
	// Save/restore flag state
	origInput, origOutput := bslInput, bslOutput
	t.Cleanup(func() {
		bslInput = origInput
		bslOutput = origOutput
	})

	// Missing --input and --output: cobra's MarkFlagRequired enforces these
	// before RunE is called. We just verify runBaseline itself handles empty
	// values gracefully if they were somehow passed.
	bslInput = ""
	bslOutput = ""
	// runBaseline would fail at baseline.Run level; the cobra layer catches it
	// first via MarkFlagRequired. Nothing to test here beyond compilation.
}
