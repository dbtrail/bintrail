package reconstruct

import "testing"

func TestShouldWarnEvents(t *testing.T) {
	const thr = 5_000_000
	cases := []struct {
		name      string
		n         int64
		threshold int64
		want      bool
	}{
		{"below threshold", thr - 1, thr, false},
		{"at threshold", thr, thr, false},
		{"above threshold", thr + 1, thr, true},
		{"threshold 0 disables", 1 << 40, 0, false},
		{"threshold negative disables", 1 << 40, -1, false},
		{"zero events never warns", 0, thr, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldWarnEvents(tc.n, tc.threshold); got != tc.want {
				t.Fatalf("shouldWarnEvents(%d,%d) = %v, want %v", tc.n, tc.threshold, got, tc.want)
			}
		})
	}
}
