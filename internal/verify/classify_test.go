package verify

import "testing"

func TestClassify(t *testing.T) {
	const (
		dA = "v1:aaaa"
		dB = "v1:bbbb"
	)
	cases := []struct {
		name         string
		srcDigest    string
		srcRows      int64
		reconDigest  string
		reconRows    int64
		deferredRepr bool
		want         Status
	}{
		{"equal digest + equal rows → match", dA, 10, dA, 10, false, StatusMatch},
		{"digest differs, equal rows, no deferred → mismatch", dA, 10, dB, 10, false, StatusMismatch},
		{"digest differs, equal rows, deferred → inconclusive", dA, 10, dB, 10, true, StatusInconclusive},
		// The load-bearing guard: a row-count difference is a conclusive mismatch
		// even when a deferred-repr column is present — data loss must never be
		// masked as inconclusive.
		{"row count differs + deferred → still mismatch", dA, 10, dA, 7, true, StatusMismatch},
		{"row count differs, no deferred → mismatch", dA, 10, dA, 7, false, StatusMismatch},
		// Equal digest must win even under deferredRepr (a deferred column that
		// did not actually diverge still matches).
		{"equal digest + deferred → match", dA, 10, dA, 10, true, StatusMatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := classify(tc.srcDigest, tc.srcRows, tc.reconDigest, tc.reconRows, tc.deferredRepr)
			if got != tc.want {
				t.Errorf("classify = %q, want %q", got, tc.want)
			}
		})
	}
}
