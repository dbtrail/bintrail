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
		// #792 digest-contract skew: a persisted pre-pin v1 digest compared
		// against a current v2 scan byte-differs even on identical data, so it
		// must degrade to inconclusive (needs-rebaseline), NEVER a false mismatch.
		{"version skew, same hash bytes → inconclusive not mismatch", "v1:aaaa", 10, "v2:aaaa", 10, false, StatusInconclusive},
		{"version skew, different hash bytes → inconclusive not mismatch", "v1:aaaa", 10, "v2:bbbb", 10, false, StatusInconclusive},
		{"untagged legacy vs tagged → inconclusive", "aaaa", 10, "v2:aaaa", 10, false, StatusInconclusive},
		// Row loss stays conclusive UNDER a version skew: row count is version-
		// independent and checked first, so real loss is never masked as needs-rebaseline.
		{"version skew but row count differs → still mismatch", "v1:aaaa", 10, "v2:aaaa", 7, false, StatusMismatch},
		// Same (current) contract on both sides compares normally.
		{"same v2 contract, equal → match", "v2:aaaa", 10, "v2:aaaa", 10, false, StatusMatch},
		{"same v2 contract, differ → mismatch", "v2:aaaa", 10, "v2:bbbb", 10, false, StatusMismatch},
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
