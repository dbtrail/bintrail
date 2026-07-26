package verify

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

func TestClassify(t *testing.T) {
	const (
		dA = "v1:aaaa"
		dB = "v1:bbbb"
		// A non-empty deferredDetail turns an equal-row-count content
		// difference into Inconclusive; the string itself is built by
		// deferredReprDetail at the call sites (see its own test below).
		deferred = "an event carried a value for column \"v\" (point) that could not be normalized"
	)
	cases := []struct {
		name           string
		srcDigest      string
		srcRows        int64
		reconDigest    string
		reconRows      int64
		deferredDetail string
		want           Status
	}{
		{"equal digest + equal rows → match", dA, 10, dA, 10, "", StatusMatch},
		{"digest differs, equal rows, no deferred → mismatch", dA, 10, dB, 10, "", StatusMismatch},
		{"digest differs, equal rows, deferred → inconclusive", dA, 10, dB, 10, deferred, StatusInconclusive},
		// The load-bearing guard: a row-count difference is a conclusive mismatch
		// even when a deferred-repr column is present — data loss must never be
		// masked as inconclusive.
		{"row count differs + deferred → still mismatch", dA, 10, dA, 7, deferred, StatusMismatch},
		{"row count differs, no deferred → mismatch", dA, 10, dA, 7, "", StatusMismatch},
		// Equal digest must win even under deferredDetail (a deferred column that
		// did not actually diverge still matches).
		{"equal digest + deferred → match", dA, 10, dA, 10, deferred, StatusMatch},
		// #792 digest-contract skew: a persisted pre-pin v1 digest compared
		// against a current v2 scan byte-differs even on identical data, so it
		// must degrade to inconclusive (needs-rebaseline), NEVER a false mismatch.
		{"version skew, same hash bytes → inconclusive not mismatch", "v1:aaaa", 10, "v2:aaaa", 10, "", StatusInconclusive},
		{"version skew, different hash bytes → inconclusive not mismatch", "v1:aaaa", 10, "v2:bbbb", 10, "", StatusInconclusive},
		{"untagged legacy vs tagged → inconclusive", "aaaa", 10, "v2:aaaa", 10, "", StatusInconclusive},
		// Row loss stays conclusive UNDER a version skew: row count is version-
		// independent and checked first, so real loss is never masked as needs-rebaseline.
		{"version skew but row count differs → still mismatch", "v1:aaaa", 10, "v2:aaaa", 7, "", StatusMismatch},
		// Same (current) contract on both sides compares normally.
		{"same v2 contract, equal → match", "v2:aaaa", 10, "v2:aaaa", 10, "", StatusMatch},
		{"same v2 contract, differ → mismatch", "v2:aaaa", 10, "v2:bbbb", 10, "", StatusMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, detail := classify(tc.srcDigest, tc.srcRows, tc.reconDigest, tc.reconRows, tc.deferredDetail)
			if got != tc.want {
				t.Errorf("classify = %q, want %q", got, tc.want)
			}
			// The deferred downgrade must report the caller's reason verbatim
			// — that is what carries the column name to the operator (#1136).
			if got == StatusInconclusive && tc.deferredDetail != "" && detail != tc.deferredDetail {
				t.Errorf("detail = %q, want the deferredDetail passed in (%q)", detail, tc.deferredDetail)
			}
		})
	}
}

// TestDeferredReprDetail pins the #1136 message contract: the Inconclusive
// reason must name the actual unresolved column and its type — the earlier
// static list ("ENUM/SET, JSON, binary or BIT") named types a spatial-only
// table does not have.
func TestDeferredReprDetail(t *testing.T) {
	got := deferredReprDetail(metadata.ColumnMeta{Name: "v", DataType: "POINT"})
	for _, want := range []string{`column "v" (point)`, "could not be normalized", "not conclusive"} {
		if !strings.Contains(got, want) {
			t.Errorf("deferredReprDetail = %q; want it to contain %q", got, want)
		}
	}
	if strings.Contains(got, "ENUM/SET") {
		t.Errorf("deferredReprDetail = %q; must not carry the old static type list", got)
	}
}
