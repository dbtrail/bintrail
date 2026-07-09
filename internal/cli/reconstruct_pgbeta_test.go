package cli

import "testing"

// TestPGReconstructBeta pins the #829 beta-warn gate: single-row reconstruct
// warns for a PostgreSQL source, detected by the recorded flavor OR the
// baseline's LSN anchor. Both clauses are load-bearing: the flavor clause
// catches an S3 PG baseline (bmeta.LSN is 0 for S3 on this local-only-metadata
// path) and a pre-#593 local baseline (LSN==0); the LSN clause catches a local
// PG baseline whose flavor probe returns "".
func TestPGReconstructBeta(t *testing.T) {
	cases := []struct {
		name   string
		flavor string
		lsn    uint64
		want   bool
	}{
		{"postgres flavor, no lsn", "postgres", 0, true},
		{"empty flavor, lsn set (local baseline, probe blank)", "", 42, true},
		{"postgres flavor and lsn", "postgres", 99, true},
		{"empty flavor, no lsn (MySQL file-index)", "", 0, false},
		{"mysql flavor", "mysql", 0, false},
		{"mariadb flavor", "mariadb", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pgReconstructBeta(tc.flavor, tc.lsn); got != tc.want {
				t.Errorf("pgReconstructBeta(%q, %d) = %v, want %v", tc.flavor, tc.lsn, got, tc.want)
			}
		})
	}
}
