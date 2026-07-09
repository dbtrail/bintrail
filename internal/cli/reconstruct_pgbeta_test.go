package cli

import "testing"

// TestPGReconstructBeta pins the #829 beta-warn gate: single-row reconstruct
// warns for a PostgreSQL source, detected by the recorded flavor OR the
// baseline's LSN anchor. Both clauses are load-bearing: the LSN clause catches
// any PG baseline carrying an anchor (local or S3, since #916 reads S3 metadata);
// the flavor clause catches a pre-#593 PG baseline with LSN==0.
func TestPGReconstructBeta(t *testing.T) {
	cases := []struct {
		name   string
		flavor string
		lsn    uint64
		want   bool
	}{
		{"postgres flavor, no lsn", "postgres", 0, true},
		{"empty flavor, lsn set (anchored baseline, probe blank)", "", 42, true},
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
