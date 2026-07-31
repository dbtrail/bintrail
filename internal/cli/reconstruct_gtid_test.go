package cli

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// TestGTIDContainment pins the go-mysql-backed containment evaluation behind
// the #1163 baseline↔first-event gap check. The contract is conservative:
// only "mysql" and "mariadb" are evaluated (each with its own parser), and
// any input that cannot be evaluated — a missing set on either side, a parse
// failure, a foreign flavor — maps to GTIDUnknown (position-heuristic
// fallback), never a panic: this runs on the recovery path. UUIDs are
// lowercase because go-mysql lowercases them on parse; the one mixed-case
// entry asserts that normalization keeps containment case-insensitive.
func TestGTIDContainment(t *testing.T) {
	const uuid = "c36f2244-89da-11f1-80b2-0aff43e443c1"

	cases := []struct {
		name     string
		flavor   string
		baseline string
		indexed  string
		want     reconstruct.GTIDContainment
	}{
		{
			// The issue's repro: baseline :1-39 inside the index's :1-2000.
			name:   "mysql contained",
			flavor: "mysql", baseline: uuid + ":1-39", indexed: uuid + ":1-2000",
			want: reconstruct.GTIDContained,
		},
		{
			name:   "mysql equal sets contained",
			flavor: "mysql", baseline: uuid + ":1-39", indexed: uuid + ":1-39",
			want: reconstruct.GTIDContained,
		},
		{
			name:   "mysql multi-uuid indexed coverage still contains baseline",
			flavor: "mysql", baseline: uuid + ":1-39",
			indexed: uuid + ":1-2000,3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
			want:    reconstruct.GTIDContained,
		},
		{
			name:   "mysql baseline ahead of indexed coverage = not contained",
			flavor: "mysql", baseline: uuid + ":1-50", indexed: uuid + ":1-39",
			want: reconstruct.GTIDNotContained,
		},
		{
			name:   "mysql disjoint lineages = not contained",
			flavor: "mysql", baseline: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-10",
			indexed: uuid + ":1-2000",
			want:    reconstruct.GTIDNotContained,
		},
		{
			// go-mysql lowercases UUIDs on parse, so a mixed-case baseline
			// (some tools render uppercase) still proves containment.
			name:   "mysql mixed-case uuid still contained",
			flavor: "mysql", baseline: "C36F2244-89DA-11F1-80B2-0AFF43E443C1:1-39",
			indexed: uuid + ":1-2000",
			want:    reconstruct.GTIDContained,
		},
		{
			name:   "mysql empty indexed = unknown",
			flavor: "mysql", baseline: uuid + ":1-39", indexed: "",
			want: reconstruct.GTIDUnknown,
		},
		{
			name:   "mysql whitespace-only indexed = unknown",
			flavor: "mysql", baseline: uuid + ":1-39", indexed: "  \n\t",
			want: reconstruct.GTIDUnknown,
		},
		{
			name:   "mysql empty baseline = unknown",
			flavor: "mysql", baseline: "", indexed: uuid + ":1-2000",
			want: reconstruct.GTIDUnknown,
		},
		{
			name:   "mysql malformed baseline = unknown, no panic",
			flavor: "mysql", baseline: "not-a-gtid-set", indexed: uuid + ":1-2000",
			want: reconstruct.GTIDUnknown,
		},
		{
			name:   "mysql malformed indexed = unknown, no panic",
			flavor: "mysql", baseline: uuid + ":1-39", indexed: "%%garbage%%",
			want: reconstruct.GTIDUnknown,
		},
		{
			name:   "mariadb contained",
			flavor: "mariadb", baseline: "0-1-100", indexed: "0-1-200",
			want: reconstruct.GTIDContained,
		},
		{
			name:   "mariadb baseline ahead = not contained",
			flavor: "mariadb", baseline: "0-1-300", indexed: "0-1-200",
			want: reconstruct.GTIDNotContained,
		},
		{
			// A MySQL-shaped set under the mariadb flavor fails the MariaDB
			// parser — unknown, never a cross-parser comparison.
			name:   "mariadb flavor with mysql-shaped sets = unknown",
			flavor: "mariadb", baseline: uuid + ":1-39", indexed: uuid + ":1-2000",
			want: reconstruct.GTIDUnknown,
		},
		{
			// Empty flavor has no GTID-set semantics to trust: valid-looking
			// sets are still not evaluated.
			name:   "empty flavor = unknown",
			flavor: "", baseline: uuid + ":1-39", indexed: uuid + ":1-2000",
			want: reconstruct.GTIDUnknown,
		},
		{
			name:   "postgres flavor = unknown",
			flavor: "postgres", baseline: uuid + ":1-39", indexed: uuid + ":1-2000",
			want: reconstruct.GTIDUnknown,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := gtidContainment(tc.flavor, tc.baseline, tc.indexed)
			if got != tc.want {
				t.Errorf("gtidContainment(%q, %q, %q) = %v, want %v",
					tc.flavor, tc.baseline, tc.indexed, got, tc.want)
			}
		})
	}
}
