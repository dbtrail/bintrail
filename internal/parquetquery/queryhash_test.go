package parquetquery

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/query"
)

const testDigest = "3f2a1b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708"

// TestBuildFilters_queryHash covers the archive half of the statement-digest
// filter, including the case that is not symmetric with MySQL: when no scanned
// file has a query_hash column, parquet_scan errors on a predicate over a
// column it cannot resolve — the same trap optionalCol handles for the SELECT
// list. Such a set provably holds no event carrying a digest, so it must
// contribute an empty result — not an error, and not unfiltered rows.
func TestBuildFilters_queryHash(t *testing.T) {
	for _, tc := range []struct {
		name     string
		cols     map[string]bool
		wantPred string
		wantArgs int
	}{
		{"column present", map[string]bool{"query_hash": true}, "query_hash = ?", 1},
		{"pre-#699 archive", map[string]bool{"event_id": true}, "1=0", 0},
		{"cols unknown (test-only builders, which project the column unconditionally)", nil, "query_hash = ?", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			where, args := buildFilters(query.Options{QueryHash: strings.ToUpper(testDigest)}, tc.cols)

			joined := strings.Join(where, " AND ")
			if !strings.Contains(joined, tc.wantPred) {
				t.Fatalf("where = %q, want it to contain %q", joined, tc.wantPred)
			}
			// A stray arg is worse than a wrong predicate here: DuckDB binds
			// positionally, so an unmatched value shifts every later filter's
			// parameter onto the wrong placeholder.
			if len(args) != tc.wantArgs {
				t.Fatalf("args = %v, want %d", args, tc.wantArgs)
			}
			if tc.wantArgs == 1 && args[0] != testDigest {
				t.Errorf("bound arg = %v, want the lowercased digest %q — DuckDB compares case-sensitively where MySQL does not", args[0], testDigest)
			}
		})
	}
}

// TestBuildFilters_queryHashUnsetEmitsNothing guards the default path: every
// query that does not ask for a digest must keep the SQL it had before, or the
// predicate would silently exclude every event captured without statement
// logging.
func TestBuildFilters_queryHashUnsetEmitsNothing(t *testing.T) {
	where, args := buildFilters(query.Options{Schema: "mydb"}, map[string]bool{"event_id": true})
	joined := strings.Join(where, " AND ")
	if strings.Contains(joined, "query_hash") || strings.Contains(joined, "1=0") {
		t.Fatalf("unset filter still shaped the query: %q (args %v)", joined, args)
	}
}

// TestDigestCoverageWarning pins the boundaries of what an operator is told
// about an archive set that can only partly answer a digest filter.
//
// The middle case is the one this exists for: cols is a UNION over the scanned
// set, so a set MIXED across the #699 upgrade emits the predicate normally and
// the older files pad to NULL — correct rows, silently narrower window. Before
// this, only the all-old set said anything, which is the case that matters
// least (it also returns nothing at all, so the operator has something to be
// suspicious of).
func TestDigestCoverageWarning(t *testing.T) {
	for _, tc := range []struct {
		name       string
		with, tot  int
		wantSubstr string
	}{
		{"fully covered", 5, 5, ""},
		{"empty scan", 0, 0, ""},
		{"none covered", 0, 4, "no archive file"},
		{"mixed across the upgrade", 3, 5, "2 of 5"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := digestCoverageWarning(tc.with, tc.tot)
			if tc.wantSubstr == "" {
				if got != "" {
					t.Fatalf("warning = %q, want silence", got)
				}
				return
			}
			if !strings.Contains(got, tc.wantSubstr) {
				t.Fatalf("warning = %q, want it to contain %q", got, tc.wantSubstr)
			}
		})
	}
}
