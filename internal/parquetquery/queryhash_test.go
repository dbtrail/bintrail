package parquetquery

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/query"
)

const testDigest = "3f2a1b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708"

// TestBuildFilters_queryHash covers the archive half of the statement-digest
// filter, including the case that is not symmetric with MySQL: an archive
// written before #699 has no query_hash column in ANY scanned file, and
// parquet_scan errors on a predicate over a column it cannot resolve — the same
// trap optionalCol handles for the SELECT list. Such a file provably holds no
// event carrying a digest, so it must contribute an empty result, not an error
// and not unfiltered rows.
func TestBuildFilters_queryHash(t *testing.T) {
	for _, tc := range []struct {
		name     string
		cols     map[string]bool
		wantPred string
		wantArgs int
	}{
		{"column present", map[string]bool{"query_hash": true}, "query_hash = ?", 1},
		{"pre-#699 archive", map[string]bool{"event_id": true}, "1=0", 0},
		{"cols unknown (legacy builders project it unconditionally)", nil, "query_hash = ?", 1},
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
