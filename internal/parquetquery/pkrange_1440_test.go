package parquetquery

import (
	"context"
	"math/big"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/query"
)

// ─── #1440: the archive mirror of the pk_min/pk_max predicate ───────────────

// TestBuildFilters_pkRange pins the DuckDB predicate on every builder: the
// cast type follows the resolved signedness, TRY_CAST (a plain CAST aborts
// the scan on a non-integer key, the empty key of a #318 drift row being the
// pinned case), and the bounds are inlined so a UBIGINT bound above 2^63-1
// never has to travel as a bind value database/sql refuses.
func TestBuildFilters_pkRange(t *testing.T) {
	top := new(big.Int)
	top.SetString("18446744073709551610", 10)
	opts := query.Options{Schema: "s", Table: "t", Limit: 10,
		PKRange: &query.PKRange{Cast: query.PKCastUnsigned, Min: big.NewInt(10), Max: top}}

	builders := map[string]func() (string, []any){
		"buildQuery":          func() (string, []any) { return buildQuery("/arc/**/*.parquet", opts) },
		"buildQueryFromFiles": func() (string, []any) { return buildQueryFromFiles([]string{"/arc/a.parquet"}, opts, nil) },
		"buildUnsortedQuery":  func() (string, []any) { return buildUnsortedQuery("/arc/a.parquet", opts) },
		"buildQueryForFile":   func() (string, []any) { return buildQueryForFile("/arc/a.parquet", opts, nil) },
	}
	for name, build := range builders {
		q, args := build()
		for _, want := range []string{
			"CAST(TRY_CAST(pk_values AS UBIGINT) AS VARCHAR) = pk_values",
			"TRY_CAST(pk_values AS UBIGINT) >= 10",
			"TRY_CAST(pk_values AS UBIGINT) <= 18446744073709551610",
		} {
			if !strings.Contains(q, want) {
				t.Errorf("%s: query missing %q:\n%s", name, want, q)
			}
		}
		if strings.Contains(q, "pk_values >= ") || strings.Contains(q, "pk_values <= ") {
			t.Errorf("%s: a bare string comparison on pk_values is lexicographic:\n%s", name, q)
		}
		for _, a := range args {
			if b, ok := a.(*big.Int); ok && b.Cmp(top) == 0 {
				t.Errorf("%s: the upper bound was bound instead of inlined", name)
			}
		}
	}

	q, _ := buildQuery("/arc/**/*.parquet", query.Options{Schema: "s", Table: "t", Limit: 10,
		PKRange: &query.PKRange{Cast: query.PKCastSigned, Min: big.NewInt(-5)}})
	if !strings.Contains(q, "TRY_CAST(pk_values AS BIGINT) >= -5") {
		t.Errorf("signed cast missing:\n%s", q)
	}
	if strings.Contains(q, "UBIGINT") {
		t.Errorf("a signed key must never be cast UBIGINT:\n%s", q)
	}
}

// TestFetchWithTuning_refusesUnresolvedPKRange: the tuned CLI path enters
// FetchWithTuning directly, so the belt has to sit there, not only in Fetch.
func TestFetchWithTuning_refusesUnresolvedPKRange(t *testing.T) {
	_, err := FetchWithTuning(context.Background(), query.Options{Schema: "s", Table: "t",
		PKRange: &query.PKRange{Min: big.NewInt(1)}}, t.TempDir(), duckdbutil.DefaultTuning())
	if err == nil || !strings.Contains(err.Error(), "not resolved") {
		t.Fatalf("unresolved range reached the archive engine: %v", err)
	}
	_, err = Fetch(context.Background(), query.Options{PKRange: &query.PKRange{Cast: query.PKCastSigned, Min: big.NewInt(1)}}, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "schema and table") {
		t.Fatalf("range without schema/table reached the archive engine: %v", err)
	}
}
