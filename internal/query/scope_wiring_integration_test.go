//go:build integration

package query_test

import (
	"context"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The scope rule is tested directly against Plan. This tests the WIRING: that
// FetchMerged actually derives a scope and hands it over.
//
// It exists because replacing `scope` with `nil` inside resolveMergeSources was
// invisible to the entire suite — the unit tests set DBName:"" so the planner
// never runs, and the sqlmock expectations are unanchored regexes with no
// WithArgs, so a scoped query with bound arguments matched identically.
//
// The lever is an archive_state row with a NULL bintrail_id. ResolveArchiveSources
// filters those out (`WHERE bintrail_id IS NOT NULL`), so such a row can never
// describe an archive this read opens — yet unscoped coverage counted it.
func TestFetchMerged_scopesCoverageToResolvedSources(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	hour := time.Date(2020, 1, 2, 3, 0, 0, 0, time.UTC)
	if _, err := db.ExecContext(ctx,
		`INSERT INTO archive_state (partition_name, bintrail_id, local_path)
		 VALUES (?, NULL, '/archives/orphan/x.parquet')`,
		indexer.PartitionName(hour)); err != nil {
		t.Fatalf("seed archive_state: %v", err)
	}

	since, until := hour, hour.Add(30*time.Minute)
	_, plan, err := query.FetchMerged(ctx, db, query.New(db), query.FetchMergedOptions{
		Opts:      query.Options{Since: &since, Until: &until, Limit: 10},
		DBName:    dbName,
		AllowGaps: true,
		// A fetcher that opens nothing: the point is the PLAN, and the
		// orphan row resolves to no source, so nothing is ever fetched.
		ArchiveFetcher: func(context.Context, query.Options, string) ([]query.ResultRow, error) {
			return nil, nil
		},
	})
	if err != nil {
		t.Fatalf("FetchMerged: %v", err)
	}
	if plan == nil {
		t.Fatal("planner did not run; this test would prove nothing")
	}
	// The row cannot describe an archive this read opens, so the hour is a
	// gap. Unscoped — the pre-#1232 behaviour, and what a `nil` slipped in at
	// the call site restores — it counted as coverage and this is 0.
	if len(plan.GapHours) != 1 {
		t.Errorf("got %d gap hour(s), want 1: an archive_state row this read can never open is being counted as coverage",
			len(plan.GapHours))
	}
}
