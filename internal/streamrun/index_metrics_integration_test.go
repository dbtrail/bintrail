//go:build integration

package streamrun

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestScrapeIndexMetrics_endToEnd drives the real scrape path (CollectStatus +
// query.Plan + Set) against a live index — the path the unit tests don't reach.
// It guards the regression where scrapeIndexMetrics called query.Plan(nil,nil)
// (which returns a nil plan) and dereferenced it: with a real event present,
// OldestEvent is non-zero so Plan IS invoked with a concrete range, and the
// scrape must complete without panicking.
func TestScrapeIndexMetrics_endToEnd(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	ts := time.Now().UTC().Add(-1 * time.Hour).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, nil,
		"shop", "orders", 1, "1", nil, nil, []byte(`{"id":1}`))

	m := observe.IndexForSource("e2e-src")
	// A panic here (the nil-plan deref) would crash the test; reaching the next
	// line is the regression assertion. CollectStatus + query.Plan both run
	// against the real schema with a non-zero OldestEvent.
	scrapeIndexMetrics(context.Background(), db, dbName, m)
}
