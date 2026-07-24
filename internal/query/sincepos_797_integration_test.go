//go:build integration

package query

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestFetch_sincePos pins the basic exact-position lower bound, mirroring
// TestFetch_untilPos but inverted: events strictly before the anchor position
// are excluded, the event AT the anchor position is included (inclusive "at
// or after", matching SHOW MASTER STATUS's "next write position" semantics —
// see Options.SincePos), and a later file is included regardless of position.
func TestFetch_sincePos(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	ts := "2026-02-19 14:00:00"
	// Anchor = binlog.000001 @ 200.
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, nil, "mydb", "orders", 1, "1", nil, nil, []byte(`{"id":1}`)) // ends before anchor pos (excluded)
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts, nil, "mydb", "orders", 1, "2", nil, nil, []byte(`{"id":2}`)) // starts == anchor (included)
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, ts, nil, "mydb", "orders", 1, "3", nil, nil, []byte(`{"id":3}`)) // starts > anchor (included)
	testutil.InsertEvent(t, db, "binlog.000002", 50, 150, ts, nil, "mydb", "orders", 1, "4", nil, nil, []byte(`{"id":4}`))  // later file (included regardless of pos)

	e := New(db)
	rows, err := e.Fetch(context.Background(), Options{
		Schema: "mydb", Table: "orders",
		SincePos: &BinlogPos{File: "binlog.000001", Pos: 200},
		Limit:    100,
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows at-or-after the anchor, got %d", len(rows))
	}
	for _, r := range rows {
		if r.PKValues == "1" {
			t.Errorf("row before the anchor leaked: pk=%s %s @ %d", r.PKValues, r.BinlogFile, r.StartPos)
		}
	}
}

// TestFetch_sincePosRollover mirrors TestFetch_untilPosRollover for the lower
// bound: after mysql-bin.999999 the server continues with mysql-bin.1000000,
// and a plain lexicographic binlog_file comparison inverts. An anchor in the
// pre-rollover file must keep every post-rollover event (a "later file" by
// length, not lexicographic order).
func TestFetch_sincePosRollover(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	ts := "2026-02-19 14:00:00"
	testutil.InsertEvent(t, db, "mysql-bin.999998", 100, 200, ts, nil, "mydb", "orders", 1, "1", nil, nil, []byte(`{"id":1}`))  // anchor file, < pos (excluded)
	testutil.InsertEvent(t, db, "mysql-bin.999999", 200, 300, ts, nil, "mydb", "orders", 1, "2", nil, nil, []byte(`{"id":2}`))  // anchor file, == pos (included)
	testutil.InsertEvent(t, db, "mysql-bin.1000000", 100, 200, ts, nil, "mydb", "orders", 1, "3", nil, nil, []byte(`{"id":3}`)) // post-rollover (included; excluded by plain lexicographic compare)

	e := New(db)
	rows, err := e.Fetch(context.Background(), Options{
		Schema: "mydb", Table: "orders",
		SincePos: &BinlogPos{File: "mysql-bin.999999", Pos: 200},
		Limit:    100,
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows at-or-after the anchor (rollover-safe), got %d", len(rows))
	}
	for _, r := range rows {
		if r.PKValues == "1" {
			t.Errorf("row before the anchor leaked: pk=%s %s @ %d", r.PKValues, r.BinlogFile, r.StartPos)
		}
	}
}

// TestFetch_sincePos_timestampSkewNotLost is the #797 repro: a transaction
// whose statement executed BEFORE the baseline's wall-clock snapshot instant,
// but which committed (and so was durably logged, gaining a binlog position at
// or after the baseline's recorded anchor) just after it, must be picked up by
// a SincePos-anchored fetch even though its event_timestamp falls in an
// EARLIER hourly partition than the anchor's own hour.
//
// The event lands 10 minutes before an hour boundary — in the partition for
// the PRECEDING hour — while its binlog position is at the anchor exactly.
// Without SincePos (Since-only, the pre-#797 behavior — also what a fallback
// fetch for an older baseline with no recorded position still does), this
// row is silently dropped: both the coarse TO_SECONDS partition-pruning hint
// and the exact `event_timestamp >= ?` filter are keyed on the snapshot
// instant, which is chronologically AFTER this row's timestamp. With
// SincePos set, the row is correctly returned as a delta — proving neither
// the coarse hint's widened lookback nor the exact position filter drop it.
func TestFetch_sincePos_timestampSkewNotLost(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	anchorTime := time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{anchorTime.Add(-time.Hour), anchorTime})

	// The skewed event: executed 10 minutes before the anchor's hour, but its
	// binlog position is exactly at the anchor (a genuine post-snapshot delta
	// by position, despite the earlier timestamp).
	skewedTS := anchorTime.Add(-10 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, skewedTS, nil, dbName, "orders", 2, "1", nil,
		[]byte(`{"id":1,"status":"a"}`), []byte(`{"id":1,"status":"zzz"}`))

	e := New(db)
	sinceTime := anchorTime

	// Pre-#797 behavior (also the fallback for an older baseline with no
	// recorded position): Since alone must NOT find this row — pinning that
	// the failure mode this issue describes is real, and that the documented
	// fallback for baselines without a recorded position keeps this
	// limitation rather than silently claiming a precision it doesn't have.
	sinceOnly, err := e.Fetch(context.Background(), Options{
		Schema: dbName, Table: "orders",
		Since: &sinceTime,
		Limit: 100,
	})
	if err != nil {
		t.Fatalf("Fetch (Since-only): %v", err)
	}
	if len(sinceOnly) != 0 {
		t.Fatalf("precondition failed: Since-only unexpectedly found the skewed row (test no longer reproduces the #797 failure mode): %+v", sinceOnly)
	}

	// #797 fix: pairing Since with the baseline's exact recorded anchor
	// position must find the row despite its earlier timestamp/partition.
	withPos, err := e.Fetch(context.Background(), Options{
		Schema: dbName, Table: "orders",
		Since:    &sinceTime,
		SincePos: &BinlogPos{File: "binlog.000001", Pos: 200},
		Limit:    100,
	})
	if err != nil {
		t.Fatalf("Fetch (Since+SincePos): %v", err)
	}
	if len(withPos) != 1 {
		t.Fatalf("expected the skewed row to be found via SincePos, got %d rows: %+v", len(withPos), withPos)
	}
	if withPos[0].PKValues != "1" {
		t.Errorf("unexpected row: %+v", withPos[0])
	}
}
