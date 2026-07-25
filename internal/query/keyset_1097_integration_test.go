//go:build integration

package query

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestFetchMergedStream_pagingIsInvariantAgainstRealMySQL executes the keyset
// predicate against a real MySQL index (#1097). The unit tests prove the SQL is
// built correctly and that the stream loop pages correctly against a stub; only
// this one proves MySQL *evaluates* the predicate as intended — that a
// time.Time binds correctly against the DATETIME column, and that the
// hour-aligned TO_SECONDS partition-pruning hint never excludes a row it should
// have kept.
//
// The fixture puts several events in the SAME second on purpose. A
// timestamp-only cursor fails there in one of two silent ways — it re-returns
// the boundary second (duplicates) or skips its remainder (data loss) — and
// both would show up here as a mismatch against the unpaged read.
//
// Page sizes are swept including 1 (every page ends mid-second) and a size
// larger than the whole window (single page), so boundary handling is exercised
// at both extremes.
func TestFetchMergedStream_pagingIsInvariantAgainstRealMySQL(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	const schema, table = "shop", "orders"
	base := time.Date(2026, 3, 4, 9, 0, 0, 0, time.UTC)

	// 12 events: four share 09:00:01, four share 09:00:02, the rest are spread
	// out — including across an hour boundary so more than one partition and
	// more than one TO_SECONDS hint value are involved.
	stamps := []time.Time{
		base,
		base.Add(1 * time.Second), base.Add(1 * time.Second), base.Add(1 * time.Second), base.Add(1 * time.Second),
		base.Add(2 * time.Second), base.Add(2 * time.Second), base.Add(2 * time.Second), base.Add(2 * time.Second),
		base.Add(90 * time.Minute),
		base.Add(90*time.Minute + time.Second),
		base.Add(3 * time.Hour),
	}
	for i, ts := range stamps {
		pk := fmt.Sprint(i + 1)
		testutil.InsertEvent(t, db, "binlog.000001", uint64(100*(i+1)), uint64(100*(i+2)),
			ts.Format("2006-01-02 15:04:05"), nil,
			schema, table, uint8(event.EventInsert), pk,
			nil, nil, []byte(`{"id":`+pk+`}`))
	}

	until := base.Add(4 * time.Hour)
	opts := Options{Schema: schema, Table: table, Until: &until}

	engine := New(db)
	unpaged, err := engine.Fetch(context.Background(), opts)
	if err != nil {
		t.Fatalf("unpaged Fetch: %v", err)
	}
	if len(unpaged) != len(stamps) {
		t.Fatalf("unpaged read %d events, want %d", len(unpaged), len(stamps))
	}

	for _, batch := range []int{1, 2, 3, 5, 12, 50} {
		t.Run(fmt.Sprintf("batch=%d", batch), func(t *testing.T) {
			var paged []ResultRow
			if _, err := FetchMergedStream(context.Background(), db, engine, FetchMergedOptions{
				Opts:      opts,
				DBName:    dbName,
				NoArchive: true,
				AllowGaps: false,
			}, batch, func(page []ResultRow) error {
				if len(page) > batch {
					t.Errorf("page of %d exceeds the batch size %d", len(page), batch)
				}
				paged = append(paged, page...)
				return nil
			}); err != nil {
				t.Fatalf("FetchMergedStream: %v", err)
			}

			if len(paged) != len(unpaged) {
				t.Fatalf("paged read %d events, unpaged read %d — the keyset cut duplicated or dropped rows",
					len(paged), len(unpaged))
			}
			for i := range unpaged {
				if paged[i].EventID != unpaged[i].EventID {
					t.Fatalf("event %d: paged id %d, unpaged id %d — paging changed the result set",
						i, paged[i].EventID, unpaged[i].EventID)
				}
			}
		})
	}
}
