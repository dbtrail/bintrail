package query

import (
	"context"
	"testing"
	"time"
)

// A snapshot's time comes from its DIRECTORY, not from the file's
// bintrail.snapshot_timestamp footer, and this pins the preference rather than
// the parser (internal/snapshotdir covers that).
//
// It matters because a table whose delta window held no events is published by
// carrying its previous Parquet file forward unchanged, so its footer keeps the
// OLDER timestamp while it sits in the newer snapshot's directory. Reading the
// footer first dated two files in the SAME snapshot differently, purely by
// whether each table happened to be cold, and a --since past the stale value
// dropped the cold table's baseline rows with a log line and no error.
//
// The nil *sql.DB is the assertion, not an accident: the directory branch must
// answer without consulting DuckDB at all. Deleting it sends this straight into
// the footer path, which cannot survive a nil handle.
func TestReadSnapshotTimestamp_prefersTheDirectoryOverTheFooter(t *testing.T) {
	want := time.Date(2026, 8, 23, 14, 50, 30, 0, time.UTC)
	for _, path := range []string{
		"/var/lib/bintrail/baselines/2026-08-23T14-50-30Z/shop/orders.parquet",
		"s3://bucket/baselines/2026-08-23T14-50-30Z/shop/orders.parquet",
	} {
		got, err := readSnapshotTimestamp(context.Background(), nil, path)
		if err != nil {
			t.Errorf("%s: %v (the directory branch must answer without a DB handle)", path, err)
			continue
		}
		if !got.Equal(want) {
			t.Errorf("%s: got %v, want %v", path, got, want)
		}
	}
}

// The footer fallback still earns its place: a file read from outside a
// snapshot layout has no directory to ask, so the call must go THROUGH to the
// footer reader rather than inventing a time.
//
// A nil *sql.DB panics inside that reader rather than returning an error, which
// is fine for the observation being made here: reaching the panic is proof the
// directory branch declined, and NOT reaching it would mean a path with no
// snapshot directory had somehow been dated from one.
func TestReadSnapshotTimestamp_fallsBackWhenThereIsNoSnapshotDirectory(t *testing.T) {
	for _, path := range []string{
		"/tmp/somewhere/orders.parquet",
		"orders.parquet",
	} {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("%s: answered without reaching the footer reader, so a path with no "+
						"snapshot directory was dated from one", path)
				}
			}()
			_, _ = readSnapshotTimestamp(context.Background(), nil, path)
		}()
	}
}
