//go:build integration

package reconstruct

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestDecodeEventBinaries_decodesEpochAware is the load-bearing proof for #666:
// DecodeEventBinaries decodes the storage-side base64 of BLOB/TEXT event values,
// typing each column at the snapshot in effect at the event's timestamp. The
// VARCHAR→TEXT widening is the trap — an old VARCHAR value reached go-mysql as a
// Go string, so it was stored as a PLAIN string (never base64); decoding it
// against the latest (TEXT) snapshot would corrupt a plain value that happens to
// be valid base64 ("test"). The epoch-aware lookup leaves the old VARCHAR value
// alone and still decodes the new TEXT value.
func TestDecodeEventBinaries_decodesEpochAware(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Hour)
	t1 := now.Add(1 * time.Minute).Format("2006-01-02 15:04:05")
	t2 := now.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	// Epoch 1: body is VARCHAR (stored plain). Epoch 2: body widened to TEXT.
	testutil.InsertSnapshot(t, db, 1, t1, "myapp", "docs", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, t1, "myapp", "docs", "body", 2, "", "varchar", "YES")
	testutil.InsertSnapshot(t, db, 2, t2, "myapp", "docs", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 2, t2, "myapp", "docs", "body", 2, "", "text", "YES")

	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
	rawBlob := "\x00\xff\x7f"
	events := []query.ResultRow{
		// Epoch-1 event: VARCHAR value stored PLAIN. "test" is valid base64, so an
		// epoch-blind decode would corrupt it.
		{
			SchemaName: "myapp", TableName: "docs",
			EventTimestamp: now.Add(5 * time.Minute),
			RowAfter:       map[string]any{"id": float64(1), "body": "test"},
		},
		// Epoch-2 event: TEXT value stored base64.
		{
			SchemaName: "myapp", TableName: "docs",
			EventTimestamp: now.Add(15 * time.Minute),
			RowAfter:       map[string]any{"id": float64(2), "body": b64("decoded text")},
		},
	}
	DecodeEventBinaries(db, "myapp", "docs", events)

	if got := events[0].RowAfter["body"]; got != "test" {
		t.Errorf("epoch-1 VARCHAR plain value = %#v, want \"test\" untouched (epoch-blind decode would corrupt it)", got)
	}
	if got := events[1].RowAfter["body"]; got != "decoded text" {
		t.Errorf("epoch-2 TEXT value = %#v, want decoded \"decoded text\"", got)
	}

	// Single-epoch BLOB decode → raw []byte (arbitrary bytes survive).
	blobEvents := []query.ResultRow{{
		SchemaName: "myapp", TableName: "docs",
		EventTimestamp: now.Add(15 * time.Minute),
		RowAfter:       map[string]any{"id": float64(3), "payload": b64(rawBlob)},
	}}
	testutil.InsertSnapshot(t, db, 2, t2, "myapp", "docs", "payload", 3, "", "blob", "YES")
	DecodeEventBinaries(db, "myapp", "docs", blobEvents)
	if got, ok := blobEvents[0].RowAfter["payload"].([]byte); !ok || string(got) != rawBlob {
		t.Errorf("BLOB payload = %#v, want decoded []byte %q", blobEvents[0].RowAfter["payload"], rawBlob)
	}
}

// TestDecodeEventBinaries_noSnapshotsLeavesBase64 confirms the safe degradation:
// with no usable schema (no snapshots → no column typing), values pass through
// as the base64 they were stored as rather than being guessed at.
func TestDecodeEventBinaries_noSnapshotsLeavesBase64(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	b64 := base64.StdEncoding.EncodeToString([]byte("hello"))
	events := []query.ResultRow{{
		SchemaName: "myapp", TableName: "docs",
		EventTimestamp: time.Now().UTC(),
		RowAfter:       map[string]any{"id": float64(1), "body": b64},
	}}
	DecodeEventBinaries(db, "myapp", "docs", events)
	if got := events[0].RowAfter["body"]; got != b64 {
		t.Errorf("with no snapshots the value must pass through as base64, got %#v", got)
	}
}

// TestDecodeEventBinaries_tableAbsentLeavesBase64 pins the non-trivial safe
// degradation — the exact branch this PR's removed-`latest`-fallback decision
// protects. A snapshot EXISTS (so a latest resolver IS available to wrongly
// decode by), but the event's table is absent from it, so per-epoch typing
// fails (Resolve errors → nil binCols). The value must be LEFT as base64, never
// decoded by the latest schema — otherwise a plain value that is valid base64
// would be corrupted.
func TestDecodeEventBinaries_tableAbsentLeavesBase64(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Hour)
	ts := now.Add(1 * time.Minute).Format("2006-01-02 15:04:05")
	// A snapshot exists, but only for `docs` — not for the event's table.
	testutil.InsertSnapshot(t, db, 1, ts, "myapp", "docs", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, ts, "myapp", "docs", "body", 2, "", "text", "YES")

	b64 := base64.StdEncoding.EncodeToString([]byte("hello"))
	events := []query.ResultRow{{
		SchemaName: "myapp", TableName: "other", // absent from the snapshot
		EventTimestamp: now.Add(5 * time.Minute),
		RowAfter:       map[string]any{"id": float64(1), "body": b64},
	}}
	DecodeEventBinaries(db, "myapp", "other", events)
	if got := events[0].RowAfter["body"]; got != b64 {
		t.Errorf("table absent from snapshot: value must pass through as base64 (never decoded by the latest schema), got %#v", got)
	}
}
