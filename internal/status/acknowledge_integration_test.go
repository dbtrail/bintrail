//go:build integration

package status_test

import (
	"context"
	"errors"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The acknowledgement (#1314) is verdict-tested against fixtures, and fixtures
// cannot cover the half that actually matters here: that the column exists on a
// freshly created index, that the write lands in it, and that LoadStreamState
// reads it back. Delete loadCaptureSkipsAck and every fixture test still passes,
// because they set the field by hand. This one goes to a real database.

func TestAcknowledgeCaptureSkips_RoundTrip(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, _ := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q, args...); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	exec("INSERT INTO stream_state (id, mode, server_id, last_checkpoint) VALUES (1, 'gtid', 7, UTC_TIMESTAMP())")

	// Nothing recorded yet: refused rather than stamped. An acknowledgement
	// written over an empty ledger would cover the NEXT skip.
	if _, err := status.AcknowledgeCaptureSkips(ctx, db, -1, time.Now()); !errors.Is(err, status.ErrNothingToAcknowledge) {
		t.Fatalf("acknowledging an empty ledger: got %v, want ErrNothingToAcknowledge", err)
	}

	exec(`UPDATE stream_state SET capture_skips = ? WHERE id = 1`,
		`{"column_count_mismatch":{"count":3,"last_at":"2026-08-04T10:00:00Z"}}`)

	// The stale-render guard: the caller says it saw 2, the ledger holds 3.
	if _, err := status.AcknowledgeCaptureSkips(ctx, db, 2, time.Now()); !errors.Is(err, status.ErrAcknowledgeStale) {
		t.Fatalf("acknowledging a stale view: got %v, want ErrAcknowledgeStale", err)
	}
	// Nothing was written on the refusal — a rejected acknowledgement must not
	// half-land, or the next read reports a record as retired that nobody
	// agreed to retire.
	if st, err := status.LoadStreamState(ctx, db); err != nil {
		t.Fatalf("LoadStreamState: %v", err)
	} else if ack := st.ParseCaptureSkipsAck(); len(ack) != 0 {
		t.Fatalf("a refused acknowledgement wrote %v", ack)
	}

	at := time.Date(2026, 8, 11, 20, 14, 30, 500, time.UTC)
	ackd, err := status.AcknowledgeCaptureSkips(ctx, db, 3, at)
	if err != nil {
		t.Fatalf("AcknowledgeCaptureSkips: %v", err)
	}
	if ackd.Total != 3 || len(ackd.Reasons) != 1 || ackd.Reasons[0] != "column_count_mismatch" {
		t.Errorf("acknowledgement report = %+v, want 3 events of column_count_mismatch", ackd)
	}

	st, err := status.LoadStreamState(ctx, db)
	if err != nil {
		t.Fatalf("LoadStreamState: %v", err)
	}
	skips, ok := st.ParseCaptureSkips()
	if !ok {
		t.Fatal("capture_skips became unreadable after acknowledging")
	}
	// The tally survives. Erasing it was the old advice and the thing this
	// feature exists to stop: the counts ARE the evidence of loss.
	if skips["column_count_mismatch"].Count != 3 {
		t.Errorf("acknowledging changed the tally: %+v", skips)
	}
	ack := st.ParseCaptureSkipsAck()
	if !status.CaptureSkipsAcknowledged(skips, ack) {
		t.Fatalf("not acknowledged after a successful acknowledgement: skips=%+v ack=%+v", skips, ack)
	}
	// Seconds resolution is what the UI prints; sub-second precision here would
	// only differ from what every surface shows.
	if got := status.CaptureSkipsAcknowledgedAt(skips, ack); !got.Equal(at.Truncate(time.Second)) {
		t.Errorf("acknowledged at %s, want %s", got, at.Truncate(time.Second))
	}

	// A LATER skip re-arms it with no operator action. This is the invariant
	// that makes "Mark as read" safe to offer at all, and it is the one a
	// future refactor is most likely to break.
	exec(`UPDATE stream_state SET capture_skips = ? WHERE id = 1`,
		`{"column_count_mismatch":{"count":4,"last_at":"2026-08-12T10:00:00Z"}}`)
	st, err = status.LoadStreamState(ctx, db)
	if err != nil {
		t.Fatalf("LoadStreamState: %v", err)
	}
	skips, _ = st.ParseCaptureSkips()
	if status.CaptureSkipsAcknowledged(skips, st.ParseCaptureSkipsAck()) {
		t.Error("a skip AFTER the acknowledgement stayed silent — the acknowledgement is muting new loss")
	}
}

// TestAcknowledgeCaptureSkips_MissingColumn pins the sentinel the console
// depends on: it runs no DDL on a registry index, so it must be able to name
// the CLI command that migrates instead of surfacing a driver error.
func TestAcknowledgeCaptureSkips_MissingColumn(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, _ := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE stream_state DROP COLUMN capture_skips_ack`); err != nil {
		t.Fatalf("simulate a pre-#1314 index: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO stream_state (id, mode, server_id, last_checkpoint, capture_skips)
		 VALUES (1, 'gtid', 7, UTC_TIMESTAMP(), '{"no_resolver":{"count":2,"last_at":"2026-08-04T10:00:00Z"}}')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := status.AcknowledgeCaptureSkips(ctx, db, -1, time.Now()); !errors.Is(err, status.ErrAckColumnMissing) {
		t.Fatalf("got %v, want ErrAckColumnMissing", err)
	}
	// And EnsureSchema adds it, which is what the error tells the operator to
	// run — an instruction nobody verified would be worse than none.
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	if _, err := status.AcknowledgeCaptureSkips(ctx, db, -1, time.Now()); err != nil {
		t.Fatalf("acknowledging after the documented migration: %v", err)
	}
}
