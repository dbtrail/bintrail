//go:build integration

package cli

import (
	"context"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestRunStatus_ackCaptureSkips drives the whole loop through the REAL command:
// a skip tally fails --fail-on-gap forever, --ack-capture-skips retires it, and
// one more skipped event brings the failure back with no operator action.
//
// The exit-code half is the point. Before #1314 an operator whose cron went red
// on a monotonic tally had two options — hand-edit the column with the daemon
// stopped, destroying the loss record, or delete --fail-on-gap. An alert nobody
// can clear is an alert everybody removes, and this test is what pins that it
// can now be cleared WITHOUT the alert losing its teeth.
func TestRunStatus_ackCaptureSkips(t *testing.T) {
	ctx := context.Background()
	db, name := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO stream_state (id, mode, server_id, last_checkpoint, capture_skips)
		 VALUES (1, 'gtid', 7, UTC_TIMESTAMP(), '{"column_count_mismatch":{"count":3,"last_at":"2026-08-04T10:00:00Z"}}')`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	saved := struct {
		dsn, format, baselineDir string
		failOnGap, ack           bool
	}{stIndexDSN, stFormat, stBaselineDir, stFailOnGap, stAckCaptureSkips}
	t.Cleanup(func() {
		stIndexDSN, stFormat, stBaselineDir = saved.dsn, saved.format, saved.baselineDir
		stFailOnGap, stAckCaptureSkips = saved.failOnGap, saved.ack
	})
	stIndexDSN = testutil.IntegrationDSN(name)
	stFormat = "text"
	stBaselineDir = ""
	statusCmd.SetContext(ctx)

	// Unacknowledged: --fail-on-gap fails, and names the acknowledgement rather
	// than the old "clear the column with the daemon stopped" advice.
	stFailOnGap, stAckCaptureSkips = true, false
	captureStdout(t, func() {
		err := runStatus(statusCmd, nil)
		if err == nil {
			t.Fatal("an unacknowledged skip tally must fail --fail-on-gap")
		}
		if !strings.Contains(err.Error(), "--ack-capture-skips") {
			t.Errorf("the failure must name how to retire it, got: %v", err)
		}
		if strings.Contains(err.Error(), "clearing stream_state.capture_skips") {
			t.Errorf("the failure still tells operators to destroy the loss record: %v", err)
		}
	})

	// Acknowledge, and report as usual in the same run.
	stAckCaptureSkips = true
	out := captureStdout(t, func() {
		if err := runStatus(statusCmd, nil); err != nil {
			t.Fatalf("acknowledging must also clear the --fail-on-gap failure in the same run: %v", err)
		}
	})
	if !strings.Contains(out, "Acknowledged 3 skipped event(s)") {
		t.Errorf("no acknowledgement was reported to the operator:\n%s", out)
	}
	// The report printed AFTER the write must show the acknowledged state —
	// otherwise the operator's screen looks like the command did nothing, which
	// is the exact confusion this feature exists to end.
	if !strings.Contains(out, "ON RECORD") {
		t.Errorf("the report printed after acknowledging still reads as an active alarm:\n%s", out)
	}
	// The tally itself is still on screen: acknowledging is not forgetting.
	if !strings.Contains(out, "3 events skipped") {
		t.Errorf("the loss record vanished from the report:\n%s", out)
	}

	// Acknowledged: repeated runs stay green.
	stAckCaptureSkips = false
	captureStdout(t, func() {
		if err := runStatus(statusCmd, nil); err != nil {
			t.Errorf("an acknowledged tally must keep exiting 0: %v", err)
		}
	})

	// One more skipped event and the alert is back, untouched by the operator.
	if _, err := db.ExecContext(ctx,
		`UPDATE stream_state SET capture_skips = '{"column_count_mismatch":{"count":4,"last_at":"2026-08-12T10:00:00Z"}}' WHERE id = 1`); err != nil {
		t.Fatalf("record a later skip: %v", err)
	}
	captureStdout(t, func() {
		if err := runStatus(statusCmd, nil); err == nil {
			t.Error("a skip AFTER the acknowledgement did not re-fail --fail-on-gap; the acknowledgement is muting new loss")
		}
	})
}

// TestRunStatus_ackCaptureSkips_nothingRecorded pins that acknowledging a clean
// index is a reported no-op, not an error: an operator who runs this on a
// healthy system got what they wanted, and a non-zero exit there would be a
// cron failure invented out of nothing.
func TestRunStatus_ackCaptureSkips_nothingRecorded(t *testing.T) {
	ctx := context.Background()
	db, name := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO stream_state (id, mode, server_id, last_checkpoint, capture_skips)
		 VALUES (1, 'gtid', 7, UTC_TIMESTAMP(), '{}')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	saved := struct {
		dsn, format, baselineDir string
		failOnGap, ack           bool
	}{stIndexDSN, stFormat, stBaselineDir, stFailOnGap, stAckCaptureSkips}
	t.Cleanup(func() {
		stIndexDSN, stFormat, stBaselineDir = saved.dsn, saved.format, saved.baselineDir
		stFailOnGap, stAckCaptureSkips = saved.failOnGap, saved.ack
	})
	stIndexDSN = testutil.IntegrationDSN(name)
	stFormat, stBaselineDir, stFailOnGap, stAckCaptureSkips = "text", "", false, true
	statusCmd.SetContext(ctx)

	out := captureStdout(t, func() {
		if err := runStatus(statusCmd, nil); err != nil {
			t.Fatalf("acknowledging a clean ledger must not be an error: %v", err)
		}
	})
	if !strings.Contains(out, "Nothing to acknowledge") {
		t.Errorf("the no-op was not reported:\n%s", out)
	}
	// And nothing was stamped, so the NEXT skip is not pre-acknowledged.
	var ack []byte
	if err := db.QueryRowContext(ctx, "SELECT capture_skips_ack FROM stream_state WHERE id = 1").Scan(&ack); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if len(ack) != 0 {
		t.Errorf("acknowledging a clean ledger stamped %q — the next skip would land pre-acknowledged", ack)
	}
}
