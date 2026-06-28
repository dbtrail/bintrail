//go:build integration

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// captureStdout redirects os.Stdout for the duration of fn and returns what was
// written. A goroutine drains the pipe so a larger report can't deadlock on a
// full pipe buffer before fn returns.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		done <- buf.String()
	}()
	fn()
	_ = w.Close()
	os.Stdout = old
	return <-done
}

// TestRunStatus_failOnGap_exitCode proves the alertable-exit contract end to end.
// With --fail-on-gap, status exits non-zero on a stamped gap AND when it cannot
// confirm the gap state (fails closed); without the flag it keeps exiting 0
// (break-nothing); a healthy stream never alerts. It also pins the JSON path: the
// full report reaches stdout as valid JSON before the non-zero exit fires (the
// CI/cron `status --format json --fail-on-gap | jq` use case).
func TestRunStatus_failOnGap_exitCode(t *testing.T) {
	ctx := context.Background()
	db, name := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	// A stream that hit an unfillable gap (gap_lost_at stamped).
	if _, err := db.ExecContext(ctx,
		`INSERT INTO stream_state (id, mode, server_id, last_checkpoint, gap_lost_at, gap_lost_detail)
		 VALUES (1, 'gtid', 7, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'unfillable binlog gap')`); err != nil {
		t.Fatalf("seed gap row: %v", err)
	}

	saved := struct {
		dsn, format, baselineDir string
		failOnGap                bool
	}{stIndexDSN, stFormat, stBaselineDir, stFailOnGap}
	t.Cleanup(func() {
		stIndexDSN, stFormat, stBaselineDir, stFailOnGap = saved.dsn, saved.format, saved.baselineDir, saved.failOnGap
	})
	stIndexDSN = testutil.IntegrationDSN(name)
	stFormat = "text"
	stBaselineDir = ""
	statusCmd.SetContext(ctx)

	// --fail-on-gap ON + a stamped gap → non-zero exit.
	stFailOnGap = true
	if out := captureStdout(t, func() {
		if err := runStatus(statusCmd, nil); err == nil {
			t.Error("want a non-nil error (non-zero exit) with --fail-on-gap and a stamped gap, got nil")
		} else if !strings.Contains(err.Error(), "events permanently lost") {
			t.Errorf("want a continuity error mentioning the loss, got: %v", err)
		}
	}); !strings.Contains(out, "GAP LOST") {
		t.Errorf("the report must still be written before the non-zero exit, got stdout:\n%s", out)
	}

	// Flag OFF (default) + same gap → exit 0 (break-nothing for existing scripts).
	stFailOnGap = false
	captureStdout(t, func() {
		if err := runStatus(statusCmd, nil); err != nil {
			t.Errorf("default (no --fail-on-gap) must still exit 0 even with a gap, got: %v", err)
		}
	})

	// --format json + --fail-on-gap: the full report must reach stdout as valid
	// JSON (continuity.status="gap_lost") BEFORE the non-zero exit fires.
	stFormat = "json"
	stFailOnGap = true
	var jsonErr error
	jsonOut := captureStdout(t, func() { jsonErr = runStatus(statusCmd, nil) })
	if jsonErr == nil {
		t.Error("want a non-zero exit for --format json --fail-on-gap with a gap, got nil")
	}
	var parsed struct {
		Stream struct {
			Continuity struct {
				Status string `json:"status"`
			} `json:"continuity"`
		} `json:"stream"`
	}
	if err := json.Unmarshal([]byte(jsonOut), &parsed); err != nil {
		t.Errorf("stdout under --format json must be valid JSON, got error %v\n%s", err, jsonOut)
	} else if parsed.Stream.Continuity.Status != "gap_lost" {
		t.Errorf("want continuity.status=gap_lost in the JSON report, got %q\n%s", parsed.Stream.Continuity.Status, jsonOut)
	}
	stFormat = "text"

	// Healthy stream + --fail-on-gap ON → exit 0 (no false alarm).
	if _, err := db.ExecContext(ctx,
		"UPDATE stream_state SET gap_lost_at=NULL, gap_lost_detail=NULL WHERE id=1"); err != nil {
		t.Fatalf("clear gap: %v", err)
	}
	stFailOnGap = true
	captureStdout(t, func() {
		if err := runStatus(statusCmd, nil); err != nil {
			t.Errorf("a healthy stream must not alert even with --fail-on-gap, got: %v", err)
		}
	})

	// No stream row at all + --fail-on-gap ON → fail closed (cannot confirm).
	if _, err := db.ExecContext(ctx, "DELETE FROM stream_state WHERE id=1"); err != nil {
		t.Fatalf("delete stream row: %v", err)
	}
	captureStdout(t, func() {
		if err := runStatus(statusCmd, nil); err == nil {
			t.Error("want a non-zero exit when --fail-on-gap cannot confirm gap state (no stream row), got nil")
		} else if !strings.Contains(err.Error(), "could not confirm gap state") {
			t.Errorf("want a fail-closed 'could not confirm' error, got: %v", err)
		}
	})
}
