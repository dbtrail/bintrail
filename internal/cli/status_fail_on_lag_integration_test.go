//go:build integration

package cli

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestRunStatus_failOnLag_exitCode proves the freshness alertable-exit contract
// end to end — the WIRING, which the pure FreshnessStatus unit tests cannot
// reach. Every assertion here fails if runStatus stops consulting the verdict,
// while every unit test in internal/status stays green.
//
// It also pins the two properties that decide whether an operator keeps the flag
// switched on: a healthy stream never alerts, and the report still reaches
// stdout (as valid JSON under --format json) BEFORE the non-zero exit fires.
func TestRunStatus_failOnLag_exitCode(t *testing.T) {
	ctx := context.Background()
	db, name := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}

	saved := struct {
		dsn, format, baselineDir string
		failOnGap                bool
		failOnLag                time.Duration
	}{stIndexDSN, stFormat, stBaselineDir, stFailOnGap, stFailOnLag}
	t.Cleanup(func() {
		stIndexDSN, stFormat, stBaselineDir, stFailOnGap, stFailOnLag =
			saved.dsn, saved.format, saved.baselineDir, saved.failOnGap, saved.failOnLag
	})
	stIndexDSN = testutil.IntegrationDSN(name)
	stFormat = "text"
	stBaselineDir = ""
	stFailOnGap = false
	statusCmd.SetContext(ctx)

	seed := func(t *testing.T, checkpointSQL, lastEventSQL string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, "DELETE FROM stream_state WHERE id = 1"); err != nil {
			t.Fatalf("clear stream_state: %v", err)
		}
		if _, err := db.ExecContext(ctx,
			`INSERT INTO stream_state (id, mode, server_id, last_checkpoint, last_event_time)
			 VALUES (1, 'gtid', 7, `+checkpointSQL+`, `+lastEventSQL+`)`); err != nil {
			t.Fatalf("seed stream_state: %v", err)
		}
	}

	// A stalled daemon: the checkpoint ticker runs with or WITHOUT traffic, so a
	// checkpoint an hour old means the daemon, not a quiet workload.
	t.Run("stalled checkpoint alerts", func(t *testing.T) {
		seed(t, "UTC_TIMESTAMP() - INTERVAL 1 HOUR", "UTC_TIMESTAMP()")
		stFailOnLag = 15 * time.Minute
		out := captureStdout(t, func() {
			err := runStatus(statusCmd, nil)
			if err == nil {
				t.Fatal("want a non-zero exit for a stalled checkpoint, got nil")
			}
			if !strings.Contains(err.Error(), "STALLED") {
				t.Errorf("want a stall error, got: %v", err)
			}
		})
		if !strings.Contains(out, "STALLED") {
			t.Errorf("the report must be written before the non-zero exit, got stdout:\n%s", out)
		}
	})

	// The flag unset must never change the exit code — break-nothing for every
	// existing script that already runs status in cron.
	t.Run("flag unset never alerts", func(t *testing.T) {
		seed(t, "UTC_TIMESTAMP() - INTERVAL 1 HOUR", "UTC_TIMESTAMP() - INTERVAL 1 HOUR")
		stFailOnLag = 0
		captureStdout(t, func() {
			if err := runStatus(statusCmd, nil); err != nil {
				t.Errorf("no --fail-on-lag must exit 0 even when stalled, got: %v", err)
			}
		})
	})

	// A healthy stream must not cry wolf, or the flag gets switched off and
	// protects nothing.
	t.Run("current stream never alerts", func(t *testing.T) {
		seed(t, "UTC_TIMESTAMP()", "UTC_TIMESTAMP()")
		stFailOnLag = 15 * time.Minute
		captureStdout(t, func() {
			if err := runStatus(statusCmd, nil); err != nil {
				t.Errorf("a current stream must not alert, got: %v", err)
			}
		})
	})

	// Checkpointing fine, but the newest indexed event is past the threshold.
	// The error must NAME the ambiguity rather than assert "you are lagging":
	// offline this is indistinguishable from a source nobody wrote to.
	t.Run("newest event past the threshold alerts and admits the ambiguity", func(t *testing.T) {
		seed(t, "UTC_TIMESTAMP()", "UTC_TIMESTAMP() - INTERVAL 30 MINUTE")
		stFailOnLag = 15 * time.Minute
		captureStdout(t, func() {
			err := runStatus(statusCmd, nil)
			if err == nil {
				t.Fatal("want a non-zero exit for an event older than the threshold, got nil")
			}
			if !strings.Contains(err.Error(), "over the 15m0s threshold") {
				t.Errorf("want the threshold named in the error, got: %v", err)
			}
			if !strings.Contains(err.Error(), "idleness, not lag") {
				t.Errorf("the error must admit that a quiet source reads identically, got: %v", err)
			}
		})
	})

	// Fail CLOSED: no stream row at all means the verdict is not evaluable. An
	// exit 0 there is the cry-wolf inversion — "couldn't check" reading as "fine".
	t.Run("unevaluable verdict fails closed", func(t *testing.T) {
		if _, err := db.ExecContext(ctx, "DELETE FROM stream_state WHERE id = 1"); err != nil {
			t.Fatalf("clear stream_state: %v", err)
		}
		stFailOnLag = 15 * time.Minute
		captureStdout(t, func() {
			err := runStatus(statusCmd, nil)
			if err == nil {
				t.Fatal("want a non-zero exit when the verdict is not evaluable, got nil")
			}
			if !strings.Contains(err.Error(), "failing closed") {
				t.Errorf("want a fail-closed error, got: %v", err)
			}
		})
	})

	// The JSON report must carry the verdict and reach stdout intact before the
	// non-zero exit — the `status --format json --fail-on-lag | jq` use case.
	t.Run("json report carries the verdict before exiting", func(t *testing.T) {
		seed(t, "UTC_TIMESTAMP() - INTERVAL 1 HOUR", "UTC_TIMESTAMP() - INTERVAL 1 HOUR")
		stFormat = "json"
		stFailOnLag = 15 * time.Minute
		t.Cleanup(func() { stFormat = "text" })

		var runErr error
		out := captureStdout(t, func() { runErr = runStatus(statusCmd, nil) })
		if runErr == nil {
			t.Error("want a non-zero exit, got nil")
		}
		var parsed struct {
			Stream struct {
				Freshness struct {
					Status            string `json:"status"`
					CheckpointAgeSecs *int64 `json:"checkpoint_age_seconds"`
				} `json:"freshness"`
			} `json:"stream"`
		}
		if err := json.Unmarshal([]byte(out), &parsed); err != nil {
			t.Fatalf("stdout under --format json must be valid JSON: %v\n%s", err, out)
		}
		if parsed.Stream.Freshness.Status != "stalled" {
			t.Errorf("want freshness.status=stalled, got %q\n%s", parsed.Stream.Freshness.Status, out)
		}
		if parsed.Stream.Freshness.CheckpointAgeSecs == nil || *parsed.Stream.Freshness.CheckpointAgeSecs < 3000 {
			t.Errorf("want checkpoint_age_seconds ≈3600, got %v", parsed.Stream.Freshness.CheckpointAgeSecs)
		}
	})
}
