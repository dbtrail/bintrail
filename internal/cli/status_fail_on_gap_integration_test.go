//go:build integration

package cli

import (
	"context"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestRunStatus_failOnGap_exitCode proves the alertable-exit contract end to end:
// with --fail-on-gap, a stream that permanently lost data makes `status` exit
// non-zero (for CI/cron), while the default (flag off) keeps exiting 0 —
// break-nothing for existing scripts. A healthy stream never alerts even with the
// flag on.
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
	if err := runStatus(statusCmd, nil); err == nil {
		t.Error("want a non-nil error (non-zero exit) with --fail-on-gap and a stamped gap, got nil")
	} else if !strings.Contains(err.Error(), "events permanently lost") {
		t.Errorf("want a continuity error mentioning the loss, got: %v", err)
	}

	// Flag OFF (default) + same gap → exit 0 (break-nothing for existing scripts).
	stFailOnGap = false
	if err := runStatus(statusCmd, nil); err != nil {
		t.Errorf("default (no --fail-on-gap) must still exit 0 even with a gap, got: %v", err)
	}

	// Healthy stream + --fail-on-gap ON → exit 0 (no false alarm).
	if _, err := db.ExecContext(ctx,
		"UPDATE stream_state SET gap_lost_at=NULL, gap_lost_detail=NULL WHERE id=1"); err != nil {
		t.Fatalf("clear gap: %v", err)
	}
	stFailOnGap = true
	if err := runStatus(statusCmd, nil); err != nil {
		t.Errorf("a healthy stream must not alert even with --fail-on-gap, got: %v", err)
	}
}
