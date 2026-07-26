//go:build integration

// Persistence coverage for the capture-skip counters (#1034): the tallies the
// StreamParser records must survive a daemon restart via the checkpoint upsert
// (saveCheckpoint) + re-seed (loadCaptureSkips/Seed), and `status` must render
// the DEGRADED verdict from the SAME row through its own production read path
// (status.LoadStreamState → WriteStatus/WriteJSON) — the end-to-end pin for
// "sustained skipping is no longer invisible".
package streamrun

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

func TestIntegrationCaptureSkipsPersistAndSurviveRestart(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// ── First daemon run: skips recorded, checkpoint persisted ────────────
	skips := parser.NewSkipCounters(nil)
	skips.RecordSkip(parser.SkipColumnCountMismatch)
	skips.RecordSkip(parser.SkipColumnCountMismatch)
	skips.RecordSkip(parser.SkipStatementFormatDML)

	state := &streamState{
		mode:       "position",
		binlogFile: "binlog.000001",
		binlogPos:  4,
		serverID:   100,
		skips:      skips,
	}
	if err := saveCheckpoint(db, state); err != nil {
		t.Fatalf("saveCheckpoint: %v", err)
	}

	// ── Restart: the persisted document seeds a fresh counter set ─────────
	raw, err := loadCaptureSkips(db)
	if err != nil {
		t.Fatalf("loadCaptureSkips: %v", err)
	}
	restarted := parser.NewSkipCounters(nil)
	if err := restarted.Seed(raw); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	if got := restarted.Total(); got != 3 {
		t.Fatalf("counters did not survive restart: Total = %d, want 3", got)
	}

	// The restarted daemon keeps counting monotonically and re-persists.
	restarted.RecordSkip(parser.SkipColumnCountMismatch)
	state.skips = restarted
	if err := saveCheckpoint(db, state); err != nil {
		t.Fatalf("saveCheckpoint after restart: %v", err)
	}
	raw2, err := loadCaptureSkips(db)
	if err != nil {
		t.Fatalf("loadCaptureSkips: %v", err)
	}
	reread := parser.NewSkipCounters(nil)
	if err := reread.Seed(raw2); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	if got := reread.Total(); got != 4 {
		t.Fatalf("post-restart counts not monotonic: Total = %d, want 4", got)
	}

	// ── A skip-less checkpoint writer must PRESERVE the counters ──────────
	// (--reset's fresh state and the gap auto-advance stamp build bare
	// streamStates with skips == nil; the upsert's COALESCE keeps the column.)
	bare := &streamState{mode: "position", binlogFile: "binlog.000002", binlogPos: 4, serverID: 100}
	if err := saveCheckpoint(db, bare); err != nil {
		t.Fatalf("saveCheckpoint (nil skips): %v", err)
	}
	raw3, err := loadCaptureSkips(db)
	if err != nil {
		t.Fatalf("loadCaptureSkips: %v", err)
	}
	if raw3 != raw2 {
		t.Fatalf("a nil-skips checkpoint wiped the counters:\n was: %s\n now: %s", raw2, raw3)
	}

	// ── `status` renders DEGRADED from the same row (production read path) ──
	ctx := context.Background()
	stream, err := status.LoadStreamState(ctx, db)
	if err != nil {
		t.Fatalf("status.LoadStreamState: %v", err)
	}
	if stream == nil || !stream.CaptureSkips.Valid {
		t.Fatalf("status did not load capture_skips: %+v", stream)
	}
	var text bytes.Buffer
	status.WriteStatus(&text, nil, nil, nil, nil, nil, stream)
	for _, want := range []string{"Capture health", "DEGRADED", "column_count_mismatch"} {
		if !strings.Contains(text.String(), want) {
			t.Errorf("status text missing %q:\n%s", want, text.String())
		}
	}
	var js bytes.Buffer
	if err := status.WriteStatusJSON(&js, nil, nil, nil, nil, nil, stream); err != nil {
		t.Fatalf("WriteStatusJSON: %v", err)
	}
	if !strings.Contains(js.String(), `"status": "degraded"`) {
		t.Errorf("status JSON missing degraded capture_health:\n%s", js.String())
	}
}
