package consoleapp

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
)

// TestRestoreSingleFlight pins that all three baseline job kinds share one
// per-server lock: a restore must not start while a dump or refresh writes
// the same store, and vice versa.
func TestRestoreSingleFlight(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), "")
	req := console.BaselineRestoreRequest{ServerID: "srv1", ServerName: "wp",
		IndexDSN: "i:p@tcp(h:3306)/idx", BaselineDir: t.TempDir(),
		At: time.Date(2026, 6, 10, 11, 0, 0, 0, time.UTC)}

	sup.mu.Lock()
	sup.refreshes["srv1"] = &console.BaselineStatus{State: "running"}
	sup.mu.Unlock()
	if err := sup.TriggerRestore(req); !errors.Is(err, console.ErrBaselineRunning) {
		t.Fatalf("restore during refresh: err = %v, want ErrBaselineRunning", err)
	}

	sup.mu.Lock()
	sup.refreshes["srv1"] = &console.BaselineStatus{State: "succeeded"}
	sup.restores["srv1"] = &console.BaselineStatus{State: "running"}
	busyForRefresh := sup.busyLocked("srv1")
	sup.mu.Unlock()
	if !busyForRefresh {
		t.Fatal("a running restore must block the other kinds (busyLocked)")
	}
}

// TestRecordRun pins the history side channel: success records the anchor as
// the snapshot time; failure records the error and NO snapshot time, because
// publication is all-or-nothing and a failed fold published nothing.
func TestRecordRun(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.json")
	h, err := console.OpenBaselineHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), "")
	sup.history = h
	at := time.Date(2026, 6, 10, 11, 0, 0, 0, time.UTC)

	sup.recordRun("srv1", "wp", console.BaselineRunRecord{
		Kind: console.BaselineRunRestore, StartedAt: "2026-06-10T11:00:00Z",
		SnapshotTime: publishedSnapshotTime(at, nil), Tables: 2}, nil)

	failErr := errors.New("capture gap")
	sup.recordRun("srv1", "wp", console.BaselineRunRecord{
		Kind: console.BaselineRunRefresh, StartedAt: "2026-06-10T12:00:00Z",
		SnapshotTime: publishedSnapshotTime(at, failErr)}, failErr)

	// Reload from disk: the records survived the round trip.
	h2, err := console.OpenBaselineHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	rec := h2.FindBySnapshot("srv1", "2026-06-10T11:00:00Z")
	if rec == nil || rec.Kind != console.BaselineRunRestore || rec.FinishedAt == "" || rec.Error != "" {
		t.Fatalf("success record = %+v", rec)
	}
	var failed *console.BaselineRunRecord
	for _, r := range h2.List("srv1") {
		if r.Kind == console.BaselineRunRefresh {
			failed = &r
		}
	}
	if failed == nil || !strings.Contains(failed.Error, "capture gap") || failed.SnapshotTime != "" {
		t.Fatalf("failed record = %+v, want error text and empty snapshot time", failed)
	}
}
