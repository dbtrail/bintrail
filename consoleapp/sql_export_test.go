package consoleapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
)

// TestSQLExportSingleFlight pins that the export shares the per-server lock
// with dump/refresh/restore in BOTH directions.
func TestSQLExportSingleFlight(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), "")
	req := console.SQLExportRequest{ServerID: "srv1", ServerName: "wp",
		IndexDSN: "i:p@tcp(h:3306)/idx", BaselineSrc: t.TempDir(),
		At: time.Date(2026, 6, 10, 11, 0, 0, 0, time.UTC)}

	sup.mu.Lock()
	sup.refreshes["srv1"] = &console.BaselineStatus{State: "running"}
	sup.mu.Unlock()
	if err := sup.TriggerSQLExport(req); !errors.Is(err, console.ErrBaselineRunning) {
		t.Fatalf("export during refresh: err = %v, want ErrBaselineRunning", err)
	}

	sup.mu.Lock()
	sup.refreshes["srv1"] = &console.BaselineStatus{State: "succeeded"}
	sup.exports["srv1"] = &console.BaselineStatus{State: "running"}
	busy := sup.busyLocked("srv1")
	sup.mu.Unlock()
	if !busy {
		t.Fatal("a running export must block the other kinds (busyLocked)")
	}
}

// TestSQLExportDirGating: the download seam refuses until the status says
// succeeded AND the directory's completeness marker agrees.
func TestSQLExportDirGating(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), "")
	if _, ok := sup.SQLExportDir("srv1"); ok {
		t.Fatal("no build ever ran: ok must be false")
	}
	sup.mu.Lock()
	sup.exports["srv1"] = &console.BaselineStatus{State: "failed"}
	sup.mu.Unlock()
	if _, ok := sup.SQLExportDir("srv1"); ok {
		t.Fatal("failed build: ok must be false")
	}
	sup.mu.Lock()
	sup.exports["srv1"] = &console.BaselineStatus{State: "succeeded"}
	sup.mu.Unlock()
	dir := sup.sqlExportDir("srv1")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// A succeeded status over a dir still wearing _INCOMPLETE (the fold's
	// crash-safety marker) must not hand out a partial dump.
	if err := os.WriteFile(filepath.Join(dir, baseline.IncompleteMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok := sup.SQLExportDir("srv1"); ok {
		t.Fatal("_INCOMPLETE dir: ok must be false even with a succeeded status")
	}
	if err := os.Remove(filepath.Join(dir, baseline.IncompleteMarker)); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteSuccessMarker(dir); err != nil {
		t.Fatal(err)
	}
	got, ok := sup.SQLExportDir("srv1")
	if !ok || got != dir {
		t.Fatalf("complete build: got (%q,%v), want (%q,true)", got, ok, dir)
	}
}

// TestSQLExportRun_failure: a build over a store with no usable snapshot
// fails with words about the missing backup, publishes nothing, and records
// a history row with the error and no snapshot time (it publishes none).
func TestSQLExportRun_failure(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), "")
	h, err := console.OpenBaselineHistory(filepath.Join(t.TempDir(), "h.json"))
	if err != nil {
		t.Fatal(err)
	}
	sup.history = h
	req := console.SQLExportRequest{ServerID: "srv1", ServerName: "wp",
		IndexDSN: "i:p@tcp(h:3306)/idx", BaselineSrc: t.TempDir(),
		At: time.Date(2026, 6, 10, 11, 0, 0, 0, time.UTC)}
	sup.mu.Lock()
	sup.exports["srv1"] = &console.BaselineStatus{State: "running", At: "2026-06-10T11:00:00Z"}
	sup.mu.Unlock()
	sup.runSQLExport(req)

	st := sup.SQLExportStatus("srv1")
	if st.State != "failed" || !strings.Contains(st.LastError, "no backup exists at or before") {
		t.Fatalf("status = %+v, want failed with the no-backup message", st)
	}
	if st.At != "2026-06-10T11:00:00Z" {
		t.Fatalf("At = %q, the requested instant must survive the failure", st.At)
	}
	if _, ok := sup.SQLExportDir("srv1"); ok {
		t.Fatal("a failed build must not be downloadable")
	}
	var rec *console.BaselineRunRecord
	for _, r := range h.List("srv1") {
		if r.Kind == console.BaselineRunSQLExport {
			c := r
			rec = &c
		}
	}
	if rec == nil || rec.Error == "" || rec.SnapshotTime != "" {
		t.Fatalf("history record = %+v, want the error and no snapshot time", rec)
	}
}

// TestSQLExportTrigger_stampsInstant: the running status carries the chosen
// instant from the very first poll (the UI labels the run region with it).
func TestSQLExportTrigger_stampsInstant(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), "")
	at := time.Date(2026, 6, 10, 11, 0, 0, 0, time.UTC)
	req := console.SQLExportRequest{ServerID: "srv1", ServerName: "wp",
		IndexDSN: "i:p@tcp(h:3306)/idx", BaselineSrc: t.TempDir(), At: at}
	if err := sup.TriggerSQLExport(req); err != nil {
		t.Fatal(err)
	}
	st := sup.SQLExportStatus("srv1")
	if st.At != "2026-06-10T11:00:00Z" || st.Since == "" {
		t.Fatalf("status right after trigger = %+v, want the instant and a start stamp", st)
	}
	// The empty store makes the goroutine fail fast without touching the DSN;
	// wait for it so the test does not leak a running fold.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if sup.SQLExportStatus("srv1").State != "running" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("export never settled")
}
