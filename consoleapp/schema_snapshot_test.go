package consoleapp

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// ─── Refresh schema snapshot: the supervisor (#1296) ─────────────────────────
//
// The reload is the load-bearing half. A stream holds its resolver in memory
// and swaps it only on a DDL event, so a snapshot written underneath a running
// stream changes nothing: without the reload this feature is a button that
// reports success and fixes the problem it was pressed for exactly never.

func testSnapshotSupervisor(t *testing.T, snap func(console.SchemaSnapshotRequest) (metadata.SnapshotStats, error), reload func(context.Context, string) error) *schemaSnapshotSupervisor {
	t.Helper()
	s := newSchemaSnapshotSupervisor(context.Background(), reload)
	s.snapshotFn = snap
	return s
}

func okSnapshot(console.SchemaSnapshotRequest) (metadata.SnapshotStats, error) {
	return metadata.SnapshotStats{SnapshotID: 7, TableCount: 12}, nil
}

func TestSchemaSnapshotSupervisor_reloadsTheStream(t *testing.T) {
	var reloaded []string
	s := testSnapshotSupervisor(t, okSnapshot, func(_ context.Context, id string) error {
		reloaded = append(reloaded, id)
		return nil
	})
	st, err := s.execute(console.SchemaSnapshotRequest{ServerID: "srv-1"})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(reloaded) != 1 || reloaded[0] != "srv-1" {
		t.Fatalf("the stream was not reloaded onto the new snapshot: %v", reloaded)
	}
	if !st.StreamReloaded {
		t.Error("stream_reloaded must be true once the stream restarted")
	}
	if st.State != "succeeded" || st.SnapshotID != 7 || st.Tables != 12 {
		t.Errorf("snapshot result not reported: %+v", st)
	}
}

// A failed reload is NOT a failed snapshot: the snapshot is durable, capture is
// simply still on the old one. Reporting it as a failure would hide that the
// snapshot worked; reporting it as a plain success would hide that nothing is
// fixed yet.
func TestSchemaSnapshotSupervisor_reloadFailureIsReportedNotSwallowed(t *testing.T) {
	s := testSnapshotSupervisor(t, okSnapshot, func(context.Context, string) error {
		return errors.New("stream did not stop within 15s")
	})
	st, err := s.execute(console.SchemaSnapshotRequest{ServerID: "srv-1"})
	if err != nil {
		t.Fatalf("a reload failure must not fail the job: %v", err)
	}
	if st.StreamReloaded {
		t.Error("stream_reloaded must stay false when the reload failed")
	}
	if !strings.Contains(st.ReloadError, "did not stop") {
		t.Errorf("reload_error = %q, want the reload's own error", st.ReloadError)
	}
	if st.State != "succeeded" {
		t.Errorf("state = %q; the snapshot itself succeeded", st.State)
	}
}

// With no reload hook the process does not supervise this stream. Saying so is
// the point: silence would read as "capture is on the new snapshot".
func TestSchemaSnapshotSupervisor_withoutReloadSaysCaptureIsUnchanged(t *testing.T) {
	s := testSnapshotSupervisor(t, okSnapshot, nil)
	st, _ := s.execute(console.SchemaSnapshotRequest{ServerID: "srv-1"})
	if st.StreamReloaded {
		t.Error("stream_reloaded must be false with no supervised stream")
	}
	if !strings.Contains(st.ReloadError, "restart its capture") {
		t.Errorf("reload_error = %q, want an actionable note", st.ReloadError)
	}
}

// A failed snapshot must never reload the stream: restarting capture would be a
// side effect the operator did not ask for and gains nothing.
func TestSchemaSnapshotSupervisor_snapshotFailureSkipsTheReload(t *testing.T) {
	reloads := 0
	s := testSnapshotSupervisor(t,
		func(console.SchemaSnapshotRequest) (metadata.SnapshotStats, error) {
			return metadata.SnapshotStats{}, errors.New("source unreachable")
		},
		func(context.Context, string) error { reloads++; return nil })
	_, err := s.execute(console.SchemaSnapshotRequest{ServerID: "srv-1"})
	if err == nil {
		t.Fatal("a failed snapshot must be reported as an error")
	}
	if reloads != 0 {
		t.Error("a failed snapshot must not restart capture")
	}
}

// Tables validation excluded stay uncaptured after this run; the operator has
// to learn that from the result, not from the next degraded banner.
func TestSchemaSnapshotSupervisor_reportsExcludedTables(t *testing.T) {
	s := testSnapshotSupervisor(t, func(console.SchemaSnapshotRequest) (metadata.SnapshotStats, error) {
		return metadata.SnapshotStats{SnapshotID: 8, TableCount: 3, ExcludedTables: []string{"shop.audit_raw"}}, nil
	}, func(context.Context, string) error { return nil })
	st, _ := s.execute(console.SchemaSnapshotRequest{ServerID: "srv-1"})
	if len(st.ExcludedTables) != 1 || st.ExcludedTables[0] != "shop.audit_raw" {
		t.Errorf("excluded tables not reported: %+v", st)
	}
}

// One run at a time per server: two concurrent runs would race to restart the
// same stream.
func TestSchemaSnapshotSupervisor_singleFlightPerServer(t *testing.T) {
	release := make(chan struct{})
	s := testSnapshotSupervisor(t, func(console.SchemaSnapshotRequest) (metadata.SnapshotStats, error) {
		<-release
		return metadata.SnapshotStats{}, nil
	}, func(context.Context, string) error { return nil })
	if err := s.Trigger(console.SchemaSnapshotRequest{ServerID: "srv-1"}); err != nil {
		t.Fatalf("first trigger: %v", err)
	}
	if err := s.Trigger(console.SchemaSnapshotRequest{ServerID: "srv-1"}); !errors.Is(err, console.ErrSchemaSnapshotRunning) {
		t.Errorf("second trigger err = %v, want ErrSchemaSnapshotRunning", err)
	}
	// A different server is unaffected.
	if err := s.Trigger(console.SchemaSnapshotRequest{ServerID: "srv-2"}); err != nil {
		t.Errorf("another server must not be blocked: %v", err)
	}
	close(release)
}

func TestSchemaSnapshotSupervisor_statusIsIdleBeforeAnyRun(t *testing.T) {
	s := testSnapshotSupervisor(t, okSnapshot, nil)
	if got := s.Status("never-run"); got.State != "idle" {
		t.Errorf("state = %q, want idle", got.State)
	}
}

// ReloadSchema on a server this process does not supervise is a no-op success:
// there is no stream here to reload and the snapshot it was called for is
// still valid. (The supervised path needs a live index and is covered by the
// monitor integration tests.)
func TestMonitorReloadSchema_unsupervisedEntryIsNoOp(t *testing.T) {
	m := &monitorSupervisor{baseCtx: context.Background(), jobs: map[string]*monitorJob{}}
	if err := m.ReloadSchema(context.Background(), console.ServerEntry{ID: "not-here"}); err != nil {
		t.Errorf("unsupervised entry: %v, want nil", err)
	}
}
