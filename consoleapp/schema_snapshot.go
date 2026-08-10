package consoleapp

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// schemaSnapshotSupervisor implements console.SchemaSnapshotController: it
// re-reads a monitored source's column layout into that source's index and then
// puts the running capture stream onto the result (#1296).
//
// The reload half is the point. A stream holds its metadata resolver in memory
// and swaps it only when it decodes a DDL event, so a snapshot written into the
// index underneath a running stream changes NOTHING — the stream keeps decoding
// against the layout it loaded at startup and keeps skipping the same table. A
// "refresh" button without the reload would look like a fix and be a no-op,
// which is a worse failure than the missing button this replaces.
type schemaSnapshotSupervisor struct {
	ctx context.Context // daemon lifecycle; bounds the snapshot query

	// reload restarts the supervised stream for one entry so it loads the
	// snapshot just written. nil is allowed (tests, and any wiring with no
	// control plane) and reports StreamReloaded=false rather than pretending.
	reload func(ctx context.Context, entryID string) error

	// snapshotFn takes the snapshot itself — a seam, like monitorSupervisor's
	// streamFn: production connects to both DSNs and calls
	// metadata.TakeSnapshotExcludingInvalid, tests substitute a stub so the
	// reload contract can be exercised without a live MySQL.
	snapshotFn func(req console.SchemaSnapshotRequest) (metadata.SnapshotStats, error)

	mu   sync.Mutex
	jobs map[string]*console.SchemaSnapshotStatus
}

// reloadStreamSchema binds the snapshot supervisor's reload hook to the monitor
// supervisor. The lookup happens at reload time, not at wiring time: the entry
// may have been edited (or deleted) between the daemon starting and the button
// being pressed, and Start needs the CURRENT entry.
func reloadStreamSchema(sup *monitorSupervisor, reg *console.Registry) func(context.Context, string) error {
	return func(ctx context.Context, entryID string) error {
		e, ok := reg.Get(entryID)
		if !ok {
			return fmt.Errorf("server %q is no longer in the registry", entryID)
		}
		return sup.ReloadSchema(ctx, e)
	}
}

func newSchemaSnapshotSupervisor(ctx context.Context, reload func(context.Context, string) error) *schemaSnapshotSupervisor {
	return &schemaSnapshotSupervisor{
		ctx:        ctx,
		reload:     reload,
		snapshotFn: takeSchemaSnapshot,
		jobs:       make(map[string]*console.SchemaSnapshotStatus),
	}
}

// takeSchemaSnapshot is the production snapshot step: connect to the source and
// the entry's index, then re-read the column layout.
//
// ExcludingInvalid matches the stream's own DDL hook: one PK-less table must not
// reject the whole snapshot and leave capture running on the stale one. The
// excluded names come back in the stats so the caller can report them — a
// "succeeded" that silently omits the tables that will KEEP being skipped is
// the same half-truth this issue is about.
func takeSchemaSnapshot(req console.SchemaSnapshotRequest) (metadata.SnapshotStats, error) {
	sourceDB, err := config.Connect(req.SourceDSN)
	if err != nil {
		return metadata.SnapshotStats{}, err
	}
	defer sourceDB.Close()
	indexDB, err := config.Connect(req.IndexDSN)
	if err != nil {
		return metadata.SnapshotStats{}, err
	}
	defer indexDB.Close()
	return metadata.TakeSnapshotExcludingInvalid(sourceDB, indexDB, req.Schemas)
}

// Trigger starts a snapshot in the background; returns
// console.ErrSchemaSnapshotRunning when one is already in flight for this
// server. One at a time per server: two concurrent runs would race to restart
// the same stream.
func (s *schemaSnapshotSupervisor) Trigger(req console.SchemaSnapshotRequest) error {
	s.mu.Lock()
	if st, ok := s.jobs[req.ServerID]; ok && st.State == "running" {
		s.mu.Unlock()
		return console.ErrSchemaSnapshotRunning
	}
	s.jobs[req.ServerID] = &console.SchemaSnapshotStatus{State: "running", Since: nowStamp()}
	s.mu.Unlock()

	slog.Info("schema snapshot: refreshing from the source", "server", req.ServerName, "id", req.ServerID)
	go s.run(req)
	return nil
}

// Status returns a copy of the latest known job state (idle if none ran here).
func (s *schemaSnapshotSupervisor) Status(serverID string) console.SchemaSnapshotStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.jobs[serverID]; ok {
		return *st
	}
	return console.SchemaSnapshotStatus{State: "idle"}
}

func (s *schemaSnapshotSupervisor) run(req console.SchemaSnapshotRequest) {
	st, err := s.execute(req)
	s.mu.Lock()
	defer s.mu.Unlock()
	prev := s.jobs[req.ServerID]
	st.Since = ""
	if prev != nil {
		st.Since = prev.Since
	}
	st.FinishedAt = nowStamp()
	if err != nil {
		// Scrub both DSNs: this string is served over HTTP, and a driver error
		// commonly embeds the whole connection string, password included.
		st.State = "failed"
		st.LastError = config.ScrubDSNError(err, req.SourceDSN, req.IndexDSN)
		slog.Warn("schema snapshot failed", "server", req.ServerName, "id", req.ServerID, "error", st.LastError)
	}
	s.jobs[req.ServerID] = &st
}

// execute does the work: snapshot, then reload the stream. A reload failure is
// NOT a job failure — the snapshot is durable and correct, capture is simply
// still running on the old one — so it is reported in its own field with the
// state left "succeeded". Folding it into LastError would hide that the
// snapshot itself worked and invite the operator to run it again.
func (s *schemaSnapshotSupervisor) execute(req console.SchemaSnapshotRequest) (console.SchemaSnapshotStatus, error) {
	st := console.SchemaSnapshotStatus{State: "succeeded"}

	stats, err := s.snapshotFn(req)
	if err != nil {
		return st, err
	}
	st.SnapshotID, st.Tables, st.ExcludedTables = stats.SnapshotID, stats.TableCount, stats.ExcludedTables
	slog.Info("schema snapshot taken", "server", req.ServerName, "snapshot_id", stats.SnapshotID,
		"tables", stats.TableCount, "excluded_tables", strings.Join(stats.ExcludedTables, ", "))

	if s.reload == nil {
		st.ReloadError = "this process does not supervise the stream for this server; restart its capture to pick the new snapshot up"
		return st, nil
	}
	if err := s.reload(s.ctx, req.ServerID); err != nil {
		st.ReloadError = config.ScrubDSNError(err, req.SourceDSN, req.IndexDSN)
		slog.Warn("schema snapshot: stream did not reload; capture is still using the previous snapshot",
			"server", req.ServerName, "id", req.ServerID, "error", st.ReloadError)
		return st, nil
	}
	st.StreamReloaded = true
	return st, nil
}
