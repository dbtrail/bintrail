package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

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
	ctx context.Context // daemon lifecycle; cancels an in-flight job on shutdown

	// reload restarts the supervised stream for one entry so it loads the
	// snapshot just written, reporting whether it ACTUALLY restarted one. A
	// false with no error means this process supervises no stream for that
	// entry — which must never render as a restart: the entry may be captured
	// by another process, still decoding against the old snapshot. nil is
	// allowed (tests, and any wiring with no control plane).
	reload func(ctx context.Context, entryID string) (bool, error)

	// snapshotFn takes the snapshot itself — a seam, like monitorSupervisor's
	// streamFn: production connects to both DSNs and calls
	// metadata.TakeSnapshotExcludingInvalid, tests substitute a stub so the
	// reload contract can be exercised without a live MySQL.
	snapshotFn func(req console.SchemaSnapshotRequest) (metadata.SnapshotStats, error)

	// timeout bounds one job. Per-supervisor rather than a package var so a
	// test that shrinks it touches only its own instance: a global is read by
	// the job goroutine of every OTHER test in the package, and those outlive
	// the test that spawned them, so writing one is a data race that -race
	// catches and a plain `go test` does not.
	timeout time.Duration

	mu   sync.Mutex
	jobs map[string]*console.SchemaSnapshotStatus
	// gens counts triggered runs PER SERVER. A run whose generation is no
	// longer its server's current one has been superseded (its predecessor
	// timed out and the operator retried) and must neither publish its outcome
	// nor restart a stream. Per-server, not global: one counter would let a
	// snapshot on server B silently abandon an in-flight one on server A.
	gens map[string]uint64
}

// reloadStreamSchema binds the snapshot supervisor's reload hook to the monitor
// supervisor. The lookup happens at reload time, not at wiring time: the entry
// may have been edited (or deleted) between the daemon starting and the button
// being pressed, and Start needs the CURRENT entry.
func reloadStreamSchema(sup *monitorSupervisor, reg *console.Registry) func(context.Context, string) (bool, error) {
	return func(ctx context.Context, entryID string) (bool, error) {
		e, ok := reg.Get(entryID)
		if !ok {
			return false, fmt.Errorf("server %q is no longer in the registry", entryID)
		}
		return sup.ReloadSchema(ctx, e)
	}
}

// defaultSchemaSnapshotTimeout is what a supervisor gets unless a caller
// narrows it.
//
// A bound exists because the snapshot itself cannot be cancelled: metadata's
// snapshot taker holds no context, and config.Connect's timeout covers only the
// TCP handshake — a source whose information_schema read blocks behind a
// metadata lock hangs the job forever. Without a deadline the job stays
// "running" for the life of the process, every later Trigger answers 409, and
// the only recovery is a daemon restart: an endpoint permanently unable to do
// the thing it exists for.
const defaultSchemaSnapshotTimeout = 10 * time.Minute

func newSchemaSnapshotSupervisor(ctx context.Context, reload func(context.Context, string) (bool, error)) *schemaSnapshotSupervisor {
	return &schemaSnapshotSupervisor{
		ctx:        ctx,
		reload:     reload,
		snapshotFn: takeSchemaSnapshot,
		timeout:    defaultSchemaSnapshotTimeout,
		jobs:       make(map[string]*console.SchemaSnapshotStatus),
		gens:       make(map[string]uint64),
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
	s.gens[req.ServerID]++
	gen := s.gens[req.ServerID]
	s.mu.Unlock()

	slog.Info("schema snapshot: refreshing from the source", "server", req.ServerName, "id", req.ServerID)
	go s.run(req, gen)
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

// run executes one job under s.timeout. On timeout the job is
// reported failed so the endpoint becomes usable again; the abandoned goroutine
// cannot corrupt anything after that, because publish drops a result from a
// superseded generation and execute declines to restart a stream it no longer
// owns.
func (s *schemaSnapshotSupervisor) run(req console.SchemaSnapshotRequest, gen uint64) {
	type outcome struct {
		st  console.SchemaSnapshotStatus
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		st, err := s.execute(req, gen)
		done <- outcome{st, err}
	}()
	select {
	case o := <-done:
		s.publish(req, gen, o.st, o.err)
	case <-time.After(s.timeout):
		slog.Warn("schema snapshot timed out; the attempt may still be finishing in the background",
			"server", req.ServerName, "id", req.ServerID, "timeout", s.timeout)
		s.publish(req, gen, console.SchemaSnapshotStatus{},
			fmt.Errorf("the source did not answer within %s; it may be holding a metadata lock. The attempt may still finish in the background — capture was not restarted", s.timeout))
	case <-s.ctx.Done():
		s.publish(req, gen, console.SchemaSnapshotStatus{}, errors.New("the daemon is shutting down; capture was not restarted"))
	}
}

// publish records a job's outcome, unless a newer run for this server has
// already superseded it — a late finisher must not overwrite the newer job's
// state (or resurrect a "succeeded" over it).
func (s *schemaSnapshotSupervisor) publish(req console.SchemaSnapshotRequest, gen uint64, st console.SchemaSnapshotStatus, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gens[req.ServerID] != gen {
		return
	}
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
func (s *schemaSnapshotSupervisor) execute(req console.SchemaSnapshotRequest, gen uint64) (console.SchemaSnapshotStatus, error) {
	st := console.SchemaSnapshotStatus{State: "succeeded"}

	stats, err := s.snapshotFn(req)
	if err != nil {
		return st, err
	}
	st.SnapshotID, st.Tables, st.ExcludedTables = stats.SnapshotID, stats.TableCount, stats.ExcludedTables
	slog.Info("schema snapshot taken", "server", req.ServerName, "snapshot_id", stats.SnapshotID,
		"tables", stats.TableCount, "excluded_tables", strings.Join(stats.ExcludedTables, ", "))

	if s.reload == nil {
		st.ReloadError = notSupervisedNote
		return st, nil
	}
	if s.superseded(req.ServerID, gen) {
		// This run timed out and the operator retried: a newer run owns the
		// stream now. Restarting it here would fight that one.
		st.ReloadError = "this attempt was superseded by a newer one; capture was not restarted by it"
		return st, nil
	}
	reloaded, err := s.reload(s.ctx, req.ServerID)
	if err != nil {
		st.ReloadError = config.ScrubDSNError(err, req.SourceDSN, req.IndexDSN)
		slog.Warn("schema snapshot: the capture stream was not restarted onto the new snapshot",
			"server", req.ServerName, "id", req.ServerID, "error", st.ReloadError)
		return st, nil
	}
	if !reloaded {
		// No stream here to restart. Never report this as a reload: whoever
		// captures this source is still on the previous snapshot.
		st.ReloadError = notSupervisedNote
		return st, nil
	}
	st.StreamReloaded = true
	return st, nil
}

// notSupervisedNote is what an operator is told when no stream was restarted
// here. It states the consequence (capture is still on the old snapshot)
// because that is the difference between a fix and a durable no-op.
const notSupervisedNote = "this process does not supervise capture for this server, so nothing was restarted; " +
	"restart whatever captures it to pick the new snapshot up"

// superseded reports whether a newer run for the same server has started.
func (s *schemaSnapshotSupervisor) superseded(serverID string, gen uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gens[serverID] != gen
}
