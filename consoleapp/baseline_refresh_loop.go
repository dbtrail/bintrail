package consoleapp

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// refreshRequest is one server's periodic baseline refresh.
type refreshRequest struct {
	ServerID    string
	ServerName  string
	IndexDSN    string
	BaselineDir string
}

// TriggerRefresh starts a periodic baseline refresh for a server, sharing the
// supervisor's single-flight with the manual dump path.
//
// The shared lock is the point, not an implementation detail: a refresh folds
// the newest snapshot forward while a dump writes a new one, and letting them
// overlap on the same server would have the refresh anchored on a snapshot that
// is being written underneath it. ErrBaselineRunning here means "something else
// is already producing this server's baseline" — the loop skips this tick and
// tries again at the next one, which is exactly right for a periodic job.
func (s *baselineSupervisor) TriggerRefresh(req refreshRequest) error {
	s.mu.Lock()
	if s.busyLocked(req.ServerID) {
		s.mu.Unlock()
		return console.ErrBaselineRunning
	}
	at := time.Now().UTC()
	s.refreshes[req.ServerID] = &console.BaselineStatus{State: "running", Since: nowStamp(),
		At: at.Format(time.RFC3339)}
	s.mu.Unlock()

	slog.Info("baseline refresh: starting", "server", req.ServerName, "id", req.ServerID)
	go s.runRefresh(req, at)
	return nil
}

// RefreshStatus reports the last periodic refresh for a server.
func (s *baselineSupervisor) RefreshStatus(serverID string) console.BaselineStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.refreshes[serverID]; ok {
		return *st
	}
	return console.BaselineStatus{State: "idle"}
}

// busyLocked reports whether any of the four baseline job kinds (dump,
// refresh, restore, sql export) is in flight for a server. Callers must
// hold s.mu.
func (s *baselineSupervisor) busyLocked(serverID string) bool {
	if st, ok := s.jobs[serverID]; ok && st.State == "running" {
		return true
	}
	if st, ok := s.refreshes[serverID]; ok && st.State == "running" {
		return true
	}
	if st, ok := s.restores[serverID]; ok && st.State == "running" {
		return true
	}
	if st, ok := s.exports[serverID]; ok && st.State == "running" {
		return true
	}
	return false
}

func (s *baselineSupervisor) runRefresh(req refreshRequest, at time.Time) {
	started := time.Now().UTC()
	tables, refused, err := s.executeRefresh(req, at)
	s.recordRun(req.ServerID, req.ServerName, console.BaselineRunRecord{
		Kind: console.BaselineRunRefresh, StartedAt: started.Format(time.RFC3339),
		SnapshotTime: publishedSnapshotTime(at, err), Tables: tables, Refused: refused,
	}, err)

	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.refreshes[req.ServerID]
	if st == nil { // defensive; never cleared under lock
		st = &console.BaselineStatus{}
		s.refreshes[req.ServerID] = st
	}
	st.FinishedAt = nowStamp()
	st.Tables = tables
	st.Refused = refused
	if err != nil {
		st.State = "failed"
		st.LastError = err.Error()
		// Warn, never Error: a refusal is the fail-closed contract working, and
		// the next tick retries. Nothing about the daemon is unhealthy.
		slog.Warn("baseline refresh: published nothing", "server", req.ServerName, "id", req.ServerID,
			"refused", refused, "error", err)
		return
	}
	st.State = "succeeded"
	st.LastError = ""
	slog.Info("baseline refresh: published", "server", req.ServerName, "id", req.ServerID, "tables", tables)
}

// executeRefresh folds the newest snapshot forward. Returns the table count and
// the number of tables that refused.
//
// RESOURCE POSTURE — read before changing. Every DuckDB budget here is left at
// its zero value on purpose, which resolves to duckdbutil.DefaultTuning (2
// threads / 4 GB) and the container-safe archive fetcher. This is a long-lived
// daemon that is also streaming replication and serving a console; --ultrafast
// exists for offline commands that own the machine (#510), and letting a
// background refresh self-tune to ~80% of host RAM would starve the capture path
// it depends on. If this ever needs to go faster, it needs its own bounded knob,
// not the offline one.
func (s *baselineSupervisor) executeRefresh(req refreshRequest, at time.Time) (tables, refused int, err error) {
	tableList, err := reconstruct.NewestSnapshotTables(s.ctx, req.BaselineDir)
	if err != nil {
		return 0, 0, fmt.Errorf("list the snapshot to refresh: %w", err)
	}
	if len(tableList) == 0 {
		return 0, 0, fmt.Errorf("no baseline snapshot to refresh under %s", req.BaselineDir)
	}
	return s.foldSnapshot(req, at, tableList)
}

// foldSnapshot is the fold both the periodic refresh and the point-in-time
// restore share: reconstruct every table at `at` and publish the result as a
// new snapshot in the server's own baseline store, all-or-nothing.
func (s *baselineSupervisor) foldSnapshot(req refreshRequest, at time.Time, tableList []string) (tables, refused int, err error) {
	_, failures, runErr := reconstruct.ReconstructTablesDetailed(s.ctx, reconstruct.FullTableConfig{
		IndexDSN:     req.IndexDSN,
		BaselineSrc:  req.BaselineDir,
		Tables:       tableList,
		At:           at,
		OutputDir:    req.BaselineDir,
		OutputFormat: reconstruct.OutputFormatParquet,
		// AllowGaps stays FALSE. An unattended job must never publish a
		// knowingly-incomplete baseline: accepting a permanent capture loss is a
		// decision with consequences for every future reconstruct, and nobody is
		// watching this one to make it.
	})
	if runErr != nil {
		return len(tableList), len(failures), runErr
	}
	return len(tableList), 0, nil
}

// startBaselineRefreshLoop launches the opt-in periodic baseline refresh
// (#1171). intervalRaw empty = disabled, which is the default.
//
// Isolation matches the rotation and prune loops: it runs in its own goroutine,
// recovers from a panic, and logs failures without touching the stream or the
// supervisor. A baseline that stopped refreshing is a degradation; a daemon that
// stopped capturing is an outage, and the first must never cause the second.
func startBaselineRefreshLoop(ctx context.Context, reg *console.Registry, sup *baselineSupervisor,
	globalDSN, globalBaselineDir, intervalRaw string) error {
	if intervalRaw == "" {
		return nil
	}
	interval, err := cliutil.ParseInterval(intervalRaw)
	if err != nil {
		return fmt.Errorf("--baseline-refresh-interval: %w", err)
	}
	if interval <= 0 {
		return fmt.Errorf("--baseline-refresh-interval must be positive, got %q", intervalRaw)
	}
	if sup == nil {
		// Unreachable from watch.go, which builds the supervisor whenever this
		// interval is set. Kept so a future caller that forgets fails loudly
		// instead of running a console with a flag that is silently inert.
		return fmt.Errorf("internal: --baseline-refresh-interval was set without a baseline supervisor")
	}
	targets := baselineRefreshTargets(registryEntries(reg), globalDSN, globalBaselineDir)
	slog.Info("baseline refresh loop enabled", "interval", interval, "servers", len(targets))
	if len(targets) == 0 {
		// WARN, not a refusal — and the distinction is load-bearing. Every tick
		// recomputes the target set, so "nothing to refresh" is a state a daemon
		// legitimately starts in and grows out of: a source-less `watch` lists no
		// servers at all until they are added FROM THE CONSOLE, and per-server
		// baseline directories live in the registry, not on the command line.
		// Refusing here would mean a compose file carrying the interval could not
		// boot a fresh install — the operator would have to add a server through a
		// console that is not running. The visibility this warning gives is what
		// the refusal was actually for.
		slog.Warn("baseline refresh: no server is refreshable yet, so nothing will run until one has BOTH an " +
			"index DSN and a LOCAL baseline directory (a refresh reads the previous snapshot and writes the new " +
			"one on disk; an S3-only baseline destination cannot be refreshed in place). Servers added later are " +
			"picked up automatically.")
	}
	// RETENTION INTERPLAY (#616), stated at startup on purpose. A refreshed
	// snapshot is written locally and is NOT uploaded, and baseline.PruneLocal
	// only reclaims a snapshot whose _SUCCESS marker it can confirm in S3 — so
	// nothing this loop publishes is prunable, with or without an S3 destination
	// configured. Unattended that is one full-table snapshot per interval,
	// forever. An operator who discovers this from a full disk discovers it far
	// too late, and the loop has no business quietly deciding to upload on their
	// behalf.
	slog.Warn("baseline refresh: snapshots from this loop are written locally and never uploaded, so retention "+
		"cannot reclaim them (a prune needs a confirmed S3 copy of the snapshot). Upload and prune on your own "+
		"schedule, or size the disk for the rate below.",
		diskArgs(interval, targets)...)
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				started := time.Now()
				runBaselineRefreshCycle(ctx, reg, sup, globalDSN, globalBaselineDir)
				reportRefreshCycleDuration(interval, time.Since(started))
			}
		}
	}()
	return nil
}

// reportRefreshCycleDuration states how long a cycle took, and says so plainly
// when it took longer than the interval that was asked for.
//
// A time.Ticker drops ticks rather than queueing them, so an overrun does not
// pile cycles up — it silently widens the interval instead. Without this the
// only symptom is that snapshots appear less often than configured, which
// reads as a broken flag rather than as the refresh costing more than the
// budget it was given.
//
// It is also the measurement that sizes the real problem. Every cycle rewrites
// each table in full, however little of it changed, so a cycle's duration is
// write amplification expressed in the unit an operator can act on. An
// estimate of that cost is a guess about someone else's data; this is theirs.
func reportRefreshCycleDuration(interval, took time.Duration) {
	if took <= interval {
		slog.Info("baseline refresh cycle finished", "took", took, "interval", interval)
		return
	}
	slog.Warn("baseline refresh: the cycle took longer than the configured interval, so refreshes cannot run as "+
		"often as requested (ticks are dropped, not queued). Each cycle rewrites every table in full regardless "+
		"of how much changed, so this is the cost of the rewrite, not of the schedule. Raise the interval to "+
		"match, or narrow the refresh to fewer tables.",
		"took", took, "interval", interval)
}

// diskArgs builds the disk warning's attributes, omitting the projection when
// it is not meaningful rather than logging a misleading zero.
func diskArgs(interval time.Duration, targets []refreshRequest) []any {
	args := []any{"interval", interval}
	if n := snapshotsPer30Days(interval); n > 0 {
		args = append(args, "full_table_snapshots_per_30d", n)
	}
	return append(args, "dirs", refreshTargetDirs(targets))
}

// snapshotsPer30Days projects how many full-table snapshots the configured
// interval produces over a month, for the startup warning.
//
// The warning has said "one snapshot per interval, forever" since the interval
// floor was an hour, where the reader could do the arithmetic and the answer
// was 24 a day. Minutes make that a much worse number and a much easier one to
// skip over: "every 5m" and "8,640 a month, none of them reclaimable" are the
// same fact and land differently.
//
// Thirty days rather than one, because the warning is about DISK and disk
// fills over weeks. A per-DAY projection also divides to zero for any interval
// longer than a day, so a --baseline-refresh-interval of 7d would have
// reported "0 per day", which reads as "none" and is the opposite of the
// truth.
//
// Returns 0 when the projection is not meaningful (a non-positive interval, or
// one longer than the horizon itself). The caller omits the figure entirely
// rather than print a zero, since the interval it logs alongside already tells
// that story. Reported as a count rather than bytes because a snapshot's size
// depends on the tables, which this loop does not know at startup.
func snapshotsPer30Days(interval time.Duration) int64 {
	if interval <= 0 {
		return 0
	}
	return int64(30 * 24 * time.Hour / interval)
}

// runBaselineRefreshCycle triggers one refresh per eligible server.
//
// Deliberately NOT run once at startup, unlike the prune loop: a refresh is a
// full-table fold over every table, and doing that in the same seconds a daemon
// is establishing replication and opening its console would make every restart
// the most expensive moment in the process's life.
func runBaselineRefreshCycle(ctx context.Context, reg *console.Registry, sup *baselineSupervisor,
	globalDSN, globalBaselineDir string) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("baseline refresh cycle panicked; refreshes continue next tick", "panic", r)
		}
	}()
	if ctx.Err() != nil {
		return
	}
	for _, req := range baselineRefreshTargets(registryEntries(reg), globalDSN, globalBaselineDir) {
		if err := sup.TriggerRefresh(req); err != nil {
			// The only expected error is "already running" — a long refresh
			// overlapping the next tick, or a manual dump in flight. Both are
			// handled by simply waiting for the next tick.
			slog.Debug("baseline refresh skipped this tick", "server", req.ServerName, "reason", err)
		}
	}
}

// refreshTargetDirs lists the directories that will grow, for the retention
// warning. Naming globalBaselineDir instead would print "" whenever the
// refreshable servers came from the registry — a disk-growth warning that names
// no directory is the wrong half of the message.
func refreshTargetDirs(targets []refreshRequest) []string {
	seen := map[string]bool{}
	var out []string
	for _, t := range targets {
		if seen[t.BaselineDir] {
			continue
		}
		seen[t.BaselineDir] = true
		out = append(out, t.BaselineDir)
	}
	return out
}

func registryEntries(reg *console.Registry) []console.ServerEntry {
	if reg == nil {
		return nil
	}
	return reg.List()
}

// baselineRefreshTargets collects the servers a refresh can run for: an index
// DSN to fold from and a LOCAL baseline directory to fold into.
//
// A server with only an S3 baseline destination is skipped WITH a warning rather
// than silently: the refresh writes files, so an in-place S3 refresh is not
// something this loop can do, and an operator who configured S3-only baselines
// and set the interval would otherwise see nothing happen and no reason why.
func baselineRefreshTargets(entries []console.ServerEntry, globalDSN, globalBaselineDir string) []refreshRequest {
	var out []refreshRequest
	seen := map[string]bool{}
	add := func(id, name, dsn, dir string) {
		if dsn == "" || dir == "" || seen[id] {
			return
		}
		seen[id] = true
		out = append(out, refreshRequest{ServerID: id, ServerName: name, IndexDSN: dsn, BaselineDir: dir})
	}
	add("default", "boot", globalDSN, globalBaselineDir)
	for _, e := range entries {
		if e.DSN != "" && e.BaselineDir == "" && e.BaselineS3 != "" {
			slog.Warn("baseline refresh: server has an S3-only baseline destination and will not be refreshed "+
				"(a refresh reads and writes snapshot files on disk)", "server", e.Name)
			continue
		}
		add(e.ID, e.Name, e.DSN, e.BaselineDir)
	}
	return out
}
