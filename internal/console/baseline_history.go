package console

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// BaselineRunHistoryCap is how many baseline runs are kept per server. Old
// records fall off the front; the history answers "how long did this backup
// take, and who made it", not "archive every run forever".
const BaselineRunHistoryCap = 40

// Kind values for BaselineRunRecord. The literals are the file/wire format.
const (
	BaselineRunDump    = "dump"    // mydumper (or pgbaseline) snapshot of the source
	BaselineRunRefresh = "refresh" // periodic fold of the newest snapshot forward
	BaselineRunRestore = "restore" // operator-chosen point-in-time fold (#backups)
)

// BaselineRunTriggerScheduled marks a run (or a skip) the per-server backup
// schedule started (#1442). Empty is everything else: the Create backup
// button, a restore, the daemon-wide refresh interval. The literal is the
// file/wire format and matches the verify history's vocabulary.
const BaselineRunTriggerScheduled = "scheduled"

// BaselineRunRecord is one completed baseline-producing run as this daemon
// performed it. The files listing joins it to a snapshot by SnapshotTime to
// report the run's exact duration; snapshots produced elsewhere (the CLI,
// another daemon) have no record and fall back to the file write span.
//
// A record with SkipReason set is NOT a run: it is a scheduled slot that
// could not start (another backup job held the server, or the schedule was
// not runnable). It has no snapshot, so the files listing never joins it;
// it exists so a schedule that never gets to run stays visible on the
// Backups page instead of silent.
type BaselineRunRecord struct {
	ServerID   string `json:"server_id"`
	ServerName string `json:"server_name,omitempty"`
	Kind       string `json:"kind"`
	// Trigger is BaselineRunTriggerScheduled for the backup schedule's own
	// runs and skips, empty otherwise.
	Trigger string `json:"trigger,omitempty"`
	// SkipReason is set on a scheduled slot that did not start; see above.
	SkipReason string `json:"skip_reason,omitempty"`
	// SnapshotTime is the published snapshot's anchor instant — its directory
	// name — in RFC3339 UTC. Empty when the run failed before publishing, or
	// when the producer does not report it (PostgreSQL dumps stamp the
	// snapshot server-side, out of this process's sight).
	SnapshotTime string `json:"snapshot_time,omitempty"`
	StartedAt    string `json:"started_at"`
	FinishedAt   string `json:"finished_at"`
	Tables       int    `json:"tables,omitempty"`
	// Carried counts tables published by reusing the previous snapshot's file
	// (refresh and restore). Persisted rather than left to the live status,
	// which the next run overwrites: whether a run cost a full rewrite is
	// exactly the thing an operator looks back at when sizing a disk or an
	// interval, and by then the live status is gone.
	Carried  int    `json:"carried,omitempty"`
	Rows     int64  `json:"rows,omitempty"`
	Uploaded int    `json:"uploaded,omitempty"`
	Refused  int    `json:"refused,omitempty"`
	Error    string `json:"error,omitempty"`
}

type baselineHistoryFile struct {
	Version int                            `json:"version"`
	Servers map[string][]BaselineRunRecord `json:"servers"`
}

const baselineHistoryVersion = 1

// BaselineRunHistory is the persisted baseline-run history: one JSON file,
// capped per server, written atomically like the server registry and the
// verify history. Console-local state on disk, deliberately NOT a table in
// the index database (registry DSNs never receive DDL).
type BaselineRunHistory struct {
	mu      sync.Mutex
	path    string
	servers map[string][]BaselineRunRecord
}

// DefaultBaselineHistoryPath returns the history file path as a sibling of
// the server registry file, so --console-servers-file relocations carry it.
func DefaultBaselineHistoryPath(serversPath string) string {
	return filepath.Join(filepath.Dir(serversPath), "console-baseline-history.json")
}

// OpenBaselineHistory loads the history at path. A missing file is an empty
// history; a corrupt or newer-versioned file is an error for the caller to
// decide on, never silently truncated.
func OpenBaselineHistory(path string) (*BaselineRunHistory, error) {
	h := &BaselineRunHistory{path: path, servers: make(map[string][]BaselineRunRecord)}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return h, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read baseline history %s: %w", path, err)
	}
	if len(data) == 0 {
		return h, nil
	}
	var f baselineHistoryFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse baseline history %s: %w", path, err)
	}
	if f.Version > baselineHistoryVersion {
		return nil, fmt.Errorf("baseline history %s has version %d, newer than this binary supports (%d)", path, f.Version, baselineHistoryVersion)
	}
	if f.Servers != nil {
		h.servers = f.Servers
	}
	return h, nil
}

// Append records one run and saves the file (oldest dropped past the cap). A
// save failure is returned for the caller to log; history is an observability
// aid and must never fail the run it describes.
func (h *BaselineRunHistory) Append(rec BaselineRunRecord) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	recs := append(h.servers[rec.ServerID], rec)
	if len(recs) > BaselineRunHistoryCap {
		recs = recs[len(recs)-BaselineRunHistoryCap:]
	}
	h.servers[rec.ServerID] = recs
	return h.save()
}

// FindBySnapshot returns the newest record for serverID whose SnapshotTime
// equals snapshotTime (RFC3339 UTC), or nil.
func (h *BaselineRunHistory) FindBySnapshot(serverID, snapshotTime string) *BaselineRunRecord {
	if snapshotTime == "" {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	recs := h.servers[serverID]
	for i := len(recs) - 1; i >= 0; i-- {
		if recs[i].SnapshotTime == snapshotTime {
			rec := recs[i]
			return &rec
		}
	}
	return nil
}

// LastScheduled returns the newest scheduled RUN for serverID and the newest
// scheduled SKIP, either nil when there is none. Both, because they answer
// different questions on the Backups page: "when did the schedule last
// produce a backup" and "is it currently unable to". A skip newer than the
// last run is the case the page has to shout about.
func (h *BaselineRunHistory) LastScheduled(serverID string) (run, skip *BaselineRunRecord) {
	h.mu.Lock()
	defer h.mu.Unlock()
	recs := h.servers[serverID]
	for i := len(recs) - 1; i >= 0 && (run == nil || skip == nil); i-- {
		if recs[i].Trigger != BaselineRunTriggerScheduled {
			continue
		}
		rec := recs[i]
		if rec.SkipReason != "" {
			if skip == nil {
				skip = &rec
			}
			continue
		}
		if run == nil {
			run = &rec
		}
	}
	return run, skip
}

// AppendSkip records a scheduled slot that did not start, unless the newest
// record for the server is already the same skip: a wedged job plus a short
// interval would otherwise append an identical skip every slot, and the
// capped history would evict the real runs, erasing exactly the "when did
// this last actually back up" answer it exists to keep. Returns whether a
// record was written.
func (h *BaselineRunHistory) AppendSkip(rec BaselineRunRecord) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	recs := h.servers[rec.ServerID]
	if n := len(recs); n > 0 && recs[n-1].Trigger == BaselineRunTriggerScheduled &&
		recs[n-1].SkipReason == rec.SkipReason && recs[n-1].Kind == rec.Kind {
		return false, nil
	}
	rec.Trigger = BaselineRunTriggerScheduled
	recs = append(recs, rec)
	if len(recs) > BaselineRunHistoryCap {
		recs = recs[len(recs)-BaselineRunHistoryCap:]
	}
	h.servers[rec.ServerID] = recs
	return true, h.save()
}

// List returns a copy of serverID's records, oldest first.
func (h *BaselineRunHistory) List(serverID string) []BaselineRunRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	recs := h.servers[serverID]
	out := make([]BaselineRunRecord, len(recs))
	copy(out, recs)
	return out
}

func (h *BaselineRunHistory) save() error {
	b, err := json.Marshal(baselineHistoryFile{Version: baselineHistoryVersion, Servers: h.servers})
	if err != nil {
		// The only step here whose failure names nothing on its own: every
		// other one returns an *os.PathError/*os.LinkError already carrying
		// the path, which is why they keep VerifyHistory.save's raw returns.
		return fmt.Errorf("marshal baseline history %s: %w", h.path, err)
	}
	// Create the tree first, exactly as the sibling savers do (Registry.save,
	// saveAuthFile, saveMCPTokenFile, VerifyHistory.save — this is
	// VerifyHistory.save's shape, the closest relative). Nothing else creates
	// ~/.config/bintrail on a fresh install, so without this the first refresh
	// on a brand-new host loses its history to ENOENT (#1487). 0700 because
	// the directory also holds the registry's DSN passwords.
	dir := filepath.Dir(h.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".baseline-history-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), h.path)
}
