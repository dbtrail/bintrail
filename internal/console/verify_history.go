package console

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
)

// verifyHistoryCap is how many runs are kept per server. Old records fall off
// the front — the history answers "when did this last verify, and how has it
// been trending", not "archive every run forever".
const verifyHistoryCap = 20

// VerifyRunRecord is one completed (or skipped) verify run as stored in the
// history file and served by GET /api/servers/{id}/verify/history. It embeds
// the same VerifyStatus shape the live status endpoint serves, so a consumer
// renders a historical run and the current one with the same code.
type VerifyRunRecord struct {
	ServerID   string `json:"server_id"`
	ServerName string `json:"server_name,omitempty"`
	// Trigger records who started the run: "manual" (the POST endpoint) or
	// "scheduled" (the watch daemon's --verify-interval loop, #1191).
	Trigger string `json:"trigger"`
	// SkipReason is set only on records with State "skipped" — a scheduled
	// cycle that could not run this server (e.g. a manual run was already in
	// flight). Recorded rather than dropped so a schedule that never actually
	// verifies is visible in the history, not silent.
	SkipReason string `json:"skip_reason,omitempty"`
	VerifyStatus
}

// verifyHistoryFile is the on-disk envelope: versioned like the server
// registry so a future shape change can be detected instead of misparsed.
type verifyHistoryFile struct {
	Version int                          `json:"version"`
	Servers map[string][]VerifyRunRecord `json:"servers"`
}

const verifyHistoryVersion = 1

// VerifyHistory is the persisted verify-run history (#1191): one JSON file,
// capped per server, written atomically (temp file + fsync + rename, 0600)
// like the server registry. Together with the registry it is the ONLY state
// the console writes — keep it that way.
//
// It deliberately lives in a console-local file rather than a table in the
// index database: scheduled verify covers registry servers, and registry DSNs
// never receive EnsureSchema/DDL (an existing invariant this must not bend).
type VerifyHistory struct {
	mu      sync.Mutex
	path    string
	servers map[string][]VerifyRunRecord
}

// DefaultVerifyHistoryPath returns the history file path as a sibling of the
// server registry file, so `--console-servers-file` relocations carry both.
func DefaultVerifyHistoryPath(serversPath string) string {
	return filepath.Join(filepath.Dir(serversPath), "console-verify-history.json")
}

// OpenVerifyHistory loads the history at path. A missing file is an empty
// history. A corrupt or newer-versioned file is reported as an error — the
// caller decides whether to run without history rather than silently
// truncating a file a newer binary may still want.
func OpenVerifyHistory(path string) (*VerifyHistory, error) {
	h := &VerifyHistory{path: path, servers: make(map[string][]VerifyRunRecord)}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return h, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read verify history %s: %w", path, err)
	}
	if len(data) == 0 {
		return h, nil
	}
	var f verifyHistoryFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse verify history %s: %w", path, err)
	}
	if f.Version > verifyHistoryVersion {
		return nil, fmt.Errorf("verify history %s has version %d, newer than this binary supports (%d)", path, f.Version, verifyHistoryVersion)
	}
	if f.Servers != nil {
		h.servers = f.Servers
	}
	return h, nil
}

// Append records one run and saves the file. The per-server cap is enforced
// here (oldest dropped). Called from the supervisor's finish path and the
// scheduler's skip path; a save failure is returned for the caller to log —
// history is an observability aid and must never fail a verify run.
func (h *VerifyHistory) Append(rec VerifyRunRecord) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	old, hadOld := h.servers[rec.ServerID]
	// Clone before appending: an in-place append could write into old's spare
	// capacity, which would corrupt the rollback below.
	recs := append(slices.Clone(old), rec)
	if len(recs) > verifyHistoryCap {
		recs = recs[len(recs)-verifyHistoryCap:]
	}
	h.servers[rec.ServerID] = recs
	if err := h.save(); err != nil {
		// Roll back so memory keeps matching the persisted file — otherwise
		// List (and the API) would serve "history" that a restart silently
		// rewinds, masking a permanent write failure behind a healthy panel.
		if hadOld {
			h.servers[rec.ServerID] = old
		} else {
			delete(h.servers, rec.ServerID)
		}
		return err
	}
	return nil
}

// List returns the recorded runs for a server, newest first. The returned
// slice is a copy — callers can hold it across later Appends.
func (h *VerifyHistory) List(serverID string) []VerifyRunRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	recs := h.servers[serverID]
	out := make([]VerifyRunRecord, len(recs))
	for i, r := range recs {
		out[len(recs)-1-i] = r
	}
	return out
}

// save writes the file atomically. Callers hold h.mu. Same temp-file + fsync
// + rename shape as Registry.save; 0600 because run notes/errors can quote
// operator table names and error strings.
func (h *VerifyHistory) save() error {
	data, err := json.MarshalIndent(verifyHistoryFile{Version: verifyHistoryVersion, Servers: h.servers}, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(h.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".console-verify-history-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
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
