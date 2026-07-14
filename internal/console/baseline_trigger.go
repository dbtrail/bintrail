package console

import (
	"errors"
	"net/http"
	"strings"
)

// ErrBaselineRunning is returned by BaselineController.Trigger when a baseline
// for that server is already in flight (one at a time per server). The handler
// maps it to 409 Conflict.
var ErrBaselineRunning = errors.New("a baseline is already running for this server")

// BaselineController runs a baseline snapshot for a monitored server entirely
// in-process — dump → convert → upload — so the console never shells a sibling
// container and never mounts the docker socket (#613). It is wired in ONLY by
// `bintrail-console watch` when the operator opts in
// (BINTRAIL_CONSOLE_BASELINE_TRIGGER=1); nil on the standalone read-only
// console, where the endpoint refuses with 403 — mirroring how MonitorController
// gates the monitor verbs.
type BaselineController interface {
	// Trigger starts a baseline in the background and returns immediately.
	// Returns ErrBaselineRunning if one is already running for req.ServerID.
	Trigger(req BaselineRequest) error
	// Status reports the latest known state for a server (idle if never run in
	// this process — the durable record is the snapshot itself, listed by
	// /api/baselines).
	Status(serverID string) BaselineStatus
}

// BaselineRequest is the in-process job description the endpoint hands the
// controller. The source DSN (a secret) stays inside the process — it is never
// written to disk or serialized to any HTTP response.
type BaselineRequest struct {
	ServerID   string
	ServerName string
	SourceDSN  string
	Schemas    []string
	// LocalDir is the per-server baseline directory (entry.BaselineDir). When
	// set, the snapshot is written there persistently and NOT uploaded. Empty
	// means S3-only: stage in a temp dir, upload to S3, discard the staging.
	LocalDir string
	// S3 is the per-server baseline S3 prefix (entry.BaselineS3, s3://…). When
	// set, the staged snapshot is uploaded there. Region/credentials come from
	// the ambient AWS chain (env / ~/.aws / IAM role), like the rest of the
	// console's S3 access.
	S3 string
	// Flavor selects the baseline producer: "postgres" runs internal/pgbaseline
	// (COPY + the slot's pgoutput anchor); anything else runs mydumper. Plain
	// strings keep internal/console pgx-free (the read-layer dependency guard).
	Flavor string
	// Slot / Publication are the PostgreSQL replication slot and publication
	// (postgres flavor only); the producer stamps the snapshot at the slot's
	// consistent-point LSN so Time-travel/reconstruct can replay deltas from it.
	// Empty for MySQL/MariaDB.
	Slot        string
	Publication string
}

// BaselineStatus is the pollable state of a server's most recent baseline job.
type BaselineStatus struct {
	State      string `json:"state"` // idle | running | succeeded | failed
	Since      string `json:"since,omitempty"`
	FinishedAt string `json:"finished_at,omitempty"`
	LastError  string `json:"last_error,omitempty"`
	Tables     int    `json:"tables,omitempty"`
	Rows       int64  `json:"rows,omitempty"`
	Uploaded   int    `json:"uploaded,omitempty"`
}

// handleBaselineTrigger enqueues an in-process baseline for the selected server.
// Gating, in order: the feature must be enabled (control-plane + opt-in), the
// entry must be a real registry server with a source configured AND a baseline
// destination (dir or S3) — without a destination there is nowhere for the
// snapshot to live, so Time-travel would never see it.
func (s *Server) handleBaselineTrigger(w http.ResponseWriter, r *http.Request) {
	if s.baselineCtrl == nil {
		writeJSONError(w, http.StatusForbidden,
			"baseline creation from the console is not enabled; start the watch daemon with BINTRAIL_CONSOLE_BASELINE_TRIGGER=1")
		return
	}
	e, ok := s.requireMonitorEntry(w, r.PathValue("id"))
	if !ok {
		return
	}
	if e.SourceDSN == "" {
		writeJSONError(w, http.StatusBadRequest, "this server has no source configured; set the source connection first")
		return
	}
	if e.BaselineDir == "" && e.BaselineS3 == "" {
		writeJSONError(w, http.StatusBadRequest,
			"this server has no baseline location set up; set a baseline directory or S3 location first (Edit → Advanced)")
		return
	}
	if e.IsPostgres() && (e.SourceSlot == "" || e.SourcePublication == "") {
		writeJSONError(w, http.StatusBadRequest,
			"this PostgreSQL server has no replication slot/publication configured; set them first (Edit → Source)")
		return
	}

	req := BaselineRequest{
		ServerID:    e.ID,
		ServerName:  e.Name,
		SourceDSN:   e.SourceDSN,
		Schemas:     splitSchemas(e.Schemas),
		LocalDir:    e.BaselineDir,
		S3:          e.BaselineS3,
		Flavor:      e.SourceFlavor(),
		Slot:        e.SourceSlot,
		Publication: e.SourcePublication,
	}
	if err := s.baselineCtrl.Trigger(req); err != nil {
		if errors.Is(err, ErrBaselineRunning) {
			writeJSONError(w, http.StatusConflict, err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"baseline": s.baselineCtrl.Status(e.ID)})
}

// handleBaselineStatus reports the latest baseline job state for the selected
// server (for the frontend to poll while a run is in flight).
func (s *Server) handleBaselineStatus(w http.ResponseWriter, r *http.Request) {
	if s.baselineCtrl == nil {
		writeJSONError(w, http.StatusForbidden,
			"baseline creation from the console is not enabled; start the watch daemon with BINTRAIL_CONSOLE_BASELINE_TRIGGER=1")
		return
	}
	e, ok := s.requireMonitorEntry(w, r.PathValue("id"))
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"baseline": s.baselineCtrl.Status(e.ID)})
}

// splitSchemas parses a comma-separated schema filter into a trimmed,
// empty-free slice (nil for an empty string = all schemas).
func splitSchemas(s string) []string {
	var out []string
	for part := range strings.SplitSeq(s, ",") {
		if t := strings.TrimSpace(part); t != "" {
			out = append(out, t)
		}
	}
	return out
}
