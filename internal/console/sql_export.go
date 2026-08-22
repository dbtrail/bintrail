package console

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// SQLExporter builds a custom .sql backup: fold the backup at-or-before an
// operator-chosen instant forward through the index and write it as a
// mydumper-format dump the operator downloads and loads with myloader (or
// the mysql client, applying mydumper's session assumptions), no bintrail
// needed on the restore side. A separate interface from the other baseline
// controllers so its wiring CAN diverge (the #1171 lesson); today it
// deliberately rides the same supervisor as BaselineRestore — see
// wireBaselineExtras for why that derivation is safe here.
type SQLExporter interface {
	// TriggerSQLExport starts the build asynchronously. ErrBaselineRunning
	// when another baseline job for the server is in flight.
	TriggerSQLExport(req SQLExportRequest) error
	// SQLExportStatus reports the last build for a server (idle if none).
	SQLExportStatus(serverID string) BaselineStatus
	// SQLExportDir returns the finished dump's directory and the status it
	// belongs to, from one locked snapshot (so the caller can never label
	// one build's bytes with another build's instant); ok is false until a
	// build has succeeded and its directory affirmatively carries the
	// completeness marker.
	SQLExportDir(serverID string) (dir string, status BaselineStatus, ok bool)
}

// SQLExportRequest identifies the server and the instant to build for.
type SQLExportRequest struct {
	ServerID    string
	ServerName  string
	IndexDSN    string
	BaselineSrc string // local directory or s3:// prefix
	At          time.Time
}

// handleSQLExportTrigger enqueues a build for the selected server:
// POST /api/servers/{id}/sql-export {"at": "YYYY-MM-DD HH:MM:SS"}.
func (s *Server) handleSQLExportTrigger(w http.ResponseWriter, r *http.Request) {
	if s.sqlExport == nil {
		writeJSONError(w, http.StatusForbidden,
			"custom .sql backups from the console are not enabled; they need the watch daemon with baseline creation or refresh turned on")
		return
	}
	e, ok := s.requireMonitorEntry(w, r.PathValue("id"))
	if !ok {
		return
	}
	// The build writes a full unredacted dump to disk (and its wipe removes
	// the previous build another operator may be about to download), so a
	// profile-scoped session may not start one — the same #1075 line the
	// download itself draws.
	if sessionRestricted(r) {
		recordProfileGateDeny(r, "sql-export-trigger")
		writeJSONError(w, http.StatusForbidden,
			"custom .sql backups are unavailable while an access-control profile is active: baseline reads aren't redacted")
		return
	}
	// An entry with no baseline of its own inherits the process-wide
	// --baseline-dir/--baseline-s3 (#1010), like the verify trigger: the
	// Backups listing the card gates on applies the same fallback, so the
	// trigger must accept what the UI was told is configured. The restore
	// trigger's shared-store refusal is about PUBLISHING into that store;
	// this build publishes nothing, and the listing and time-travel already
	// attribute those snapshots to this entry.
	e = s.cm.withBaselineDefaults(e)
	src := e.BaselineDir
	if src == "" {
		src = e.BaselineS3
	}
	if src == "" {
		writeJSONError(w, http.StatusBadRequest,
			"this server has no backup location set up; set a backup directory or S3 location first (Edit → Advanced)")
		return
	}
	var body struct {
		At string `json:"at"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	at, ok2 := parseSnapshotAt(body.At)
	if !ok2 {
		writeJSONError(w, http.StatusBadRequest, "at must be a UTC time, YYYY-MM-DD HH:MM:SS")
		return
	}
	if at.After(time.Now().UTC()) {
		writeJSONError(w, http.StatusBadRequest, "at is in the future; pick a past moment")
		return
	}
	req := SQLExportRequest{
		ServerID: e.ID, ServerName: e.Name, IndexDSN: e.DSN, BaselineSrc: src, At: at,
	}
	if err := s.sqlExport.TriggerSQLExport(req); err != nil {
		if errors.Is(err, ErrBaselineRunning) {
			writeJSONError(w, http.StatusConflict, err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"sql_export": s.sqlExport.SQLExportStatus(e.ID)})
}

// handleSQLExportStatus reports the latest build state for polling.
func (s *Server) handleSQLExportStatus(w http.ResponseWriter, r *http.Request) {
	if s.sqlExport == nil {
		writeJSONError(w, http.StatusForbidden,
			"custom .sql backups from the console are not enabled; they need the watch daemon with baseline creation or refresh turned on")
		return
	}
	e, ok := s.requireMonitorEntry(w, r.PathValue("id"))
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sql_export": s.sqlExport.SQLExportStatus(e.ID)})
}

// handleSQLExportDownload streams the finished dump as one tar.gz. Same
// abort and audit contract as the backup download: a mid-stream failure
// cuts the connection (a truncated archive must never look like a
// success), and the emission fires unconditionally once streaming starts,
// aborted streams included — this hands over every row of every table.
func (s *Server) handleSQLExportDownload(w http.ResponseWriter, r *http.Request) {
	if s.sqlExport == nil {
		writeJSONError(w, http.StatusForbidden,
			"custom .sql backups from the console are not enabled; they need the watch daemon with baseline creation or refresh turned on")
		return
	}
	e, ok := s.requireMonitorEntry(w, r.PathValue("id"))
	if !ok {
		return
	}
	// Same invariant as every baseline read (#1075): the dump bypasses RBAC
	// redaction, so a session carrying a data profile is refused.
	if sessionRestricted(r) {
		recordProfileGateDeny(r, "sql-export-download")
		writeJSONError(w, http.StatusForbidden,
			"backups are unavailable while an access-control profile is active: baseline reads aren't redacted")
		return
	}
	dir, st, ready := s.sqlExport.SQLExportDir(e.ID)
	if !ready {
		writeJSONError(w, http.StatusConflict,
			"no finished .sql backup to download; build one first (it may still be running, the last build may have failed, or its files were removed from the staging directory; building again fixes all three)")
		return
	}
	stamp := "backup"
	if t, err := time.Parse(time.RFC3339, st.At); err == nil {
		stamp = t.UTC().Format("2006-01-02T15-04-05Z")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "read the built backup: "+err.Error())
		return
	}
	var names []string
	for _, ent := range entries {
		name := ent.Name()
		// The engine writes a flat dump; a subdirectory means the layout
		// changed under this handler, and silently skipping it would ship an
		// archive missing whatever it holds.
		if ent.IsDir() {
			writeJSONError(w, http.StatusInternalServerError,
				"the built backup holds an unexpected subdirectory ("+name+"); build it again")
			return
		}
		// The completeness markers describe the BUILD; the dump myloader
		// consumes is everything else (schema files, chunks, metadata).
		if name == baseline.SuccessMarker || name == baseline.IncompleteMarker {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) == 0 {
		writeJSONError(w, http.StatusConflict, "the built backup is empty; build it again")
		return
	}

	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", `attachment; filename="dbtrail-sql-`+stamp+`.tar.gz"`)

	var sent int64
	var sentFiles int
	completed := false
	defer func() {
		detail := map[string]string{
			"format": "sql",
			"at":     st.At,
			"files":  strconv.Itoa(sentFiles),
			"bytes":  strconv.FormatInt(sent, 10),
		}
		if !completed {
			detail["aborted"] = "true"
		}
		recordConsoleAccess(r, "baseline.download", "", "", detail)
	}()
	abort := func(msg string, err error) {
		if errors.Is(err, context.Canceled) || r.Context().Err() != nil {
			slog.Info("sql backup download canceled by the client", "server", e.ID, "bytes", sent)
		} else {
			slog.Warn(msg, "server", e.ID, "error", err)
		}
		panic(http.ErrAbortHandler)
	}

	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	prefix := "dbtrail-sql-" + stamp + "/"
	for _, name := range names {
		full := filepath.Join(dir, name)
		info, err := os.Stat(full)
		if err != nil {
			abort("sql backup download aborted: file unreadable", err)
		}
		hdr := &tar.Header{Name: prefix + name, Mode: 0o644, Size: info.Size(), ModTime: info.ModTime()}
		if err := tw.WriteHeader(hdr); err != nil {
			abort("sql backup download aborted: tar header write failed", err)
		}
		f, err := os.Open(full)
		if err != nil {
			abort("sql backup download aborted: file unreadable", err)
		}
		n, err := io.Copy(tw, f)
		f.Close()
		sent += n
		if err != nil {
			abort("sql backup download aborted mid-file", err)
		}
		if n != info.Size() {
			abort("sql backup download aborted: file changed mid-stream", errors.New("short read"))
		}
		sentFiles++
	}
	if err := tw.Close(); err != nil {
		abort("sql backup download: tar finalize failed", err)
	}
	if err := gz.Close(); err != nil {
		abort("sql backup download: gzip finalize failed", err)
	}
	// A rebuild's teardown racing this stream can hand the ReadDir above a
	// subset whose every surviving file then streams cleanly — the one shape
	// the per-file guards cannot see. If the wipe happened, the _SUCCESS
	// marker is gone with it: re-check before declaring the archive whole.
	if _, err := os.Stat(filepath.Join(dir, baseline.SuccessMarker)); err != nil {
		abort("sql backup download aborted: the build was replaced mid-stream", err)
	}
	completed = true
}
