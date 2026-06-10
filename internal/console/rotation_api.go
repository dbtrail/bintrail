package console

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/cliutil"
)

// rotationDTO is the effective global built-in-rotation policy on the wire.
type rotationDTO struct {
	Retain    string `json:"retain"`
	Interval  string `json:"interval"`
	AddFuture int    `json:"add_future"`
	// Source is "override" when the values come from a console-saved policy,
	// "default" when they are the daemon's --rotate-* flags/env.
	Source string `json:"source"`
	// Enabled is false only when the daemon was started with rotation off and
	// no override is saved — the loop is not running, so a saved change would
	// need a restart. The UI warns in that state.
	Enabled bool `json:"enabled"`
}

// rotationRequest is the PUT /api/rotation body.
type rotationRequest struct {
	Retain    string `json:"retain"`
	Interval  string `json:"interval"`
	AddFuture int    `json:"add_future"`
}

// handleRotationGet serves GET /api/rotation: the effective global rotation
// policy (a saved override, else the daemon's --rotate-* defaults). Always
// readable — it leaks no secret and the panel needs it to prefill.
func (s *Server) handleRotationGet(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.effectiveRotation())
}

// effectiveRotation resolves the policy the daemon is actually running: a
// console-saved override wins, else the injected daemon defaults.
func (s *Server) effectiveRotation() rotationDTO {
	if rc, ok := s.cm.reg.Rotation(); ok {
		return rotationDTO{
			Retain:    rc.Retain,
			Interval:  rc.Interval,
			AddFuture: rc.AddFuture,
			Source:    "override",
			Enabled:   true,
		}
	}
	d := s.rotationDefaults
	return rotationDTO{
		Retain:    d.Retain,
		Interval:  d.Interval,
		AddFuture: d.AddFuture,
		Source:    "default",
		Enabled:   d.Enabled,
	}
}

// handleRotationUpdate serves PUT /api/rotation: validate and persist a global
// rotation override. It applies live — the watch loop re-reads the registry
// each cycle. Refused on the read-only console (no loop to consume it) and on a
// newer-version (read-only) registry file.
func (s *Server) handleRotationUpdate(w http.ResponseWriter, r *http.Request) {
	if s.monitorCtrl == nil {
		writeJSONError(w, http.StatusForbidden,
			"rotation is configured by the watch daemon (bintrail-console watch), not the read-only console")
		return
	}
	var req rotationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	retain := strings.TrimSpace(req.Retain)
	interval := strings.TrimSpace(req.Interval)
	// Retain must be a concrete window (Nd/Nh). Disabling rotation entirely
	// stays a daemon-level decision (--rotate-retain off); the panel tunes a
	// running loop rather than turning it off (which would need a restart).
	if _, err := cliutil.ParseRetain(retain); err != nil {
		writeJSONError(w, http.StatusBadRequest, "retain must be a window like 30d or 24h: "+err.Error())
		return
	}
	iv, err := time.ParseDuration(interval)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "interval must be a duration like 1h or 30m: "+err.Error())
		return
	}
	if iv <= 0 {
		writeJSONError(w, http.StatusBadRequest, "interval must be positive")
		return
	}
	if req.AddFuture < 0 {
		writeJSONError(w, http.StatusBadRequest, "add_future cannot be negative")
		return
	}
	if err := s.cm.reg.SetRotation(RotationConfig{Retain: retain, Interval: interval, AddFuture: req.AddFuture}); err != nil {
		writeJSONError(w, registryErrStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, s.effectiveRotation())
}
