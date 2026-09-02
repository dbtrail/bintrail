package console

import (
	"encoding/json"
	"net/http"
)

// baselineRefreshDTO is the effective global baseline-refresh policy on the
// wire.
type baselineRefreshDTO struct {
	CarryForwardUnchanged bool `json:"carry_forward_unchanged"`
	// Source is "override" when the value comes from a console-saved policy,
	// "default" when it is the daemon's own flag/env.
	Source string `json:"source"`
	// Enabled reports whether any consumer of the setting is live in this
	// daemon: the periodic refresh loop, the point-in-time restore, or both.
	// Independent of whether an override exists, because liveness is decided at
	// boot and a saved setting is dormant until a restart when nothing consumes
	// it.
	Enabled bool `json:"enabled"`
	// Scheduled reports the narrower fact that a periodic refresh loop is
	// running. Enabled without Scheduled is the --baseline-trigger daemon: the
	// setting governs restores today and nothing is on a timer.
	Scheduled bool `json:"scheduled"`
	// Targets is how many servers the NEXT refresh tick will cover, computed
	// live at request time (the loop recomputes per tick, so a boot snapshot
	// would go stale the moment a server is added). A pointer so it is
	// OMITTED where the daemon wired no counter (serve, or no loop) and a
	// real zero where the loop runs over nothing — the enabled+scheduled+
	// zero-targets shape the page previously reported as everything-running
	// (#1579).
	Targets *int `json:"targets,omitempty"`
	// SkippedS3Only counts servers the tick skips for keeping baselines only
	// in S3: a refresh writes Parquet to a filesystem, so it needs a local
	// directory to fold into. The reason "covers every server" was false.
	SkippedS3Only int `json:"skipped_s3_only,omitempty"`
}

// baselineRefreshRequest is the PUT /api/baseline-refresh body.
type baselineRefreshRequest struct {
	CarryForwardUnchanged bool `json:"carry_forward_unchanged"`
	// UseDefault clears the saved override instead of writing one, handing the
	// decision back to the daemon's own flag and environment.
	//
	// A separate field rather than a null CarryForwardUnchanged, because the
	// two absences must not mean the same thing: a body with no keys has to
	// stay "off", the conservative value, and making absence mean "clear"
	// would turn a truncated request into whatever the daemon flag happens to
	// say, including on.
	UseDefault bool `json:"use_default"`
}

// handleBaselineRefreshGet serves GET /api/baseline-refresh. Always readable:
// it leaks no secret and the panel needs it to prefill.
func (s *Server) handleBaselineRefreshGet(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.effectiveBaselineRefresh())
}

// effectiveBaselineRefresh resolves what the daemon is actually running: a
// console-saved override wins, else the injected daemon default. Enabled comes
// from the daemon's boot-time liveness in BOTH branches, never forced true by
// the presence of an override.
func (s *Server) effectiveBaselineRefresh() baselineRefreshDTO {
	d := s.baselineRefreshDefaults
	dto := baselineRefreshDTO{
		CarryForwardUnchanged: d.CarryForwardUnchanged,
		Source:                "default",
		Enabled:               d.Enabled,
		Scheduled:             d.Scheduled,
	}
	if bc, ok := s.cm.reg.BaselineRefresh(); ok {
		dto.CarryForwardUnchanged = bc.CarryForwardUnchanged
		dto.Source = "override"
	}
	if s.baselineRefreshTargets != nil {
		targets, skipped := s.baselineRefreshTargets()
		dto.Targets = &targets
		dto.SkippedS3Only = skipped
	}
	return dto
}

// handleBaselineRefreshUpdate serves PUT /api/baseline-refresh: persist a
// global override, or clear one. Refused on the read-only console, which runs
// no loop to consume it, and on a newer-version (read-only) registry file.
//
// It applies to the next run of whatever consumes it, with no restart: the
// refresh loop re-reads the registry every cycle, and a restore reads it when
// the operator asks for one. On a daemon that runs neither it applies to
// nothing until a restart, which is what the DTO's Enabled reports and what the
// panel has to keep saying.
//
// The body is decoded into a struct with plain bools rather than read as a
// partial patch on purpose: a body with the key absent decodes to false, which
// is the conservative value, so a body that lost keys can only ever turn the
// behaviour OFF, never on.
func (s *Server) handleBaselineRefreshUpdate(w http.ResponseWriter, r *http.Request) {
	if s.monitorCtrl == nil {
		writeJSONError(w, http.StatusForbidden,
			"baseline refresh is configured by the watch daemon (bintrail-console watch), not the read-only console")
		return
	}
	var req baselineRefreshRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeBodyDecodeError(w, err)
		return
	}
	var bc *BaselineRefreshConfig
	if !req.UseDefault {
		bc = &BaselineRefreshConfig{CarryForwardUnchanged: req.CarryForwardUnchanged}
	}
	if err := s.cm.reg.SetBaselineRefresh(bc); err != nil {
		writeJSONError(w, registryErrStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, s.effectiveBaselineRefresh())
}
