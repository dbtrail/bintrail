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
	// Enabled reports whether a refresh loop is actually RUNNING, which is
	// false when the daemon was started without --baseline-refresh-interval.
	// Independent of whether an override exists: the loop's run/skip decision
	// is taken once at boot, so a saved setting is dormant until a restart and
	// the panel must keep saying so rather than implying it is live.
	Enabled bool `json:"enabled"`
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
	if bc, ok := s.cm.reg.BaselineRefresh(); ok {
		return baselineRefreshDTO{
			CarryForwardUnchanged: bc.CarryForwardUnchanged,
			Source:                "override",
			Enabled:               d.Enabled,
		}
	}
	return baselineRefreshDTO{
		CarryForwardUnchanged: d.CarryForwardUnchanged,
		Source:                "default",
		Enabled:               d.Enabled,
	}
}

// handleBaselineRefreshUpdate serves PUT /api/baseline-refresh: persist a
// global override, or clear one. Refused on the read-only console, which runs
// no loop to consume it, and on a newer-version (read-only) registry file.
//
// It applies on the next tick of a loop that is ALREADY running, because every
// cycle re-reads the registry. With no --baseline-refresh-interval there is no
// loop and it applies to nothing until the daemon is restarted with one, which
// is what the DTO's Enabled reports and what the panel has to keep saying.
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
