package console

import (
	"encoding/json"
	"testing"
)

// GET reports the injected daemon default until an override is saved, then the
// override, and the source field flips so the panel can say which one an
// operator is looking at.
func TestBaselineRefreshGet_defaultThenOverride(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	srv.baselineRefreshDefaults = BaselineRefreshDefaults{CarryForwardUnchanged: true, Enabled: true}

	rec, body := doServersReq(t, srv, "GET", "/api/baseline-refresh", "")
	if rec.Code != 200 {
		t.Fatalf("GET code=%d body=%s", rec.Code, body)
	}
	var got baselineRefreshDTO
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if !got.CarryForwardUnchanged || got.Source != "default" || !got.Enabled {
		t.Fatalf("before any override: %+v, want the daemon flag reported as the default", got)
	}

	// An override that says FALSE is the case a value type could not express:
	// it must be distinguishable from "nobody has saved anything", or turning
	// the behaviour off in the panel would silently fall back to the flag that
	// turns it on.
	rec, body = doServersReq(t, srv, "PUT", "/api/baseline-refresh", `{"carry_forward_unchanged":false}`)
	if rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, body)
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.CarryForwardUnchanged || got.Source != "override" {
		t.Fatalf("after saving false: %+v, want the override to win over the daemon flag", got)
	}

	rec, body = doServersReq(t, srv, "GET", "/api/baseline-refresh", "")
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.CarryForwardUnchanged || got.Source != "override" {
		t.Fatalf("the override did not survive a re-read: %+v (code=%d)", got, rec.Code)
	}
}

// Enabled is the loop's boot-time liveness and must NOT be implied by the
// presence of an override: a daemon started with no refresh schedule runs no
// loop, so a saved setting is dormant until a restart and the panel has to keep
// saying so.
func TestBaselineRefreshGet_enabledIsNotImpliedByAnOverride(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	srv.baselineRefreshDefaults = BaselineRefreshDefaults{Enabled: false}

	if rec, body := doServersReq(t, srv, "PUT", "/api/baseline-refresh", `{"carry_forward_unchanged":true}`); rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, body)
	}
	_, body := doServersReq(t, srv, "GET", "/api/baseline-refresh", "")
	var got baselineRefreshDTO
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Enabled {
		t.Error("saving an override reported the loop as running; it is not, and the panel would stop " +
			"warning that a restart is needed")
	}
	if !got.CarryForwardUnchanged {
		t.Error("the override was lost")
	}
}

// The read-only console runs no refresh loop, so there is nothing for a saved
// setting to reach. Refusing is better than storing a value that will never be
// consulted.
func TestBaselineRefreshUpdate_refusedOnTheReadOnlyConsole(t *testing.T) {
	srv := newRegistryServer(t) // no MonitorCtrl
	rec, body := doServersReq(t, srv, "PUT", "/api/baseline-refresh", `{"carry_forward_unchanged":true}`)
	if rec.Code != 403 {
		t.Fatalf("PUT on the read-only console: code=%d body=%s, want 403", rec.Code, body)
	}
	// GET still works there: reading the effective policy leaks nothing and the
	// panel needs it to render.
	if rec, _ := doServersReq(t, srv, "GET", "/api/baseline-refresh", ""); rec.Code != 200 {
		t.Errorf("GET on the read-only console: code=%d, want 200", rec.Code)
	}
}

// A body with the key missing decodes to false, which is the conservative
// value. Pinned because the alternative reading (treat absent as "leave it
// alone") would make a truncated request silently turn the behaviour ON when
// the daemon flag has it on.
func TestBaselineRefreshUpdate_missingKeyMeansOff(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	srv.baselineRefreshDefaults = BaselineRefreshDefaults{CarryForwardUnchanged: true, Enabled: true}

	_, body := doServersReq(t, srv, "PUT", "/api/baseline-refresh", `{}`)
	var got baselineRefreshDTO
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.CarryForwardUnchanged {
		t.Error("an empty body was read as consent to reuse files; absent must mean off")
	}
}

// TestBaselineRefreshUpdate_useDefaultClearsTheOverride: the panel must not be
// a one-way door.
//
// The tri-state that lets a saved false beat a daemon flag saying true also
// means that once ANYTHING is saved, the flag can never be heard again. Without
// a clear, an operator who passes --baseline-carry-forward-unchanged watches
// every table get rewritten because of a toggle from months ago, with nothing
// anywhere naming the reason.
func TestBaselineRefreshUpdate_useDefaultClearsTheOverride(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	srv.baselineRefreshDefaults = BaselineRefreshDefaults{CarryForwardUnchanged: true, Enabled: true}

	rec, body := doServersReq(t, srv, "PUT", "/api/baseline-refresh", `{"carry_forward_unchanged":false}`)
	if rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, body)
	}
	if _, ok := srv.cm.reg.BaselineRefresh(); !ok {
		t.Fatal("saving false stored no override")
	}

	rec, body = doServersReq(t, srv, "PUT", "/api/baseline-refresh", `{"use_default":true}`)
	if rec.Code != 200 {
		t.Fatalf("clear code=%d body=%s", rec.Code, body)
	}
	if _, ok := srv.cm.reg.BaselineRefresh(); ok {
		t.Fatal("use_default did not clear the override; the daemon flag is still unreachable")
	}
	var got baselineRefreshDTO
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if !got.CarryForwardUnchanged || got.Source != "default" {
		t.Fatalf("after clearing: %+v, want the daemon flag reported as the default again", got)
	}
}

// TestBaselineRefreshUpdate_useDefaultWinsOverTheValue: use_default is an
// instruction, not a value, so a body carrying both must clear rather than save
// whatever carry_forward_unchanged happened to hold. The console never sends
// both; the point is that the handler cannot be talked into storing a value it
// was told to discard.
func TestBaselineRefreshUpdate_useDefaultWinsOverTheValue(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	srv.baselineRefreshDefaults = BaselineRefreshDefaults{CarryForwardUnchanged: false, Enabled: true}

	if rec, body := doServersReq(t, srv, "PUT", "/api/baseline-refresh",
		`{"carry_forward_unchanged":true,"use_default":true}`); rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, body)
	}
	if bc, ok := srv.cm.reg.BaselineRefresh(); ok {
		t.Fatalf("stored an override %+v when the body asked for the daemon default", bc)
	}
}

// TestBaselineRefreshGet_defaultsTravelThroughNew closes the last hop of the
// READ path: console.Config -> Server -> the DTO the panel renders.
//
// Every other test in this file sets srv.baselineRefreshDefaults by direct
// field write, which is convenient and skips exactly the assignment that could
// be deleted. Built through New() instead, so dropping that line reports the
// zero value: reuse off, no schedule, on a daemon running with both.
func TestBaselineRefreshGet_defaultsTravelThroughNew(t *testing.T) {
	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{
		Listen: "127.0.0.1:8090", Token: "t", Registry: reg, MonitorCtrl: &stubMonitorCtrl{},
		BaselineRefreshDefaults: BaselineRefreshDefaults{CarryForwardUnchanged: true, Enabled: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	rec, body := doServersReq(t, srv, "GET", "/api/baseline-refresh", "")
	if rec.Code != 200 {
		t.Fatalf("GET code=%d body=%s", rec.Code, body)
	}
	var got baselineRefreshDTO
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if !got.CarryForwardUnchanged {
		t.Error("the daemon's reuse flag did not survive New(); the panel would offer to turn on what is already on")
	}
	if !got.Enabled {
		t.Error("the daemon's loop liveness did not survive New(); the panel would call a live setting dormant")
	}
	if got.Source != "default" {
		t.Errorf("Source=%q, want \"default\"", got.Source)
	}
}
