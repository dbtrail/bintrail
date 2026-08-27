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
