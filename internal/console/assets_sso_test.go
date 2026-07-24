package console

import (
	"os"
	"strings"
	"testing"
)

// The SSO login-screen plumbing crosses the Go↔JS seam on two literal key
// names (sso_name/sso_start in GET /api/auth) and one client-side convention
// (every gate raised from an /api/auth probe threads ssoStart/ssoName into
// showLoginOverlay, and every gate mode renders appendSSOEntry). The repo has
// no JS harness, so these are static tripwires: a rename or a dropped call
// site on the client silently suppresses the only UI entry point for an
// external provider (appendSSOEntry no-ops on a missing ssoStart) while every
// Go test stays green. TestAuthInfoAdvertisesSSOWithProvider pins the server
// side of the same contract.
func TestAppJSThreadsSSOProbeFields(t *testing.T) {
	data, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(data)

	// (a) the client consumes the exact probe keys the server emits.
	for _, needle := range []string{"auth.sso_name", "auth.sso_start"} {
		if !strings.Contains(js, needle) {
			t.Errorf("assets/app.js no longer reads %q — the /api/auth probe keys are a wire contract with handleAuthInfo", needle)
		}
	}

	// (b) every showLoginOverlay call built from a probe response (it
	// references auth.<field>) must thread the SSO fields, or that gate mode
	// silently loses the "Continue with <name>" entry.
	for i, line := range strings.Split(js, "\n") {
		if !strings.Contains(line, "showLoginOverlay(") || !strings.Contains(line, "auth.") {
			continue
		}
		if !strings.Contains(line, "ssoStart: auth.sso_start") || !strings.Contains(line, "ssoName: auth.sso_name") {
			t.Errorf("assets/app.js:%d raises the sign-in gate from an /api/auth probe without threading ssoName/ssoStart:\n%s", i+1, strings.TrimSpace(line))
		}
	}

	// (c) all three gate modes (setup form, password form, token/SSO hint)
	// render the SSO entry point.
	if got := strings.Count(js, "appendSSOEntry(panel, opts)"); got < 3 {
		t.Errorf("appendSSOEntry(panel, opts) appears %d times in assets/app.js, want >= 3 (setup, password, and token-hint gate modes)", got)
	}
	if !strings.Contains(js, "if (!opts.ssoStart) return;") {
		t.Error("appendSSOEntry lost its no-provider guard (if (!opts.ssoStart) return;) — without it the stock build would render a dead SSO link")
	}
}
