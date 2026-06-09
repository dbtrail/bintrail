package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRunServeStartupGuards covers the three pre-connect guards in runServe —
// they all return before config.Connect, so they are unit-testable without a
// database. A regression in the --profile/--baseline guards would crash later
// against a nil DB (LoadProfileRules / a DB-less boot bundle). This mirrors
// TestRunConsoleStartupGuards on the core `bintrail console` command: the two
// commands must stay behaviorally identical during the transition.
func TestRunServeStartupGuards(t *testing.T) {
	reset := func() {
		conIndexDSN, conProfile, conBaselineDir, conBaselineS3, conServersFile = "", "", "", "", ""
	}

	emptyReg := filepath.Join(t.TempDir(), "servers.yaml") // never created
	popReg := filepath.Join(t.TempDir(), "servers.yaml")
	if err := os.WriteFile(popReg,
		[]byte("version: 1\nservers:\n  - id: abcd\n    name: x\n    index_dsn: u:p@tcp(h:3306)/db\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name    string
		setup   func()
		wantSub string
	}{
		{
			"no dsn and empty registry",
			func() { conServersFile = emptyReg },
			"either --index-dsn",
		},
		{
			"profile requires index-dsn",
			func() { conServersFile = popReg; conProfile = "dev" },
			"--profile requires --index-dsn",
		},
		{
			"baseline requires index-dsn",
			func() { conServersFile = popReg; conBaselineDir = "/tmp/b" },
			"require --index-dsn",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reset()
			t.Cleanup(reset)
			// Neutralize ambient env: runServe falls back to these when the flag
			// was not Changed (we set the package globals directly). Unlike the
			// core console — which binds BINTRAIL_INDEX_DSN once at init() —
			// runServe reads it per-call, so it must be neutralized too or a dev
			// shell with BINTRAIL_INDEX_DSN set would defeat the empty-DSN guard.
			t.Setenv("BINTRAIL_INDEX_DSN", "")
			t.Setenv("BINTRAIL_CONSOLE_SERVERS", "")
			tc.setup()
			err := runServe(serveCmd, nil)
			if err == nil || !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("err = %v, want error containing %q", err, tc.wantSub)
			}
		})
	}
}

// TestRunServeCorruptRegistryFailsLoud: a corrupt registry file aborts startup
// — silently starting without the operator's saved servers would look like
// data loss.
func TestRunServeCorruptRegistryFailsLoud(t *testing.T) {
	conIndexDSN, conProfile, conBaselineDir, conBaselineS3 = "", "", "", ""
	corrupt := filepath.Join(t.TempDir(), "servers.yaml")
	if err := os.WriteFile(corrupt, []byte("{{{ not yaml"), 0o600); err != nil {
		t.Fatal(err)
	}
	conServersFile = corrupt
	t.Cleanup(func() { conServersFile = "" })
	t.Setenv("BINTRAIL_INDEX_DSN", "")
	t.Setenv("BINTRAIL_CONSOLE_SERVERS", "")

	err := runServe(serveCmd, nil)
	if err == nil || !strings.Contains(err.Error(), "parse server registry") {
		t.Fatalf("err = %v, want a loud registry-parse failure", err)
	}
}

// TestRunServeIndexDSNFromEnv asserts the POSITIVE env-fallback path — the one
// behavior unique to runServe with no parity baseline in runConsole (the core
// binds BINTRAIL_INDEX_DSN once at init() via bindCommandEnv; runServe reads it
// per-call). A decouple regression here — wrong var name, inverted Changed
// check, a dropped block — would pass every mirrored guard test silently, so it
// needs its own assertion. We give a DSN with NO database name: when the env is
// consumed, conIndexDSN becomes non-empty, the empty-registry guard is bypassed,
// and the db-name check fires ("must include a database name"); if the env were
// ignored, conIndexDSN stays "" and the "either --index-dsn" guard fires
// instead — so the error substring discriminates the two outcomes without a DB.
func TestRunServeIndexDSNFromEnv(t *testing.T) {
	conIndexDSN, conProfile, conBaselineDir, conBaselineS3 = "", "", "", ""
	conServersFile = filepath.Join(t.TempDir(), "servers.yaml") // empty registry
	t.Cleanup(func() { conIndexDSN, conServersFile = "", "" })
	t.Setenv("BINTRAIL_CONSOLE_SERVERS", "")
	t.Setenv("BINTRAIL_INDEX_DSN", "u:p@tcp(h:3306)/") // present, no db name

	err := runServe(serveCmd, nil)
	if err == nil || !strings.Contains(err.Error(), "must include a database name") {
		t.Fatalf("err = %v, want the env DSN consumed (db-name guard)", err)
	}
}
