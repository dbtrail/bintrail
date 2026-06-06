package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRunConsoleStartupGuards covers the three pre-connect guards in
// runConsole — they all return before config.Connect, so they are unit-testable
// without a database. A regression in the --profile/--baseline guards would
// crash later against a nil DB (LoadProfileRules / a DB-less boot bundle).
func TestRunConsoleStartupGuards(t *testing.T) {
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
			// Neutralize ambient env: runConsole falls back to these when the
			// flag was not Changed (we set the package globals directly).
			t.Setenv("BINTRAIL_CONSOLE_SERVERS", "")
			tc.setup()
			err := runConsole(consoleCmd, nil)
			if err == nil || !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("err = %v, want error containing %q", err, tc.wantSub)
			}
		})
	}
}

// TestRunConsoleCorruptRegistryFailsLoud: a corrupt registry file aborts
// startup — silently starting without the operator's saved servers would look
// like data loss.
func TestRunConsoleCorruptRegistryFailsLoud(t *testing.T) {
	conIndexDSN, conProfile, conBaselineDir, conBaselineS3 = "", "", "", ""
	corrupt := filepath.Join(t.TempDir(), "servers.yaml")
	if err := os.WriteFile(corrupt, []byte("{{{ not yaml"), 0o600); err != nil {
		t.Fatal(err)
	}
	conServersFile = corrupt
	t.Cleanup(func() { conServersFile = "" })
	t.Setenv("BINTRAIL_CONSOLE_SERVERS", "")

	err := runConsole(consoleCmd, nil)
	if err == nil || !strings.Contains(err.Error(), "parse server registry") {
		t.Fatalf("err = %v, want a loud registry-parse failure", err)
	}
}
