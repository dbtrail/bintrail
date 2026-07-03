package cliapp

import "testing"

// TestForensicsCommandsWiredOnRoot pins that main.go calls
// cli.AddForensicsCommands(rootCmd) — the read_commands_test.go convention:
// the registration set itself is tested in internal/cli against a throwaway
// root; only this test proves the shipped binary wires the commands in.
//
// The forensics commands are deliberately NOT in cli.AddReadCommands: they
// interrogate MySQL-family sources (performance_schema, audit plugins), so
// bintrail-pg — which shares AddReadCommands — must not grow them. Like
// doctor, they are registered by this binary only.
func TestForensicsCommandsWiredOnRoot(t *testing.T) {
	want := []string{"who-changed", "user-activity", "connection-history", "ddl-history"}

	have := make(map[string]bool)
	for _, c := range rootCmd.Commands() {
		have[c.Use] = true
	}
	for _, name := range want {
		if !have[name] {
			t.Errorf("forensics command %q not registered on rootCmd — main.go must call cli.AddForensicsCommands(rootCmd)", name)
		}
	}
}
