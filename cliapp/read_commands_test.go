package cliapp

import "testing"

// TestReadCommandsWiredOnRoot asserts that the source-agnostic read commands
// migrated to internal/cli (#529) are actually registered on the real rootCmd —
// i.e. that main.go calls cli.AddReadCommands(rootCmd). Each command's own
// registration test moved into internal/cli with it (where it tests
// AddReadCommands against a throwaway root), so only THIS test pins that the
// shipped binary wires them in: a dropped AddReadCommands call would otherwise
// let a command silently vanish from the CLI with the unit suite still green.
//
// Add to `want` whenever a new read command is registered by cli.AddReadCommands.
func TestReadCommandsWiredOnRoot(t *testing.T) {
	want := []string{"status", "query", "recover", "reconstruct", "shim"}

	have := make(map[string]bool)
	for _, c := range rootCmd.Commands() {
		have[c.Use] = true
	}
	for _, name := range want {
		if !have[name] {
			t.Errorf("read command %q not registered on rootCmd — main.go must call cli.AddReadCommands(rootCmd)", name)
		}
	}
}
