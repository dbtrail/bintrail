package cli

import (
	"testing"

	"github.com/spf13/cobra"
)

// TestShimCmd_registered mirrors the sibling *_registered tests: it pins that
// AddReadCommands wires shim onto a root command. The binary-level wiring (that
// main.go actually calls cli.AddReadCommands) is covered separately by
// cmd/bintrail/read_commands_test.go.
func TestShimCmd_registered(t *testing.T) {
	root := &cobra.Command{Use: "root"}
	AddReadCommands(root)
	found := false
	for _, cmd := range root.Commands() {
		if cmd.Use == "shim" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'shim' command to be registered by AddReadCommands")
	}
}
