package main

import (
	"testing"

	"github.com/spf13/cobra"
)

// TestRootHookIsNotShadowed — see the identical guard in cliapp. Cobra runs
// only the closest PersistentPreRun hook, so a subcommand growing its own would
// silently un-instrument its whole subtree and drop observe.Setup with it.
func TestRootHookIsNotShadowed(t *testing.T) {
	var walk func(*cobra.Command)
	walk = func(parent *cobra.Command) {
		for _, sub := range parent.Commands() {
			if sub.PersistentPreRunE != nil || sub.PersistentPreRun != nil {
				t.Errorf("%q defines its own PersistentPreRun hook, which shadows the root's: "+
					"every command at or below it loses both telemetry and observe.Setup",
					sub.CommandPath())
			}
			walk(sub)
		}
	}
	walk(rootCmd)
}
