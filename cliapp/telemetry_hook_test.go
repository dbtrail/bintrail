package cliapp

import (
	"testing"

	"github.com/spf13/cobra"
)

// TestRootHookIsNotShadowed guards the "instrument once" property.
//
// Cobra runs only the CLOSEST PersistentPreRun hook, not every one up the
// chain. So a subcommand that grows its own would silently stop the root hook
// from running for that command AND its entire subtree — telemetry would go
// quiet there with nothing failing, and the aggregates would simply be wrong in
// a way nobody could see. observe.Setup lives in the same hook, so such a
// subtree would also lose its logging configuration.
//
// If you genuinely need per-command setup, call it from the command's own
// RunE, or have the new hook invoke the root's.
func TestRootHookIsNotShadowed(t *testing.T) {
	assertNoShadowedHooks(t, rootCmd)
}

func assertNoShadowedHooks(t *testing.T, root *cobra.Command) {
	t.Helper()
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
	walk(root)
}
