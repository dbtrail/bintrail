package cliapp

import (
	"testing"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cli"
)

// collectCommands returns cmd and every command reachable from it.
func collectCommands(cmd *cobra.Command) []*cobra.Command {
	cmds := []*cobra.Command{cmd}
	for _, sub := range cmd.Commands() {
		cmds = append(cmds, collectCommands(sub)...)
	}
	return cmds
}

// envBindingConditional lists bindings whose flag is deliberately NOT
// registered anywhere on the bintrail command tree (e.g. a flag that exists
// only on another binary or under a build tag). Every entry needs a comment
// explaining why the exception is legitimate. Empty today: all bindings
// resolve on this tree.
var envBindingConditional = map[string]bool{}

// TestEnvBindingsResolveToRegisteredFlags asserts every entry in
// cli.EnvBindings names a flag that exists on at least one command in the
// bintrail command tree. cli.BindCommandEnv silently no-ops when a binding's
// flag is missing from the command it is called on, so a flag rename, move,
// or a typo in the table would silently disconnect the BINTRAIL_* env var —
// the documented configuration channel for containerized deployments — while
// the flag itself kept working (#1130). This turns that silent disconnection
// into a failing build.
func TestEnvBindingsResolveToRegisteredFlags(t *testing.T) {
	// Positive anchors: an empty table or an empty tree must fail, not pass
	// vacuously.
	if len(cli.EnvBindings) == 0 {
		t.Fatal("cli.EnvBindings is empty; the walk below would pass vacuously")
	}
	cmds := collectCommands(rootCmd)
	// The bintrail tree has 20+ commands; a walk that finds almost none means
	// the tree was not constructed (init() not run, wrong root), not that the
	// CLI shrank.
	if len(cmds) < 10 {
		t.Fatalf("command tree walk found only %d commands; expected the full bintrail tree", len(cmds))
	}

	for _, b := range cli.EnvBindings {
		found := false
		for _, cmd := range cmds {
			// Same lookup order BindCommandEnv uses: local flags, then
			// persistent flags.
			if cmd.Flags().Lookup(b.Flag) != nil || cmd.PersistentFlags().Lookup(b.Flag) != nil {
				found = true
				break
			}
		}
		if found {
			if envBindingConditional[b.Flag] {
				t.Errorf("binding %q -> %s is in envBindingConditional but its flag DOES resolve; remove the stale exception", b.Flag, b.EnvVar)
			}
			continue
		}
		if envBindingConditional[b.Flag] {
			continue
		}
		t.Errorf("binding %q -> %s names a flag that no command registers; the env var is silently disconnected (BindCommandEnv no-ops on a missing flag)", b.Flag, b.EnvVar)
	}
}
