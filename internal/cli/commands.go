package cli

import "github.com/spf13/cobra"

// AddReadCommands registers the source-agnostic read/recover commands on root.
// Each binary (the core bintrail, and the planned PostgreSQL-native bintrail-pg
// — #527) calls this so both expose the same query/recover/reconstruct/status/
// shim surface without duplicating the command definitions.
//
// Commands are migrated into internal/cli one slice at a time (#529); today this
// registers only status. As the others move, they join this function.
func AddReadCommands(root *cobra.Command) {
	root.AddCommand(statusCmd)
}
