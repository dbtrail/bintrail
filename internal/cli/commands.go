package cli

import "github.com/spf13/cobra"

// AddReadCommands registers the source-agnostic read/recover commands on root.
// Each binary (the core bintrail, and the planned PostgreSQL-native bintrail-pg
// — #527) calls this so both expose the same status/query/recover/reconstruct/
// shim surface without duplicating the command definitions.
//
// Commands are migrated into internal/cli one slice at a time (#529); this now
// registers the full read-plane surface: status, query, recover, reconstruct,
// and shim. "Read" is the serve/read plane (these read from or serve the shared
// index) as opposed to the capture plane (stream/index/snapshot/agent), which
// stays source-specific in each binary's cmd/ package. shim is a long-running
// MySQL-protocol server rather than a one-shot query, but it serves the same
// index, so it belongs here too.
func AddReadCommands(root *cobra.Command) {
	root.AddCommand(statusCmd)
	root.AddCommand(queryCmd)
	root.AddCommand(recoverCmd)
	root.AddCommand(recoverCascadeCmd)
	root.AddCommand(reconstructCmd)
	root.AddCommand(verifyCmd)
	root.AddCommand(shimCmd)
}

// AddMaintenanceCommands registers the index-side maintenance commands: rotate
// and archive reconcile. Both operate purely on the shared MySQL index
// (partition rotation, archive_state reconciliation) and are source-agnostic,
// so every capture binary registers them — the core bintrail AND bintrail-pg
// (#951). Without this, a PostgreSQL-only install has no way to bound index
// growth: its named partitions fill within ~2 days and every later event piles
// into p_future unbounded, the disk-full mode #406/#420 closed for MySQL. The
// long-running daemons (up, watch, and now bintrail-pg stream) additionally run
// the built-in rotation loop; these standalone commands cover offline/manual
// maintenance and the archive-then-drop retention path the loop does not.
func AddMaintenanceCommands(root *cobra.Command) {
	root.AddCommand(rotateCmd)
	root.AddCommand(archiveCmd)
}
