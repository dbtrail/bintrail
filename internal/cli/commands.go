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

// AddForensicsCommands registers the forensics read commands: who-changed,
// user-activity, connection-history, and ddl-history (#706).
//
// Deliberately NOT part of AddReadCommands: these commands interrogate
// MySQL-family sources (performance_schema, the audit-plugin family, binlog
// connection ids), so registering them on the shared read plane would expose
// dead commands on bintrail-pg. Only cmd/bintrail calls this — the same
// scoping rule that keeps the MySQL-only doctor out of the shared set. Each
// command's RunE checks forensics.Enabled() at entry (the entitlement seam,
// epic #701 D1); the library underneath stays mechanism-only.
func AddForensicsCommands(root *cobra.Command) {
	root.AddCommand(whoChangedCmd)
	root.AddCommand(userActivityCmd)
	root.AddCommand(connectionHistoryCmd)
	root.AddCommand(ddlHistoryCmd)
}
