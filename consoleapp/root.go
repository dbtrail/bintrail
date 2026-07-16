// Package consoleapp implements the bintrail-console command layer — the
// web console's cobra root command and its subcommands (serve, watch,
// user). cmd/bintrail-console is a thin wrapper around it; embedding
// distributions — builds that import the OSS core and wrap it — call
// Main from their own main() the same way, passing their
// -ldflags-injected build metadata.
//
// Same startup-only contract as cliapp: package-level state (the root
// command, subcommand registration via init()) is assembled before
// dispatch and is not safe for concurrent use with command execution.
package consoleapp

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/observe"
)

// Build metadata, set by Main from the caller's -ldflags-injected values.
var (
	Version   = "dev"
	CommitSHA = "none"
	BuildDate = "unknown"
)

var (
	logLevel  string
	logFormat string
)

var rootCmd = &cobra.Command{
	Use:   "bintrail-console",
	Short: "Read-only web console over the Bintrail binlog index",
	Long: `bintrail-console serves a local, read-only web UI over the binlog index:
browse indexed row events with full before/after diffs, generate recovery
(undo) SQL, and run point-in-time reconstruct when baselines are configured.

It is the web console's own binary (formerly the core CLI's "console"
command), shipped separately so the core bintrail CLI carries no web UI. The
console NEVER executes SQL; recover produces a script you review and apply
yourself.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		observe.Setup(os.Stderr, logFormat, logLevel)
		return nil
	},
	SilenceErrors: true, // we handle error output ourselves in Main()
	SilenceUsage:  true, // don't print usage/help on errors — users can use --help
}

func init() {
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "Log level: debug, info, warn, error")
	rootCmd.PersistentFlags().StringVar(&logFormat, "log-format", "text", "Log format: text or json")
}

// Main configures build metadata and runs the bintrail-console root command,
// returning the process exit code. Callers (cmd/bintrail-console, and
// external distributions embedding the console) are expected to pass their
// -ldflags-injected version values and os.Exit with the result.
func Main(version, commitSHA, buildDate string) int {
	Version, CommitSHA, BuildDate = version, commitSHA, buildDate
	rootCmd.Version = fmt.Sprintf("%s (commit %s, built %s)", Version, CommitSHA, BuildDate)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}
