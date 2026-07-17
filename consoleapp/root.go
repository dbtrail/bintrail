// Package consoleapp is the importable command layer of the bintrail-console
// binary — the console sibling of the cliapp seam. cmd/bintrail-console is a
// thin main() over it; embedding distributions build their own console binary
// the same way and may install ext seams (e.g. ext.SetConsoleAuth for an
// external login flow) from main() before calling Main. The console server
// reads those seams at construction time, so installs after Main returns are
// never picked up.
//
// The console is the MCP server with a web face: browse indexed row events
// with full before/after diffs, generate recovery (undo) SQL, and — when
// baselines are configured — run point-in-time reconstruct, all from a
// browser. The console NEVER executes SQL; recover produces a script you
// review and apply.
//
//	bintrail-console serve --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index"
//
// Configuration mirrors the core CLI: a .bintrail.env (or
// ~/.config/bintrail/config.env) file is loaded on startup and the
// BINTRAIL_INDEX_DSN / BINTRAIL_CONSOLE_* env vars are honored with
// flag > env > default precedence.
package consoleapp

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/observe"
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
	rootCmd.Version = fmt.Sprintf("%s (commit %s, built %s)", version, commitSHA, buildDate)
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}
