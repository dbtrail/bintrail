// Command bintrail-console serves the read-only Bintrail web console as a
// standalone binary, decoupled from the core `bintrail` CLI.
//
// It is the MCP server with a web face: browse indexed MySQL row events with
// full before/after diffs, generate recovery (undo) SQL, and — when baselines
// are configured — run point-in-time reconstruct, all from a browser. The
// console NEVER executes SQL; recover produces a script you review and apply.
//
//	bintrail-console serve --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index"
//
// Configuration mirrors `bintrail console`: a .bintrail.env (or
// ~/.config/bintrail/config.env) file is loaded on startup and the
// BINTRAIL_INDEX_DSN / BINTRAIL_CONSOLE_* env vars are honored with
// flag > env > default precedence.
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/dbtrail/bintrail/internal/observe"
)

// Build-time variables injected via -ldflags. The names are deliberately the
// same as the bintrail binary's (main.Version/CommitSHA/BuildDate) so the
// Makefile's BINTRAIL_LDFLAGS applies to this binary verbatim.
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

It is the standalone form of "bintrail console", shipped as its own binary so
the core bintrail CLI carries no web UI. The console NEVER executes SQL;
recover produces a script you review and apply yourself.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		observe.Setup(os.Stderr, logFormat, logLevel)
		return nil
	},
	SilenceErrors: true, // we handle error output ourselves in main()
	SilenceUsage:  true, // don't print usage/help on errors — users can use --help
}

func init() {
	rootCmd.Version = fmt.Sprintf("%s (commit %s, built %s)", Version, CommitSHA, BuildDate)
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "Log level: debug, info, warn, error")
	rootCmd.PersistentFlags().StringVar(&logFormat, "log-format", "text", "Log format: text or json")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
