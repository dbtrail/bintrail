package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/bintrail/bintrail/internal/observe"
)

var (
	logLevel  string
	logFormat string
)

// Build-time variables injected via -ldflags.
var (
	Version   = "dev"
	CommitSHA = "none"
	BuildDate = "unknown"
)

var rootCmd = &cobra.Command{
	Use:   "bintrail",
	Short: "MySQL binlog indexer and recovery tool",
	Long: `Bintrail parses MySQL ROW-format binary logs, indexes every row event into a
MySQL table with full before/after images, and provides query and recovery
capabilities. The index is self-contained — recovery does not depend on
binlog files still existing on disk.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		observe.Setup(os.Stderr, logFormat, logLevel)
		return nil
	},
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
