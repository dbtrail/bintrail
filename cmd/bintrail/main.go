package main

import (
	"encoding/json"
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
		if wantsJSON(rootCmd) {
			json.NewEncoder(os.Stderr).Encode(map[string]string{"error": err.Error()})
		} else {
			fmt.Fprintln(os.Stderr, err)
		}
		os.Exit(1)
	}
}

// outputJSON encodes v as indented JSON to stdout.
func outputJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// wantsJSON reports whether the active command has a --format flag set to "json".
func wantsJSON(root *cobra.Command) bool {
	// CalledAs returns the command that was actually invoked.
	// Walk the command tree to find the leaf command.
	cmd, _, _ := root.Find(os.Args[1:])
	if cmd == nil {
		return false
	}
	f := cmd.Flags().Lookup("format")
	if f == nil {
		return false
	}
	return f.Value.String() == "json"
}
