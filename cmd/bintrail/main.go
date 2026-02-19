package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "bintrail",
	Short: "MySQL binlog indexer and recovery tool",
	Long: `Bintrail parses MySQL ROW-format binary logs, indexes every row event into a
MySQL table with full before/after images, and provides query and recovery
capabilities. The index is self-contained — recovery does not depend on
binlog files still existing on disk.`,
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
