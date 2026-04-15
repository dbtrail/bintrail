package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/spf13/cobra"
)

// envBinding maps a CLI flag name to its BINTRAIL_ environment variable.
type envBinding struct {
	Flag   string
	EnvVar string
}

// envBindings defines all flag-to-env-var mappings. Used by bindCommandEnv
// to apply environment variables to Cobra flags. The corresponding
// envSections variable in config.go defines the same env vars grouped
// by category for template generation.
var envBindings = []envBinding{
	{"index-dsn", "BINTRAIL_INDEX_DSN"},
	{"source-dsn", "BINTRAIL_SOURCE_DSN"},
	{"schemas", "BINTRAIL_SCHEMAS"},
	{"tables", "BINTRAIL_TABLES"},
	{"bintrail-id", "BINTRAIL_ID"},
	{"archive-dir", "BINTRAIL_ARCHIVE_DIR"},
	{"archive-s3", "BINTRAIL_ARCHIVE_S3"},
	{"archive-s3-region", "BINTRAIL_ARCHIVE_S3_REGION"},
	{"server-id", "BINTRAIL_SERVER_ID"},
	{"batch-size", "BINTRAIL_BATCH_SIZE"},
	{"s3-bucket", "BINTRAIL_S3_BUCKET"},
	{"s3-region", "BINTRAIL_S3_REGION"},
	{"s3-arn", "BINTRAIL_S3_ARN"},
	{"metrics-addr", "BINTRAIL_METRICS_ADDR"},
	{"ssl-mode", "BINTRAIL_SSL_MODE"},
	{"ssl-ca", "BINTRAIL_SSL_CA"},
	{"ssl-cert", "BINTRAIL_SSL_CERT"},
	{"ssl-key", "BINTRAIL_SSL_KEY"},
	{"api-key", "BINTRAIL_API_KEY"},
	{"endpoint", "BINTRAIL_AGENT_ENDPOINT"},
	{"buffer-retain", "BINTRAIL_BUFFER_RETAIN"},
	{"buffer-max-events", "BINTRAIL_BUFFER_MAX_EVENTS"},
	{"buffer-max-bytes", "BINTRAIL_BUFFER_MAX_BYTES"},
	{"start-gtid", "BINTRAIL_START_GTID"},
	{"s3-prefix", "BINTRAIL_S3_PREFIX"},
	{"flush-interval", "BINTRAIL_FLUSH_INTERVAL"},
	{"gap-timeout", "BINTRAIL_STREAM_GAP_TIMEOUT"},
	{"max-reconnect-attempts", "BINTRAIL_AGENT_MAX_RECONNECT_ATTEMPTS"},
	{"column-eq", "BINTRAIL_COLUMN_EQ"},
}

var envOnce sync.Once

// loadEnvFile reads the first found env file and loads its key=value pairs
// into the process environment without overwriting already-set variables.
// Locations tried in order:
//  1. .bintrail.env in the current directory
//  2. ~/.config/bintrail/config.env
func loadEnvFile() {
	paths := []string{".bintrail.env"}
	if home, err := os.UserHomeDir(); err == nil {
		paths = append(paths, filepath.Join(home, ".config", "bintrail", "config.env"))
	}
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				fmt.Fprintf(os.Stderr, "warning: found %s but could not read it: %v\n", p, err)
			}
			continue
		}
		parseAndSetEnv(string(data))
		return // use first found
	}
}

// parseAndSetEnv parses key=value lines from data and sets them as
// environment variables. Blank lines and lines whose first non-whitespace
// character is # are skipped. Lines without an = sign produce a warning.
// Values may be surrounded by single or double quotes (stripped).
// Variables already set in the environment are not overwritten.
func parseAndSetEnv(data string) {
	scanner := bufio.NewScanner(strings.NewReader(data))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || line[0] == '#' {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			fmt.Fprintf(os.Stderr, "warning: skipping malformed line in env file (no '='): %s\n", line)
			continue
		}
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)

		// Strip surrounding quotes.
		if len(val) >= 2 {
			if (val[0] == '"' && val[len(val)-1] == '"') ||
				(val[0] == '\'' && val[len(val)-1] == '\'') {
				val = val[1 : len(val)-1]
			}
		}

		// Don't overwrite already-set env vars.
		if _, exists := os.LookupEnv(key); !exists {
			os.Setenv(key, val)
		}
	}
}

// bindCommandEnv loads the env file (once) and applies BINTRAIL_*
// environment variables to any matching flags on cmd. Only flags that
// exist on the command are affected. The flag is marked as Changed so
// that MarkFlagRequired is satisfied.
//
// Call this in each command's init() after defining flags:
//
//	func init() {
//	    myCmd.Flags().StringVar(&myFlag, "index-dsn", "", "...")
//	    bindCommandEnv(myCmd)
//	    rootCmd.AddCommand(myCmd)
//	}
func bindCommandEnv(cmd *cobra.Command) {
	envOnce.Do(loadEnvFile)
	for _, b := range envBindings {
		v := os.Getenv(b.EnvVar)
		if v == "" {
			continue
		}
		// Try local flags first, then persistent flags.
		if f := cmd.Flags().Lookup(b.Flag); f != nil {
			if err := cmd.Flags().Set(b.Flag, v); err != nil {
				fmt.Fprintf(os.Stderr, "warning: cannot set --%s from %s: %v\n", b.Flag, b.EnvVar, err)
			}
			continue
		}
		if cmd.PersistentFlags().Lookup(b.Flag) != nil {
			if err := cmd.PersistentFlags().Set(b.Flag, v); err != nil {
				fmt.Fprintf(os.Stderr, "warning: cannot set --%s from %s: %v\n", b.Flag, b.EnvVar, err)
			}
		}
	}
}
