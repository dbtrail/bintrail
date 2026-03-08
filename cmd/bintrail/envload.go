package main

import (
	"bufio"
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
// to apply environment variables to Cobra flags and by config init to
// generate the .bintrail.env template.
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
			continue
		}
		parseAndSetEnv(string(data))
		return // use first found
	}
}

// parseAndSetEnv parses key=value lines from data and sets them as
// environment variables. Lines starting with # and blank lines are
// skipped. Values may be surrounded by single or double quotes
// (stripped). Variables already set in the environment are not
// overwritten.
func parseAndSetEnv(data string) {
	scanner := bufio.NewScanner(strings.NewReader(data))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || line[0] == '#' {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
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
			_ = cmd.Flags().Set(b.Flag, v)
			continue
		}
		if cmd.PersistentFlags().Lookup(b.Flag) != nil {
			_ = cmd.PersistentFlags().Set(b.Flag, v)
		}
	}
}
