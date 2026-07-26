package consoleapp

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// envOnce guards loadEnvFile so the env file is read at most once per process.
var envOnce sync.Once

// loadEnvFile reads the first found env file and loads its key=value pairs
// into the process environment without overwriting already-set variables.
// Locations tried in order:
//  1. .bintrail.env in the current directory
//  2. ~/.config/bintrail/config.env
//
// loadEnvFile/parseAndSetEnv mirror the core bintrail loader (now in
// internal/cli/env.go since #529; was cmd/bintrail/envload.go) — the console
// keeps its own copy. Since #529 placed these helpers in internal/cli, this
// copy is a consolidation candidate (the "dedup env loader" follow-up).
// `serve` reads only
// BINTRAIL_INDEX_DSN / BINTRAIL_CONSOLE_* directly in runServe; `watch`
// additionally binds the stream/rotation BINTRAIL_* vars to its flags via
// bindWatchEnv (watch.go), the same role the core's bindCommandEnv plays.
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
	// Allow long values (base64 keys, DSN lists): the default 64KiB token
	// limit would abort the scan silently. See scanner.Err() check below.
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
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
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "warning: stopped reading env file (%v): variables after this point were NOT loaded\n", err)
	}
}
