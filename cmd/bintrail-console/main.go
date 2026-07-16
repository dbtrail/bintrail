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
// Configuration mirrors the core CLI: a .bintrail.env (or
// ~/.config/bintrail/config.env) file is loaded on startup and the
// BINTRAIL_INDEX_DSN / BINTRAIL_CONSOLE_* env vars are honored with
// flag > env > default precedence.
//
// All command behavior lives in the importable consoleapp package; this
// main exists only to receive the -ldflags-injected build metadata (which
// must target main.* so the Makefile and .goreleaser.yaml ldflags keep
// working unchanged) and to own the process exit.
package main

import (
	"os"

	"github.com/dbtrail/dbtrail/consoleapp"
)

// Build-time variables injected via -ldflags. The names are deliberately the
// same as the bintrail binary's (main.Version/CommitSHA/BuildDate) so the
// Makefile's BINTRAIL_LDFLAGS applies to this binary verbatim.
var (
	Version   = "dev"
	CommitSHA = "none"
	BuildDate = "unknown"
)

func main() {
	os.Exit(consoleapp.Main(Version, CommitSHA, BuildDate))
}
