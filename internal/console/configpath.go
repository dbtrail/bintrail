package console

import (
	"os"
	"path/filepath"
)

// configPath returns ~/.config/bintrail/<name> — the console's on-disk state
// directory, shared by the server registry, the credential file, the managed
// MCP token, and (as siblings derived from the registry path) the verify and
// baseline run histories.
//
// With no home directory — a daemon started by a process manager that passes
// no HOME, systemd DynamicUser, a scrubbed container — the fallback anchors in
// the working directory EXPLICITLY. The obvious spelling,
// filepath.Join(".", ".config", ...), does NOT do that: Join Cleans the
// leading "." away and yields a RELATIVE path. That path then resolves against
// wherever the process happens to be at write time, and its failures name a
// directory no operator can find (#1487: every baseline refresh logged
// `open .config/bintrail/.baseline-history-…: no such file or directory`).
func configPath(name string) string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		wd, wdErr := os.Getwd()
		if wdErr != nil {
			// Neither a home nor a working directory: there is nothing left to
			// anchor to. Return the bare relative path rather than an empty
			// one so the caller's error at least names the file.
			return filepath.Join(".config", "bintrail", name)
		}
		home = wd
	}
	return filepath.Join(home, ".config", "bintrail", name)
}
