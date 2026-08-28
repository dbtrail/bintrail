package console

import (
	"path/filepath"
	"testing"
)

// TestDefaultConfigPathsAreAbsoluteWithoutHome pins #1487. A daemon started by
// a process manager with no HOME (systemd without an explicit Environment=HOME,
// DynamicUser, a scrubbed container) must still name an absolute path for its
// state files. The old fallback built filepath.Join(".", ".config", …), and
// Join CLEANS the leading "." away — so it returned a RELATIVE path that
// resolved against whatever working directory the process happened to have.
// The reported symptom was the baseline-run history: every refresh logged
// `open .config/bintrail/.baseline-history-…: no such file or directory`, an
// error naming a directory no operator could locate.
//
// Not parallel: t.Setenv.
func TestDefaultConfigPathsAreAbsoluteWithoutHome(t *testing.T) {
	// os.UserHomeDir errors on an EMPTY value, not only on an unset one, so
	// this reproduces the homeless daemon. USERPROFILE is its source on
	// Windows (see TestMain, which sets both).
	t.Setenv("HOME", "")
	t.Setenv("USERPROFILE", "")

	registry := DefaultRegistryPath()
	paths := map[string]string{
		"DefaultRegistryPath":  registry,
		"DefaultAuthPath":      DefaultAuthPath(),
		"DefaultMCPTokenPath":  DefaultMCPTokenPath(),
		"DefaultVerifyHistory": DefaultVerifyHistoryPath(registry),
		// The baseline-run history is a sibling of the registry file, so it
		// inherits both the defect and the fix — this is the write #1487
		// reported losing on all seven refreshes of a benchmark run.
		"DefaultBaselineHistoryPath": DefaultBaselineHistoryPath(registry),
	}
	for name, p := range paths {
		if !filepath.IsAbs(p) {
			t.Errorf("%s = %q with no HOME; want an absolute path (a relative one resolves against the daemon's working directory, and its write errors name a directory the operator cannot find)", name, p)
		}
	}
}
