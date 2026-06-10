package console

import (
	"os"
	"testing"
)

// TestMain isolates the package from the developer's real home directory:
// console.New probes DefaultAuthPath() (and DefaultRegistryPath() exists in
// the same class) when no explicit path is configured, so a real
// ~/.config/bintrail/console-auth.yaml on the dev machine would flip
// password mode on for every test that constructs a Server.
func TestMain(m *testing.M) {
	tmp, err := os.MkdirTemp("", "console-test-home-*")
	if err == nil {
		os.Setenv("HOME", tmp)
		// USERPROFILE is os.UserHomeDir's source on Windows.
		os.Setenv("USERPROFILE", tmp)
	}
	code := m.Run()
	if tmp != "" {
		os.RemoveAll(tmp)
	}
	os.Exit(code)
}
