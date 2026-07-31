package consoleapp

import "testing"

// TestWatchEnvBindingsResolveToRegisteredFlags asserts every entry in
// watchEnvBindings names a flag registered on watchCmd. bindWatchEnv
// silently skips a binding whose flag is missing (the same no-op hazard as
// the core CLI's BindCommandEnv, #1130), so a flag rename or a typo in the
// table would silently disconnect the BINTRAIL_* env var for the compose
// path while the flag itself kept working.
func TestWatchEnvBindingsResolveToRegisteredFlags(t *testing.T) {
	// Positive anchor: an empty table must fail, not pass vacuously.
	if len(watchEnvBindings) == 0 {
		t.Fatal("watchEnvBindings is empty; the loop below would pass vacuously")
	}
	for _, b := range watchEnvBindings {
		if watchCmd.Flags().Lookup(b.Flag) == nil {
			t.Errorf("binding %q -> %s names a flag watch does not register; the env var is silently disconnected (bindWatchEnv skips a missing flag)", b.Flag, b.EnvVar)
		}
	}
}
