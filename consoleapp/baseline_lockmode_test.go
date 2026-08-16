package consoleapp

import (
	"slices"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// TestConsoleBaselineDefaultsToConsistent: the console's Create-baseline button
// and the `bintrail dump` CLI must not disagree about what a baseline is. This
// pins the console half of that — a torn snapshot is not something an operator
// should get without asking (#1377).
func TestConsoleBaselineDefaultsToConsistent(t *testing.T) {
	if !upConsoleBaselineLockMode.PointConsistent() {
		t.Fatalf("console baseline defaults to %s, which can emit a torn snapshot", upConsoleBaselineLockMode)
	}
	if upConsoleBaselineLockMode != baseline.DefaultLockMode {
		t.Errorf("console default %s disagrees with baseline.DefaultLockMode %s; the two baseline surfaces must not drift",
			upConsoleBaselineLockMode, baseline.DefaultLockMode)
	}
}

// TestBuildConsoleMydumperArgsCarriesLockMode drives the real argv builder, so
// a builder that ignored its argument and hardcoded a mode fails here.
func TestBuildConsoleMydumperArgsCarriesLockMode(t *testing.T) {
	for _, tc := range []struct {
		mode baseline.LockMode
		want string
	}{
		{baseline.LockModeFTWRL, "FTWRL"},
		{baseline.LockModeSafeNoLock, "SAFE_NO_LOCK"},
		{baseline.LockModeNoLock, "NO_LOCK"},
	} {
		args := buildConsoleMydumperArgs("127.0.0.1", 3306, "root", []string{"demo"}, "/tmp/d", tc.mode)
		i := slices.Index(args, "--sync-thread-lock-mode")
		if i < 0 || i+1 >= len(args) {
			t.Fatalf("mode %s: no --sync-thread-lock-mode in argv", tc.mode)
		}
		if args[i+1] != tc.want {
			t.Errorf("mode %s produced %q, want %q", tc.mode, args[i+1], tc.want)
		}
	}
}
