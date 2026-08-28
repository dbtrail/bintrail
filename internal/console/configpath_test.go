package console

import (
	"bytes"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
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

// TestConfigPathWarnsWhenHomeIsMissing pins the signal that the MkdirAll fix
// would otherwise delete.
//
// Before this PR, a homeless daemon's registry, auth file and MCP token were
// written successfully (their savers all MkdirAll) while the baseline history
// write failed every cycle. That repeated ENOENT was, by accident, the ONLY
// indication anywhere that HOME was unset. Making the history write succeed
// removes it, and every reader on this path is quiet by design: a missing
// registry is an empty registry with no error, a missing auth file lands in
// the first-run setup flow, a missing MCP token just 401s. The composite is a
// console that comes up with no servers and offers to create a password,
// looking merely unconfigured rather than detached from its state.
//
// So the fallback has to say so itself. Warning, never fatal: the console is a
// recovery path and must still boot.
//
// Not parallel: t.Setenv and it swaps the default logger.
func TestConfigPathWarnsWhenHomeIsMissing(t *testing.T) {
	t.Setenv("HOME", "")
	t.Setenv("USERPROFILE", "")

	// The warning is once-per-process so a per-cycle caller cannot spam the
	// log; reset it so this test does not depend on which test ran first.
	configPathWarnOnce = sync.Once{}
	t.Cleanup(func() { configPathWarnOnce = sync.Once{} })

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)

	got := configPath("console-servers.yaml")
	logged := buf.String()

	if logged == "" {
		t.Fatal("no warning logged when the home directory could not be resolved; a homeless daemon would silently anchor all console state at its working directory")
	}
	// It must name the directory it actually anchored to, so the operator can
	// go and look at it.
	if dir := filepath.Dir(got); !strings.Contains(logged, dir) {
		t.Errorf("warning does not name the directory it anchored to (%s): %s", dir, logged)
	}
	// ...and the levers that fix it.
	for _, want := range []string{"HOME", "servers-file", "auth-file"} {
		if !strings.Contains(logged, want) {
			t.Errorf("warning does not mention %q, so it names no way out: %s", want, logged)
		}
	}

	// Once per process: a second call must stay silent.
	buf.Reset()
	configPath("console-auth.yaml")
	if second := buf.String(); second != "" {
		t.Errorf("warning repeated on a later call; it must fire once per process, got: %s", second)
	}
}
