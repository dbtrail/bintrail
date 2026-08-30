package consoleapp

import (
	"fmt"
	"os"
	"regexp"
	"testing"

	"go.yaml.in/yaml/v2"
)

// The SQL page is env-gated (#1177) and the shipped stack has to turn it on,
// or the quickstart's console never shows a page the public guides tell the
// reader to open. A variable nobody wires into the compose enables nothing,
// so this guard asserts the WIRING and then hands the wired value to the real
// parser: a typo like "yes" would sit in the file looking correct and gate
// nothing.
//
// It reads the value the way compose does (`${SQL_PANEL:-1}` with SQL_PANEL
// unset), so the documented opt-out is checked on the same string the shipped
// file actually carries.

// composeInterp resolves the one substitution form this file uses for the
// variable under test, `${NAME:-default}`. Anything else fails the test rather
// than being guessed at: a form this cannot read is a form it cannot check.
var composeInterp = regexp.MustCompile(`^\$\{([A-Za-z_][A-Za-z0-9_]*):-([^}]*)\}$`)

func resolveComposeValue(t *testing.T, raw, override string, overridden bool) string {
	t.Helper()
	m := composeInterp.FindStringSubmatch(raw)
	if m == nil {
		if overridden {
			t.Fatalf("value %q carries no ${VAR:-default} substitution, so nothing in .env can override it", raw)
		}
		return raw
	}
	if overridden {
		return override
	}
	return m[2]
}

func composeConsoleEnvValue(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatalf("read %s: %v", composePath, err)
	}
	var doc composeFile
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatalf("parse %s: %v", composePath, err)
	}
	svc, ok := doc.Services[composeService]
	if !ok {
		t.Fatalf("no %q service in %s", composeService, composePath)
	}
	raw, ok := svc.Environment[name]
	if !ok {
		t.Fatalf("the %q service in %s does not set %s, so the bundled stack never shows the page it gates",
			composeService, composePath, name)
	}
	return fmt.Sprint(raw)
}

func TestComposeEnablesTheSQLPanel(t *testing.T) {
	raw := composeConsoleEnvValue(t, "BINTRAIL_CONSOLE_SQL_PANEL")

	// The shipped default: no .env, so the substitution falls back.
	t.Setenv("BINTRAIL_CONSOLE_SQL_PANEL", resolveComposeValue(t, raw, "", false))
	if !sqlPanelEnabled() {
		t.Errorf("BINTRAIL_CONSOLE_SQL_PANEL resolves to %q in the shipped stack, which the daemon reads as OFF",
			resolveComposeValue(t, raw, "", false))
	}

	// The documented opt-out, on the same string: SQL_PANEL=0 in .env.
	t.Setenv("BINTRAIL_CONSOLE_SQL_PANEL", resolveComposeValue(t, raw, "0", true))
	if sqlPanelEnabled() {
		t.Error("SQL_PANEL=0 in .env leaves the page on, so the documented way to hide it does nothing")
	}
}

// The gate itself does not move: a bare `bintrail-console serve` or `watch`
// stays off until the operator sets the variable. Only the bundled stack,
// whose console is published on the host loopback, opts in for them.
func TestSQLPanelStaysOptInWithoutTheEnvVar(t *testing.T) {
	t.Setenv("BINTRAIL_CONSOLE_SQL_PANEL", "")
	if sqlPanelEnabled() {
		t.Error("the SQL panel is on with no environment variable set; a bare invocation must stay off")
	}
}
