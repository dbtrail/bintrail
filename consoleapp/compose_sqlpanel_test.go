package consoleapp

import (
	"fmt"
	"os"
	"regexp"
	"strings"
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

// composePortsFile decodes the one field the loopback guard below reads.
type composePortsFile struct {
	Services map[string]struct {
		Ports []string `yaml:"ports"`
	} `yaml:"services"`
}

// TestComposePublishesTheConsoleOnLoopbackOnly is the OTHER half of the
// default-on decision above, and the reason it is defensible at all.
//
// The SQL page answers a DuckDB SELECT inside the daemon. Turning it on by
// default is justified in docker-compose.yml and docs/docker.md by one fact
// about this stack and nothing else: the console is published on the host
// loopback only. Change that mapping to "8090:8090" and the justification is
// gone while every other test here stays green, so the fact is asserted where
// it lives.
//
// Loopback here means the PUBLISHING side. The console binds 0.0.0.0 inside
// the container on purpose (the mapping is what controls exposure), so a
// short-syntax entry with no host part publishes on every interface and is
// exactly the edit this guards against.
func TestComposePublishesTheConsoleOnLoopbackOnly(t *testing.T) {
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatalf("read %s: %v", composePath, err)
	}
	var doc composePortsFile
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatalf("parse %s: %v", composePath, err)
	}
	svc, ok := doc.Services[composeService]
	if !ok {
		t.Fatalf("no %q service in %s", composeService, composePath)
	}
	if len(svc.Ports) == 0 {
		t.Fatalf("the %q service publishes no ports; this guard reads that list and would otherwise pass vacuously", composeService)
	}
	for _, p := range svc.Ports {
		host, ok := composePublishedHost(p)
		if !ok {
			t.Errorf("port mapping %q publishes on every interface; the SQL page defaults ON in this stack "+
				"because the console is reachable from the host only", p)
			continue
		}
		if !composeLoopback(host) {
			t.Errorf("port mapping %q publishes on %s, not the host loopback; the SQL page defaults ON in this stack "+
				"because the console is reachable from the host only", p, host)
		}
	}
}

// composePublishedHost returns the HOST part of a compose short-syntax port
// mapping, and whether the mapping names one at all.
//
// Splitting on the first colon does not work, which is the bug this replaces:
// an IPv6 host is bracketed ("[::1]:8090:8090"), so the first colon falls
// INSIDE the address and the guard read the host as "[", matched neither
// loopback form, and reported the wrong reason for a mapping that was in fact
// fine. The bracketed form has to be read before any splitting.
//
// A mapping with no host part ("8090", "8090:8090", "8090:8090/tcp")
// publishes on every interface of the machine, which is exactly the edit this
// guards against, so it returns false rather than an empty host that could be
// mistaken for one.
func composePublishedHost(mapping string) (string, bool) {
	m := strings.TrimSpace(mapping)
	if strings.HasPrefix(m, "[") {
		end := strings.Index(m, "]")
		if end < 0 {
			return "", false // malformed; not a host this guard can vouch for
		}
		return m[1:end], true
	}
	// host:hostPort:containerPort is the only short form that names a host.
	if parts := strings.Split(m, ":"); len(parts) >= 3 {
		return parts[0], true
	}
	return "", false
}

// composeLoopback reports whether a published host reaches only this machine.
// The two literals, not a name: "localhost" resolves through the host's own
// configuration, and this guard is about what the mapping guarantees.
func composeLoopback(host string) bool {
	return host == "127.0.0.1" || host == "::1"
}

// TestComposePublishedHost pins the parser above on the forms compose accepts,
// including the bracketed IPv6 one the guard's own comment allows and the old
// implementation could never have matched.
func TestComposePublishedHost(t *testing.T) {
	for _, tc := range []struct {
		mapping  string
		host     string
		hasHost  bool
		loopback bool
	}{
		{mapping: "127.0.0.1:8090:8090", host: "127.0.0.1", hasHost: true, loopback: true},
		{mapping: "[::1]:8090:8090", host: "::1", hasHost: true, loopback: true},
		{mapping: "127.0.0.1::8090", host: "127.0.0.1", hasHost: true, loopback: true},
		{mapping: "0.0.0.0:8090:8090", host: "0.0.0.0", hasHost: true},
		{mapping: "[::]:8090:8090", host: "::", hasHost: true},
		{mapping: "192.168.1.10:8090:8090", host: "192.168.1.10", hasHost: true},
		{mapping: "8090:8090"},
		{mapping: "8090:8090/tcp"},
		{mapping: "8090"},
	} {
		t.Run(tc.mapping, func(t *testing.T) {
			host, ok := composePublishedHost(tc.mapping)
			if ok != tc.hasHost || host != tc.host {
				t.Fatalf("composePublishedHost(%q) = (%q, %v), want (%q, %v)", tc.mapping, host, ok, tc.host, tc.hasHost)
			}
			if got := ok && composeLoopback(host); got != tc.loopback {
				t.Errorf("%q reads as loopback = %v, want %v", tc.mapping, got, tc.loopback)
			}
		})
	}
}
