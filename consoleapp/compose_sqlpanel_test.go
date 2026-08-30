package consoleapp

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"go.yaml.in/yaml/v2"
)

// Where the SQL page's default lives (#1529).
//
// It used to live in the bundled docker-compose.yml (`${SQL_PANEL:-1}`) while
// the binary defaulted OFF. The compose file belongs to the operator and is
// downloaded once, so that decision reached new installs only: pulling a newer
// image delivered the page to nobody. The default now lives in the daemon and
// the compose file does not mention the variable at all, so an image upgrade
// carries it.
//
// These two guards are a pair. One asserts the daemon's default, the other
// asserts the compose file stays out of it: put the line back and the decision
// silently splits across two files again.

// composeConsoleEnvValue returns an environment value the bundled compose sets
// on the console service, and whether it sets it at all.
func composeConsoleEnvValue(t *testing.T, name string) (string, bool) {
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
	if len(svc.Environment) == 0 {
		t.Fatalf("the %q service in %s declares no environment in map form; this guard reads that form and would otherwise pass vacuously", composeService, composePath)
	}
	raw, ok := svc.Environment[name]
	if !ok {
		return "", false
	}
	return fmt.Sprint(raw), true
}

// TestSQLPanelDefaultsOnInTheDaemon pins the default where it now lives, on
// the real parser the two console commands call.
func TestSQLPanelDefaultsOnInTheDaemon(t *testing.T) {
	t.Setenv("BINTRAIL_CONSOLE_SQL_PANEL", "")
	if !sqlPanelEnabled() {
		t.Error("the SQL page is off with no environment variable set; the daemon's default is what an image upgrade delivers, so it has to be ON here")
	}
	// Fail CLOSED on anything this does not understand. The variable is now an
	// opt-OUT, so an operator types it when they want the page gone: reading
	// "off" or "no" as ON would keep serving a server-side SQL surface to
	// someone who believes they closed it, and the only signal would be a log
	// line nobody re-reads. Under the old opt-in body every one of these
	// spellings meant off, so this is the direction that was already true.
	for _, off := range []string{"0", "false", "FALSE", "f", "off", "OFF", "no", "n", "disabled", "nope"} {
		t.Setenv("BINTRAIL_CONSOLE_SQL_PANEL", off)
		if sqlPanelEnabled() {
			t.Errorf("BINTRAIL_CONSOLE_SQL_PANEL=%q left the SQL page on; an operator who typed it meant to hide the page", off)
		}
	}
	for _, on := range []string{"1", "true", "TRUE"} {
		t.Setenv("BINTRAIL_CONSOLE_SQL_PANEL", on)
		if !sqlPanelEnabled() {
			t.Errorf("BINTRAIL_CONSOLE_SQL_PANEL=%q turned the SQL page off", on)
		}
	}
}

// TestComposeLeavesTheSQLPanelDefaultToTheBinary is the other half: the
// bundled stack must not set the variable at all. A line here would pin the
// page's state to a file the operator downloaded once, which is the drift
// #1529 is about, and it would do it invisibly — the page would still be
// there, so nothing would look broken until the default changed.
func TestComposeLeavesTheSQLPanelDefaultToTheBinary(t *testing.T) {
	if v, ok := composeConsoleEnvValue(t, "BINTRAIL_CONSOLE_SQL_PANEL"); ok {
		t.Errorf("%s sets BINTRAIL_CONSOLE_SQL_PANEL=%s on the %s service; the default belongs in the daemon, so an image upgrade delivers it and a stale compose file cannot freeze it",
			composePath, v, composeService)
	}
}

// composePortsFile decodes the one field the loopback guard below reads.
type composePortsFile struct {
	Services map[string]struct {
		Ports []string `yaml:"ports"`
	} `yaml:"services"`
}

// TestComposePublishesTheConsoleOnLoopbackOnly asserts the one fact this
// stack's first-run posture rests on.
//
// The compose file sets BINTRAIL_CONSOLE_ALLOW_SETUP=1, which lets a browser
// create the first console username and password. That is justified by one
// fact about this stack and nothing else: the console is published on the host
// loopback only, so reaching the setup screen already implies local access.
// Change that mapping to "8090:8090" and the justification is gone while every
// other test here stays green, so the fact is asserted where it lives.
//
// (It used to be stated as the reason the SQL page defaults on. That default
// moved into the daemon in #1529 and rests on POST /api/sql sitting behind
// console auth, carrying PermQueryExecute, and being refused outright while an
// access-control profile is active — not on where the port is published.)
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
			t.Errorf("port mapping %q publishes on every interface; this stack lets a browser create the "+
				"first console password because the console is reachable from the host only", p)
			continue
		}
		if !composeLoopback(host) {
			t.Errorf("port mapping %q publishes on %s, not the host loopback; this stack lets a browser create "+
				"the first console password because the console is reachable from the host only", p, host)
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
