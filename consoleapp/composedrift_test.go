package consoleapp

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"go.yaml.in/yaml/v2"
)

// The stack-drift check (#1529), tested from both ends.
//
// The fixture for "everything is wired" is BUILT FROM ../docker-compose.yml,
// not hand-written: the mounts come off the console service's volume list, the
// state paths off its environment, and the index DSN's host off the entrypoint
// script that builds it. A hand-written fixture would keep passing after the
// compose file changed, which is the one failure this check must never have —
// a false alarm on a correctly wired stack teaches operators to ignore the
// line, and then the real one goes unread too.

// composeDriftFile decodes the fields these fixtures read.
type composeDriftFile struct {
	Version  int `yaml:"x-bintrail-compose-version"`
	Services map[string]struct {
		Environment map[string]any `yaml:"environment"`
		Volumes     []string       `yaml:"volumes"`
		Command     []string       `yaml:"command"`
	} `yaml:"services"`
}

func readComposeDriftFile(t *testing.T) composeDriftFile {
	t.Helper()
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatalf("read %s: %v", composePath, err)
	}
	var doc composeDriftFile
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatalf("parse %s: %v", composePath, err)
	}
	if _, ok := doc.Services[composeService]; !ok {
		t.Fatalf("no %q service in %s", composeService, composePath)
	}
	return doc
}

// composeStackMounts is the mount table a container running the console
// service has: an overlay root (the image layer plus the writable layer that a
// recreate throws away) and one mount per volume entry, plus the three files
// Docker binds into every container. The volume TARGETS are read off the
// compose file, so moving a volume moves the fixture with it.
func composeStackMounts(t *testing.T) []mountEntry {
	t.Helper()
	doc := readComposeDriftFile(t)
	svc := doc.Services[composeService]
	if len(svc.Volumes) == 0 {
		t.Fatalf("the %q service in %s mounts no volumes; this fixture reads that list and would otherwise assert nothing", composeService, composePath)
	}
	mounts := []mountEntry{{point: "/", fstype: "overlay"}}
	for _, v := range svc.Volumes {
		_, target, found := strings.Cut(v, ":")
		if !found {
			t.Fatalf("volume entry %q on the %q service names no container path", v, composeService)
		}
		target, _, _ = strings.Cut(target, ":") // drop the :ro option
		mounts = append(mounts, mountEntry{point: target, fstype: "ext4"})
	}
	// Docker binds these into every container; they are here so the fixture
	// exercises the same longest-prefix walk a real mount table forces.
	for _, f := range []string{"/etc/resolv.conf", "/etc/hostname", "/etc/hosts"} {
		mounts = append(mounts, mountEntry{point: f, fstype: "ext4"})
	}
	return mounts
}

// composeEntrypointIndexHost pulls the bundled index's host out of the DSN the
// entrypoint script builds. It is what makes composeIndexHost more than a
// hopeful constant: rename the service in the compose file and this fails,
// instead of the detection going quietly blind.
func composeEntrypointIndexHost(t *testing.T) string {
	t.Helper()
	doc := readComposeDriftFile(t)
	svc := doc.Services[composeService]
	script := strings.Join(svc.Command, "\n")
	m := regexp.MustCompile(`@tcp\(([^:)]+)[:)]`).FindStringSubmatch(script)
	if m == nil {
		t.Fatalf("the %q service's command in %s builds no tcp(...) index DSN; this guard reads it and would otherwise assert nothing", composeService, composePath)
	}
	return m[1]
}

// composeEntrypointDatadirRO pulls the read-only index mount the entrypoint
// exports. It is set by the script rather than the environment map, so it has
// to be read from there or the "wired" fixture would be missing it and this
// whole file would pass for the wrong reason.
func composeEntrypointDatadirRO(t *testing.T) string {
	t.Helper()
	doc := readComposeDriftFile(t)
	svc := doc.Services[composeService]
	script := strings.Join(svc.Command, "\n")
	m := regexp.MustCompile(`export BINTRAIL_INDEX_DATADIR_RO=(\S+)`).FindStringSubmatch(script)
	if m == nil {
		t.Fatalf("the %q service's command in %s exports no BINTRAIL_INDEX_DATADIR_RO; the index's free disk space cannot be measured in this stack", composeService, composePath)
	}
	return m[1]
}

// composeStateEnv reads a console state path off the compose environment.
func composeStateEnv(t *testing.T, name string) string {
	t.Helper()
	v, ok := composeConsoleEnvValue(t, name)
	if !ok {
		t.Fatalf("%s does not set %s on the %s service", composePath, name, composeService)
	}
	return v
}

// wiredStack is the current, fully wired compose stack as this check sees it.
// Every finding must be silent here.
func wiredStack(t *testing.T) driftInputs {
	t.Helper()
	datadirRO := composeEntrypointDatadirRO(t)
	env := map[string]string{
		"BINTRAIL_INDEX_DATADIR_RO": datadirRO,
		"BINTRAIL_COMPOSE_VERSION":  strconv.Itoa(bundledComposeVersion),
	}
	return driftInputs{
		getenv:   func(k string) string { return env[k] },
		indexDSN: fmt.Sprintf("root:pw@tcp(%s:3306)/bintrail_index", composeEntrypointIndexHost(t)),
		isDir:    func(p string) bool { return p == datadirRO },
		// The console's config directory is empty in a wired stack: every OSS
		// state file is redirected onto the volume and nothing writes there.
		dirIsUsed: func(string) bool { return false },
		mounts:    composeStackMounts(t),
		state: []statePath{
			{what: "your console username and password", path: composeStateEnv(t, "BINTRAIL_CONSOLE_AUTH")},
			{what: "the servers you added", path: composeStateEnv(t, "BINTRAIL_CONSOLE_SERVERS")},
			{what: "the AI connection token", path: composeStateEnv(t, "BINTRAIL_CONSOLE_MCP_TOKEN_FILE")},
		},
		configDir:      "/home/bintrail/.config/bintrail",
		shippedVersion: bundledComposeVersion,
		versionAdded:   composeVersionAdded,
	}
}

func TestComposeDriftIsSilentOnTheWiredStack(t *testing.T) {
	if got := composeDriftFindings(wiredStack(t)); len(got) != 0 {
		for _, f := range got {
			t.Errorf("the shipped stack reports drift against itself: %s %v", f.msg, f.attrs)
		}
	}
}

// TestComposeDriftFindsAStaleStack takes the wired stack apart the way real
// history did: the read-only index mount landed in 2026-07 and the token path
// override this week, so a file downloaded before either has the volume but
// not these.
func TestComposeDriftFindsAStaleStack(t *testing.T) {
	in := wiredStack(t)
	in.getenv = func(string) string { return "" } // no version, no datadir mount
	in.isDir = func(string) bool { return false }
	in.mounts = removeMount(in.mounts, composeEntrypointDatadirRO(t))
	// The token override does not exist in this file, so the token falls back
	// into the console's config directory, which nothing keeps.
	in.state[2].path = in.configDir + "/console-mcp-token.yaml"
	in.dirIsUsed = func(p string) bool { return p == in.configDir }

	got := composeDriftFindings(in)
	if len(got) != 2 {
		t.Fatalf("want 2 findings (disk space, console settings), got %d: %v", len(got), got)
	}
	joined := fmt.Sprint(got)
	for _, want := range []string{
		"free disk space for the index cannot be measured",
		"console settings are stored inside the container",
		"console-mcp-token.yaml",
		"docker-compose.yml",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("the report never mentions %q: %s", want, joined)
		}
	}
	// The two files that ARE on the volume must not be reported as lost.
	if strings.Contains(joined, "console-auth.yaml") || strings.Contains(joined, "console-servers.yaml") {
		t.Errorf("the report names state that is on the volume and is not at risk: %s", joined)
	}
}

func TestComposeDriftStaysSilentWhereItCannotTell(t *testing.T) {
	stale := func(t *testing.T) driftInputs {
		in := wiredStack(t)
		in.getenv = func(string) string { return "" }
		in.isDir = func(string) bool { return false }
		in.dirIsUsed = func(string) bool { return true }
		in.state[2].path = in.configDir + "/console-mcp-token.yaml"
		return in
	}
	t.Run("no mount table to read", func(t *testing.T) {
		in := stale(t)
		in.mounts = nil
		if got := composeDriftFindings(in); len(got) != 0 {
			t.Errorf("reported drift with no mount table to prove anything: %v", got)
		}
	})
	t.Run("not a container", func(t *testing.T) {
		in := stale(t)
		in.mounts = []mountEntry{{point: "/", fstype: "ext4"}, {point: "/home", fstype: "ext4"}}
		if got := composeDriftFindings(in); len(got) != 0 {
			t.Errorf("a bare-metal daemon has no compose file to be stale, but got: %v", got)
		}
	})
	t.Run("bring your own index", func(t *testing.T) {
		in := stale(t)
		in.indexDSN = "root:pw@tcp(my-own-mysql.internal:3306)/bintrail_index"
		for _, f := range composeDriftFindings(in) {
			if strings.Contains(f.msg, "free disk space") {
				t.Errorf("a bring-your-own index has no bundled volume to mount, but got: %s", f.msg)
			}
		}
	})
	t.Run("nothing durable to compare against", func(t *testing.T) {
		in := wiredStack(t)
		// A bare `docker run` with no volumes at all: every path is on the
		// writable layer, and that is as likely a throwaway evaluation
		// container as a stale stack. Only the config directory, which
		// something has written to, is reported.
		in.mounts = []mountEntry{{point: "/", fstype: "overlay"}}
		in.dirIsUsed = func(string) bool { return false }
		for _, f := range composeDriftFindings(in) {
			if strings.Contains(f.msg, "console settings are stored") {
				t.Errorf("reported console state loss with no durable mount anywhere to compare against: %s", f.msg)
			}
		}
	})
}

// TestConsoleStateFindingReadsTheConfigDirectory pins the second shape: state
// that no environment variable relocates (an add-on's own settings file) is
// only reachable through the directory it lands in. A file there is the
// evidence; an empty directory is silence.
func TestConsoleStateFindingReadsTheConfigDirectory(t *testing.T) {
	base := wiredStack(t)
	for _, tc := range []struct {
		name string
		used bool
		want bool
	}{
		{name: "something writes there", used: true, want: true},
		{name: "nothing writes there", used: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := base
			in.dirIsUsed = func(string) bool { return tc.used }
			_, got := consoleStateFinding(in)
			if got != tc.want {
				t.Errorf("config directory used=%v reported=%v, want %v", tc.used, got, tc.want)
			}
		})
	}
}

func TestComposeVersionFinding(t *testing.T) {
	added := map[int]string{3: "a read-only mount of the index volume"}
	for _, tc := range []struct {
		name    string
		value   string
		shipped int
		want    bool
		mention string
	}{
		{name: "no version passed", value: "", shipped: 3},
		{name: "same version", value: "3", shipped: 3},
		{name: "file is newer than the binary", value: "4", shipped: 3},
		{name: "not a number", value: "v3", shipped: 3},
		{name: "file is behind", value: "2", shipped: 3, want: true, mention: "a read-only mount of the index volume"},
		{name: "file is behind, nothing recorded", value: "1", shipped: 2, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := driftInputs{
				getenv:         func(string) string { return tc.value },
				shippedVersion: tc.shipped,
				versionAdded:   added,
			}
			f, ok := composeVersionFinding(in)
			if ok != tc.want {
				t.Fatalf("BINTRAIL_COMPOSE_VERSION=%q against %d reported=%v, want %v", tc.value, tc.shipped, ok, tc.want)
			}
			if tc.mention != "" && !strings.Contains(fmt.Sprint(f.attrs), tc.mention) {
				t.Errorf("the line does not name what is missing: %v", f.attrs)
			}
		})
	}
}

// TestComposeVersionMatchesTheBinary: three places carry the number and all
// three have to agree, or the daemon compares against a file it is not
// shipping.
func TestComposeVersionMatchesTheBinary(t *testing.T) {
	doc := readComposeDriftFile(t)
	if doc.Version != bundledComposeVersion {
		t.Errorf("x-bintrail-compose-version is %d in %s and bundledComposeVersion is %d; bump them together",
			doc.Version, composePath, bundledComposeVersion)
	}
	raw, ok := composeConsoleEnvValue(t, "BINTRAIL_COMPOSE_VERSION")
	if !ok {
		t.Fatalf("%s does not pass BINTRAIL_COMPOSE_VERSION to the %s service, so the daemon can never tell the file's age", composePath, composeService)
	}
	if raw != strconv.Itoa(bundledComposeVersion) {
		t.Errorf("the %s service is passed BINTRAIL_COMPOSE_VERSION=%s, want %d", composeService, raw, bundledComposeVersion)
	}
	for v := range composeVersionAdded {
		if v > bundledComposeVersion {
			t.Errorf("composeVersionAdded describes version %d, which this build does not ship", v)
		}
	}
	// And the complement: every version ABOVE the first has to say what it
	// added, or the finding degrades to "your file is behind" with nothing
	// named, which is the diff-two-files problem the version key exists to
	// remove. Version 1 is the first numbered file and has nothing before it,
	// so this loop is empty today ON PURPOSE: the failure it prevents can only
	// exist after a bump, and a bump is exactly when nobody is looking at this
	// file.
	for v := 2; v <= bundledComposeVersion; v++ {
		if _, ok := composeVersionAdded[v]; !ok {
			t.Errorf("compose version %d has no composeVersionAdded entry, so a stack on %d is told its file is behind and not what that costs",
				v, v-1)
		}
	}
}

func TestParseMountinfo(t *testing.T) {
	// Line 1 carries an optional field ("shared:1") before the separator, which
	// is what makes a fixed-offset read of the filesystem type wrong. Line 2
	// has none. Line 3 has an escaped space in the mount point.
	const sample = `25 30 0:23 / / rw,relatime shared:1 - overlay overlay rw,lowerdir=/a
30 25 0:24 / /var/lib/bintrail rw,relatime - ext4 /dev/sda1 rw
31 25 0:25 / /mnt/two\040words rw - tmpfs tmpfs rw
short line
`
	got := parseMountinfo(strings.NewReader(sample))
	want := []mountEntry{
		{point: "/", fstype: "overlay"},
		{point: "/var/lib/bintrail", fstype: "ext4"},
		{point: "/mnt/two words", fstype: "tmpfs"},
	}
	if len(got) != len(want) {
		t.Fatalf("parsed %d mounts, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("mount %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestCoveringMountComparesWholeComponents is the bug this compose file would
// have hit on day one: it mounts /var/lib/bintrail AND
// /var/lib/bintrail-index-ro, and a raw string prefix match puts the second
// inside the first.
func TestCoveringMountComparesWholeComponents(t *testing.T) {
	mounts := []mountEntry{
		{point: "/", fstype: "overlay"},
		{point: "/var/lib/bintrail", fstype: "ext4"},
	}
	m, ok := coveringMount("/var/lib/bintrail-index-ro/mysql.ibd", mounts)
	if !ok || m.point != "/" {
		t.Errorf("/var/lib/bintrail-index-ro resolved onto %+v; it is not inside /var/lib/bintrail", m)
	}
	if m, _ := coveringMount("/var/lib/bintrail/console-auth.yaml", mounts); m.point != "/var/lib/bintrail" {
		t.Errorf("a file on the state volume resolved onto %+v", m)
	}
}

func TestEphemeralPaths(t *testing.T) {
	mounts := []mountEntry{
		{point: "/", fstype: "overlay"},
		{point: "/var/lib/bintrail", fstype: "ext4"},
		{point: "/tmp", fstype: "tmpfs"},
	}
	for path, want := range map[string]bool{
		"/var/lib/bintrail/console-auth.yaml":     false,
		"/home/bintrail/.config/bintrail/x.yaml":  true,
		"/tmp/console-auth.yaml":                  true,
		"/var/lib/bintrail-index-ro/console.yaml": true,
	} {
		if got := ephemeral(path, mounts); got != want {
			t.Errorf("ephemeral(%q) = %v, want %v", path, got, want)
		}
	}
}

func removeMount(mounts []mountEntry, point string) []mountEntry {
	out := make([]mountEntry, 0, len(mounts))
	for _, m := range mounts {
		if m.point != point {
			out = append(out, m)
		}
	}
	return out
}
