package consoleapp

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// The console writes exactly three files: the server registry, the auth file
// and the managed MCP token (the verify and baseline run histories are named
// as siblings of the registry, so they follow it). In the shipped compose
// stack every one of them has to be redirected onto the state volume, or it
// lands in the container's writable layer and is destroyed by the next
// `docker compose up` that recreates the container.
//
// That is #1493: the MCP token had no override at all, so it resolved under
// $HOME inside the image and did not survive a restart. The console then came
// up reporting nothing (a missing token file is the ordinary not-configured
// state), and the operator's AI client 401'd with no server-side explanation.
//
// An env override nobody wires into the compose fixes nothing, so this guard
// asserts the wiring, not the flag. The mount point is READ OFF the volume
// line rather than hardcoded: moving the volume elsewhere and leaving these
// paths behind is the same bug again.
const composePath = "../docker-compose.yml"

// consoleStateEnvVars are the env vars that place console state on disk. Each
// must point inside the state volume in the shipped stack.
var consoleStateEnvVars = []string{
	"BINTRAIL_CONSOLE_SERVERS",
	"BINTRAIL_CONSOLE_AUTH",
	"BINTRAIL_CONSOLE_MCP_TOKEN",
}

var (
	composeStateMountRE = regexp.MustCompile(`^\s*-\s*bintrail-state:([^\s:]+)\s*(?::(ro))?\s*(?:#.*)?$`)
	composeEnvRE        = regexp.MustCompile(`^\s*(BINTRAIL_CONSOLE_[A-Z_]+):\s*(\S+)`)
)

func TestComposeKeepsConsoleStateOnTheVolume(t *testing.T) {
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatalf("read %s: %v", composePath, err)
	}
	block := composeServiceBlock(t, string(data), "bintrail")

	// The writable state mount. A :ro mount of the same volume exists on
	// another service; only a read-write one can hold state this daemon writes.
	var mount string
	for _, line := range block {
		m := composeStateMountRE.FindStringSubmatch(line)
		if m == nil || m[2] == "ro" {
			continue
		}
		mount = strings.TrimSuffix(m[1], "/")
	}
	if mount == "" {
		t.Fatalf("the bintrail service in %s has no read-write bintrail-state mount; nothing it writes would survive a container recreation", composePath)
	}

	env := map[string]string{}
	for _, line := range block {
		if m := composeEnvRE.FindStringSubmatch(line); m != nil {
			env[m[1]] = m[2]
		}
	}
	for _, name := range consoleStateEnvVars {
		v, ok := env[name]
		if !ok {
			t.Errorf("%s does not set %s on the bintrail service, so that file resolves under $HOME in the image and is destroyed by the next container recreation", composePath, name)
			continue
		}
		if !strings.HasPrefix(v, mount+"/") {
			t.Errorf("%s = %s in %s, which is outside the state volume mounted at %s: it lives in the container's writable layer and does not survive a restart", name, v, composePath, mount)
		}
	}
}

// composeServiceBlock returns the lines of one service in the compose file.
// Services are the two-space-indented keys under `services:`; the block ends
// at the next key with that indent.
func composeServiceBlock(t *testing.T, doc, service string) []string {
	t.Helper()
	lines := strings.Split(doc, "\n")
	start := -1
	for i, line := range lines {
		if line == "  "+service+":" {
			start = i + 1
			break
		}
	}
	if start < 0 {
		t.Fatalf("no %q service in %s", service, composePath)
	}
	end := len(lines)
	for i := start; i < len(lines); i++ {
		line := lines[i]
		if strings.HasPrefix(line, "  ") && !strings.HasPrefix(line, "   ") && strings.TrimSpace(line) != "" {
			end = i
			break
		}
	}
	block := lines[start:end]
	if len(block) == 0 {
		t.Fatalf("the %q service block in %s is empty; the checks below would pass vacuously", service, composePath)
	}
	return block
}
