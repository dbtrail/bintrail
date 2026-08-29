package consoleapp

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"go.yaml.in/yaml/v2"
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
// entry rather than hardcoded: moving the volume elsewhere and leaving these
// paths behind is the same bug again.
const (
	composePath    = "../docker-compose.yml"
	composeService = "bintrail"
	composeVolume  = "bintrail-state"
)

// consoleStateEnvVars are the env vars that place console state on disk. Each
// must point inside the state volume in the shipped stack.
var consoleStateEnvVars = []string{
	"BINTRAIL_CONSOLE_SERVERS",
	"BINTRAIL_CONSOLE_AUTH",
	"BINTRAIL_CONSOLE_MCP_TOKEN_FILE",
}

// composeFile decodes only what this guard reads. Parsing the YAML rather than
// scanning lines is deliberate: a line scanner reports the wrong cause for
// edits that change nothing real (a quoted value reads as a path outside the
// volume, a comment at service indent truncates the service).
type composeFile struct {
	Services map[string]struct {
		Environment map[string]any `yaml:"environment"`
		Volumes     []string       `yaml:"volumes"`
	} `yaml:"services"`
}

func TestComposeKeepsConsoleStateOnTheVolume(t *testing.T) {
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

	// The writable state mount. A :ro mount of the same volume exists on
	// another service; only a read-write one can hold state this daemon writes.
	var mounts []string
	for _, v := range svc.Volumes {
		name, target, found := strings.Cut(v, ":")
		if !found || name != composeVolume {
			continue
		}
		target, opts, _ := strings.Cut(target, ":")
		if opts == "ro" {
			continue
		}
		mounts = append(mounts, strings.TrimSuffix(target, "/"))
	}
	if len(mounts) != 1 {
		t.Fatalf("the %q service in %s mounts %s read-write at %v; want exactly one such mount, since these paths are checked against it", composeService, composePath, composeVolume, mounts)
	}
	mount := mounts[0]

	for _, name := range consoleStateEnvVars {
		raw, ok := svc.Environment[name]
		if !ok {
			t.Errorf("%s does not set %s on the %s service, so that file resolves under $HOME in the image and is destroyed by the next container recreation", composePath, name, composeService)
			continue
		}
		v := fmt.Sprint(raw)
		if !strings.HasPrefix(v, mount+"/") {
			t.Errorf("%s = %s in %s, which is outside the state volume mounted at %s: it lives in the container's writable layer and does not survive a restart", name, v, composePath, mount)
		}
	}
}
