package cliapp

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"go.yaml.in/yaml/v2"
)

// The shipped stack can run the Iceberg export (#1466) as a one-shot, so an
// operator gets the tables without installing anything. Two things about that
// service are load-bearing rather than cosmetic, and neither shows up in
// `docker compose config`:
//
//   - it runs the CORE image. The console image ships without the export
//     commands on purpose, and no daemon links the Iceberg writer
//     (TestDaemonBinariesAreIcebergFree above), so pointing this service at
//     the console image would produce a service that cannot work.
//   - it is behind its own profile, so it never runs as part of `up -d`. The
//     export writes a new copy of the data and is kept out of the capture
//     process; a service that started with the stack would put it back in.
const icebergComposePath = "../docker-compose.yml"

type icebergComposeFile struct {
	Volumes  map[string]any `yaml:"volumes"`
	Services map[string]struct {
		Image       string            `yaml:"image"`
		Profiles    []string          `yaml:"profiles"`
		Command     []string          `yaml:"command"`
		Volumes     []string          `yaml:"volumes"`
		Environment map[string]string `yaml:"environment"`
		DependsOn   map[string]struct {
			Condition string `yaml:"condition"`
		} `yaml:"depends_on"`
	} `yaml:"services"`
}

func TestComposeIcebergExportProfile(t *testing.T) {
	data, err := os.ReadFile(icebergComposePath)
	if err != nil {
		t.Fatalf("read %s: %v", icebergComposePath, err)
	}
	var doc icebergComposeFile
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatalf("parse %s: %v", icebergComposePath, err)
	}
	svc, ok := doc.Services["iceberg-export"]
	if !ok {
		t.Fatalf("no iceberg-export service in %s", icebergComposePath)
	}

	// The core image, by name. bintrail-console is a PREFIX-sharing name, so
	// the check is exact rather than a Contains that both would satisfy.
	image, _, _ := strings.Cut(svc.Image, ":")
	if image != "ghcr.io/dbtrail/bintrail" {
		t.Errorf("iceberg-export runs image %q; the export commands ship only in the core image", svc.Image)
	}
	// Behind a profile, so `docker compose up -d` never starts it.
	if len(svc.Profiles) != 1 || svc.Profiles[0] != "iceberg-export" {
		t.Errorf("iceberg-export profiles = %v, want exactly [iceberg-export] so it stays opt-in", svc.Profiles)
	}
	cmd := strings.Join(svc.Command, "\n")
	if !strings.Contains(cmd, "bintrail export iceberg") {
		t.Error("the iceberg-export service does not run `bintrail export iceberg`")
	}
	if !strings.Contains(cmd, "--warehouse") {
		t.Error("the iceberg-export service names no warehouse, which the command requires")
	}
	// The export folds events out of the index, so it must not start before
	// the index is answering.
	if dep, ok := svc.DependsOn["index-mysql"]; !ok || dep.Condition != "service_healthy" {
		t.Errorf("iceberg-export depends_on index-mysql = %+v, want service_healthy", dep)
	}
	// The warehouse it writes to must be a declared volume mounted WRITABLE,
	// or the tables either vanish with the container or the run fails on a
	// read-only mount. Checked as a path prefix and not a substring: the
	// state volume's /var/lib/bintrail is a substring of the warehouse path
	// and is mounted read-only, so a Contains would pass on the wrong mount.
	dir := composeDefault(svc.Environment["WAREHOUSE_DIR"])
	if dir == "" {
		t.Fatal("iceberg-export sets no WAREHOUSE_DIR, so this guard cannot tell where the tables go")
	}
	var warehouse string
	for _, v := range svc.Volumes {
		name, rest, found := strings.Cut(v, ":")
		if !found {
			continue
		}
		target, opts, _ := strings.Cut(rest, ":")
		target = strings.TrimSuffix(target, "/")
		if _, declared := doc.Volumes[name]; !declared || opts == "ro" {
			continue
		}
		if dir == target || strings.HasPrefix(dir, target+"/") {
			warehouse = name
		}
	}
	if warehouse == "" {
		t.Errorf("no declared, writable volume covers %s; the exported tables would not survive the run", dir)
	}
}

// composeDefault reads the `${VAR:-default}` form back. A value in any other
// shape is returned as written, which is what a plain path is.
func composeDefault(raw string) string {
	if m := regexp.MustCompile(`^\$\{[A-Za-z_][A-Za-z0-9_]*:-([^}]*)\}$`).FindStringSubmatch(raw); m != nil {
		return m[1]
	}
	return raw
}
