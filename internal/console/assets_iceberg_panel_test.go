package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"go.yaml.in/yaml/v2"
)

// The Iceberg panel on the Backups page (#1466) is rendered entirely in the
// browser from data the page already fetched, so it has no handler and no
// route test can cover it. These are the properties that make the command it
// prints safe to paste and true for the server it is shown for.
func TestIcebergExportPanel(t *testing.T) {
	raw, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(raw)
	cmd := functionBody(t, js, "function icebergExportCommand(")
	panel := functionBody(t, js, "function icebergExportPanel(")

	// The command itself, not a description of one.
	for _, want := range []string{"bintrail export iceberg", "--index-dsn", "--warehouse"} {
		if !strings.Contains(cmd, want) {
			t.Errorf("the rendered command is missing %q", want)
		}
	}
	// The password is elided, never rendered. The server DTO carries no
	// password at all (only has_password), so the placeholder is what stands
	// in for it, and a future DTO change must not quietly start filling it.
	if !strings.Contains(cmd, `":***"`) {
		t.Error("the command does not elide the index password with a placeholder")
	}
	if strings.Contains(cmd, "cur.password") || strings.Contains(cmd, ".passwd") {
		t.Error("the command reads a password field; the DSN in it is meant to be shown on screen")
	}
	// The destination is the RESOLVED one from /api/baselines, and its kind
	// picks the flag: a server whose backups live in S3 gets --baseline-s3,
	// and a command naming a local path it does not have would just fail.
	if !strings.Contains(cmd, "--baseline-s3") || !strings.Contains(cmd, "--baseline-dir") {
		t.Error("the command does not carry both destination forms")
	}
	if !strings.Contains(cmd, `baselines.kind === "s3"`) {
		t.Error("the destination flag is not chosen from the resolved backup kind")
	}

	// Why it is a command and not a button, in the panel itself.
	if !strings.Contains(panel, "writes a new copy of your data") ||
		!strings.Contains(panel, "process that captures changes") {
		t.Error("the panel does not say why this is a command rather than a button")
	}
	// The schedule the guide recommends, so the reader does not have to
	// invent one. The Docker form, not the bare command: the bare one carries
	// the DSN we just told the reader to fill with the index password, and a
	// crontab is a file other people read.
	if !strings.Contains(panel, "17 * * * * cd /path/to/stack && docker compose --profile iceberg-export") {
		t.Error("the panel does not offer the cron line, or does not prefer the form that keeps the password out of crontab")
	}
	if strings.Contains(panel, `"17 * * * * " + cmd`) {
		t.Error("the cron line interpolates the DSN the reader fills with the index password")
	}
	// What the command does NOT carry. ParseDSN absorbs tls, timeouts and the
	// rest into fields the server never sends here, so an index that requires
	// TLS fails on this line with a message about transport, and the panel is
	// the only place that can say why.
	if !strings.Contains(panel, "nothing else about the connection") {
		t.Error("the panel does not say that connection settings are not carried into the command")
	}
	// The compose note is in the VISIBLE body, above the collapsed block: the
	// Docker route can export a different dataset than the command printed
	// here, and it does that successfully.
	bodyHalf, fineHalf, split := strings.Cut(panel, `cnFine("How to run it"`)
	if !split {
		t.Fatal("the panel no longer has a How to run it block; this guard reads the split")
	}
	if !strings.Contains(bodyHalf, "icebergComposeNote(cur)") {
		if strings.Contains(fineHalf, "icebergComposeNote(cur)") {
			t.Error("the compose note is inside the collapsed block; a reader who never opens it can export the wrong dataset")
		} else {
			t.Error("the panel never renders the compose note, so nothing says the Docker route may export a different dataset")
		}
	}
	if !strings.Contains(panel, "copyText(cmd") {
		t.Error("the command cannot be copied, which is the whole point of printing it")
	}
	// The address and the path in the command are as THIS process sees them.
	// In the bundled stack both are container-scoped, so a line pasted into a
	// host shell reaches neither: the panel has to say so and point at the
	// compose profile, which is the answer for exactly that operator.
	if !strings.Contains(panel, "the ones this console uses") {
		t.Error("the panel does not warn that the address and folder are the console's own")
	}
	if !strings.Contains(panel, "--profile iceberg-export") {
		t.Error("the panel does not offer the compose route for a console running in Docker")
	}

	// And it is actually on the page: a panel nothing calls is invisible, and
	// every check above would still pass.
	if !strings.Contains(functionBody(t, js, "async function renderBaselines("), "icebergExportPanel(") {
		t.Error("renderBaselines never calls icebergExportPanel, so the panel never renders")
	}
}

// The console must not grow an in-process Iceberg export: the writer library
// is linked by one package and no daemon carries it (cliapp/icebergfree_test.go
// pins the binaries). This panel is text, so the guard here is that the page
// asks no server for the export.
func TestIcebergExportPanelCallsNoAPI(t *testing.T) {
	raw, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	for _, fn := range []string{"function icebergExportCommand(", "function icebergExportPanel("} {
		body := functionBody(t, string(raw), fn)
		for _, forbidden := range []string{"api(", "apiText(", "fetch("} {
			if strings.Contains(body, forbidden) {
				t.Errorf("%s calls %s: the export is a command the operator runs, not something this daemon does", fn, forbidden)
			}
		}
	}
}

// TestIcebergExportCommandRendersRunnableLines EXECUTES the renderer instead of
// matching strings over it, because the bug this exists for was invisible to
// string matching: the function's comment claimed it returned null for an index
// reached over a unix socket, the code checked only the host, and `cur.port ||
// "3306"` turned a socket path into
// `tcp(/var/run/mysqld/mysqld.sock:3306)` — a DSN that parses and then dies at
// dial. Every assertion above passed on that.
//
// It runs the REAL source sliced out of app.js (the renderer plus the shell
// quoter it calls), so a change to either is what this sees.
// requireNodeEnv turns this test's node skip into a failure. Set in the CI
// unit-test step (asserted by TestCIRequiresNodeForTheRenderedCommand below).
const requireNodeEnv = "BINTRAIL_REQUIRE_NODE"

func TestIcebergExportCommandRendersRunnableLines(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		// This is the ONLY executing coverage of icebergExportCommand, and it
		// runs today because the CI runner happens to ship node. That is not
		// a guarantee: the same treatment as the DuckDB/Iceberg leg, whose
		// CI step sets BINTRAIL_REQUIRE_DUCKDB_ICEBERG so it can never
		// silently skip there. With the variable set, a missing node is a
		// FAILURE; elsewhere it is a named skip.
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH: this is the only test that RUNS "+
				"icebergExportCommand, and skipping it here would leave the rendered command uncovered", requireNodeEnv)
		}
		t.Skip("node is not installed; the rendered-command cases are not covered on this machine")
	}
	raw, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(raw)
	script := functionBody(t, js, "function shellWord(") + "\n" +
		functionBody(t, js, "function icebergExportCommand(") + "\n" +
		functionBody(t, js, "function icebergComposeNote(") + `
const cases = JSON.parse(process.argv[2]);
console.log(JSON.stringify(cases.map((c) => (
  c.note ? icebergComposeNote(c.server) : icebergExportCommand(c.server, c.baselines)
))));
`
	dir := t.TempDir()
	path := filepath.Join(dir, "case.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}

	tcp := map[string]any{"host": "db.internal", "port": "3307", "user": "reader", "dbname": "idx", "has_password": true}
	local := map[string]any{"source": "/data/baselines", "kind": "dir"}
	type tc struct {
		name     string
		server   map[string]any
		baseline map[string]any
		want     []string // substrings that must be present
		absent   []string
		null     bool
		note     bool // render icebergComposeNote instead of the command
	}
	cases := []tc{
		{
			name:   "tcp index",
			server: tcp, baseline: local,
			// Quoted: shellWord's bare set has no path separator, so every
			// destination comes through single-quoted.
			want: []string{"@tcp(db.internal:3307)/idx", "--baseline-dir '/data/baselines'", "--warehouse"},
		},
		{
			// The bug. A unix-socket connection has no port once the server
			// splits the address, and there is no host:port to print.
			name:     "unix socket index",
			server:   map[string]any{"host": "/var/run/mysqld/mysqld.sock", "port": "", "user": "root", "dbname": "idx", "has_password": true},
			baseline: local,
			null:     true,
		},
		{
			// The address arrives bare; without brackets it reads as a
			// different host with no port.
			name:     "ipv6 index",
			server:   map[string]any{"host": "2001:db8::1", "port": "3306", "user": "reader", "dbname": "idx", "has_password": true},
			baseline: local,
			want:     []string{"@tcp([2001:db8::1]:3306)/idx"},
			absent:   []string{"tcp(2001:db8::1:3306)"},
		},
		{
			// Legal MySQL identifiers that a shell would expand.
			name:     "shell metacharacters in the names",
			server:   map[string]any{"host": "db.internal", "port": "3306", "user": "re`ader", "dbname": `idx$(whoami)`, "has_password": true},
			baseline: map[string]any{"source": "/data/back ups", "kind": "dir"},
			want:     []string{`--index-dsn 're`, `--baseline-dir '/data/back ups'`},
			absent:   []string{`--index-dsn re`},
		},
		{
			// The one character shellWord exists for. A name holding a single
			// quote has to close the quoting, escape the quote and reopen it
			// ('\''), or everything after it is outside the quotes.
			name:     "a single quote in a name",
			server:   map[string]any{"host": "db.internal", "port": "3306", "user": "reader", "dbname": "idx'or'1", "has_password": true},
			baseline: local,
			want:     []string{`'reader:***@tcp(db.internal:3306)/idx'\''or'\''1'`},
		},
		{
			name: "s3 destination", server: tcp,
			baseline: map[string]any{"source": "s3://bkt/baselines/", "kind": "s3"},
			want:     []string{"--baseline-s3 's3://bkt/baselines/'"},
			absent:   []string{"--baseline-dir"},
		},
		{
			// The Docker route exports the STACK's index and backups. For a
			// registry server that is a different dataset, exported
			// successfully and with nothing to see, which is why the note is
			// in the visible body rather than the collapsed block.
			name: "compose note for a registry server", note: true,
			server: map[string]any{"kind": "registry", "host": "db.internal", "port": "3306", "dbname": "idx"},
			want:   []string{"not the server picked here", "at this server", "INDEX_DSN", "BASELINE_S3"},
		},
		{
			name: "compose note for the stack's own index", note: true,
			server: map[string]any{"kind": "ephemeral", "host": "index-mysql", "port": "3306", "dbname": "bintrail_index"},
			want:   []string{"this stack's own index", "somewhere else", "INDEX_DSN", "BASELINE_DIR"},
			absent: []string{"not the server picked here"},
		},
		{name: "no destination", server: tcp, baseline: map[string]any{}, null: true},
		{name: "no server", server: nil, baseline: local, null: true},
	}

	payload := make([]map[string]any, len(cases))
	for i, c := range cases {
		payload[i] = map[string]any{"server": c.server, "baselines": c.baseline, "note": c.note}
	}
	arg, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(node, path, string(arg)).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
	var got []*string
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode node output %q: %v", out, err)
	}
	if len(got) != len(cases) {
		t.Fatalf("got %d results for %d cases", len(got), len(cases))
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.null {
				if got[i] != nil {
					t.Fatalf("rendered a command where none can be correct: %s", *got[i])
				}
				return
			}
			if got[i] == nil {
				t.Fatal("rendered no command")
			}
			line := *got[i]
			for _, w := range c.want {
				if !strings.Contains(line, w) {
					t.Errorf("command is missing %q:\n%s", w, line)
				}
			}
			for _, a := range c.absent {
				if strings.Contains(line, a) {
					t.Errorf("command contains %q, which it must not:\n%s", a, line)
				}
			}
		})
	}
}

// copyText is the Copy button behind both new panels (and every older one).
// navigator.clipboard is UNDEFINED outside a secure context, where the old
// body threw on property access and the operator saw nothing at all: no toast,
// no text copied, a button that looked like it worked.
func TestCopyTextHandlesAnInsecureContext(t *testing.T) {
	raw, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	body := functionBody(t, string(raw), "function copyText(")
	if !strings.Contains(body, "writeText") || !strings.Contains(body, "if (!clip") {
		t.Error("copyText does not check that the clipboard API exists before using it")
	}
	if !strings.Contains(body, "toastError") {
		t.Error("copyText fails silently when it cannot copy; the operator has to be told")
	}
}

// TestCIRequiresNodeForTheRenderedCommand: the variable above only means
// something if CI sets it, and a variable nobody wires in enables nothing. It
// has to be on the step that RUNS the unit tests, not merely somewhere in the
// file.
func TestCIRequiresNodeForTheRenderedCommand(t *testing.T) {
	const path = "../../.github/workflows/ci.yml"
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var doc struct {
		Jobs map[string]struct {
			Steps []struct {
				Name string            `yaml:"name"`
				Run  string            `yaml:"run"`
				Env  map[string]string `yaml:"env"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	var ran, required bool
	for _, job := range doc.Jobs {
		for _, step := range job.Steps {
			// The step that runs the whole unit suite, found by what it RUNS
			// rather than by its name: a rename must not quietly empty this.
			if !strings.Contains(step.Run, "go test ./...") {
				continue
			}
			ran = true
			if step.Env[requireNodeEnv] != "" {
				required = true
			}
		}
	}
	if !ran {
		t.Fatalf("no step in %s runs `go test ./...`; this guard covers nothing", path)
	}
	if !required {
		t.Errorf("no step running the unit suite in %s sets %s, so the rendered-command test can skip in CI "+
			"and leave icebergExportCommand with no executing coverage", path, requireNodeEnv)
	}
}
