package console

import (
	"os"
	"strings"
	"testing"
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
	// invent one.
	if !strings.Contains(panel, `"17 * * * * "`) {
		t.Error("the panel does not offer the cron line")
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
