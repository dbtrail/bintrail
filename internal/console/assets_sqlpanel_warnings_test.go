package console

import (
	"os"
	"strings"
	"testing"
)

// TestRunSQLRendersWarnings guards the only consumer of sqlPanelResult.Warnings
// (#1456). The server side is pinned by TestSQLPanel_registryReadFailure; if
// runSQL stopped reading data.warnings, every server-side test would stay
// green while the operator saw "0 rows in 3 ms" over half a layout, which is
// the silence the field exists to remove. Scoped to runSQL's body: a file-wide
// search passes when an unrelated handler happens to read the same name.
func TestRunSQLRendersWarnings(t *testing.T) {
	js, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	body := functionBody(t, string(js), "async function runSQL(")
	if !strings.Contains(body, "data.warnings") {
		t.Fatal("runSQL no longer reads data.warnings: the SQL panel's degraded-session note is not shown")
	}
}

// functionBody returns the text of one top-level function in app.js, from its
// declaration to the next top-level declaration.
func functionBody(t *testing.T, js, decl string) string {
	t.Helper()
	i := strings.Index(js, decl)
	if i < 0 {
		t.Fatalf("%s is gone from assets/app.js; this guard covers nothing", strings.TrimSuffix(decl, "("))
	}
	rest := js[i:]
	for _, stop := range []string{"\nfunction ", "\nasync function "} {
		if j := strings.Index(rest[1:], stop); j > 0 {
			rest = rest[:j+1]
		}
	}
	return rest
}
