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

// TestRunSQLRendersTheTimingSplit guards the reader-facing half of #1526. The
// server sends the whole wait and the statement's share of it; a panel that
// prints only one of them puts the operator back where they started, looking at
// a query for a cost that is in the layout. Scoped to runSQL's body for the
// reason the guard above is.
func TestRunSQLRendersTheTimingSplit(t *testing.T) {
	js, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	body := functionBody(t, string(js), "async function runSQL(")
	for _, want := range []string{"data.elapsed_ms", "data.query_ms"} {
		if !strings.Contains(body, want) {
			t.Errorf("runSQL does not render %s: the SQL panel reports one number where it needs two", want)
		}
	}
	// And on the path that waits LONGEST: a statement naming a relation the
	// layout does not define builds every view before it fails. The server
	// carries elapsed_ms on that body; a page that ignores it blanks the status
	// line after the slowest thing the panel does.
	if !strings.Contains(body, "j.elapsed_ms") {
		t.Error("runSQL drops the wait on a failed statement: the operator is told nothing " +
			"about the longest wait the panel has")
	}
	if !strings.Contains(body, "failed after ") {
		t.Error("runSQL parses the failure's elapsed_ms but never puts it on the status line")
	}
}
