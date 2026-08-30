package console

import (
	"os"
	"strings"
	"testing"
)

// The Query in DuckDB card is the only caller of GET /api/views.sql, so the
// include_live parameter the handler grew (#1480) reaches the operator only if
// this card sends it. The server-side tests all pass with a card that has no
// checkbox at all.
//
// Scoped to duckdbCard's body: a file-wide search for "include_live" would be
// satisfied by any other mention, this one included in a comment.
func TestDuckDBCardOffersTheLiveLeg(t *testing.T) {
	js, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	body := functionBody(t, string(js), "function duckdbCard(")

	if !strings.Contains(body, `type: "checkbox"`) {
		t.Error("the card has no checkbox, so the live leg cannot be asked for from the UI")
	}
	if !strings.Contains(body, "include_live=1") {
		t.Error("the card never sends include_live=1, so ticking the box would download the archives-only file")
	}
	// Conditional, not always: the leg reads the live capture index, so a
	// download nobody asked it for must not carry it.
	if !strings.Contains(body, "checked ?") {
		t.Error("the card does not make the parameter conditional on the checkbox")
	}
	// The cost belongs on the page, not only in the generated file's comments:
	// by the time an operator reads those, the query is already running.
	for _, want := range []string{"live capture index", "competes"} {
		if !strings.Contains(body, want) {
			t.Errorf("the card does not state the cost of the live leg (missing %q)", want)
		}
	}
}
