package console

import (
	"os"
	"strings"
	"testing"
)

// The Download a DuckDB schema card is the only caller of GET /api/views.sql, so the
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
	// The change log is opt-in since #1535, so the card has to offer it or the
	// only file anyone can download from here is state-views-only.
	if !strings.Contains(body, "include_events=1") {
		t.Error("the card never sends include_events=1, so the change log cannot be asked for from the UI")
	}
	// The live leg hangs on the change-log view. Ticked on its own it earns a
	// 400 from the route, so the UI must not let that state exist: the box
	// starts disabled and is CLEARED when the change log is turned back off (a
	// disabled checkbox keeps its checked state, and the request reads it).
	if !strings.Contains(body, "disabled: true") {
		t.Error("the live-leg box is not disabled to begin with, so it can be ticked " +
			"without the view it is a leg of")
	}
	if !strings.Contains(body, "live.checked = false") {
		t.Error("turning the change log back off leaves the live box checked, so the " +
			"next download sends include_live=1 without include_events and is refused")
	}
	if !strings.Contains(body, "events.checked && live.checked") {
		t.Error("the live parameter is not conditional on the change log being on too")
	}
	// Conditional, not always: the leg reads the live capture index and the
	// change log binds every archived file, so a download nobody asked either
	// of them for must carry neither. The two `events.checked` guards above are
	// what enforce it now; a bare-URL assertion keeps the default honest.
	if !strings.Contains(body, `"/api/views.sql" + (params.length`) {
		t.Error("the card does not send a bare URL when nothing is ticked, so the " +
			"default download is not the cheap one")
	}
	// The cost belongs on the page, not only in the generated file's comments:
	// by the time an operator reads those, the query is already running.
	for _, want := range []string{"live capture index", "competes"} {
		if !strings.Contains(body, want) {
			t.Errorf("the card does not state the cost of the live leg (missing %q)", want)
		}
	}
}
