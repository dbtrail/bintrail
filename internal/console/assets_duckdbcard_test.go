package console

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The Download a DuckDB schema card is the only caller of GET /api/views.sql,
// so what it can ask for IS the console's whole surface over that endpoint.
//
// It carried three checkboxes and three multi-line caveats until #1549's
// follow-up. The caveats are what these guards used to pin, one string at a
// time. They pin the SHAPE now, because the card explains itself by drawing
// what the file will hold instead of describing it, and a drawing fails
// differently: wrong prose reads wrong, a wrong picture looks fine.

// TestDuckDBCardOffersOneDecision. --pin-snapshot and --include-live remain
// route parameters and CLI flags; they are not decisions to put in front of a
// first-time reader, and the live leg in particular can compete with capture on
// the source server. So exactly one box, and it is the change log.
func TestDuckDBCardOffersOneDecision(t *testing.T) {
	body := functionBody(t, readAsset(t, "app.js"), "function duckdbPanel(")

	// Two, and the second one is CONDITIONAL. The card is a first visit for
	// most readers, so the bar for a control is that skipping it would produce
	// the wrong file: the change log is left out by default and has to be
	// asked for, and the backup location only has an answer to give when this
	// server has two of them (#1551). Everything else stays a CLI flag.
	boxes := strings.Count(body, `type: "checkbox"`)
	if boxes != 2 {
		t.Errorf("duckdbPanel renders %d checkboxes, want exactly 2 (the change log, and the "+
			"backup location when the server has two); pin-snapshot and include-live are CLI "+
			"flags and route parameters, not first-visit decisions", boxes)
	}
	// The point of the pair above: unconditional, it is a control that changes
	// nothing for every server with one backup location, which is most of them.
	if !strings.Contains(body, "if (capsCache.views_portable_baseline) {") {
		t.Error("the backup-location box is not gated on the capability, so it is offered for " +
			"servers with only one location, where it changes nothing")
	}
	if !strings.Contains(body, "include_events=1") {
		t.Error("the card never sends include_events=1, so the change log cannot be asked for at all")
	}
	if !strings.Contains(body, "portable_baseline=1") {
		t.Error("the card never sends portable_baseline=1, so its box changes nothing")
	}
	// Conditional, not always: the change log binds every archived file, so a
	// download nobody asked for it must not carry it.
	if !strings.Contains(body, `if (events.checked) q.push("include_events=1")`) {
		t.Error("the change log is not conditional on the box, so the default download is not the cheap one")
	}
	// Its own filename. Saved as views.sql, the second download silently
	// replaces the first in the reader's downloads folder.
	if !strings.Contains(body, `"views-portable.sql"`) {
		t.Error("both downloads are saved under one filename, so one silently overwrites the other")
	}
	// The two retired controls must not come back as silent always-on
	// parameters, which would be worse than the checkboxes were.
	for _, gone := range []string{"include_live", "pin_snapshot"} {
		if strings.Contains(body, gone) {
			t.Errorf("duckdbPanel names %s again; it was removed from this surface, not moved into a default", gone)
		}
	}
}

// TestDuckDBCardDrawsTheCostInsteadOfStatingIt is the guard for the drawing.
//
// The card used to carry "It takes longer to open the further back your archive
// goes, because it reads a piece of every archived file". That sentence is the
// shape of the data, and the shape is drawable: your tables are one file each,
// the change log is one file per archived hour. Ticking the box lights the
// strip, so the cost is seen.
//
// A picture can lie in a way prose cannot: nobody reports a diagram that merely
// looks plausible. So the two halves are pinned to the one parameter the card
// can send. If the strip stops being tied to the change-log box, the drawing
// starts claiming something the download does not do.
func TestDuckDBCardDrawsTheCostInsteadOfStatingIt(t *testing.T) {
	js := readAsset(t, "app.js")
	shape := functionBody(t, js, "function duckdbShape(")
	card := functionBody(t, js, "function duckdbPanel(")

	// Both halves exist, and the change log is drawn as MANY units against the
	// tables' few. That contrast is the whole explanation; equal counts would
	// render a picture that says the two cost the same.
	tiles := regexp.MustCompile(`tiles\((\d+), "dk-tile"`).FindStringSubmatch(shape)
	bars := regexp.MustCompile(`tiles\((\d+), "dk-bar"`).FindStringSubmatch(shape)
	if tiles == nil || bars == nil {
		t.Fatal("duckdbShape no longer draws both a tile row (your tables) and a bar row (the change log)")
	}
	// Compared as NUMBERS. This read len() of the digit strings, which is not
	// quantity: it passed 24-vs-3 for the wrong reason and would have failed a
	// perfectly good 9-vs-3. A picture guard that cannot compare the two
	// quantities it exists to compare is decoration.
	nTiles, err1 := strconv.Atoi(tiles[1])
	nBars, err2 := strconv.Atoi(bars[1])
	if err1 != nil || err2 != nil {
		t.Fatalf("could not read the drawn counts (tiles=%q bars=%q)", tiles[1], bars[1])
	}
	if nBars < nTiles*4 {
		t.Errorf("the change log is drawn with %d units against the tables' %d; the picture has to read as a "+
			"different order of quantity, not as slightly more", nBars, nTiles)
	}

	// The strip is dimmed until the box is ticked, and the card is what wires
	// them together. Either half alone leaves a drawing that never changes.
	if !strings.Contains(shape, "dk-off") {
		t.Error("duckdbShape never applies dk-off, so the change-log half is drawn as if it were always included")
	}
	if !strings.Contains(card, `shape.events.classList.toggle("dk-off"`) {
		t.Error("the change-log box does not light the strip, so ticking it explains nothing")
	}
	if !strings.Contains(card, "events.onchange();") {
		t.Error("the initial state is never synced, so the strip renders lit before the box is ticked")
	}

	// Built with el(), not svgEl: that helper DOMParses STATIC icon constants
	// only, and routing a drawing through it is how a string-to-DOM path grows
	// an interpolated argument later.
	if strings.Contains(shape, "svgEl(") {
		t.Error("duckdbShape builds through svgEl, which is for static icon constants; draw with el()")
	}
}

// TestDuckDBCardStaysNearlyTextless is the Go half of the 300-character budget
// the e2e enforces on the rendered card. It cannot count rendered text, so it
// counts what the source can produce, and exists to fail on the desk rather
// than in CI when someone starts explaining again.
func TestDuckDBCardStaysNearlyTextless(t *testing.T) {
	js := readAsset(t, "app.js")
	body := functionBody(t, js, "function duckdbPanel(")
	// Only text OUTSIDE the fold: cnFine's contents are one click away and are
	// not what a first-time reader meets. Everything up to the fold is visible.
	visible := body
	if i := strings.Index(body, "cnFine("); i >= 0 {
		visible = body[:i]
	}
	total := 0
	for _, m := range regexp.MustCompile(`text:\s*((?:"(?:[^"\\]|\\.)*"\s*\+?\s*)+)`).FindAllStringSubmatch(visible, -1) {
		for _, lit := range regexp.MustCompile(`"((?:[^"\\]|\\.)*)"`).FindAllStringSubmatch(m[1], -1) {
			total += len(lit[1])
		}
	}
	if total == 0 {
		t.Fatal("no visible text found in duckdbPanel; this guard covers nothing")
	}
	if total > 200 {
		t.Errorf("duckdbPanel's visible text is %d characters. The card explains itself by drawing; "+
			"if something needs saying, either draw it or put it behind cnFine", total)
	}
}

// functionBody returns the text of one top-level function in app.js, from its
// declaration to the next top-level declaration.
//
// It lived in assets_sqlpanel_warnings_test.go until #1549 deleted that file
// with the SQL panel. Three other guards depend on it (capacity notes, the
// Iceberg panel, and the card above), so it moves here rather than going with
// the panel — this file is its heaviest remaining user.
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
