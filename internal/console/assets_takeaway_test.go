package console

import (
	"regexp"
	"strings"
	"testing"
)

// The Backups page answers one question: what do I download to open this in
// DuckDB, what do I download to load it into MySQL. The drawing carries the
// difference before any sentence does -- two file tiles on the DuckDB lane,
// one on the MySQL lane -- and a drawing can lie in a way prose cannot: it
// keeps rendering the old answer after the code stops producing it, and no
// screenshot notices because a picture of two files looks correct on its own.
//
// The first cut of this guard compared the tiles a lane PASSES against a
// hardcoded sentence, and review killed it twice over. It counted the
// argument list rather than the drawing, so slicing backupFilesShape to one
// tile passed; and once the views tile became conditional, the fixed sentence
// was wrong by construction on the ungated arm.
//
// So the sentence is no longer written by hand at all. backupLane derives the
// count word from the tiles it is handed, and these three checks pin that
// arrangement: nothing downstream may re-hardcode a count, and nothing may
// draw fewer tiles than it was given.
func TestTakeAwayLaneCountIsDerivedNotWritten(t *testing.T) {
	js := readAsset(t, "app.js")
	lane := stripJSLineComments(functionBody(t, js, "function backupLane("))
	if !strings.Contains(lane, "files.length") || !strings.Contains(lane, "LANE_COUNT_WORD") {
		t.Error("backupLane no longer derives its count word from the tiles it is handed. A lane " +
			"that states its own count can disagree with its own drawing, which is the whole " +
			"defect this panel's guard exists for")
	}
	// A lane that spelled its own count would silently win over the derived
	// one, so no caller may contain a count word.
	for _, fn := range []string{"function backupDuckLane(", "function backupSQLLane("} {
		body := stripJSLineComments(functionBody(t, js, fn))
		for _, word := range []string{"One download", "Two downloads", "One file", "Two files"} {
			if strings.Contains(body, word) {
				t.Errorf("%s writes %q by hand. The count comes from backupLane, which reads the "+
					"tiles actually drawn; a hand-written one drifts the first time a tile is gated",
					fn, word)
			}
		}
	}
	// And the drawing must render every tile it is given. Slicing here was a
	// surviving mutation: the DuckDB lane drew one tile under the words for
	// two, and every assertion stayed green.
	shape := stripJSLineComments(functionBody(t, js, "function backupFilesShape("))
	if !strings.Contains(shape, "files.forEach(") || strings.Contains(shape, ".slice(") {
		t.Error("backupFilesShape no longer draws one tile per file it is handed. The tile count " +
			"is the message on this panel, so a shape that drops one lies about what you need")
	}
}

// The views half is a promise this page cannot keep on its own: the file is
// produced by the Connect AI panel, which is gated on capsCache.views.
// Ungated here, the lane drew a views.sql tile and a button leading to a page
// where that filename does not appear -- exactly the trade views_api.go says
// this codebase refuses ("a button that only 404s is a lie").
func TestDuckLaneGatesTheFileItDoesNotProduce(t *testing.T) {
	js := readAsset(t, "app.js")
	lane := stripJSLineComments(functionBody(t, js, "function backupDuckLane("))
	if !strings.Contains(lane, "capsCache.views") {
		t.Fatal("the DuckDB lane no longer checks capsCache.views, so it can promise a views file " +
			"that Connect AI will not render")
	}
	// Both halves must sit behind it, and "behind" means INSIDE the branch,
	// not merely later in the file. Comparing byte offsets was the first cut
	// and it passed a mutation that gated the tile and left the button loose:
	// the button still came after `const hasViews = ...`.
	for _, half := range []string{"DUCKDB_VIEWS_FILE, cap:", `navigate("connect")`} {
		if !guardedByHasViews(lane, half) {
			t.Errorf("%q is not inside an `if (hasViews)` branch. A gated tile beside an ungated "+
				"button still sends the reader to a page with no views file on it", half)
		}
	}
}

// guardedByHasViews reports whether every line mentioning needle sits inside
// an `if (hasViews)` branch -- either the single-statement form on the same
// line, or a braced block. Line-based on purpose: the lane uses both forms,
// so a brace matcher alone would miss the one-liner that pushes the tile.
func guardedByHasViews(body, needle string) bool {
	depth, seen := 0, false
	for _, line := range strings.Split(body, "\n") {
		guardOpensHere := strings.Contains(line, "if (hasViews)")
		if strings.Contains(line, needle) {
			seen = true
			if depth == 0 && !guardOpensHere {
				return false
			}
		}
		if guardOpensHere && strings.HasSuffix(strings.TrimSpace(line), "{") {
			depth++
			continue
		}
		if depth > 0 && strings.TrimSpace(line) == "}" {
			depth--
		}
	}
	return seen
}

// The views file is named on two surfaces: the Connect AI panel that builds
// it, and the Backups lane that sends a reader there to get it. They share a
// constant so they cannot drift; this pins that neither re-hardcodes it.
func TestViewsFileIsNamedFromOneConstant(t *testing.T) {
	js := stripJSLineComments(readAsset(t, "app.js"))
	decl := regexp.MustCompile(`const DUCKDB_VIEWS_FILE = "([^"]+)"`).FindStringSubmatch(js)
	if decl == nil {
		t.Fatal("DUCKDB_VIEWS_FILE is gone: the Backups lane sends readers to Connect AI for that " +
			"file by name, and two literals in two panels drift the moment one is renamed")
	}
	// Counted as a bare substring, not as a standalone "views.sql" literal: a
	// real re-hardcode embeds the name in a longer string (text: "Get
	// views.sql"), which a quoted-literal check walks straight past. The route
	// /api/views.sql legitimately contains it and is removed first.
	body := strings.ReplaceAll(js, "/api/"+decl[1], "")
	if n := strings.Count(body, decl[1]); n != 1 {
		t.Errorf("%q appears %d times outside a comment line; only its own declaration may spell "+
			"it out. Everywhere else it must come from DUCKDB_VIEWS_FILE, or the Backups lane can "+
			"promise a file Connect AI no longer produces", decl[1], n)
	}
}

// A panel nobody mounts answers nothing. This is the wiring half: the guards
// above prove the lanes are right, not that a reader ever sees them.
func TestTakeAwayPanelIsMountedAboveTheList(t *testing.T) {
	body := stripJSLineComments(functionBody(t, readAsset(t, "app.js"), "async function renderBaselines("))
	// v.append(takeAway), not backupTakeAway(: dropping only the append leaves
	// the call sitting there, and a guard that reads the call reports a panel
	// that no reader can see.
	mount := strings.Index(body, "v.append(takeAway)")
	list := strings.Index(body, "baselinesPanel(")
	switch {
	case mount < 0:
		t.Fatal("renderBaselines no longer mounts backupTakeAway: the Parquet download goes back " +
			"inside a row expand, and the .sql builder leaves the page entirely — backupTakeAway " +
			"is its only caller")
	case list < 0:
		t.Fatal("renderBaselines no longer mounts baselinesPanel")
	case mount > list:
		t.Error("the two lanes render BELOW the backups list. They are the answer to why the page " +
			"was opened; the list is how you pick a different one")
	}
}
