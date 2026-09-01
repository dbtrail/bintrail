package console

import (
	"regexp"
	"strings"
	"testing"
)

// The Backups page answers one question: what do I download to open this in
// DuckDB, what do I download to load it into MySQL. The drawing carries the
// difference before any sentence does -- TWO file tiles on the DuckDB lane,
// ONE on the MySQL lane -- and a drawing can lie in a way prose cannot: it
// keeps rendering the old answer after the code stops producing it, and no
// screenshot notices because a picture of two files looks correct on its own.
//
// So the count is pinned in three places at once and they must agree: the
// tiles the lane draws, the sentence the lane says, and the lane it is.
func TestTakeAwayLanesDrawAsManyFilesAsTheyClaim(t *testing.T) {
	js := readAsset(t, "app.js")
	for _, c := range []struct {
		fn, says string
		files    int
	}{
		{"function backupDuckLane(", "Two files", 2},
		{"function backupSQLLane(", "One file", 1},
	} {
		body := stripJSLineComments(functionBody(t, js, c.fn))
		call := strings.Index(body, "backupLane(")
		if call < 0 {
			t.Errorf("%s no longer builds a lane, so nothing draws its files", c.fn)
			continue
		}
		end := strings.Index(body[call:], "\n  const ")
		if end < 0 {
			end = len(body) - call
		}
		spec := body[call : call+end]
		if n := strings.Count(spec, "{ name:"); n != c.files {
			t.Errorf("%s draws %d file tile(s) but this lane is %d file(s). The tile count IS the "+
				"message on this panel, so a drawing that outlives its lane is worse than no drawing.",
				c.fn, n, c.files)
		}
		if !strings.Contains(spec, c.says) {
			t.Errorf("%s draws %d tile(s) but no longer says %q. The picture and the sentence have "+
				"to agree, or one of them is lying to a reader who only read the other.",
				c.fn, c.files, c.says)
		}
	}
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
		t.Fatal("renderBaselines no longer mounts backupTakeAway: both downloads go back to being " +
			"invisible, one inside a row expand and one inside a fold")
	case list < 0:
		t.Fatal("renderBaselines no longer mounts baselinesPanel")
	case mount > list:
		t.Error("the two lanes render BELOW the backups list. They are the answer to why the page " +
			"was opened; the list is how you pick a different one")
	}
}
