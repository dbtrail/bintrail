package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestBackupsListPagesAndSaysItOpens guards the two halves of #1572.
//
// The list showed every backup it had, which pushed the per-row Download and
// the restore controls below the fold on a server with a real retention
// history. And the only thing that said a row OPENS was a hover background and
// a pointer cursor, so the download lived behind an affordance nobody could
// see. Both are UI-only; no Go test in this package can reach them.
func TestBackupsListPagesAndSaysItOpens(t *testing.T) {
	raw, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(raw)
	panel := functionBody(t, js, "function baselinesPanel(")

	if !strings.Contains(panel, "BACKUPS_PAGE_SIZE") || !strings.Contains(panel, ".slice(start,") {
		t.Error("the list is not paged; every backup renders and the per-row Download goes below the fold")
	}
	if !strings.Contains(panel, "backupsPager(") {
		t.Error("the panel renders no pager, so a reader on page one cannot reach page two")
	}
	// The row treatment is decided by position in the WHOLE list. Taking it
	// from the page would crown the first row of every page "Newest", which is
	// a claim about which backup a restore uses.
	if !strings.Contains(panel, "const idx = start + i") {
		t.Error("the row index is not offset by the page; the first row of page two would be " +
			"labelled Newest and given the treatment reserved for the backup restores use")
	}
	if !strings.Contains(panel, `class: "bk-chev"`) {
		t.Error("rows carry no chevron. The per-row Download lives inside the fold a click opens, " +
			"so with no visible affordance the feature is unreachable in practice")
	}

	// Page state must OUTLIVE a render: the panel repaints every ~10s while a
	// run is in flight, and state held in the DOM would snap a reader back to
	// page one under their hands.
	if !strings.Contains(js, "let backupsPage = {") {
		t.Error("page state is not held outside the render, so a repaint resets it")
	}
}

// backupsPageIndex is pure, so it is EXECUTED. The clamp is the half that only
// shows up when the list SHRINKS under a reader who had paged away, which no
// source assertion can express.
func TestBackupsPageIndexClamps(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	raw, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(raw)
	script := "let backupsPage = { server: null, index: 0 };\n" +
		functionBody(t, js, "function backupsPageIndex(") + `
const out = {};
out.first = backupsPageIndex("a", 5);
backupsPage.index = 3;
out.kept = backupsPageIndex("a", 5);
out.shrunk = backupsPageIndex("a", 2);          // retention pruned the list
out.switched = backupsPageIndex("b", 5);        // another server
out.noPages = backupsPageIndex("c", 0);         // nothing to show
console.log(JSON.stringify(out));
`
	dir := t.TempDir()
	path := filepath.Join(dir, "page.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(node, path).Output()
	if err != nil {
		t.Fatalf("node: %v", err)
	}
	var got struct{ First, Kept, Shrunk, Switched, NoPages int }
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	if got.First != 0 || got.Kept != 3 {
		t.Errorf("first=%d kept=%d, want 0 and 3 — the chosen page has to survive a repaint", got.First, got.Kept)
	}
	if got.Shrunk != 1 {
		t.Errorf("a list that shrank to 2 pages left the reader on index %d; anything past the end "+
			"renders an empty page with no way back", got.Shrunk)
	}
	if got.Switched != 0 {
		t.Errorf("switching server kept index %d; page 4 of a two-page list is not where a "+
			"different server's history starts", got.Switched)
	}
	if got.NoPages != 0 {
		t.Errorf("an empty list gave index %d, want 0", got.NoPages)
	}
}
