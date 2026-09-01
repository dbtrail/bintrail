package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
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
	// Stripped: every check below is a substring test, and a comment line
	// mentioning one of these names would satisfy it without any code doing so.
	panel := stripJSLineComments(functionBody(t, js, "function baselinesPanel("))
	js = stripJSLineComments(js)

	if !strings.Contains(panel, "backupsPageSlice(") {
		t.Error("the list is not paged; every backup renders and the per-row Download goes below the fold")
	}
	if !strings.Contains(panel, "backupsPager(") {
		t.Error("the panel renders no pager, so a reader on page one cannot reach page two")
	}
	// The row treatment is decided by position in the WHOLE list. Taking it
	// from the page would crown the first row of every page "Newest", which is
	// a claim about which backup a restore uses.
	if !strings.Contains(panel, "window.start + i") {
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

// The page window, EXECUTED. The source assertions above can only see that
// baselinesPanel calls backupsPageSlice; they cannot see whether it returns
// the right rows. Rewriting `start` to 0 kept every string the old guard
// required while making Older a dead control -- that mutation is what this
// test exists to kill.
func TestBackupsPageSliceWindowsTheList(t *testing.T) {
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
	script := "const BACKUPS_PAGE_SIZE = " + backupsPageSizeFromSource(t, js) + ";\n" +
		functionBody(t, js, "function backupsPageSlice(") + `
const list = [];
for (let i = 0; i < 8; i++) list.push({ n: i });
const out = {};
const p0 = backupsPageSlice(list, 0), p1 = backupsPageSlice(list, 1);
out.p0start = p0.start;
out.p0 = p0.rows.map((r) => r.n).join(",");
out.p1start = p1.start;
out.p1 = p1.rows.map((r) => r.n).join(",");
console.log(JSON.stringify(out));
`
	cmd := exec.Command(node, "-e", script)
	outBytes, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, outBytes)
	}
	var got struct {
		P0start, P1start int
		P0, P1           string
	}
	if err := json.Unmarshal(outBytes, &got); err != nil {
		t.Fatalf("parsing node output %q: %v", outBytes, err)
	}
	// Page one is the newest five. Page two CONTINUES the list; it does not
	// restart it, and its offset is what keeps the "Newest" treatment on the
	// backup a restore actually reads.
	if got.P0start != 0 || got.P0 != "0,1,2,3,4" {
		t.Errorf("page one is {start:%d, rows:%s}, want {0, 0,1,2,3,4}", got.P0start, got.P0)
	}
	if got.P1start != 5 || got.P1 != "5,6,7" {
		t.Errorf("page two is {start:%d, rows:%s}, want {5, 5,6,7} — a page that restarts at 0 "+
			"makes Older a dead control and re-crowns a row Newest on every page", got.P1start, got.P1)
	}
}

func backupsPageSizeFromSource(t *testing.T, js string) string {
	t.Helper()
	m := regexp.MustCompile(`const BACKUPS_PAGE_SIZE = (\d+);`).FindStringSubmatch(js)
	if m == nil {
		t.Fatal("BACKUPS_PAGE_SIZE is gone from app.js")
	}
	return m[1]
}
