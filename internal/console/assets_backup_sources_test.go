package console

import (
	"os"
	"strings"
	"testing"
)

// TestBackupsPanelRendersEveryLocation guards the UI half of #1542.
//
// The endpoint can report two locations and an incomplete listing perfectly and
// the page can still show what it always showed: one path, and a list of
// snapshots with nothing saying it is a subset. That is the same bug one layer
// up, and it is invisible to every Go test in this package.
func TestBackupsPanelRendersEveryLocation(t *testing.T) {
	raw, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(raw)
	panel := functionBody(t, js, "function baselinesPanel(")

	if !strings.Contains(panel, "backupIncompleteNotice(b)") {
		t.Error("the panel never renders the incomplete notice, so a listing missing a whole " +
			"unreadable location renders as if it were the complete set")
	}
	// Both branches: a source can fail with snapshots present AND with none, and
	// the empty branch is the one where "no backups found" is most misleading.
	if strings.Count(panel, "backupIncompleteNotice(b)") < 2 {
		t.Error("the incomplete notice is rendered on only one branch; a failed location that " +
			"leaves the list EMPTY renders as a flat \"no backups found\"")
	}
	if !strings.Contains(panel, "backupSourceList(b)") {
		t.Error("the empty state prints a single source path; with two locations configured it names " +
			"only one of the places that came back empty")
	}

	strip := functionBody(t, js, "function baselineContextStrip(")
	if !strings.Contains(strip, "b.sources") {
		t.Error("the context strip prints only the primary source; on a server with a local " +
			"directory and a bucket, the bucket holding the snapshots is never named")
	}

	notice := functionBody(t, js, "function backupIncompleteNotice(")
	if !strings.Contains(notice, "s.error") {
		t.Error("the notice does not print the per-location error, so the operator is told " +
			"something failed but not what or where")
	}
	if !strings.Contains(notice, "error-box") {
		t.Error("the notice is not rendered as an error; the list under it is a SUBSET and a " +
			"quiet hint gets read past")
	}

	where := functionBody(t, js, "function backupWhereChip(")
	if !strings.Contains(where, "srcs.length < 2") {
		t.Error("the per-row location chip is not gated on there being more than one location; " +
			"repeating the single configured source on every row is noise")
	}
}
