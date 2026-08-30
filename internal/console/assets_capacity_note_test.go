package console

import (
	"os"
	"strings"
	"testing"
)

// #1527: the Index disk card explained an unmeasurable volume with "The index
// runs on another host or container", which the check never knew and which is
// plainly false on a single-machine install whose index data directory is not
// mounted into the console. The card now reads the branch the doctor landed on
// (free_reason) and says what would make free space measurable. Scoped to the
// two note functions and the card body, so a mention anywhere else in app.js
// cannot satisfy these guards.
func TestCapacityCardNamesWhyFreeSpaceIsUnmeasurable(t *testing.T) {
	js, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	src := string(js)
	note := functionBody(t, src, "function capacityNote(")
	free := functionBody(t, src, "function capacityFreeNote(")
	card := functionBody(t, src, "function capacityCard(")
	// capacityBox is in the sweep too: it is the other place this card's copy
	// is written, and the claim could come back there just as easily.
	box := functionBody(t, src, "function capacityBox(")

	for _, guess := range []string{"another host or container", "runs on another host"} {
		for where, js := range map[string]string{"capacityNote": note, "capacityFreeNote": free, "capacityBox": box} {
			if strings.Contains(js, guess) {
				t.Errorf("%s still asserts a topology the check cannot know: %q", where, guess)
			}
		}
	}

	// One branch per fallback the doctor reports, and the two that name a fix
	// name the same variable and file the bundled stack wires.
	for _, want := range []string{`case "mount_unset"`, `case "mount_unusable"`, `case "index_not_local"`, "BINTRAIL_INDEX_DATADIR_RO", "docker-compose.yml"} {
		if !strings.Contains(free, want) {
			t.Errorf("capacityFreeNote is missing %q, so that state stays a dead end", want)
		}
	}

	// The remote branch must offer NO mount fix: a mount that is not the
	// index's would report the wrong volume's free space, which is worse than
	// reporting nothing.
	remote := free[strings.Index(free, `case "index_not_local"`):]
	if i := strings.Index(remote, "default:"); i > 0 {
		remote = remote[:i]
	}
	if strings.Contains(remote, "BINTRAIL_INDEX_DATADIR_RO") {
		t.Error("the index_not_local branch offers a mount fix that would measure the wrong filesystem")
	}

	// Keyed on free_known, not on the free_unknown grade: the "free on disk"
	// row also reads "not measurable from here" under a fresh index and a
	// short history, and those states need the explanation too.
	if !strings.Contains(free, "cap.free_known") || !strings.Contains(free, "cap.free_reason") {
		t.Error("capacityFreeNote must key on free_known and free_reason")
	}
	if !strings.Contains(card, "capacityFreeNote(") {
		t.Error("capacityCard never renders the free-space reason, so it reaches nobody")
	}
	if strings.Contains(free, "—") {
		t.Error("em dash in card copy")
	}
}
