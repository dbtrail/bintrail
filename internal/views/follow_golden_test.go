package views

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// followInput is goldenInput moved to a LOCAL baselines root reading through
// the pointer. It is one of TWO shapes that follow: an S3 root has no pointer
// object to rewrite, so it follows by selecting the newest marked snapshot at
// query time instead, and newestInput below is that render.
func followInput() Input {
	in := goldenInput()
	root := "/data/baselines"
	in.BaselineSource = root
	for i := range in.Baselines {
		t := in.Baselines[i]
		in.Baselines[i].Path = filepath.Join(root, baseline.CurrentLinkName,
			t.Schema, filepath.Base(t.Path))
		in.Baselines[i].Rel = t.Schema + "/" + filepath.Base(t.Path)
	}
	in.Follow = FollowPointer
	return in
}

// TestGenerate_followGolden pins the pointer-followed render as bytes.
//
// Every sentence following introduces replaces one that says the
// opposite: "stays bound to the snapshot it was generated against" becomes
// "the state views below move to it when it completes". A guard written as an
// absence check would pass on a revert, because reverting restores the OTHER
// sentence and matches neither string. A golden cannot fail that way.
//
// Regenerate with `go test ./internal/views -update` after an INTENTIONAL
// change, and read the diff before committing it.
func TestGenerate_followGolden(t *testing.T) {
	got := Generate(followInput())
	golden := filepath.Join("testdata", "views.follow.golden.sql")

	if *update {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatalf("mkdir testdata: %v", err)
		}
		if err := os.WriteFile(golden, []byte(got), 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		t.Log("golden updated")
		return
	}

	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("read golden (run with -update to create it): %v", err)
	}
	if got != string(want) {
		t.Errorf("generated SQL differs from %s.\n--- got ---\n%s\n--- want ---\n%s", golden, got, want)
	}
}

// TestGenerate_followingDropsTheStaleWarning is the pair the golden cannot
// express on its own: the two renders must be MUTUALLY EXCLUSIVE about whether
// the state views stop changing. A build that emitted both paragraphs would
// diff the golden, but a reader would not know which half to believe, so the
// contradiction is asserted directly.
func TestGenerate_followingDropsTheStaleWarning(t *testing.T) {
	const stale = "stays bound to the snapshot it was generated against"
	// One LINE of the generated prose: the generator wraps at a fixed width and
	// prefixes every line with "-- ", so a phrase that reads as continuous is not
	// a substring of the output.
	const moves = "Replacing the pointer is a single step"

	following := Generate(followInput())
	if strings.Contains(following, stale) {
		t.Error("a following file still warns that its state views stop changing")
	}
	if !strings.Contains(following, moves) {
		t.Error("a following file does not say its state views move")
	}

	pinned := followInput()
	pinned.Follow = FollowNone
	out := Generate(pinned)
	if !strings.Contains(out, stale) {
		t.Error("a pinned file dropped the warning that its state views stop changing")
	}
	if strings.Contains(out, moves) {
		t.Error("a pinned file claims its state views move")
	}
}

// newestInput is goldenInput following by marker instead of by pointer, which
// is what an S3 root does. It goes through ApplyFollow rather than setting the
// mode by hand: the decision to follow an S3 root at all is half of #1550, and
// a test that assigned FollowNewest itself would still pass if the producer
// stopped reaching it.
func newestInput() Input {
	in := goldenInput()
	ApplyFollow(&in, in.BaselineSource, false)
	return in
}

// TestGenerate_newestGolden pins the marker-followed render as bytes.
//
// Its own golden, not a variant of the pointer one: this render is the only
// place several sentences and the whole CASE body appear, and #1546 already
// showed that guards phrased as absence checks pass on a revert. A golden is
// the shape that cannot.
//
// Regenerate with `go test ./internal/views -update` after an INTENTIONAL
// change, and read the diff before committing it.
func TestGenerate_newestGolden(t *testing.T) {
	got := Generate(newestInput())
	golden := filepath.Join("testdata", "views.newest.golden.sql")

	if *update {
		if err := os.WriteFile(golden, []byte(got), 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		t.Log("golden updated")
		return
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("read golden (run with -update to create it): %v", err)
	}
	if got != string(want) {
		t.Errorf("generated SQL differs from %s.\n--- got ---\n%s\n--- want ---\n%s", golden, got, want)
	}
}
