package views

import (
	"os"
	"path/filepath"
	"testing"
)

// TestGenerate_omitEventsGolden pins the DEFAULT render as bytes.
//
// Both existing goldens cover a file WITH the events view, so every sentence
// that only appears when it is absent — the header's else arm, the state
// block's else arm, the "not included" note — was guarded by absence checks
// alone, and a revert to the pre-flip wording matched none of them and passed.
// A golden cannot fail open: any change to those bytes is a diff.
//
// Regenerate with `go test ./internal/views -update` after an INTENTIONAL
// change, and read the diff before committing it.
func TestGenerate_omitEventsGolden(t *testing.T) {
	in := goldenInput()
	in.OmitEvents = true
	got := Generate(in)
	golden := filepath.Join("testdata", "views.omitevents.golden.sql")

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
