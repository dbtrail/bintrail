package views

import (
	"os"
	"path/filepath"
	"testing"
)

// liveGoldenInput is goldenInput plus a live index: the file that carries every
// kind of statement this package emits — S3 preamble, state views, ATTACH, and
// a two-leg events view — in the order they have to appear in.
func liveGoldenInput() Input {
	in := goldenInput()
	in.LiveIndex = &LiveIndex{
		Host:         "index.example.com",
		Port:         3306,
		Database:     "bintrail_index",
		User:         "reader",
		BintrailID:   "11111111-2222-3333-4444-555555555555",
		TableColumns: []string{"event_id", "schema_name", "table_name", "event_type", "event_timestamp"},
	}
	return in
}

// TestGenerate_liveGolden is the ordering assertion stated as bytes.
//
// live_order_test.go pins the ordering as a set of index comparisons, which is
// what makes a violation legible. This pins the whole file, which is what
// catches a change nobody thought to write an assertion for: a statement that
// moved across the ATTACH, a comment that now describes the wrong neighbour, a
// second events definition creeping back in. The two are not redundant — the
// comparisons say WHY, the golden says WHAT, and the ordering in this file is
// load-bearing for what a reader is left holding when their index is
// unreachable (#1536).
//
// Regenerate with `go test ./internal/views -update` after an INTENTIONAL
// change, and read the diff before committing it.
func TestGenerate_liveGolden(t *testing.T) {
	got := Generate(liveGoldenInput())
	golden := filepath.Join("testdata", "views.live.golden.sql")

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
