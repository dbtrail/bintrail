package views

import (
	"strings"
	"testing"
)

// TestHeader_localBaselineRootSaysWhereItResolves pins the note in BOTH
// directions, which is the whole point of it.
//
// A views file travels: the console serves it to a browser, and a generated file
// gets copied to whichever machine will run it. A local baseline root resolves on
// exactly one of them, at exactly one path, and without the note the mismatch
// arrives as DuckDB's "No files found", which reads as a missing or corrupt
// backup rather than as the right file on the wrong host.
//
// The negative half matters as much: an s3:// root resolves anywhere, so the
// same line there would be false, and a warning that fires on the shape that
// does NOT have the problem teaches the reader to ignore it.
func TestHeader_localBaselineRootSaysWhereItResolves(t *testing.T) {
	const note = "resolve only where it is mounted"

	local := goldenInput()
	local.BaselineSource = "/data/baselines"
	for i := range local.Baselines {
		local.Baselines[i].Path = "/data/baselines/2026-04-30T03-00-00Z/shop/t.parquet"
	}
	if got := Generate(local); !strings.Contains(got, note) {
		t.Error("a file over a local baseline directory does not say its state views resolve " +
			"only on the machine that holds it")
	}

	// goldenInput's baseline root is already s3://.
	if got := Generate(goldenInput()); strings.Contains(got, note) {
		t.Error("a file over an s3:// baseline root carries the host-bound note; an S3 root is " +
			"readable from anywhere, so the note is false there")
	}
}
