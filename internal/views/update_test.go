package views

import "flag"

// update regenerates the golden file instead of comparing against it:
//
//	go test ./internal/views -update
//
// Declared in its own file so the flag registration stays out of the test that
// reads it.
var update = flag.Bool("update", false, "rewrite the golden files instead of comparing")
