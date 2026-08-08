package cli

import (
	"strings"
	"testing"
)

// Only the two actionable tokens are load-bearing (#1274: flagless reconcile
// is a dry-run); the rest of the wording is free to evolve.
func TestSourceEmptyHintNamesActingFlags(t *testing.T) {
	if !strings.Contains(sourceEmptyHint, "archive reconcile --repair") {
		t.Errorf("hint must name the acting reconcile form, got: %s", sourceEmptyHint)
	}
	if !strings.Contains(sourceEmptyHint, "--allow-gaps") {
		t.Errorf("hint must name the lossy override for the CLI surface, got: %s", sourceEmptyHint)
	}
}
