package query

import "testing"

// SourceFlavor is best-effort: a nil handle must map to "" (MySQL-family
// default at every caller) rather than panic — mirrors the nil guard contract
// of recovery.DialectForIndex, which now delegates here.
func TestSourceFlavorNilDB(t *testing.T) {
	if got := SourceFlavor(nil); got != "" {
		t.Errorf("SourceFlavor(nil) = %q, want empty", got)
	}
}

// StreamGTIDSet shares the best-effort contract: a nil handle must map to ""
// (which degrades the baseline↔first-event gap check to its position
// heuristic) rather than panic.
func TestStreamGTIDSetNilDB(t *testing.T) {
	if got := StreamGTIDSet(nil); got != "" {
		t.Errorf("StreamGTIDSet(nil) = %q, want empty", got)
	}
}
