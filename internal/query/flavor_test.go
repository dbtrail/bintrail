package query

import "testing"

// SourceFlavor is best-effort: a nil handle must map to "" (MySQL-family
// default at every caller) rather than panic — mirrors the nil guard contract
// of recovery.DialectForIndex, which now delegates here.
// OldestIndexedEvent shares the best-effort contract: a nil handle must map
// to (zero, false) — which degrades the baseline↔first-event gap check to its
// hedged unproven verdict — rather than panic.
func TestOldestIndexedEventNilDB(t *testing.T) {
	if s, ok := OldestIndexedEvent(nil); ok || s.BinlogFile != "" || s.StartPos != 0 {
		t.Errorf("OldestIndexedEvent(nil) = (%+v, %v), want (zero, false)", s, ok)
	}
}

func TestSourceFlavorNilDB(t *testing.T) {
	if got := SourceFlavor(nil); got != "" {
		t.Errorf("SourceFlavor(nil) = %q, want empty", got)
	}
}
