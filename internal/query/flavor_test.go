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
