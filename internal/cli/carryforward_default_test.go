package cli

import "testing"

// TestReconstructCarryForwardIsOffByDefault: the opt-in's default, asserted at
// the surface that declares it.
//
// Everything else about this feature is guarded through the value an operator
// asks for. Nothing guarded the value they get when they ask for NOTHING, which
// is the one that ships. Flipping a single `false` to `true` in a flag
// declaration converts a documented opt-in into opt-out for every user, changes
// the on-disk representation of their backups without consent, and passes the
// entire suite: the engine test drives ReconstructTables with an explicit
// config, so it structurally cannot see a CLI default.
func TestReconstructCarryForwardIsOffByDefault(t *testing.T) {
	f := reconstructCmd.Flags().Lookup("carry-forward-unchanged")
	if f == nil {
		t.Fatal("--carry-forward-unchanged is gone from reconstruct; this guard covers nothing")
	}
	if f.DefValue != "false" {
		t.Fatalf("default = %q, want \"false\": reusing files is opt-in, and this hands it to everyone", f.DefValue)
	}
}
