package console

import (
	"os"
	"path/filepath"
	"testing"
)

// TestBaselineHistoryAppendCreatesMissingConfigDir pins the second half of
// #1487. Making the config path absolute stops the history landing in an
// arbitrary working directory, but it does not by itself make the write
// succeed: BaselineRunHistory.save was the only one of five sibling atomic
// savers (Registry.save, saveAuthFile, saveMCPTokenFile, VerifyHistory.save)
// that did not create its directory first.
//
// That reproduces the reported symptom — the run history lost on every
// refresh — on a fresh install with a perfectly good HOME. Nothing creates
// ~/.config/bintrail on its own: LoadRegistry treats a missing file as an
// empty registry, and Registry.save only runs on a mutation, so the first
// baseline refresh can genuinely be the tree's first writer.
//
// Deliberately independent of the absolute-path fix: the path here is
// explicit, so this test stays red whenever the MkdirAll is missing, and the
// absolute-path test stays red whenever the fallback is relative. Neither fix
// alone turns both green.
func TestBaselineHistoryAppendCreatesMissingConfigDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), ".config", "bintrail")
	path := filepath.Join(dir, "console-baseline-history.json")

	h, err := OpenBaselineHistory(path)
	if err != nil {
		t.Fatalf("OpenBaselineHistory under a not-yet-created tree: %v", err)
	}
	rec := BaselineRunRecord{
		ServerID:     "s1",
		Kind:         BaselineRunRefresh,
		SnapshotTime: "2026-08-27T10:00:00Z",
		StartedAt:    "2026-08-27T09:59:00Z",
	}
	if err := h.Append(rec); err != nil {
		t.Fatalf("Append into a not-yet-created config directory: %v", err)
	}

	// Durability, not just a nil error: a save that wrote nothing would pass
	// an error check on its own.
	reloaded, err := OpenBaselineHistory(path)
	if err != nil {
		t.Fatalf("reopen history: %v", err)
	}
	if got := reloaded.FindBySnapshot(rec.ServerID, rec.SnapshotTime); got == nil {
		t.Fatalf("record did not survive the write to %s", path)
	}

	// 0700 like every sibling saver: this directory holds the registry's DSN
	// passwords and the console credential file.
	st, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat the created config directory: %v", err)
	}
	if perm := st.Mode().Perm(); perm != 0o700 {
		t.Errorf("created config directory mode = %04o, want 0700 (matching Registry.save and VerifyHistory.save)", perm)
	}
}
