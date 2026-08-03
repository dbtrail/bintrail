package assurance

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/status"
)

// The facade must ALIAS the internal types, never restate them. A separately
// declared struct with identical fields would satisfy every call in this
// package and still give an embedder its own copy to drift with — and these
// types carry verdicts (`bintrail status` renders the same ones). Assignment
// between two distinct named types does not compile, so this pins it.
func TestFacadeTypesAreTheInternalTypes(t *testing.T) {
	var (
		_ status.CoverageSummary          = CoverageSummary{}
		_ CoverageSummary                 = status.CoverageSummary{}
		_ status.DeltaFloor               = DeltaFloor{}
		_ DeltaFloor                      = status.DeltaFloor{}
		_ status.BaselineInfo             = BaselineInfo{}
		_ BaselineInfo                    = status.BaselineInfo{}
		_ status.StreamStateInfo          = StreamStateInfo{}
		_ status.BaselineStalenessVerdict = BaselineUnknown
		_ reconstruct.BaselineFile        = BaselineFile{}
		_ BaselineFile                    = reconstruct.BaselineFile{}
		_ console.VerifyRunRecord         = VerifyRunRecord{}
		_ VerifyRunRecord                 = console.VerifyRunRecord{}
		_ console.VerifyStatus            = VerifyStatus{}
		_ console.VerifySummary           = VerifySummary{}
		_ console.VerifyTableResult       = VerifyTableResult{}
	)
	if VerifyTriggerScheduled != console.VerifyTriggerScheduled || VerifyStateSkipped != console.VerifyStateSkipped {
		t.Fatal("re-exported verify constants drifted from the core's")
	}
}

func TestContinuityStatusReachesTheOneRule(t *testing.T) {
	// The two verdicts a facade is most likely to get wrong by re-deriving:
	// no stream row is a genuine no-claim, an unreadable one is not "ok".
	if got := ContinuityStatus(nil, nil); got != "none" {
		t.Fatalf("no stream row = %q, want none", got)
	}
	if got := ContinuityStatus(nil, errors.New("boom")); got != "unavailable" {
		t.Fatalf("unreadable stream state = %q, want unavailable", got)
	}
}

// The #1219 demotion has to survive the trip through the facade: below an
// unattributable floor a snapshot grades unknown, not broken. An embedder
// that got "broken" here would page an operator whose archives are fine.
func TestStalenessGradingKeepsTheAmbiguityDemotion(t *testing.T) {
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	below := []BaselineInfo{{Database: "shop", Table: "orders", SnapshotTime: now.Add(-72 * time.Hour)}}

	attributable := DeltaFloor{Hour: now.Add(-24 * time.Hour)}
	AnnotateBaselineStaleness(below, attributable, now)
	if below[0].Staleness != BaselineBroken {
		t.Fatalf("below an attributable floor = %q, want broken", below[0].Staleness)
	}

	ambiguous := DeltaFloor{Hour: now.Add(-24 * time.Hour), BelowIsUnknown: true}
	AnnotateBaselineStaleness(below, ambiguous, now)
	if below[0].Staleness != BaselineUnknown {
		t.Fatalf("below an unattributable floor = %q, want unknown", below[0].Staleness)
	}
	if got := OverallBaselineStaleness(below); got != BaselineUnknown {
		t.Fatalf("overall = %q, want unknown", got)
	}
}

// A caller reporting on verification activity must be able to see that there
// is no history at all — the file is written only by `bintrail-console
// watch`, so an empty List on a CLI-only deployment means "never verified",
// not "nothing failed".
func TestVerifyHistoryAbsenceIsVisibleThroughTheFacade(t *testing.T) {
	path := DefaultVerifyHistoryPath(filepath.Join(t.TempDir(), "console-servers.yaml"))
	if base := filepath.Base(path); base != "console-verify-history.json" {
		t.Fatalf("history path = %q", base)
	}
	// The default location has to be derivable through the facade too, or a
	// caller hardcodes it and reads nothing once the registry moves.
	if got, want := DefaultRegistryPath(), console.DefaultRegistryPath(); got != want {
		t.Fatalf("DefaultRegistryPath() = %q, want %q", got, want)
	}
	if filepath.Dir(DefaultVerifyHistoryPath(DefaultRegistryPath())) != filepath.Dir(DefaultRegistryPath()) {
		t.Fatal("the default history does not sit beside the default registry")
	}
	h, err := OpenVerifyHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	if h.Found() {
		t.Fatal("Found() true with no history file on disk")
	}
	if ids := h.ServerIDs(); len(ids) != 0 {
		t.Fatalf("server ids from an absent history: %v", ids)
	}
}
