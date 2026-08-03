package assurance_test

// This file compiles the facade from an EMBEDDER's vantage: it imports only
// github.com/dbtrail/dbtrail/assurance and never an internal package, the way
// a consumer in another module must. Nothing else in the repo does that — the
// in-package tests can always name the internal types, so a wrapper that
// returned an un-aliased internal type would compile there and be unusable
// everywhere it matters. It breaks here and nowhere else.

import (
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/assurance"
)

// Names every exported type and calls every wrapper that needs no database,
// building a plausible report shape end to end.
func TestFacadeIsUsableWithoutInternalPackages(t *testing.T) {
	var (
		sum      assurance.CoverageSummary
		floor    assurance.DeltaFloor
		stream   *assurance.StreamStateInfo
		verdict  assurance.BaselineStalenessVerdict
		file     assurance.BaselineFile
		rec      assurance.VerifyRunRecord
		tableRes assurance.VerifyTableResult
		tally    assurance.VerifySummary
		st       assurance.VerifyStatus
		mode     assurance.VerifyMode
	)
	_, _, _ = sum, st, tally

	if got := assurance.ContinuityStatus(stream, nil); got != assurance.ContinuityNone {
		t.Fatalf("continuity of a missing checkpoint = %q, want %q", got, assurance.ContinuityNone)
	}

	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	file = assurance.BaselineFile{SnapshotTime: now.Add(-72 * time.Hour), Schema: "shop", Table: "orders"}
	// The one conversion a consumer writes by hand — BaselineFile names the
	// schema "Schema", BaselineInfo names it "Database".
	infos := []assurance.BaselineInfo{{
		Database: file.Schema, Table: file.Table, SnapshotTime: file.SnapshotTime,
	}}
	floor = assurance.DeltaFloor{Hour: now.Add(-24 * time.Hour), BelowIsUnknown: true}
	assurance.AnnotateBaselineStaleness(infos, floor, now)
	verdict = assurance.OverallBaselineStaleness(infos)
	if verdict != assurance.BaselineUnknown {
		t.Fatalf("overall staleness = %q, want %q", verdict, assurance.BaselineUnknown)
	}

	// The absent-history path, which is what a CLI-only deployment hits.
	path := assurance.DefaultVerifyHistoryPath(assurance.DefaultRegistryPath())
	if path == "" {
		t.Fatal("no default history path")
	}
	h, err := assurance.OpenVerifyHistory(t.TempDir() + "/console-verify-history.json")
	if err != nil {
		t.Fatal(err)
	}
	if h.Found() {
		t.Fatal("Found() true for a history that does not exist")
	}
	for _, id := range h.ServerIDs() {
		for _, r := range h.List(id) {
			rec, mode = r, r.Mode
			if len(r.Results) > 0 {
				tableRes = r.Results[0]
			}
		}
	}
	_, _, _ = rec, mode, tableRes

	// The verdict vocabularies a summary has to branch on must all be
	// nameable from out here — hardcoding them is the drift this prevents.
	for _, s := range []string{
		assurance.ContinuityOK, assurance.ContinuityGapLost, assurance.ContinuityUnknown,
		assurance.ContinuityUnavailable, assurance.ContinuityNone,
		assurance.VerifyStateSucceeded, assurance.VerifyStateFailed, assurance.VerifyStateSkipped,
		assurance.VerifyStateIdle, assurance.VerifyStateRunning,
		assurance.VerifyTriggerManual, assurance.VerifyTriggerScheduled,
		assurance.VerifyTableMatch, assurance.VerifyTableMismatch,
		assurance.VerifyTableInconclusive, assurance.VerifyTableError,
	} {
		if s == "" {
			t.Fatal("an exported verdict constant is empty")
		}
	}
	if assurance.VerifyHistoryCap <= 0 {
		t.Fatal("VerifyHistoryCap is not a usable bound")
	}
}
