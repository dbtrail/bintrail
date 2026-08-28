package cascade_test

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// The #1460 unit halves: a Phase-2 refusal caused by an unsupported PK type is
// a PERMANENT property of the child table, so it must be filed under its own
// caveat key (never the transient `baselinefail:` bucket) and memoized per
// table, while Phase-1 keeps running — unlike the #1273 generated-PK refusal,
// which also makes the binlog candidates unsafe and skips both phases.

// pkTypeProvider fakes a Phase-2 provider whose lookup refuses with the
// reconstruct.ErrUnsupportedPKType sentinel — what cascadebaseline returns
// for a FLOAT/BIT/JSON-keyed child since #1460 (the REAL provider's
// classification is pinned by errors.Is in
// internal/cascadebaseline/provider_pktype_1460_test.go). It counts calls,
// which is the behavioural half of the issue: the refusal cannot change
// between parent keys, so the engine must ask exactly once per child table.
type pkTypeProvider struct{ calls int }

func (p *pkTypeProvider) BaselineChildren(ctx context.Context, schema, table, fkCol, parentPK string, at time.Time, limit int) (cascade.BaselineLookup, bool, error) {
	p.calls++
	reason := reconstruct.PKTypeGateReason(
		metadata.ColumnMeta{Name: "id", IsPK: true, DataType: "float"},
		"the cascade baseline fallback", "read")
	return cascade.BaselineLookup{}, false, reconstruct.PKTypeRefusalError(
		"baseline scan of "+schema+"."+table, reason)
}

// twoParentDeletes builds one CASCADE-delete edge to app.child and TWO parent
// DELETE roots with DIFFERENT keys. Two roots is the minimum that makes the
// per-parent-key rescan observable: with one, a memo and no memo look alike.
// UpdateRule is RESTRICT so the DELETE path's checkKeyChain probe stays
// disarmed and the query count below stays about the candidate scan.
func twoParentDeletes() ([]cascade.CascadeFK, []query.ResultRow) {
	fks := []cascade.CascadeFK{{
		Schema: "app", Table: "child", ConstraintName: "fk_del", Column: "pid",
		ReferencedSchema: "app", ReferencedTable: "parent", ReferencedColumn: "id",
		DeleteRule: "CASCADE", UpdateRule: "RESTRICT",
	}}
	now := time.Now()
	var parents []query.ResultRow
	for _, id := range []string{"1", "2"} {
		n, _ := strconv.ParseFloat(id, 64)
		parents = append(parents, query.ResultRow{
			SchemaName: "app", TableName: "parent", EventType: event.EventDelete,
			PKValues:       id,
			RowBefore:      map[string]any{"id": n},
			EventTimestamp: now,
		})
	}
	return fks, parents
}

// TestSynthesizeVictims_pkTypeRefusalIsPermanentAndAsksOnce is the whole
// issue in one run. The engine gets a provider that refuses with the
// PK-type sentinel and two parent keys on the same edge, and must:
//
//	(1) file the refusal under a PERMANENT caveat, never `baseline lookup
//	    failed ... (recovery may be partial)`, which reads as worth retrying;
//	(2) ask the provider exactly ONCE — the refusal is a static property of
//	    the child table, so repeating FindBaseline + the Parquet metadata read
//	    + ReadBaselineRows per parent key is work whose outcome cannot change;
//
// Phase-1 fall-through has its OWN test below, with ONE parent key. Asserting
// it here would pass for an unrelated reason: the SECOND parent reaches the
// candidate fetch through the memo skip even in the broken shape where the
// refusal returns childScan{failed: true}, so the caveat appears either way.
func TestSynthesizeVictims_pkTypeRefusalIsPermanentAndAsksOnce(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	fks, parents := twoParentDeletes()
	prov := &pkTypeProvider{}
	res, serr := cascade.SynthesizeVictims(context.Background(), query.New(db), fks, parents, cascade.Options{
		Baseline: prov,
	})
	// The candidate fetch fails on purpose (see the doc comment), so a
	// non-nil error is expected here and is not what this test judges.
	if serr == nil {
		t.Fatal("expected the unexpected-query error from the Phase-1 fetch; the engine must have attempted it")
	}

	if prov.calls != 1 {
		t.Errorf("provider must be asked once per child table, got %d calls for %d parent keys", prov.calls, len(parents))
	}

	var permanent, transient bool
	for _, msg := range res.Incomplete {
		if strings.Contains(msg, "app.child") && strings.Contains(msg, "permanent") &&
			strings.Contains(msg, "unsupported by the baseline canonicalizer") {
			permanent = true
		}
		if strings.Contains(msg, "baseline lookup failed") {
			transient = true
		}
	}
	if !permanent {
		t.Errorf("Incomplete must carry the permanent PK-type caveat naming the shared reason, got: %v", res.Incomplete)
	}
	if transient {
		t.Errorf("a PK-type refusal must not land in the transient baselinefail bucket: %v", res.Incomplete)
	}
	// A PK-type refusal is not "no baseline covers this table" — the baseline
	// may well cover it; it is the join key that cannot be built. This is the
	// assertion that catches a memo which skips only the provider CALL and
	// lets the surrounding switch fall into its "no baseline" arm.
	for _, msg := range res.Incomplete {
		if strings.Contains(msg, "no baseline covers app.child") {
			t.Errorf("a PK-type refusal must not be reported as a missing baseline: %v", res.Incomplete)
		}
	}
}

// TestSynthesizeVictims_pkTypeRefusalStillRunsPhase1 pins the asymmetry with
// #1273 on the ONE shape that can prove it: a single parent key, so the
// refusal and the Phase-1 scan happen on the same pass and no memo skip can
// stand in for the fall-through.
//
// An unsupported PK type blocks the baseline-side join key. It says nothing
// about the binlog row-images, so the candidate scan is safe and skipping it
// would drop children this tool can still recover. The sqlmock index has NO
// expectations, so that scan fails and leaves a `victim query for app.child
// failed` caveat: positive proof it was attempted.
//
// Written after the multi-parent version of this assertion SURVIVED the
// mutation that copies the generated-PK branch's `return childScan{failed:
// true}` into the PK-type branch. With two parents the second one reaches the
// fetch through the memo skip, so the caveat appeared even in the broken
// shape. One parent removes that path.
func TestSynthesizeVictims_pkTypeRefusalStillRunsPhase1(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	fks, parents := twoParentDeletes()
	parents = parents[:1]
	prov := &pkTypeProvider{}
	res, serr := cascade.SynthesizeVictims(context.Background(), query.New(db), fks, parents, cascade.Options{
		Baseline: prov,
	})
	if serr == nil {
		t.Fatal("expected the unexpected-query error from the Phase-1 fetch; the engine must have attempted it")
	}
	if prov.calls != 1 {
		t.Fatalf("provider call count = %d, want 1 (fixture sanity)", prov.calls)
	}
	var phase1 bool
	for _, msg := range res.Incomplete {
		if strings.Contains(msg, "victim query for app.child failed") {
			phase1 = true
		}
	}
	if !phase1 {
		t.Errorf("Phase-1 must still run on the pass that refused Phase-2 (only the baseline join is blocked), got: %v", res.Incomplete)
	}
}
