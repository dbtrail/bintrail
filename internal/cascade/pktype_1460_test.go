package cascade_test

import (
	"context"
	"database/sql/driver"
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
		if strings.Contains(msg, "app.child") && strings.Contains(msg, "PERMANENT") &&
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
	// The caveat must name the boundary the code actually enforces. With
	// Phase-2 refused, `since` is never widened to the snapshot time (that
	// assignment lives only in the `covered` arm), so what is NOT recovered is
	// everything untouched within the LOOKBACK WINDOW, which on an older
	// baseline is a strictly larger set than "untouched since the snapshot".
	// Naming the snapshot would tell an operator a child touched 45 days ago
	// against a 60-day-old baseline was recovered, when it was not. The
	// sibling `nobaseline:` caveat states the same boundary in the same words;
	// these two must not drift.
	for _, msg := range res.Incomplete {
		if !strings.HasPrefix(msg, "app.child cannot be augmented") {
			continue
		}
		if !strings.Contains(msg, "untouched within the lookback window") {
			t.Errorf("the caveat must name the lookback window as the boundary, got: %q", msg)
		}
		if strings.Contains(msg, "since the baseline snapshot") {
			t.Errorf("the caveat must not claim the snapshot bounds what is recovered; the window is never widened on this path: %q", msg)
		}
		// scanChildren serves BOTH root kinds (the ON UPDATE path at the
		// key-reversal site and the ON DELETE path), so the caveat cannot
		// assume a deleted parent.
		if strings.Contains(msg, "deleted parent") {
			t.Errorf("the caveat is emitted for ON UPDATE roots too and must not assume a deleted parent: %q", msg)
		}
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

// captureTimes is an sqlmock Argument that records every value bound at its
// position and always matches, so the test can read the `since`/`until`
// bounds the engine actually sent instead of inferring them.
type captureTimes struct{ got *[]time.Time }

func (c captureTimes) Match(v driver.Value) bool {
	if t, ok := v.(time.Time); ok {
		*c.got = append(*c.got, t)
	}
	return true
}

// TestSynthesizeVictims_pkTypeRefusalLeavesTheLookbackWindowNarrow pins the
// claim the caveat makes. A refused edge never widens its Phase-1 window to
// the baseline snapshot, because `since = bl.SnapshotTime` lives only in the
// `covered` arm, and the memo skip on the second parent must not resurrect a
// widened bound from a stale lookup either. Both passes must scan
// [rootTS-Lookback, rootTS].
//
// Without this, nothing in the suite connects the caveat's wording to the
// window the engine uses, and the two could drift into the caveat overstating
// what was recovered. Lookback is set to a value no default equals so the
// assertion cannot pass by coincidence.
func TestSynthesizeVictims_pkTypeRefusalLeavesTheLookbackWindowNarrow(t *testing.T) {
	const lookback = 73 * time.Hour

	var sinceArgs []time.Time
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)
	// Two candidate fetches (one per parent) plus the skew probe each leaves
	// behind; extras are harmless because ExpectationsWereMet is not asserted.
	for i := 0; i < 8; i++ {
		mock.ExpectQuery(".*").WithArgs(
			sqlmock.AnyArg(), sqlmock.AnyArg(),
			captureTimes{got: &sinceArgs}, captureTimes{got: &sinceArgs},
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		).WillReturnRows(sqlmock.NewRows([]string{"event_id"}))
	}

	fks, parents := twoParentDeletes()
	rootTS := parents[0].EventTimestamp
	prov := &pkTypeProvider{}
	if _, serr := cascade.SynthesizeVictims(context.Background(), query.New(db), fks, parents, cascade.Options{
		Baseline: prov,
		Lookback: lookback,
	}); serr != nil {
		t.Fatalf("SynthesizeVictims: %v", serr)
	}
	if prov.calls != 1 {
		t.Fatalf("provider call count = %d, want 1 (fixture sanity)", prov.calls)
	}
	if len(sinceArgs) < 2 {
		t.Fatalf("expected at least one candidate fetch per parent, captured %d time bounds", len(sinceArgs))
	}
	want := rootTS.Add(-lookback)
	checked := 0
	for i, got := range sinceArgs {
		if got.Equal(rootTS) || got.After(rootTS) {
			continue // the `until` bound of the same query
		}
		checked++
		if !got.Equal(want) {
			t.Errorf("time bound %d = %v, want the lookback floor %v; a refused edge must never scan from the baseline snapshot", i, got, want)
		}
	}
	// Without this the loop above is vacuous whenever the capture misses the
	// lower bound: zero iterations assert nothing and the test still passes.
	if checked < 2 {
		t.Fatalf("expected a lower bound captured per parent, checked %d of %d captured bounds", checked, len(sinceArgs))
	}
}
