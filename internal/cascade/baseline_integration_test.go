//go:build integration

package cascade_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// fakeBaseline is a canned BaselineProvider — it lets the Phase-1/Phase-2 merge
// logic be tested without a real Parquet snapshot (the binlog side still uses a
// real but possibly-empty index DB).
type fakeBaseline struct {
	snap  time.Time
	rows  []cascade.BaselineRow
	ok    bool
	trunc bool
	err   error
	calls int
}

func (f *fakeBaseline) BaselineChildren(_ context.Context, _, _, _, _ string, _ time.Time, _ int) (cascade.BaselineLookup, bool, error) {
	f.calls++
	if f.err != nil {
		return cascade.BaselineLookup{}, false, f.err
	}
	return cascade.BaselineLookup{SnapshotTime: f.snap, Rows: f.rows, Truncated: f.trunc}, f.ok, nil
}

func cascadeFK(schema string) []cascade.CascadeFK {
	return []cascade.CascadeFK{{
		Schema: schema, Table: "child", ConstraintName: "fk", Column: "pid",
		ReferencedSchema: schema, ReferencedTable: "parent", ReferencedColumn: "id",
		DeleteRule: "CASCADE",
	}}
}

func parentDelete(schema string, at time.Time) []query.ResultRow {
	return []query.ResultRow{{
		SchemaName: schema, TableName: "parent", EventType: 3 /* DELETE */, PKValues: "1",
		RowBefore: map[string]any{"id": json.Number("1")}, EventTimestamp: at,
	}}
}

func victimKeys(rows []query.ResultRow) map[string]bool {
	m := map[string]bool{}
	for _, r := range rows {
		m[r.TableName+":"+r.PKValues] = true
	}
	return m
}

// TestPhase2_untouchedBaselineChildRecovered is the headline correctness case:
// a child present in the baseline with NO binlog event in the window (the gap
// Phase-1 misses) is recovered from its baseline row.
func TestPhase2_untouchedBaselineChildRecovered(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)
	T := time.Now().UTC()

	fb := &fakeBaseline{
		ok:   true,
		snap: T.Add(-2 * time.Hour),
		rows: []cascade.BaselineRow{{PKValues: "10", Row: map[string]any{"id": int64(10), "pid": int64(1), "payload": "keep"}}},
	}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: fb})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if !res.Complete() {
		t.Errorf("a fully-covered baseline cascade should be complete, got Incomplete=%v", res.Incomplete)
	}
	if k := victimKeys(res.Victims); len(res.Victims) != 1 || !k["child:10"] {
		t.Fatalf("want exactly child:10 from baseline, got %v", res.Victims)
	}
	if res.Victims[0].RowBefore["payload"] != "keep" {
		t.Errorf("baseline victim should carry the baseline row, got %v", res.Victims[0].RowBefore)
	}
}

// TestPhase2_reparentedNotResurrected pins the touched-exclusion: a child that
// was re-parented away (X→Y) after the baseline appears in the binlog scan and
// is correctly filtered; the baseline augmentation must NOT resurrect it from
// its stale baseline (fk=X) state.
func TestPhase2_reparentedNotResurrected(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)

	T := time.Now().UTC()
	h := T.Add(-30 * time.Minute).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	ts := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	// child 10: inserted pointing at parent 1, then re-parented to parent 2.
	testutil.InsertEvent(t, db, "b.000001", 10, 20, ts, nil, dbName, "child", 1 /*INSERT*/, "10", nil, nil, []byte(`{"id":10,"pid":1,"payload":"x"}`))
	testutil.InsertEvent(t, db, "b.000001", 20, 30, ts, nil, dbName, "child", 2 /*UPDATE*/, "10", nil, []byte(`{"id":10,"pid":1,"payload":"x"}`), []byte(`{"id":10,"pid":2,"payload":"x"}`))

	fb := &fakeBaseline{
		ok:   true,
		snap: h.Add(-time.Hour),
		rows: []cascade.BaselineRow{{PKValues: "10", Row: map[string]any{"id": int64(10), "pid": int64(1)}}},
	}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: fb})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.Victims) != 0 {
		t.Errorf("re-parented child must NOT be a victim (neither binlog nor resurrected from baseline), got %v", res.Victims)
	}
}

// TestPhase2_windowWidenedToSnapshot pins the window widening: a child inserted
// after the baseline but BEFORE the (short) lookback window is still caught,
// because a configured baseline widens the binlog scan to the snapshot time.
func TestPhase2_windowWidenedToSnapshot(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)

	T := time.Now().UTC()
	h := T.Add(-2 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	ts := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	// child 11 inserted ~2h before T — outside a 1-minute lookback, after the snapshot.
	testutil.InsertEvent(t, db, "b.000001", 10, 20, ts, nil, dbName, "child", 1 /*INSERT*/, "11", nil, nil, []byte(`{"id":11,"pid":1,"payload":"y"}`))

	fb := &fakeBaseline{ok: true, snap: h.Add(-time.Hour)} // child 11 NOT in baseline (inserted after)
	res, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: fb, Lookback: time.Minute})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if k := victimKeys(res.Victims); len(res.Victims) != 1 || !k["child:11"] {
		t.Fatalf("widened window should catch child:11 (insert after snapshot, before lookback), got %v", res.Victims)
	}
}

// TestPhase2_truncationSkipsBaseline pins the stale-resurrection guard: when the
// binlog scan truncates, `touched` is incomplete, so baseline augmentation is
// skipped (a truncated-out re-parented child must not be resurrected) and flagged.
func TestPhase2_truncationSkipsBaseline(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)

	T := time.Now().UTC()
	h := T.Add(-30 * time.Minute).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	ts := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "b.000001", 10, 20, ts, nil, dbName, "child", 1, "10", nil, nil, []byte(`{"id":10,"pid":1}`))
	testutil.InsertEvent(t, db, "b.000001", 20, 30, ts, nil, dbName, "child", 1, "11", nil, nil, []byte(`{"id":11,"pid":1}`))

	fb := &fakeBaseline{ok: true, snap: h.Add(-time.Hour),
		rows: []cascade.BaselineRow{{PKValues: "12", Row: map[string]any{"id": int64(12), "pid": int64(1)}}}}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: fb, CandidateLimit: 1})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if res.Complete() || !strings.Contains(strings.Join(res.Incomplete, " "), "baseline augmentation") {
		t.Errorf("binlog truncation must skip+flag baseline augmentation; Incomplete=%v", res.Incomplete)
	}
	// The baseline child 12 must NOT have been added under truncation.
	if victimKeys(res.Victims)["child:12"] {
		t.Errorf("baseline child must not be added when the binlog scan truncated")
	}
}

// TestPhase2_noBaselineCoverageFlagged: a provider configured but with no
// baseline for the table flags incompleteness; a provider error flags it too.
func TestPhase2_noBaselineCoverageFlagged(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)
	T := time.Now().UTC()

	notCovered := &fakeBaseline{ok: false}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: notCovered})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if res.Complete() || !strings.Contains(strings.Join(res.Incomplete, " "), "no baseline covers") {
		t.Errorf("uncovered table must flag incompleteness; Incomplete=%v", res.Incomplete)
	}

	failing := &fakeBaseline{err: context.DeadlineExceeded}
	res2, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: failing})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if res2.Complete() || !strings.Contains(strings.Join(res2.Incomplete, " "), "baseline lookup failed") {
		t.Errorf("baseline lookup error must flag incompleteness; Incomplete=%v", res2.Incomplete)
	}
}

// TestPhase2_archivesSkipBaseline pins the archive-gap guard (the #569 critical):
// when the index has archived partitions, the live [snapshot,T] scan may be
// gapped, so baseline augmentation is SKIPPED (a child re-parented/deleted in an
// archived partition must not be resurrected from its stale baseline row).
func TestPhase2_archivesSkipBaseline(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)
	T := time.Now().UTC()

	fb := &fakeBaseline{ok: true, snap: T.Add(-2 * time.Hour),
		rows: []cascade.BaselineRow{{PKValues: "10", Row: map[string]any{"id": int64(10), "pid": int64(1)}}}}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: fb, ArchivesPresent: true})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.Victims) != 0 {
		t.Errorf("baseline augmentation must be skipped when archives present; got %v", res.Victims)
	}
	if res.Complete() || !strings.Contains(strings.Join(res.Incomplete, " "), "archived") {
		t.Errorf("archive-gap skip must flag incompleteness; Incomplete=%v", res.Incomplete)
	}
}

// TestPhase2_baselineTruncationFlagged pins the baseTrunc branch: the baseline
// scan hit its cap (binlog NOT truncated) → the capped rows are still emitted but
// the run is flagged incomplete.
func TestPhase2_baselineTruncationFlagged(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)
	T := time.Now().UTC()

	fb := &fakeBaseline{ok: true, trunc: true, snap: T.Add(-2 * time.Hour),
		rows: []cascade.BaselineRow{{PKValues: "10", Row: map[string]any{"id": int64(10), "pid": int64(1)}}}}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, cascadeFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: fb})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if k := victimKeys(res.Victims); len(res.Victims) != 1 || !k["child:10"] {
		t.Errorf("capped baseline rows must still be emitted, got %v", res.Victims)
	}
	if res.Complete() || !strings.Contains(strings.Join(res.Incomplete, " "), "baseline children") {
		t.Errorf("baseline truncation must be flagged; Incomplete=%v", res.Incomplete)
	}
}

// setNullFK is cascadeFK's ON DELETE SET NULL sibling for the Phase-2 tests.
func setNullFK(schema string) []cascade.CascadeFK {
	return []cascade.CascadeFK{{
		Schema: schema, Table: "child", ConstraintName: "fk", Column: "pid",
		ReferencedSchema: schema, ReferencedTable: "parent", ReferencedColumn: "id",
		DeleteRule: "SET NULL",
	}}
}

// TestPhase2_setNullUntouchedBaselineChild: a child of a SET NULL FK present in
// the baseline but untouched in the window becomes a SetNullRestore (its FK is
// nulled, the row survives), NOT a delete victim — the SET NULL analogue of
// TestPhase2_untouchedBaselineChildRecovered, exercising the Phase-2 br.Row path.
func TestPhase2_setNullUntouchedBaselineChild(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)
	T := time.Now().UTC()

	fb := &fakeBaseline{
		ok:   true,
		snap: T.Add(-2 * time.Hour),
		rows: []cascade.BaselineRow{{PKValues: "10", Row: map[string]any{"id": int64(10), "pid": int64(1), "payload": "keep"}}},
	}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, setNullFK(dbName), parentDelete(dbName, T),
		cascade.Options{Baseline: fb})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.Victims) != 0 {
		t.Errorf("SET NULL must produce no delete victims, got %v", res.Victims)
	}
	if len(res.SetNullRows) != 1 || res.SetNullRows[0].PKValues != "10" || res.SetNullRows[0].Column != "pid" {
		t.Fatalf("want one baseline SET NULL restore for child:10/pid, got %+v", res.SetNullRows)
	}
	if res.SetNullRows[0].Row["payload"] != "keep" {
		t.Errorf("baseline SET NULL restore should carry the baseline row, got %v", res.SetNullRows[0].Row)
	}
	if !res.Complete() {
		t.Errorf("fully-covered baseline SET NULL should be complete, got %v", res.Incomplete)
	}
}
