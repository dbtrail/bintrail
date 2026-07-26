//go:build integration

package cascade_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The remaining silent-zero paths (#1125): the delete-path analog of the
// key-chain blind spot, and the key-chain probe's own truncation. Both must
// surface Incomplete instead of a clean zero.

// probeLimit mirrors cascade.keyChainProbeLimit (unexported). If the constant
// ever changes, TestSynthesizeKeyUpdate_probeCapFlagged fails loudly at the
// KeyUpdates assertion rather than silently probing the wrong budget.
const probeLimit = 8

// TestSynthesizeDelete_priorKeyMoveFlagged pins the DELETE-root analog of the
// key-chain blind spot (#1125): the parent's referenced key moved A → B under
// ON UPDATE CASCADE (children rewritten below the binlog), then the parent was
// DELETEd. The delete-cascade scan searches children by the pre-delete key (B),
// but their last INDEXED image still says A — zero matches. A clean
// "0 victims, Complete" would tell the operator the recovery is complete while
// every child is orphaned; the run must be flagged Incomplete.
func TestSynthesizeDelete_priorKeyMoveFlagged(t *testing.T) {
	e := newUpdEnv(t)
	// The child's last logged image predates the parent key move: pcode = 'A'.
	testutil.InsertEvent(t, e.db, "b.000001", 10, 20, e.ts, nil, e.dbName, "child", 1, "10", nil, nil, []byte(`{"id":10,"pcode":"A"}`))
	// The earlier parent key update (A → B), which IS in the index.
	testutil.InsertEvent(t, e.db, "b.000001", 20, 30, e.ts, nil, e.dbName, "parent", 2, "1",
		nil, []byte(`{"id":1,"code":"A"}`), []byte(`{"id":1,"code":"B"}`))

	fks := []cascade.CascadeFK{{
		Schema: e.dbName, Table: "child", ConstraintName: "fk", Column: "pcode",
		ReferencedSchema: e.dbName, ReferencedTable: "parent", ReferencedColumn: "code",
		DeleteRule: "CASCADE", UpdateRule: "CASCADE",
	}}
	// The ROOT is the later DELETE of the parent, whose before-image carries B.
	roots := []query.ResultRow{{
		SchemaName: e.dbName, TableName: "parent", EventType: 3 /* DELETE */, PKValues: "1",
		RowBefore:      map[string]any{"id": json.Number("1"), "code": "B"},
		EventTimestamp: e.T,
	}}
	res, err := cascade.SynthesizeVictims(context.Background(), e.eng, fks, roots, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.Victims) != 0 {
		t.Fatalf("the child's indexed image still says A, so nothing matches B; got %v", victimList(res.Victims))
	}
	joined := strings.Join(res.Incomplete, " ")
	if res.Complete() || !strings.Contains(joined, "EARLIER update of referenced column") {
		t.Errorf("a prior key move hides the delete-cascade's children and MUST be flagged; Incomplete=%v", res.Incomplete)
	}
	if !strings.Contains(joined, "cascade-deleted children") {
		t.Errorf("the caveat must name the DELETE-root consequence (cascade-deleted children), got %v", res.Incomplete)
	}
}

// TestSynthesizeDelete_priorKeyMoveNotFlaggedWithoutUpdateCascade is the
// control pinning the rule gate: the same prior key move under ON UPDATE
// SET NULL nulled the children instead of dragging them to the new key, so at
// delete time nothing pointed at B and the zero-child result is genuinely
// correct — it must stay Complete, not manufacture a false caveat.
func TestSynthesizeDelete_priorKeyMoveNotFlaggedWithoutUpdateCascade(t *testing.T) {
	e := newUpdEnv(t)
	testutil.InsertEvent(t, e.db, "b.000001", 10, 20, e.ts, nil, e.dbName, "child", 1, "10", nil, nil, []byte(`{"id":10,"pcode":"A"}`))
	testutil.InsertEvent(t, e.db, "b.000001", 20, 30, e.ts, nil, e.dbName, "parent", 2, "1",
		nil, []byte(`{"id":1,"code":"A"}`), []byte(`{"id":1,"code":"B"}`))

	fks := []cascade.CascadeFK{{
		Schema: e.dbName, Table: "child", ConstraintName: "fk", Column: "pcode",
		ReferencedSchema: e.dbName, ReferencedTable: "parent", ReferencedColumn: "code",
		DeleteRule: "CASCADE", UpdateRule: "SET NULL",
	}}
	roots := []query.ResultRow{{
		SchemaName: e.dbName, TableName: "parent", EventType: 3 /* DELETE */, PKValues: "1",
		RowBefore:      map[string]any{"id": json.Number("1"), "code": "B"},
		EventTimestamp: e.T,
	}}
	res, err := cascade.SynthesizeVictims(context.Background(), e.eng, fks, roots, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.Victims) != 0 {
		t.Fatalf("want no victims, got %v", victimList(res.Victims))
	}
	if !res.Complete() {
		t.Errorf("ON UPDATE SET NULL cannot have moved children onto the deleted key; want Complete, got %v", res.Incomplete)
	}
}

// TestSynthesizeKeyUpdate_probeCapFlagged pins the bounded-probe fix (#1125):
// the key-chain probe fetches at most keyChainProbeLimit prior UPDATEs, ASC. A
// parent row parked on the key accumulating unrelated-column updates fills the
// whole page without any of them being the arrival, so a chain beyond the cap
// cannot be ruled out — the run must be flagged Incomplete rather than let the
// bounded probe support an unbounded "no chain" conclusion.
func TestSynthesizeKeyUpdate_probeCapFlagged(t *testing.T) {
	e := newUpdEnv(t)
	// The child genuinely points at B — the restore itself is the positive anchor.
	testutil.InsertEvent(t, e.db, "b.000001", 10, 20, e.ts, nil, e.dbName, "child", 1, "10", nil, nil, []byte(`{"id":10,"pcode":"B"}`))
	// Exactly probeLimit unrelated-column updates while the parent sat on B:
	// every one matches the ColumnEq probe for code=B, none moved the key.
	for i := 0; i < probeLimit; i++ {
		testutil.InsertEvent(t, e.db, "b.000001", uint64(100+i*10), uint64(110+i*10), e.ts, nil, e.dbName, "parent", 2, "1",
			nil, []byte(fmt.Sprintf(`{"id":1,"code":"B","n":%d}`, i)), []byte(fmt.Sprintf(`{"id":1,"code":"B","n":%d}`, i+1)))
	}

	fks := []cascade.CascadeFK{{
		Schema: e.dbName, Table: "child", ConstraintName: "fk", Column: "pcode",
		ReferencedSchema: e.dbName, ReferencedTable: "parent", ReferencedColumn: "code",
		DeleteRule: "RESTRICT", UpdateRule: "CASCADE",
	}}
	roots := parentKeyUpdate(e.dbName, "code", "B", "C", e.T)
	res, err := cascade.SynthesizeVictims(context.Background(), e.eng, fks, roots, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.KeyUpdates) != 1 {
		t.Fatalf("want the one restore for child 10 (probeLimit=%d must match cascade.keyChainProbeLimit), got %+v",
			probeLimit, res.KeyUpdates)
	}
	if res.Complete() || !strings.Contains(strings.Join(res.Incomplete, " "), "could not be ruled out") {
		t.Errorf("a probe that exhausted its cap without a verdict must flag Incomplete; got %v", res.Incomplete)
	}
}

// TestSynthesizeKeyUpdate_probeUnderCapNotFlagged is the control for the cap:
// one fewer matching prior UPDATE leaves headroom in the page, so "no arrival
// in the window" is a real conclusion and the run stays Complete.
func TestSynthesizeKeyUpdate_probeUnderCapNotFlagged(t *testing.T) {
	e := newUpdEnv(t)
	testutil.InsertEvent(t, e.db, "b.000001", 10, 20, e.ts, nil, e.dbName, "child", 1, "10", nil, nil, []byte(`{"id":10,"pcode":"B"}`))
	for i := 0; i < probeLimit-1; i++ {
		testutil.InsertEvent(t, e.db, "b.000001", uint64(100+i*10), uint64(110+i*10), e.ts, nil, e.dbName, "parent", 2, "1",
			nil, []byte(fmt.Sprintf(`{"id":1,"code":"B","n":%d}`, i)), []byte(fmt.Sprintf(`{"id":1,"code":"B","n":%d}`, i+1)))
	}

	fks := []cascade.CascadeFK{{
		Schema: e.dbName, Table: "child", ConstraintName: "fk", Column: "pcode",
		ReferencedSchema: e.dbName, ReferencedTable: "parent", ReferencedColumn: "code",
		DeleteRule: "RESTRICT", UpdateRule: "CASCADE",
	}}
	roots := parentKeyUpdate(e.dbName, "code", "B", "C", e.T)
	res, err := cascade.SynthesizeVictims(context.Background(), e.eng, fks, roots, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.KeyUpdates) != 1 {
		t.Fatalf("want the one restore for child 10, got %+v", res.KeyUpdates)
	}
	if !res.Complete() {
		t.Errorf("an under-cap probe with no arrival is conclusive; want Complete, got %v", res.Incomplete)
	}
}
