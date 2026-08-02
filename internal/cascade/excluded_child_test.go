package cascade_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// These are the no-MySQL unit halves of the #1051 excluded-child guard: both
// synthesis sites must flag a ChildExcludedFromSnapshot edge BEFORE any index
// scan, so a nil-DB engine proves the skip happens up front (any fetch would
// panic). Hand-built edges are acceptable here because the real
// writer→loader→flag chain is pinned by the integration tests
// (TestSynthesizeVictims_excludedChildFlagged and the loader assertions in
// TestLoadCascadeFKsFromIndex).

// TestSynthesizeVictims_excludedChildDeleteUnit: a parent DELETE over a
// flagged ON DELETE CASCADE edge must synthesize nothing and report the
// recovery as provably partial, naming the child.
func TestSynthesizeVictims_excludedChildDeleteUnit(t *testing.T) {
	fks := []cascade.CascadeFK{{
		Schema: "app", Table: "child", ConstraintName: "fk_del", Column: "pid",
		ReferencedSchema: "app", ReferencedTable: "parent", ReferencedColumn: "id",
		DeleteRule: "CASCADE", UpdateRule: "RESTRICT",
		ChildExcludedFromSnapshot: true,
	}}
	parentDel := query.ResultRow{
		SchemaName: "app", TableName: "parent", EventType: event.EventDelete,
		PKValues: "1", RowBefore: map[string]any{"id": float64(1)},
		EventTimestamp: time.Now(),
	}

	res, err := cascade.SynthesizeVictims(context.Background(), query.New(nil), fks,
		[]query.ResultRow{parentDel}, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if res.Complete() {
		t.Fatal("a flagged ON DELETE edge must not report Complete")
	}
	if len(res.Victims) != 0 || len(res.SetNullRows) != 0 {
		t.Errorf("flagged edge must synthesize nothing, got %d victims / %d set-null rows",
			len(res.Victims), len(res.SetNullRows))
	}
	var flagged bool
	for _, msg := range res.Incomplete {
		if strings.Contains(msg, "app.child") &&
			strings.Contains(msg, "could NOT be reconstructed") {
			flagged = true
		}
	}
	if !flagged {
		t.Errorf("Incomplete must name the excluded child, got: %v", res.Incomplete)
	}
}

// TestSynthesizeVictims_excludedChildUpdateUnit: a parent UPDATE that moves a
// referenced key under a flagged ON UPDATE CASCADE edge must (a) report the
// recovery as provably partial with the UPDATE-specific wording — the parent's
// key reversal IS still emitted, stranding the uncaptured child rows on the
// removed key — and (b) still land the parent in KeyUpdateParents, pinning the
// cascadedHere-before-skip ordering (moving the guard above `cascadedHere =
// true` would silently drop the parent's own reversal from the output).
func TestSynthesizeVictims_excludedChildUpdateUnit(t *testing.T) {
	fks := []cascade.CascadeFK{{
		Schema: "app", Table: "child", ConstraintName: "fk_upd", Column: "pid",
		ReferencedSchema: "app", ReferencedTable: "parent", ReferencedColumn: "id",
		DeleteRule: "RESTRICT", UpdateRule: "CASCADE",
		ChildExcludedFromSnapshot: true,
	}}
	parentUpd := query.ResultRow{
		SchemaName: "app", TableName: "parent", EventType: event.EventUpdate,
		PKValues:       "1",
		RowBefore:      map[string]any{"id": float64(1)},
		RowAfter:       map[string]any{"id": float64(2)},
		EventTimestamp: time.Now(),
	}

	res, err := cascade.SynthesizeVictims(context.Background(), query.New(nil), fks,
		[]query.ResultRow{parentUpd}, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if res.Complete() {
		t.Fatal("a flagged ON UPDATE edge with a moved key must not report Complete")
	}
	if len(res.KeyUpdates) != 0 {
		t.Errorf("flagged edge must synthesize no FK restores, got %d", len(res.KeyUpdates))
	}
	if len(res.KeyUpdateParents) != 1 || res.KeyUpdateParents[0].PKValues != "1" {
		t.Errorf("the parent UPDATE must still land in KeyUpdateParents (its own reversal is real), got: %v",
			res.KeyUpdateParents)
	}
	var flagged bool
	for _, msg := range res.Incomplete {
		if strings.Contains(msg, "app.child") &&
			strings.Contains(msg, "referencing a key that no longer exists") {
			flagged = true
		}
	}
	if !flagged {
		t.Errorf("Incomplete must carry the UPDATE-specific stranded-key wording, got: %v", res.Incomplete)
	}
}
