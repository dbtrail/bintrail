package cascade_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// The no-MySQL unit halves of the #1273 generated-PK cascade gate, in the
// excluded_child_test.go style: the gates must fire BEFORE any index or
// baseline scan, so a nil-DB engine proves the skip happens up front (any
// fetch would panic).

// cascadeDeleteFixture builds one CASCADE-delete edge to app.child plus a
// parent DELETE root. updateRule is the mutation-sensitivity dial: "CASCADE"
// arms the DELETE path's checkKeyChain probe, so deleting the hoisted gate
// (which sits ABOVE the probe) makes the gated tests die on the nil-DB fetch
// inside checkKeyChain instead of passing via scanChildren's internal gate.
func cascadeDeleteFixture(updateRule string) ([]cascade.CascadeFK, []query.ResultRow) {
	fks := []cascade.CascadeFK{{
		Schema: "app", Table: "child", ConstraintName: "fk_del", Column: "pid",
		ReferencedSchema: "app", ReferencedTable: "parent", ReferencedColumn: "id",
		DeleteRule: "CASCADE", UpdateRule: updateRule,
	}}
	parentDel := query.ResultRow{
		SchemaName: "app", TableName: "parent", EventType: event.EventDelete,
		PKValues: "1", RowBefore: map[string]any{"id": float64(1)},
		EventTimestamp: time.Now(),
	}
	return fks, []query.ResultRow{parentDel}
}

// svChildPKMetas is the probe both gate tests wire: app.child's PK carries a
// generated member (the MariaDB system-versioning shape).
func svChildPKMetas(schema, table string) []metadata.ColumnMeta {
	if schema == "app" && table == "child" {
		return []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
			{Name: "row_end", OrdinalPosition: 4, IsPK: true, DataType: "timestamp", IsGenerated: true},
		}
	}
	return nil
}

func assertGeneratedPKCaveat(t *testing.T, res cascade.Result) {
	t.Helper()
	if res.Complete() {
		t.Fatal("a generated-PK child edge must not report Complete")
	}
	var flagged bool
	for _, msg := range res.Incomplete {
		if strings.Contains(msg, "app.child") && strings.Contains(msg, "generated column") &&
			strings.Contains(msg, "PERMANENT") {
			flagged = true
		}
	}
	if !flagged {
		t.Fatalf("Incomplete must carry the permanent generated-PK caveat, got: %v", res.Incomplete)
	}
}

// TestSynthesizeVictims_generatedPKChildSkippedViaProbe: with the PKMetas
// probe wired (as the CLI/console/MCP call sites do), a versioned child edge
// is skipped in BOTH phases with the permanent generatedpk caveat, before any
// candidate scan could rebuild children from history-polluted state. The
// fixture's UpdateRule "CASCADE" makes the DELETE-path HOIST independently
// observable: without it, checkKeyChain (which the hoist sits above) fetches
// on the nil DB and this test dies by panic rather than passing through
// scanChildren's internal gate.
func TestSynthesizeVictims_generatedPKChildSkippedViaProbe(t *testing.T) {
	fks, parents := cascadeDeleteFixture("CASCADE")
	res, err := cascade.SynthesizeVictims(context.Background(), query.New(nil), fks, parents, cascade.Options{
		PKMetas: svChildPKMetas,
	})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.Victims) != 0 || len(res.SetNullRows) != 0 {
		t.Errorf("gated edge must synthesize nothing, got %d victims / %d set-null rows", len(res.Victims), len(res.SetNullRows))
	}
	assertGeneratedPKCaveat(t, res)
}

// TestSynthesizeVictims_generatedPKUpdateRootHoist covers the ON UPDATE
// root path's hoist and the caveat's boldest claim: the gate sits
// deliberately AFTER `cascadedHere = true`, so the parent's OWN key reversal
// is still emitted (KeyUpdateParents) while the versioned child edge is
// skipped — exactly what the caveat text warns about. Deleting that hoist
// makes checkKeyChain fetch on the nil DB (panic); moving the gate ABOVE
// `cascadedHere = true` silently drops the parent reversal and the
// KeyUpdateParents assertion catches it.
func TestSynthesizeVictims_generatedPKUpdateRootHoist(t *testing.T) {
	fks := []cascade.CascadeFK{{
		Schema: "app", Table: "child", ConstraintName: "fk_upd", Column: "pid",
		ReferencedSchema: "app", ReferencedTable: "parent", ReferencedColumn: "id",
		DeleteRule: "RESTRICT", UpdateRule: "CASCADE",
	}}
	parentUpd := query.ResultRow{
		SchemaName: "app", TableName: "parent", EventType: event.EventUpdate,
		PKValues:       "1",
		RowBefore:      map[string]any{"id": float64(1)},
		RowAfter:       map[string]any{"id": float64(2)},
		EventTimestamp: time.Now(),
	}
	res, err := cascade.SynthesizeVictims(context.Background(), query.New(nil), fks, []query.ResultRow{parentUpd}, cascade.Options{
		PKMetas: svChildPKMetas,
	})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.KeyUpdates) != 0 {
		t.Errorf("gated edge must synthesize no FK restores, got %d", len(res.KeyUpdates))
	}
	if len(res.KeyUpdateParents) != 1 {
		t.Fatalf("the parent's own key reversal must still be emitted for a gated edge (gate sits after cascadedHere), got %d KeyUpdateParents", len(res.KeyUpdateParents))
	}
	assertGeneratedPKCaveat(t, res)
}

// generatedPKProvider fakes a Phase-2 provider whose lookup refuses with the
// reconstruct.ErrGeneratedPK sentinel — what cascadebaseline returns for a
// versioned child since #1269/#1273 (the REAL provider's wrap is pinned by
// errors.Is in internal/cascadebaseline/provider_generatedpk_1266_test.go).
type generatedPKProvider struct{}

func (generatedPKProvider) BaselineChildren(ctx context.Context, schema, table, fkCol, parentPK string, at time.Time, limit int) (cascade.BaselineLookup, bool, error) {
	return cascade.BaselineLookup{}, false, fmt.Errorf("baseline scan of %s.%s: %w: primary-key column \"row_end\" is generated",
		schema, table, reconstruct.ErrGeneratedPK)
}

// TestSynthesizeVictims_generatedPKBackstopViaProviderError: WITHOUT a PKMetas
// probe, the Phase-2 provider's sentinel-wrapped refusal must land in the
// permanent generatedpk caveat — never the transient-sounding baselinefail
// bucket — and must skip the edge entirely: falling through to Phase-1 would
// scan the same history-polluted candidates (the nil-DB engine proves the
// skip; a Phase-1 fetch would panic). UpdateRule RESTRICT here on purpose:
// with no probe the hoists cannot fire, so an armed checkKeyChain would panic
// before the provider branch this test exists to reach.
func TestSynthesizeVictims_generatedPKBackstopViaProviderError(t *testing.T) {
	fks, parents := cascadeDeleteFixture("RESTRICT")
	res, err := cascade.SynthesizeVictims(context.Background(), query.New(nil), fks, parents, cascade.Options{
		Baseline: generatedPKProvider{},
	})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if res.Complete() {
		t.Fatal("a generated-PK provider refusal must not report Complete")
	}
	if len(res.Victims) != 0 {
		t.Errorf("gated edge must synthesize nothing, got %d victims", len(res.Victims))
	}
	var permanent, transient bool
	for _, msg := range res.Incomplete {
		if strings.Contains(msg, "PERMANENT") && strings.Contains(msg, "app.child") {
			permanent = true
		}
		if strings.Contains(msg, "baseline lookup failed") {
			transient = true
		}
	}
	if !permanent {
		t.Errorf("Incomplete must carry the permanent generated-PK caveat, got: %v", res.Incomplete)
	}
	if transient {
		t.Errorf("the sentinel must not land in the transient baselinefail bucket: %v", res.Incomplete)
	}
}

// TestPKMetasFromResolver pins the adapter's two documented degrade paths:
// a nil resolver yields a nil probe (gate off — the call sites' best-effort
// resolver loading), and a table the resolver cannot resolve degrades to nil
// metas (gate silent for that table) instead of erroring the run.
func TestPKMetasFromResolver(t *testing.T) {
	if cascade.PKMetasFromResolver(nil) != nil {
		t.Fatal("nil resolver must yield a nil probe")
	}
	r := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"app.child": {Schema: "app", Table: "child", Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
			{Name: "row_end", OrdinalPosition: 4, IsPK: true, DataType: "timestamp", IsGenerated: true},
		}},
	})
	probe := cascade.PKMetasFromResolver(r)
	metas := probe("app", "child")
	if len(metas) != 2 || metas[1].Name != "row_end" || !metas[1].IsGenerated {
		t.Fatalf("probe must return the PK metas in ordinal order, got: %+v", metas)
	}
	if got := probe("app", "missing"); got != nil {
		t.Fatalf("a resolve failure must degrade to nil metas, got: %+v", got)
	}
}
