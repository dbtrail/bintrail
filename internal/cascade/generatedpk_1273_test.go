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
// excluded_child_test.go style: both gates must fire BEFORE any index or
// baseline scan, so a nil-DB engine proves the skip happens up front (any
// fetch would panic).

func cascadeDeleteFixture() ([]cascade.CascadeFK, []query.ResultRow) {
	fks := []cascade.CascadeFK{{
		Schema: "app", Table: "child", ConstraintName: "fk_del", Column: "pid",
		ReferencedSchema: "app", ReferencedTable: "parent", ReferencedColumn: "id",
		DeleteRule: "CASCADE", UpdateRule: "RESTRICT",
	}}
	parentDel := query.ResultRow{
		SchemaName: "app", TableName: "parent", EventType: event.EventDelete,
		PKValues: "1", RowBefore: map[string]any{"id": float64(1)},
		EventTimestamp: time.Now(),
	}
	return fks, []query.ResultRow{parentDel}
}

// TestSynthesizeVictims_generatedPKChildSkippedViaProbe: with the PKMetas
// probe wired (as the CLI/console/MCP call sites do), a child whose PK
// contains a generated column — the MariaDB system-versioning shape — is
// skipped in BOTH phases with the permanent generatedpk caveat, before any
// candidate scan could rebuild children from history-polluted state.
func TestSynthesizeVictims_generatedPKChildSkippedViaProbe(t *testing.T) {
	fks, parents := cascadeDeleteFixture()
	res, err := cascade.SynthesizeVictims(context.Background(), query.New(nil), fks, parents, cascade.Options{
		PKMetas: func(schema, table string) []metadata.ColumnMeta {
			if schema == "app" && table == "child" {
				return []metadata.ColumnMeta{
					{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
					{Name: "row_end", OrdinalPosition: 4, IsPK: true, DataType: "timestamp", IsGenerated: true},
				}
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if res.Complete() {
		t.Fatal("a generated-PK child edge must not report Complete")
	}
	if len(res.Victims) != 0 || len(res.SetNullRows) != 0 {
		t.Errorf("gated edge must synthesize nothing, got %d victims / %d set-null rows", len(res.Victims), len(res.SetNullRows))
	}
	var flagged bool
	for _, msg := range res.Incomplete {
		if strings.Contains(msg, "app.child") && strings.Contains(msg, "generated column") &&
			strings.Contains(msg, "PERMANENT") {
			flagged = true
		}
	}
	if !flagged {
		t.Errorf("Incomplete must carry the permanent generated-PK caveat, got: %v", res.Incomplete)
	}
}

// generatedPKProvider fakes a Phase-2 provider whose lookup refuses with the
// reconstruct.ErrGeneratedPK sentinel — what cascadebaseline returns for a
// versioned child since #1269/#1273.
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
// skip; a Phase-1 fetch would panic).
func TestSynthesizeVictims_generatedPKBackstopViaProviderError(t *testing.T) {
	fks, parents := cascadeDeleteFixture()
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
