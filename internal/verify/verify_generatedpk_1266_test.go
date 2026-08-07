package verify

import (
	"context"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// svResolver stubs the MariaDB system-versioning PK shape (#1266): the PK
// silently extended with the STORED GENERATED ROW END period column.
func svResolver() *metadata.Resolver {
	return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"app.orders": {Schema: "app", Table: "orders", Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
			{Name: "row_end", OrdinalPosition: 4, IsPK: true, DataType: "timestamp", ColumnType: "timestamp(6)", IsGenerated: true},
		}},
	})
}

// TestVerifyTable_generatedPKInconclusive: a versioned table is a PERMANENT
// shape, so live-source verify must report it inconclusive — never a mismatch
// or an error that fails every run forever (#1266). The gate fires before any
// DB access, so SourceDB/IndexDB stay nil (deleting the gate makes this test
// die on the nil source checksum instead).
func TestVerifyTable_generatedPKInconclusive(t *testing.T) {
	res, err := VerifyTable(context.Background(), Config{Resolver: svResolver()}, "app", "orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != StatusInconclusive {
		t.Fatalf("status = %q, want inconclusive", res.Status)
	}
	if !strings.Contains(res.Detail, "generated column") || !strings.Contains(res.Detail, `"row_end"`) {
		t.Fatalf("want the generated-PK reason naming row_end, got: %q", res.Detail)
	}
}

// TestVerifyBaselinePair_generatedPKInconclusive: same stance on the
// baseline-anchored path. The detail must be the generated-PK reason, not the
// anchor precondition that would fire next if the gate were deleted (the
// BaselinePair carries no usable anchor on purpose, so the two outcomes are
// distinguishable).
func TestVerifyBaselinePair_generatedPKInconclusive(t *testing.T) {
	cfg := BaselineConfig{Resolver: svResolver(), SourceFlavor: ""} // "" == mysql
	res, err := VerifyBaselinePair(context.Background(), cfg, BaselinePair{Schema: "app", Table: "orders"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != StatusInconclusive {
		t.Fatalf("status = %q, want inconclusive", res.Status)
	}
	if !strings.Contains(res.Detail, "generated column") {
		t.Fatalf("want the generated-PK reason, got: %q", res.Detail)
	}
}

// TestVerifyBaselinePair_pgBypassesGeneratedPKGate pins the gate's placement
// inside the !pg branch: the PK canonicalizer it protects is bypassed entirely
// on the PostgreSQL path, so a PG run must fall through to the LSN-anchor
// precondition, never the MariaDB-shaped generated-PK message.
func TestVerifyBaselinePair_pgBypassesGeneratedPKGate(t *testing.T) {
	cfg := BaselineConfig{Resolver: svResolver(), SourceFlavor: flavorPostgres}
	res, err := VerifyBaselinePair(context.Background(), cfg, BaselinePair{Schema: "app", Table: "orders", NewLSN: 0})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(res.Detail, "generated column") {
		t.Fatalf("PG path must bypass the generated-PK gate, got: %q", res.Detail)
	}
	if !strings.Contains(res.Detail, "LSN anchor") {
		t.Fatalf("want the PG LSN-anchor reason, got: %q", res.Detail)
	}
}

// pgShapedGeneratedResolver combines the two gate triggers on one PK column:
// empty DataType (the PG snapshot shape, #1009) AND IsGenerated. The type gate
// must win — see the ordering tests below, which pin the loop-then-gate order
// the in-code comments claim.
func pgShapedGeneratedResolver() *metadata.Resolver {
	return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"app.orders": {Schema: "app", Table: "orders", Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "", IsGenerated: true},
		}},
	})
}

func TestVerifyTable_emptyDataTypeKeepsWrongPathVerdict(t *testing.T) {
	res, err := VerifyTable(context.Background(), Config{Resolver: pgShapedGeneratedResolver()}, "app", "orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != StatusInconclusive {
		t.Fatalf("status = %q, want inconclusive", res.Status)
	}
	if strings.Contains(res.Detail, "generated column") {
		t.Fatalf("empty DataType must win the #1009 wrong-path verdict, got the generated-PK reason: %q", res.Detail)
	}
	if !strings.Contains(res.Detail, "PostgreSQL") {
		t.Fatalf("want the PG wrong-path reason, got: %q", res.Detail)
	}
}

func TestVerifyBaselinePair_emptyDataTypeKeepsWrongPathVerdict(t *testing.T) {
	cfg := BaselineConfig{Resolver: pgShapedGeneratedResolver(), SourceFlavor: ""} // "" == mysql
	res, err := VerifyBaselinePair(context.Background(), cfg, BaselinePair{Schema: "app", Table: "orders"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != StatusInconclusive {
		t.Fatalf("status = %q, want inconclusive", res.Status)
	}
	if strings.Contains(res.Detail, "generated column") {
		t.Fatalf("empty DataType must win the #1009 wrong-path verdict, got the generated-PK reason: %q", res.Detail)
	}
	if !strings.Contains(res.Detail, "PostgreSQL") {
		t.Fatalf("want the PG wrong-path reason, got: %q", res.Detail)
	}
}
