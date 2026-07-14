package verify

import (
	"context"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// pgResolver stubs a one-table schema whose PK column has an EMPTY DataType —
// the PostgreSQL shape (no MySQL DATA_TYPE token). SupportedPKType("") is false,
// so on the MySQL path this table is inconclusive; on the PG path the type gate
// is bypassed entirely.
func pgResolver() *metadata.Resolver {
	return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"app.orders": {Schema: "app", Table: "orders", Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: ""},
		}},
	})
}

// TestVerifyBaselinePair_pgBypassesPKTypeGate proves B1: with SourceFlavor
// "postgres" the SupportedPKType gate is skipped, so an empty-DATA_TYPE PK does
// NOT go inconclusive as "unsupported by the canonicalizer" — it proceeds to the
// LSN-anchor precondition (B3) and reports the LSN reason instead. No index DB is
// touched (the anchor check returns before any fetch), so IndexDB stays nil.
func TestVerifyBaselinePair_pgBypassesPKTypeGate(t *testing.T) {
	cfg := BaselineConfig{Resolver: pgResolver(), SourceFlavor: flavorPostgres}
	// NewLSN == 0 → the PG anchor precondition fires (B3), which is only reachable
	// if the PK-type gate (B1) was skipped first.
	res, err := VerifyBaselinePair(context.Background(), cfg, BaselinePair{Schema: "app", Table: "orders", NewLSN: 0})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != StatusInconclusive {
		t.Fatalf("status = %q, want inconclusive", res.Status)
	}
	if strings.Contains(res.Detail, "unsupported by the baseline canonicalizer") {
		t.Errorf("PG must bypass the PK-type gate, but got the MySQL type message: %q", res.Detail)
	}
	if !strings.Contains(res.Detail, "LSN anchor") {
		t.Errorf("want the PG LSN-anchor reason, got: %q", res.Detail)
	}
	if !strings.HasPrefix(res.Anchor, "LSN:") {
		t.Errorf("PG anchor label = %q, want an LSN: form", res.Anchor)
	}
}

// TestVerifyBaselinePair_mysqlKeepsPKTypeGate is the contrast: the SAME
// empty-DATA_TYPE PK on the MySQL path still trips the SupportedPKType gate, so
// the flag — not some unrelated change — is what gates B1.
func TestVerifyBaselinePair_mysqlKeepsPKTypeGate(t *testing.T) {
	cfg := BaselineConfig{Resolver: pgResolver(), SourceFlavor: ""} // "" == mysql
	res, err := VerifyBaselinePair(context.Background(), cfg, BaselinePair{Schema: "app", Table: "orders"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != StatusInconclusive {
		t.Fatalf("status = %q, want inconclusive", res.Status)
	}
	if !strings.Contains(res.Detail, "unsupported by the baseline canonicalizer") {
		t.Errorf("MySQL must keep the PK-type gate, got: %q", res.Detail)
	}
}
