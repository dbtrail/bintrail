package verify

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
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

// TestBaselineFetchOptions_pgNeverBoundsByPosition guards the load-bearing B4
// invariant that VerifyBaselinePair and ExplainBaselinePairMismatch share: a PG
// delta window is time-bounded ONLY (its binlog_file is a non-monotonic "X/Y"
// LSN the position filter can't order), while MySQL pins the exact binlog cut.
// A refactor that set a position bound for PG — the exact regression the review
// flagged — fails here without a live DB.
func TestBaselineFetchOptions_pgNeverBoundsByPosition(t *testing.T) {
	p := BaselinePair{
		Schema: "app", Table: "orders",
		PrevSnapshot: time.Unix(100, 0).UTC(), NewSnapshot: time.Unix(200, 0).UTC(),
		NewAnchor:  query.BinlogPos{File: "mysql-bin.000007", Pos: 4242},
		PrevAnchor: query.BinlogPos{File: "mysql-bin.000006", Pos: 100},
	}

	pgOpts := baselineFetchOptions(p, true)
	if pgOpts.UntilPos != nil || pgOpts.SincePos != nil {
		t.Errorf("PG window must set NO position bound: UntilPos=%v SincePos=%v", pgOpts.UntilPos, pgOpts.SincePos)
	}
	if pgOpts.Since == nil || pgOpts.Until == nil || pgOpts.LimitPerPK != 1 {
		t.Errorf("PG window must keep the time bounds + LimitPerPK: %+v", pgOpts)
	}

	myOpts := baselineFetchOptions(p, false)
	if myOpts.UntilPos == nil || *myOpts.UntilPos != p.NewAnchor {
		t.Errorf("MySQL window must cut at the new anchor, got %v", myOpts.UntilPos)
	}
	if myOpts.SincePos == nil || *myOpts.SincePos != p.PrevAnchor {
		t.Errorf("MySQL window must lower-bound at the prev anchor, got %v", myOpts.SincePos)
	}

	// A MySQL baseline with no recorded prev anchor (#797) falls back to the time
	// lower bound — SincePos stays nil.
	p.PrevAnchor = query.BinlogPos{}
	if got := baselineFetchOptions(p, false); got.SincePos != nil {
		t.Errorf("a zero prev anchor must not set SincePos, got %v", got.SincePos)
	}
}
