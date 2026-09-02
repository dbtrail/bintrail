package console

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/views"
)

// A canceled caller must never be memoized as a fact about the snapshot
// (#1583 put a cancelable caller, the backup download, in front of this
// resolver). Before the guard, one browser hitting Stop mid-download wrote
// either a negative entry (five minutes of uncast decimals for every later
// file of that snapshot) or a PARTIAL positive one, which never expires.
func TestResolveBaselineDecimals_neverMemoizesACanceledRead(t *testing.T) {
	s := &Server{}
	in := views.Input{
		BaselineSource:   "/nowhere/baselines",
		BaselineSnapshot: time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC),
		Baselines: []views.BaselineTable{
			{Schema: "shop", Table: "orders", Path: "/nowhere/baselines/x/shop/orders.parquet", Rel: "shop/orders.parquet"},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the caller is already gone; whatever the read returns is not evidence
	s.resolveBaselineDecimals(ctx, &in)

	s.baselineDecimalMu.Lock()
	n := len(s.baselineDecimals)
	s.baselineDecimalMu.Unlock()
	if n != 0 {
		t.Fatalf("a canceled read was memoized (%d entries); the cache never forgets a "+
			"\"successful\" answer, so this poisons every later views file of the snapshot", n)
	}
}
