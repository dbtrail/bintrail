package reconstruct

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
)

// TestTrimPartialTailTransaction_emptyEvents confirms the empty-input fast
// path never touches the DB (nil db/engine would panic if it did).
func TestTrimPartialTailTransaction_emptyEvents(t *testing.T) {
	got, err := TrimPartialTailTransaction(context.Background(), nil, nil, query.FetchMergedOptions{}, nil, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("want 0 events, got %d", len(got))
	}
}

// TestTrimPartialTailTransaction_noGTIDSkipsProbe confirms an event with a
// nil or empty GTID passes straight through without a lookahead DB query
// (nil db/engine would panic if the probe ran) — the documented degradation
// for sources replicating without GTIDs (#783).
func TestTrimPartialTailTransaction_noGTIDSkipsProbe(t *testing.T) {
	events := []query.ResultRow{{EventTimestamp: time.Now(), GTID: nil}}
	got, err := TrimPartialTailTransaction(context.Background(), nil, nil, query.FetchMergedOptions{}, events, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("want the no-GTID event passed through unchanged, got %d events", len(got))
	}

	empty := ""
	events2 := []query.ResultRow{{EventTimestamp: time.Now(), GTID: &empty}}
	got2, err := TrimPartialTailTransaction(context.Background(), nil, nil, query.FetchMergedOptions{}, events2, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got2) != 1 {
		t.Errorf("want the empty-GTID event passed through unchanged, got %d events", len(got2))
	}
}
