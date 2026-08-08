package reconstruct

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

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

// TestTrimPartialTailTransaction_probeErrorIsSurfaceNeutral pins #1286: the
// MCP reconstruct tool hands the boundary-probe error verbatim to ErrorResult,
// and the tool parameter there is `at` — so the library string must not name
// the `--at` CLI flag; each surface owns its own wording (the
// query.GapError / SourceEmptyError precedent). A dead index DB (nothing
// listens on port 1) forces the probe's error path without needing MySQL.
func TestTrimPartialTailTransaction_probeErrorIsSurfaceNeutral(t *testing.T) {
	db, err := sql.Open("mysql", "u:p@tcp(127.0.0.1:1)/x?timeout=200ms")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	gtid := "3e11fa47-71ca-11e1-9e33-c80aa9429562:23"
	events := []query.ResultRow{{EventTimestamp: time.Now(), GTID: &gtid}}
	_, err = TrimPartialTailTransaction(context.Background(), db, query.New(db), query.FetchMergedOptions{}, events, time.Now())
	if err == nil {
		t.Fatal("a dead index DB must fail the probe")
	}
	if strings.Contains(err.Error(), "--at") {
		t.Errorf("boundary-probe error must stay surface-neutral (no --at CLI flag), got %q", err)
	}
	// Positive half: the negative alone would also pass if the error came
	// from a layer before the wrapper — pin that the probe's wrapper ran.
	if !strings.Contains(err.Error(), "continues past") {
		t.Errorf("expected the boundary-probe wrapper in the error, got %q", err)
	}
}
