package buffer

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

// TestFetch_queryHashFilterExcludesTheWholeBuffer pins the BYOS agent's half of
// the statement-digest filter.
//
// A buffered event never carries a digest: STATEMENT_DIGEST is computed on the
// index connection when a batch is inserted, and these events have not been
// inserted yet. So the correct answer is "no rows" — and the reason this needs
// a test is the failure mode of leaving the filter out. It is not an empty
// result; it is buffered rows flowing UNFILTERED into a digest-scoped answer,
// where MergeResults folds them in beside genuinely matching MySQL rows and the
// operator reads them as that statement's work.
func TestFetch_queryHashFilterExcludesTheWholeBuffer(t *testing.T) {
	const digest = "3f2a1b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708"

	buf := New(Config{MaxAge: time.Hour})
	buf.Insert([]parser.Event{{
		BinlogFile: "binlog.000001", StartPos: 100, EndPos: 200,
		Timestamp: time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC),
		Schema:    "mydb", Table: "orders",
		EventType: parser.EventInsert, PKValues: "1",
		RowAfter:  map[string]any{"id": 1},
		QueryText: "INSERT INTO mydb.orders (id) VALUES (1)",
	}})

	// Control: every other filter matches, so nothing but the digest can be
	// responsible for the difference below.
	if got := buf.Fetch(context.Background(), query.Options{Schema: "mydb", Table: "orders"}); len(got) != 1 {
		t.Fatalf("unfiltered rows = %d, want 1", len(got))
	}

	got := buf.Fetch(context.Background(), query.Options{Schema: "mydb", Table: "orders", QueryHash: digest})
	if len(got) != 0 {
		t.Fatalf("rows = %d, want 0 — a buffered event carries no digest and must never satisfy a digest filter", len(got))
	}
}
