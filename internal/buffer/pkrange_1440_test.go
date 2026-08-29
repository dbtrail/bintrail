package buffer

import (
	"context"
	"math/big"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

// TestFetch_pkRange (#1440): the in-memory buffer is the third engine a
// query.Options can reach (agent handler: buffer, then MySQL, then archives),
// and it has no SQL to cast for it. It must apply the same numeric window as
// the two SQL predicates, never a lexicographic one, and never ignore it.
func TestFetch_pkRange(t *testing.T) {
	buf := New(Config{MaxAge: 6 * time.Hour})
	now := time.Now().UTC()
	var events []parser.Event
	// "007" and "+5" parse as numbers but are not the canonical spelling
	// the index stores; the buffer must exclude them like both engines do.
	for i, pk := range []string{"-5", "9", "10", "100", "9223372036854775800", "", "007", "+5"} {
		events = append(events, parser.Event{
			BinlogFile: "binlog.000001", StartPos: uint64(i * 100), EndPos: uint64(i*100 + 50),
			Timestamp: now.Add(time.Duration(i) * time.Second),
			Schema:    "db", Table: "t", EventType: parser.EventInsert, PKValues: pk,
			RowAfter: map[string]any{"id": pk},
		})
	}
	buf.Insert(events)

	keys := func(rows []query.ResultRow) string {
		var out []string
		for _, r := range rows {
			out = append(out, r.PKValues)
		}
		sort.Strings(out)
		return strings.Join(out, ",")
	}

	rows := buf.Fetch(context.Background(), query.Options{Schema: "db", Table: "t",
		PKRange: &query.PKRange{Cast: query.PKCastSigned, Min: big.NewInt(10)}})
	if got, want := keys(rows), "10,100,9223372036854775800"; got != want {
		t.Errorf("min=10: got %s, want %s (9 must be OUT, 100 IN: numeric, not string order)", got, want)
	}
	rows = buf.Fetch(context.Background(), query.Options{Schema: "db", Table: "t",
		PKRange: &query.PKRange{Cast: query.PKCastSigned, Min: big.NewInt(-5), Max: big.NewInt(9)}})
	if got, want := keys(rows), "-5,9"; got != want {
		t.Errorf("[-5,9]: got %s, want %s (007 and +5 are not canonical spellings and stay OUT)", got, want)
	}
	// Unresolved cast: the buffer cannot error, so it must match nothing.
	rows = buf.Fetch(context.Background(), query.Options{Schema: "db", Table: "t",
		PKRange: &query.PKRange{Min: big.NewInt(0)}})
	if len(rows) != 0 {
		t.Errorf("an unresolved range returned %d rows; it must filter everything, not nothing", len(rows))
	}
}
