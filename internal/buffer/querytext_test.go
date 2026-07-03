package buffer

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

// TestFetch_queryTextPropagationAndProfileBlanking pins two #699 contracts on
// the live-buffer path: (a) Insert carries the captured statement into Fetch
// results; (b) under any active profile the statement fields are withheld
// from the RETURNED rows without mutating the stored entries.
func TestFetch_queryTextPropagationAndProfileBlanking(t *testing.T) {
	buf := New(Config{MaxAge: time.Hour})
	base := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	buf.Insert([]parser.Event{{
		BinlogFile: "binlog.000001", StartPos: 100, EndPos: 200,
		Timestamp: base, Schema: "mydb", Table: "users",
		EventType: parser.EventInsert, PKValues: "1",
		RowAfter:  map[string]any{"id": 1},
		QueryText: "INSERT INTO mydb.users (id) VALUES (1)",
	}})

	plain := buf.Fetch(context.Background(), query.Options{Schema: "mydb", Table: "users"})
	if len(plain) != 1 {
		t.Fatalf("rows = %d, want 1", len(plain))
	}
	if plain[0].QueryText == nil || *plain[0].QueryText != "INSERT INTO mydb.users (id) VALUES (1)" {
		t.Fatalf("Insert must propagate QueryText into Fetch results, got %v", plain[0].QueryText)
	}

	blanked := buf.Fetch(context.Background(), query.Options{Schema: "mydb", Table: "users", ProfileActive: true})
	if len(blanked) != 1 {
		t.Fatalf("rows = %d, want 1", len(blanked))
	}
	if blanked[0].QueryText != nil || blanked[0].QueryHash != nil {
		t.Errorf("statement fields must be withheld under an active profile, got %v / %v",
			blanked[0].QueryText, blanked[0].QueryHash)
	}

	// The stored entry must be untouched: a later unrestricted Fetch still
	// sees the text.
	again := buf.Fetch(context.Background(), query.Options{Schema: "mydb", Table: "users"})
	if again[0].QueryText == nil {
		t.Error("profile blanking must not mutate the stored buffer entry")
	}
}
