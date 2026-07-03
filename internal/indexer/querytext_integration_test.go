//go:build integration

package indexer

import (
	"database/sql"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestEnsureSchemaAddsQueryTextColumns mirrors the flavor/source_health
// migration tests: simulate a pre-#699 install by dropping the columns, then
// assert EnsureSchema restores them (nullable, right types) and is idempotent.
func TestEnsureSchemaAddsQueryTextColumns(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	testutil.MustExec(t, db, `ALTER TABLE binlog_events DROP COLUMN query_hash`)
	testutil.MustExec(t, db, `ALTER TABLE binlog_events DROP COLUMN query_text`)

	if err := EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	for col, wantType := range map[string]string{
		"query_text": "mediumtext",
		"query_hash": "char",
	} {
		var dataType, isNullable string
		err := db.QueryRow(`SELECT DATA_TYPE, IS_NULLABLE FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'binlog_events' AND COLUMN_NAME = ?`,
			col).Scan(&dataType, &isNullable)
		if err != nil {
			t.Fatalf("column %s not found after EnsureSchema: %v", col, err)
		}
		if dataType != wantType {
			t.Errorf("%s DATA_TYPE = %q, want %q", col, dataType, wantType)
		}
		if isNullable != "YES" {
			t.Errorf("%s IS_NULLABLE = %q, want YES (pre-#699 rows must read back NULL)", col, isNullable)
		}
	}

	// Second run must be a no-op.
	if err := EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema (second run): %v", err)
	}
}

// TestInsertBatch_queryTextAndDigestRoundTrip drives the #699 write path
// against a real index MySQL: query_text round-trips, query_hash is a real
// STATEMENT_DIGEST (64-hex, computed on the index connection), same-shape
// statements with different literals collapse to the SAME hash, and events
// without a captured statement store NULL in both columns.
func TestInsertBatch_queryTextAndDigestRoundTrip(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	idx := New(db, 1000)
	ts := time.Date(2026, 2, 19, 12, 0, 0, 0, time.UTC)
	mkEvent := func(pk, queryText string) parser.Event {
		return parser.Event{
			BinlogFile: "binlog.000001", StartPos: 100, EndPos: 200,
			Timestamp: ts, Schema: "mydb", Table: "orders",
			EventType: parser.EventInsert, PKValues: pk,
			RowAfter:  map[string]any{"id": pk},
			QueryText: queryText,
		}
	}

	batch := []parser.Event{
		mkEvent("1", "INSERT INTO orders (id) VALUES (1)"),
		mkEvent("2", "INSERT INTO orders (id) VALUES (2)"), // same shape, different literal
		mkEvent("3", ""), // statement not captured
	}
	if _, err := idx.InsertBatch(batch); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	rows, err := db.Query(`SELECT pk_values, query_text, query_hash
		FROM binlog_events ORDER BY event_id`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	defer rows.Close()

	type got struct {
		text, hash sql.NullString
	}
	byPK := map[string]got{}
	for rows.Next() {
		var pk string
		var g got
		if err := rows.Scan(&pk, &g.text, &g.hash); err != nil {
			t.Fatalf("scan: %v", err)
		}
		byPK[pk] = g
	}
	if len(byPK) != 3 {
		t.Fatalf("rows = %d, want 3", len(byPK))
	}

	hexRe := regexp.MustCompile(`^[0-9a-f]{64}$`)
	for _, pk := range []string{"1", "2"} {
		g := byPK[pk]
		if !g.text.Valid {
			t.Fatalf("pk %s: query_text is NULL, want the captured statement", pk)
		}
		if !g.hash.Valid || !hexRe.MatchString(g.hash.String) {
			t.Errorf("pk %s: query_hash = %+v, want a 64-hex STATEMENT_DIGEST", pk, g.hash)
		}
	}
	if byPK["1"].text.String == byPK["2"].text.String {
		t.Error("distinct statements must store distinct query_text")
	}
	if byPK["1"].hash.String != byPK["2"].hash.String {
		t.Errorf("same-shape statements must share one digest: %q vs %q",
			byPK["1"].hash.String, byPK["2"].hash.String)
	}
	if byPK["3"].text.Valid || byPK["3"].hash.Valid {
		t.Errorf("uncaptured statement must store NULL/NULL, got text=%+v hash=%+v",
			byPK["3"].text, byPK["3"].hash)
	}
}

// TestInsertBatch_digestPoisonAndTruncationFallback pins the degradation
// contract the #699 review forced: (a) a truncated statement (ends mid-token)
// is never digested — NULL hash, text still stored; (b) an unparseable text
// fails the combined digest SELECT as a unit (MySQL error 3676), and the
// per-text fallback must still hash the batch's parseable statements instead
// of nulling the whole batch.
func TestInsertBatch_digestPoisonAndTruncationFallback(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	idx := New(db, 1000)
	ts := time.Date(2026, 2, 19, 12, 0, 0, 0, time.UTC)
	mkEvent := func(pk, queryText string) parser.Event {
		return parser.Event{
			BinlogFile: "binlog.000001", StartPos: 100, EndPos: 200,
			Timestamp: ts, Schema: "mydb", Table: "orders",
			EventType: parser.EventInsert, PKValues: pk,
			RowAfter:  map[string]any{"id": pk},
			QueryText: queryText,
		}
	}

	// A statement over the cap: SanitizeQueryText truncates it mid-token and
	// appends the marker — it must be stored truncated and NOT digested.
	huge := "INSERT INTO orders (blob_col) VALUES ('" + strings.Repeat("x", event.MaxQueryTextBytes) + "')"
	batch := []parser.Event{
		mkEvent("1", "INSERT INTO orders (id) VALUES (1)"), // parseable
		mkEvent("2", "bad ((( not sql"),                    // poisons the combined SELECT (3676)
		mkEvent("3", huge),                                 // truncated → skipped from digesting
	}
	if _, err := idx.InsertBatch(batch); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	rows, err := db.Query(`SELECT pk_values, query_text, query_hash FROM binlog_events ORDER BY event_id`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	defer rows.Close()
	type got struct{ text, hash sql.NullString }
	byPK := map[string]got{}
	for rows.Next() {
		var pk string
		var g got
		if err := rows.Scan(&pk, &g.text, &g.hash); err != nil {
			t.Fatalf("scan: %v", err)
		}
		byPK[pk] = g
	}

	// (b) the parseable statement keeps its hash despite the poisoned batch.
	if !byPK["1"].hash.Valid {
		t.Error("parseable statement lost its digest to a poisoned batch — per-text fallback broken")
	}
	// The unparseable text stores NULL hash but keeps its text.
	if byPK["2"].hash.Valid {
		t.Errorf("unparseable statement must have NULL query_hash, got %q", byPK["2"].hash.String)
	}
	if !byPK["2"].text.Valid {
		t.Error("unparseable statement must still store its query_text")
	}
	// (a) truncated: text stored with the marker, hash NULL.
	if !byPK["3"].text.Valid || !strings.HasSuffix(byPK["3"].text.String, event.QueryTextTruncationMarker) {
		t.Errorf("truncated statement must be stored ending in the truncation marker")
	}
	if byPK["3"].hash.Valid {
		t.Errorf("truncated statement must never be digested (mid-token text), got %q", byPK["3"].hash.String)
	}
}
