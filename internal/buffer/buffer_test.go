package buffer

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/parser"
	"github.com/dbtrail/bintrail/internal/query"
)

func makeEvents(n int, schema, table string, base time.Time) []parser.Event {
	events := make([]parser.Event, n)
	for i := range n {
		events[i] = parser.Event{
			BinlogFile:    "binlog.000001",
			StartPos:      uint64(i * 100),
			EndPos:        uint64((i + 1) * 100),
			Timestamp:     base.Add(time.Duration(i) * time.Second),
			GTID:          "abc:1",
			Schema:        schema,
			Table:         table,
			EventType:     parser.EventInsert,
			PKValues:      string(rune('0' + i)),
			RowAfter:      map[string]any{"id": i, "name": "test"},
			SchemaVersion: 1,
		}
	}
	return events
}

func makeUpdate(schema, table, pk string, ts time.Time) parser.Event {
	return parser.Event{
		BinlogFile: "binlog.000001",
		StartPos:   100,
		EndPos:     200,
		Timestamp:  ts,
		Schema:     schema,
		Table:      table,
		EventType:  parser.EventUpdate,
		PKValues:   pk,
		RowBefore:  map[string]any{"id": 1, "email": "old@test.com"},
		RowAfter:   map[string]any{"id": 1, "email": "new@test.com"},
	}
}

func TestNew(t *testing.T) {
	buf := New(6*time.Hour, nil)
	if buf.Len() != 0 {
		t.Errorf("new buffer Len() = %d, want 0", buf.Len())
	}
}

func TestInsert_empty(t *testing.T) {
	buf := New(6*time.Hour, nil)
	buf.Insert(nil)
	if buf.Len() != 0 {
		t.Errorf("Len after nil insert = %d, want 0", buf.Len())
	}
}

func TestInsert_roundTrip(t *testing.T) {
	buf := New(6*time.Hour, nil)
	now := time.Now().UTC().Truncate(time.Second)
	events := makeEvents(3, "mydb", "users", now)

	buf.Insert(events)

	if buf.Len() != 3 {
		t.Fatalf("Len = %d, want 3", buf.Len())
	}

	rows := buf.Fetch(context.Background(), query.Options{})
	if len(rows) != 3 {
		t.Fatalf("Fetch all: got %d rows, want 3", len(rows))
	}

	// Verify first row fields.
	r := rows[0]
	if r.SchemaName != "mydb" {
		t.Errorf("SchemaName = %q, want mydb", r.SchemaName)
	}
	if r.TableName != "users" {
		t.Errorf("TableName = %q, want users", r.TableName)
	}
	if r.EventType != parser.EventInsert {
		t.Errorf("EventType = %d, want %d", r.EventType, parser.EventInsert)
	}
	if r.BinlogFile != "binlog.000001" {
		t.Errorf("BinlogFile = %q, want binlog.000001", r.BinlogFile)
	}
	if r.GTID == nil || *r.GTID != "abc:1" {
		t.Errorf("GTID = %v, want abc:1", r.GTID)
	}
	// makeEvents does not set ConnectionID, so it should be nil (0 → nil).
	if r.ConnectionID != nil {
		t.Errorf("ConnectionID = %v, want nil for zero-value", r.ConnectionID)
	}

	// EventIDs should be in the offset range.
	if r.EventID < idOffset {
		t.Errorf("EventID = %d, expected >= %d (idOffset)", r.EventID, idOffset)
	}
}

func TestInsert_emptyGTID(t *testing.T) {
	buf := New(6*time.Hour, nil)
	ev := parser.Event{
		BinlogFile: "binlog.000001",
		Timestamp:  time.Now().UTC(),
		Schema:     "mydb",
		Table:      "t",
		EventType:  parser.EventInsert,
		PKValues:   "1",
	}
	buf.Insert([]parser.Event{ev})

	rows := buf.Fetch(context.Background(), query.Options{})
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if rows[0].GTID != nil {
		t.Errorf("GTID = %v, want nil for empty GTID", rows[0].GTID)
	}
}

func TestInsert_connectionID(t *testing.T) {
	buf := New(6*time.Hour, nil)
	ev := parser.Event{
		BinlogFile:   "binlog.000001",
		Timestamp:    time.Now().UTC(),
		Schema:       "mydb",
		Table:        "t",
		EventType:    parser.EventInsert,
		PKValues:     "1",
		ConnectionID: 42,
	}
	buf.Insert([]parser.Event{ev})

	rows := buf.Fetch(context.Background(), query.Options{})
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if rows[0].ConnectionID == nil || *rows[0].ConnectionID != 42 {
		t.Errorf("ConnectionID = %v, want 42", rows[0].ConnectionID)
	}
}

func TestFetch_filterBySchema(t *testing.T) {
	buf := New(6*time.Hour, nil)
	now := time.Now().UTC()
	buf.Insert(makeEvents(2, "db1", "t1", now))
	buf.Insert(makeEvents(2, "db2", "t1", now))

	rows := buf.Fetch(context.Background(), query.Options{Schema: "db1"})
	if len(rows) != 2 {
		t.Errorf("got %d rows, want 2", len(rows))
	}
	for _, r := range rows {
		if r.SchemaName != "db1" {
			t.Errorf("SchemaName = %q, want db1", r.SchemaName)
		}
	}
}

func TestFetch_filterByTable(t *testing.T) {
	buf := New(6*time.Hour, nil)
	now := time.Now().UTC()
	buf.Insert(makeEvents(2, "db", "users", now))
	buf.Insert(makeEvents(2, "db", "orders", now))

	rows := buf.Fetch(context.Background(), query.Options{Table: "orders"})
	if len(rows) != 2 {
		t.Errorf("got %d rows, want 2", len(rows))
	}
}

func TestFetch_filterByPKValues(t *testing.T) {
	buf := New(6*time.Hour, nil)
	now := time.Now().UTC()
	buf.Insert(makeEvents(5, "db", "t", now))

	rows := buf.Fetch(context.Background(), query.Options{PKValues: "2"})
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if rows[0].PKValues != "2" {
		t.Errorf("PKValues = %q, want 2", rows[0].PKValues)
	}
}

func TestFetch_filterByEventType(t *testing.T) {
	buf := New(6*time.Hour, nil)
	now := time.Now().UTC()
	buf.Insert(makeEvents(2, "db", "t", now))
	buf.Insert([]parser.Event{makeUpdate("db", "t", "99", now)})

	et := parser.EventUpdate
	rows := buf.Fetch(context.Background(), query.Options{EventType: &et})
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if rows[0].EventType != parser.EventUpdate {
		t.Errorf("EventType = %d, want UPDATE", rows[0].EventType)
	}
}

func TestFetch_filterBySinceUntil(t *testing.T) {
	buf := New(6*time.Hour, nil)
	base := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	buf.Insert(makeEvents(5, "db", "t", base))

	since := base.Add(2 * time.Second)
	until := base.Add(3 * time.Second)
	rows := buf.Fetch(context.Background(), query.Options{Since: &since, Until: &until})
	if len(rows) != 2 {
		t.Errorf("got %d rows, want 2 (events at t+2s and t+3s)", len(rows))
	}
}

func TestFetch_filterByChangedColumn(t *testing.T) {
	buf := New(6*time.Hour, nil)
	now := time.Now().UTC()
	buf.Insert([]parser.Event{makeUpdate("db", "t", "1", now)})
	buf.Insert(makeEvents(1, "db", "t", now.Add(time.Second)))

	rows := buf.Fetch(context.Background(), query.Options{ChangedColumn: "email"})
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if rows[0].EventType != parser.EventUpdate {
		t.Errorf("expected UPDATE event for changed column filter")
	}
}

func TestFetch_filterByGTID(t *testing.T) {
	buf := New(6*time.Hour, nil)
	now := time.Now().UTC()

	ev1 := parser.Event{
		BinlogFile: "b.000001", Timestamp: now, Schema: "db", Table: "t",
		EventType: parser.EventInsert, PKValues: "1", GTID: "abc:1",
	}
	ev2 := parser.Event{
		BinlogFile: "b.000001", Timestamp: now, Schema: "db", Table: "t",
		EventType: parser.EventInsert, PKValues: "2", GTID: "abc:2",
	}
	buf.Insert([]parser.Event{ev1, ev2})

	rows := buf.Fetch(context.Background(), query.Options{GTID: "abc:2"})
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if rows[0].PKValues != "2" {
		t.Errorf("PKValues = %q, want 2", rows[0].PKValues)
	}
}

func TestFetch_limit(t *testing.T) {
	buf := New(6*time.Hour, nil)
	buf.Insert(makeEvents(10, "db", "t", time.Now().UTC()))

	rows := buf.Fetch(context.Background(), query.Options{Limit: 3})
	if len(rows) != 3 {
		t.Errorf("got %d rows, want 3", len(rows))
	}
}

func TestFetch_denyTables(t *testing.T) {
	buf := New(6*time.Hour, nil)
	now := time.Now().UTC()
	buf.Insert(makeEvents(2, "db", "public_t", now))
	buf.Insert(makeEvents(2, "db", "secret_t", now))

	rows := buf.Fetch(context.Background(), query.Options{
		DenyTables: []query.SchemaTable{{Schema: "db", Table: "secret_t"}},
	})
	if len(rows) != 2 {
		t.Errorf("got %d rows, want 2 (secret_t denied)", len(rows))
	}
	for _, r := range rows {
		if r.TableName == "secret_t" {
			t.Error("secret_t should be denied")
		}
	}
}

func TestResolvePK_found(t *testing.T) {
	buf := New(6*time.Hour, nil)
	buf.Insert(makeEvents(3, "db", "t", time.Now().UTC()))

	// The pk_values for the second event is "1" (rune '0'+1).
	hash := pkHash("1")
	val, ok := buf.ResolvePK(hash, "db", "t")
	if !ok {
		t.Fatal("expected found=true")
	}
	if val != "1" {
		t.Errorf("pk_values = %q, want 1", val)
	}
}

func TestResolvePK_notFound(t *testing.T) {
	buf := New(6*time.Hour, nil)
	buf.Insert(makeEvents(3, "db", "t", time.Now().UTC()))

	_, ok := buf.ResolvePK("nonexistent_hash", "db", "t")
	if ok {
		t.Error("expected found=false for nonexistent hash")
	}
}

func TestResolvePK_wrongSchemaTable(t *testing.T) {
	buf := New(6*time.Hour, nil)
	buf.Insert(makeEvents(1, "db", "t", time.Now().UTC()))

	hash := pkHash("0")
	_, ok := buf.ResolvePK(hash, "other_db", "t")
	if ok {
		t.Error("expected found=false for wrong schema")
	}
}

func TestEvict(t *testing.T) {
	buf := New(1*time.Hour, nil)
	old := time.Now().UTC().Add(-2 * time.Hour)
	recent := time.Now().UTC()

	buf.Insert(makeEvents(3, "db", "t", old))
	buf.Insert(makeEvents(2, "db", "t", recent))

	evicted := buf.Evict()
	if evicted != 3 {
		t.Errorf("evicted = %d, want 3", evicted)
	}
	if buf.Len() != 2 {
		t.Errorf("Len after evict = %d, want 2", buf.Len())
	}
}

func TestEvict_nothingToEvict(t *testing.T) {
	buf := New(6*time.Hour, nil)
	buf.Insert(makeEvents(3, "db", "t", time.Now().UTC()))

	evicted := buf.Evict()
	if evicted != 0 {
		t.Errorf("evicted = %d, want 0", evicted)
	}
	if buf.Len() != 3 {
		t.Errorf("Len = %d, want 3", buf.Len())
	}
}

func TestEvict_empty(t *testing.T) {
	buf := New(1*time.Hour, nil)
	if buf.Evict() != 0 {
		t.Error("evict on empty buffer should return 0")
	}
}

func TestSnapshot(t *testing.T) {
	buf := New(6*time.Hour, nil)
	buf.Insert(makeEvents(5, "db", "t", time.Now().UTC()))

	snap := buf.Snapshot()
	if len(snap) != 5 {
		t.Fatalf("snapshot len = %d, want 5", len(snap))
	}

	// Verify snapshot is independent — inserting more events doesn't change it.
	buf.Insert(makeEvents(3, "db", "t2", time.Now().UTC()))
	if len(snap) != 5 {
		t.Error("snapshot should be independent of subsequent inserts")
	}
}

func TestChangedColumns_computed(t *testing.T) {
	buf := New(6*time.Hour, nil)
	buf.Insert([]parser.Event{makeUpdate("db", "t", "1", time.Now().UTC())})

	rows := buf.Fetch(context.Background(), query.Options{})
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if len(rows[0].ChangedColumns) != 1 || rows[0].ChangedColumns[0] != "email" {
		t.Errorf("ChangedColumns = %v, want [email]", rows[0].ChangedColumns)
	}
}
