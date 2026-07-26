//go:build integration

package indexer

import (
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestInsertBatch_binaryPrimaryKey pins #1132: a table whose PRIMARY KEY is
// BINARY/VARBINARY used to take the WHOLE stream daemon down. Its raw key bytes
// were written verbatim into binlog_events.pk_values — VARCHAR(512) CHARACTER
// SET utf8mb4 — so MySQL rejected the multi-row INSERT with error 1366
// ("Incorrect string value"), and because a batch flush failure is fail-loud by
// contract (internal/streamrun, #652) the process exited and capture stopped
// for every table in the source, not just the offending one.
//
// The assertion has to run against a REAL utf8mb4 index: a unit test on
// event.BuildPKValues can prove the hex spelling but never touches a charset,
// so it cannot prove error 1366 is gone. This drives the production chain end
// to end — go-mysql's raw bytes → metadata.MapRow (which reinterprets a
// BINARY/VARBINARY column as []byte, #756) → event.BuildPKValues →
// Indexer.InsertBatch → MySQL — and then reads the row back.
func TestInsertBatch_binaryPrimaryKey(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// The bytes from the issue's own error message. 0xB2 is a UTF-8
	// continuation byte and can never lead a sequence, so this is exactly the
	// unstorable shape a BINARY(16) MD5/UUID key produces in practice.
	keyBytes := []byte{
		0xB2, 0x81, 0x5C, 0xC3, 0xC2, 0x00, 0xFF, 0x7C,
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x80,
	}
	const wantPK = "0xB2815CC3C200FF7C0102030405060780"

	// A resolver over the repro schema: BINARY(16) PK + a VARCHAR payload.
	// Building the event through MapRow rather than hand-writing PKValues is
	// the point — it is MapRow's #756 string→[]byte reinterpretation that
	// feeds formatPKValue, and hand-writing the encoded string would test the
	// literal instead of the code path that produces it.
	tm := &metadata.TableMeta{
		Schema: "mydb", Table: "t_pk_binary",
		Columns: []metadata.ColumnMeta{
			{Name: "k", DataType: "binary", ColumnType: "binary(16)", IsPK: true},
			{Name: "val", DataType: "varchar", ColumnType: "varchar(255)", CharacterSet: "utf8mb4"},
		},
		PKColumns: []string{"k"},
	}
	res := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{"mydb.t_pk_binary": tm})

	// go-mysql delivers a BINARY column as a Go string of raw source bytes.
	row, err := res.MapRow("mydb", "t_pk_binary", []any{string(keyBytes), "bin pk 1"})
	if err != nil {
		t.Fatalf("MapRow: %v", err)
	}
	if _, ok := row["k"].([]byte); !ok {
		t.Fatalf("MapRow returned %T for the BINARY PK column, want []byte "+
			"(the #756 coercion is what routes this value into formatPKValue)", row["k"])
	}

	pkValues := event.BuildPKValues(tm.PKColumnMetas(), row)
	if pkValues != wantPK {
		t.Fatalf("BuildPKValues = %q, want %q", pkValues, wantPK)
	}

	idx := New(db, 1000)
	ts := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	n, err := idx.InsertBatch([]event.Event{{
		BinlogFile: "binlog.000001", StartPos: 100, EndPos: 200,
		Timestamp: ts, Schema: "mydb", Table: "t_pk_binary",
		EventType: event.EventDelete, PKValues: pkValues,
		RowBefore: row,
	}})
	// Before the fix this is where the daemon died: Error 1366 on pk_values.
	if err != nil {
		t.Fatalf("InsertBatch with a BINARY(16) PK failed (#1132 regression): %v", err)
	}
	if n != 1 {
		t.Fatalf("InsertBatch inserted %d rows, want 1", n)
	}

	// Read back through the exact predicate every read path uses: pk_hash for
	// the index scan AND pk_values for the collision guard (CLAUDE.md). If the
	// stored spelling and the rebuilt one ever diverged, this returns zero rows
	// — the silent-miss failure mode, not a loud one.
	var got string
	err = db.QueryRow(
		`SELECT pk_values FROM binlog_events WHERE pk_hash = SHA2(?, 256) AND pk_values = ?`,
		pkValues, pkValues).Scan(&got)
	if err != nil {
		t.Fatalf("indexed row is not findable by its own pk_values: %v", err)
	}
	if got != wantPK {
		t.Errorf("stored pk_values = %q, want %q", got, wantPK)
	}

	// The stored form must be reproducible from the source table, which is the
	// whole reason for the "0x" + uppercase-HEX spelling: an operator can run
	// SELECT CONCAT('0x', HEX(k)) on the source and paste the result into --pk.
	var mysqlSpelling string
	if err := db.QueryRow(`SELECT CONCAT('0x', HEX(?))`, keyBytes).Scan(&mysqlSpelling); err != nil {
		t.Fatalf("HEX() probe: %v", err)
	}
	if mysqlSpelling != wantPK {
		t.Errorf("stored pk_values %q does not match MySQL's own CONCAT('0x', HEX(k)) = %q",
			wantPK, mysqlSpelling)
	}
}

// TestInsertBatch_binaryPrimaryKeyValidUTF8Unchanged is the other half of
// #1132's contract: hex-encoding is content-gated, so a BINARY/VARBINARY PK
// whose bytes ARE valid UTF-8 keeps the byte-identical spelling it had before
// the fix. Those rows store and query correctly today; changing their
// pk_values would change their generated pk_hash and orphan every row already
// indexed under the old spelling.
func TestInsertBatch_binaryPrimaryKeyValidUTF8Unchanged(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Valid UTF-8: 7-bit ASCII, plus U+07AD written as the 2-byte sequence
	// {0xDE,0xAD} — the literal case the #756 unit test pinned.
	keyBytes := append([]byte("id-"), 0xDE, 0xAD)
	wantPK := string(keyBytes)

	tm := &metadata.TableMeta{
		Schema: "mydb", Table: "t_pk_varbinary",
		Columns: []metadata.ColumnMeta{
			{Name: "k", DataType: "varbinary", ColumnType: "varbinary(32)", IsPK: true},
		},
		PKColumns: []string{"k"},
	}
	res := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{"mydb.t_pk_varbinary": tm})

	row, err := res.MapRow("mydb", "t_pk_varbinary", []any{string(keyBytes)})
	if err != nil {
		t.Fatalf("MapRow: %v", err)
	}
	pkValues := event.BuildPKValues(tm.PKColumnMetas(), row)
	if pkValues != wantPK {
		t.Fatalf("BuildPKValues = %q, want the unchanged raw spelling %q "+
			"(hex-encoding must only touch bytes utf8mb4 cannot store)", pkValues, wantPK)
	}

	idx := New(db, 1000)
	n, err := idx.InsertBatch([]event.Event{{
		BinlogFile: "binlog.000001", StartPos: 100, EndPos: 200,
		Timestamp: time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC),
		Schema:    "mydb", Table: "t_pk_varbinary",
		EventType: event.EventInsert, PKValues: pkValues,
		RowAfter:  row,
	}})
	if err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if n != 1 {
		t.Fatalf("InsertBatch inserted %d rows, want 1", n)
	}

	var got string
	if err := db.QueryRow(
		`SELECT pk_values FROM binlog_events WHERE pk_hash = SHA2(?, 256) AND pk_values = ?`,
		pkValues, pkValues).Scan(&got); err != nil {
		t.Fatalf("indexed row is not findable by its own pk_values: %v", err)
	}
	if got != wantPK {
		t.Errorf("stored pk_values = %q, want %q", got, wantPK)
	}
}
