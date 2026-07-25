//go:build integration

package indexer

import (
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestEnsureSchemaAddsCommitTsColumn simulates a pre-#18 install by dropping
// the column, then asserts EnsureSchema restores it nullable and is idempotent.
// Nullability is the load-bearing part: every row indexed before this column
// existed, and every row from a source that writes no commit timestamp, must
// read back as NULL rather than as the epoch.
func TestEnsureSchemaAddsCommitTsColumn(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	testutil.MustExec(t, db, `ALTER TABLE binlog_events DROP COLUMN commit_ts_us`)

	if err := EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	var dataType, isNullable, columnType string
	err := db.QueryRow(`SELECT DATA_TYPE, IS_NULLABLE, COLUMN_TYPE FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'binlog_events' AND COLUMN_NAME = 'commit_ts_us'`).
		Scan(&dataType, &isNullable, &columnType)
	if err != nil {
		t.Fatalf("commit_ts_us not found after EnsureSchema: %v", err)
	}
	if dataType != "bigint" {
		t.Errorf("commit_ts_us DATA_TYPE = %q, want bigint", dataType)
	}
	if isNullable != "YES" {
		t.Errorf("commit_ts_us IS_NULLABLE = %q, want YES (rows indexed before this column must read back NULL)", isNullable)
	}
	// Epoch microseconds already exceed 2^50 and grow; an accidental signed
	// column would still fit, but unsigned is what the schema declares and the
	// archive column mirrors, so a drift between the two is worth catching.
	if columnType != "bigint unsigned" {
		t.Errorf("commit_ts_us COLUMN_TYPE = %q, want \"bigint unsigned\"", columnType)
	}

	if err := EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema (second run): %v", err)
	}
}

// TestInsertBatch_commitTsRoundTrip drives the #18 write path against a real
// index MySQL: a captured microsecond stamp round-trips EXACTLY (no truncation
// to seconds anywhere between the parser and the column), and an event without
// one stores NULL — the two states a consumer has to tell apart.
func TestInsertBatch_commitTsRoundTrip(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// A stamp with non-zero sub-second digits: truncation to seconds anywhere
	// in the path would land on ...000000 and this value would not survive.
	const stamped = uint64(1767225600_123456)
	ts := time.Unix(int64(stamped/1_000_000), 0).UTC()

	idx := New(db, 100)
	n, err := idx.InsertBatch([]parser.Event{
		{
			BinlogFile: "binlog.000001", StartPos: 100, EndPos: 200, Timestamp: ts,
			CommitTsUS: stamped,
			Schema:     "shop", Table: "orders", EventType: parser.EventInsert,
			PKValues: "1", RowAfter: map[string]any{"id": 1},
		},
		{
			// No commit timestamp: MariaDB, or MySQL older than 8.0.1.
			BinlogFile: "binlog.000001", StartPos: 200, EndPos: 300, Timestamp: ts,
			Schema:   "shop", Table: "orders", EventType: parser.EventInsert,
			PKValues: "2", RowAfter: map[string]any{"id": 2},
		},
	})
	if err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if n != 2 {
		t.Fatalf("InsertBatch wrote %d rows, want 2", n)
	}

	rows, err := db.Query(`SELECT pk_values, commit_ts_us FROM binlog_events ORDER BY pk_values`)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	defer rows.Close()

	got := map[string]*uint64{}
	for rows.Next() {
		var pk string
		var us *uint64
		if err := rows.Scan(&pk, &us); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[pk] = us
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}

	if got["1"] == nil {
		t.Fatalf("row 1: commit_ts_us is NULL, want %d", stamped)
	}
	if *got["1"] != stamped {
		t.Errorf("row 1: commit_ts_us = %d, want %d (exact microseconds, no rounding)", *got["1"], stamped)
	}
	if got["2"] != nil {
		t.Errorf("row 2: commit_ts_us = %d, want NULL — a zero from the parser means "+
			"\"this source wrote no commit timestamp\", and storing it as 0 would read as the epoch", *got["2"])
	}
}
