package query

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"testing"
	"time"
)

// ─── Stub driver ─────────────────────────────────────────────────────────────
//
// A minimal database/sql/driver implementation serving one canned resultset,
// so scanRows can be exercised without a MySQL instance. Rows.Next hands back
// driver values exactly as go-sql-driver would: uint64 for BIGINT UNSIGNED on
// the TEXT protocol (ParseUint), and on the BINARY protocol []byte above 2^63
// (uint64ToString fallback) / int64 below it.

type stubRows struct {
	cols []string
	vals [][]driver.Value
	i    int
}

func (r *stubRows) Columns() []string { return r.cols }
func (r *stubRows) Close() error      { return nil }
func (r *stubRows) Next(dest []driver.Value) error {
	if r.i >= len(r.vals) {
		return io.EOF
	}
	copy(dest, r.vals[r.i])
	r.i++
	return nil
}

type stubStmt struct{ rows *stubRows }

func (s *stubStmt) Close() error  { return nil }
func (s *stubStmt) NumInput() int { return 0 }
func (s *stubStmt) Exec([]driver.Value) (driver.Result, error) {
	return nil, driver.ErrSkip
}
func (s *stubStmt) Query([]driver.Value) (driver.Rows, error) { return s.rows, nil }

type stubConn struct{ rows *stubRows }

func (c *stubConn) Prepare(string) (driver.Stmt, error) { return &stubStmt{c.rows}, nil }
func (c *stubConn) Close() error                        { return nil }
func (c *stubConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }

type stubConnector struct{ rows *stubRows }

func (c stubConnector) Connect(context.Context) (driver.Conn, error) {
	return &stubConn{c.rows}, nil
}
func (c stubConnector) Driver() driver.Driver { return nil }

// scanRowsColumns matches the SELECT order scanRows expects.
var scanRowsColumns = []string{
	"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
	"gtid", "connection_id", "schema_name", "table_name", "event_type",
	"pk_values", "changed_columns", "row_before", "row_after",
	"schema_version", "query_text", "query_hash", "commit_ts_us",
}

func scanOneRow(t *testing.T, vals []driver.Value) ResultRow {
	t.Helper()
	db := sql.OpenDB(stubConnector{rows: &stubRows{
		cols: scanRowsColumns,
		vals: [][]driver.Value{vals},
	}})
	defer db.Close()
	rows, err := db.Query("SELECT stub")
	if err != nil {
		t.Fatalf("stub query: %v", err)
	}
	defer rows.Close()
	results, err := scanRows(rows)
	if err != nil {
		t.Fatalf("scanRows: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("scanRows returned %d rows, want 1", len(results))
	}
	return results[0]
}

// TestScanRows_positionAbove2to63 pins the #1202 widening: start_pos/end_pos
// are BIGINT UNSIGNED, and a stored position above 2^63 (the #986/#1117
// underflow shape written by pre-#1180 builds, still present in customer
// indexes) must scan losslessly instead of failing the whole resultset
// through sql.NullInt64.
func TestScanRows_positionAbove2to63(t *testing.T) {
	const bigStart = uint64(1)<<63 + 42
	const bigEnd = uint64(1)<<63 + 100

	base := func(start, end driver.Value) []driver.Value {
		return []driver.Value{
			int64(7), []byte("mariadb-bin.000001"), start, end,
			time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC),
			nil, nil, []byte("s"), []byte("t"), int64(1), []byte("1"),
			nil, nil, nil, int64(0), nil, nil, nil,
		}
	}

	t.Run("text protocol uint64", func(t *testing.T) {
		r := scanOneRow(t, base(bigStart, bigEnd))
		if r.StartPos != bigStart || r.EndPos != bigEnd {
			t.Errorf("positions [%d, %d], want [%d, %d]", r.StartPos, r.EndPos, bigStart, bigEnd)
		}
	})

	t.Run("binary protocol bytes (>2^63)", func(t *testing.T) {
		r := scanOneRow(t, base([]byte("9223372036854775850"), []byte("9223372036854775908")))
		if r.StartPos != bigStart || r.EndPos != bigEnd {
			t.Errorf("positions [%d, %d], want [%d, %d]", r.StartPos, r.EndPos, bigStart, bigEnd)
		}
	})

	t.Run("NULL positions stay zero", func(t *testing.T) {
		r := scanOneRow(t, base(nil, nil))
		if r.StartPos != 0 || r.EndPos != 0 {
			t.Errorf("positions [%d, %d], want [0, 0]", r.StartPos, r.EndPos)
		}
	})
}
