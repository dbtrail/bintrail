package query

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// TestFetch_profileActiveBlanksQueryText pins the #699 gate: a named profile
// that resolved to ZERO deny/redact rules (nonexistent or empty profile) must
// still withhold QueryText/QueryHash — ProfileActive alone triggers the
// redaction pass.
func TestFetch_profileActiveBlanksQueryText(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	cols := []string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
		"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
	}
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	rows := sqlmock.NewRows(cols).AddRow(
		int64(1), "bin.000001", int64(4), int64(40), ts,
		nil, nil, "app", "users", int64(2), "7",
		nil, []byte(`{"ssn":"123-45-6789"}`), []byte(`{"ssn":"999"}`), int64(0),
		"UPDATE app.users SET ssn='999' WHERE id=7", "cafe0000",
	)
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(rows)

	results, err := New(db).Fetch(context.Background(), Options{ProfileActive: true})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("rows = %d, want 1", len(results))
	}
	if results[0].QueryText != nil || results[0].QueryHash != nil {
		t.Errorf("QueryText/QueryHash must be withheld under ProfileActive with zero rules, got %v / %v",
			results[0].QueryText, results[0].QueryHash)
	}
	// Row images stay intact: no redact rules matched.
	if results[0].RowBefore["ssn"] == nil {
		t.Error("row images must not be touched when no redact rule matches")
	}
}
