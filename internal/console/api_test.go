package console

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/bintrail/internal/parser"
	"github.com/dbtrail/bintrail/internal/query"
)

func TestClampLimit(t *testing.T) {
	cases := []struct{ n, def, max, want int }{
		{0, 100, 1000, 100},     // unset → default
		{-5, 100, 1000, 100},    // negative → default
		{50, 100, 1000, 50},     // in range → unchanged
		{5000, 100, 1000, 1000}, // over max → capped
		{1000, 100, 1000, 1000}, // at max → unchanged
	}
	for _, c := range cases {
		if got := clampLimit(c.n, c.def, c.max); got != c.want {
			t.Errorf("clampLimit(%d,%d,%d) = %d, want %d", c.n, c.def, c.max, got, c.want)
		}
	}
}

func TestBuildOptionsValidation(t *testing.T) {
	s := &Server{}
	bad := []struct {
		name string
		p    filterParams
	}{
		{"pk without schema/table", filterParams{PK: "1"}},
		{"changed_column without schema/table", filterParams{ChangedColumn: "x"}},
		{"invalid event type", filterParams{EventType: "BOGUS"}},
		{"invalid since", filterParams{Since: "not-a-time"}},
		{"invalid until", filterParams{Until: "nope"}},
	}
	for _, tc := range bad {
		if _, err := s.buildOptions(tc.p, 100, 1000); err == nil {
			t.Errorf("%s: expected error, got nil", tc.name)
		}
	}
}

func TestBuildOptionsValues(t *testing.T) {
	s := &Server{}

	opts, err := s.buildOptions(filterParams{
		Schema: "app", Table: "users", PK: "42", EventType: "update", Limit: 0,
	}, 100, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if opts.Limit != 100 {
		t.Errorf("Limit = %d, want default 100", opts.Limit)
	}
	if opts.Order != "DESC" {
		t.Errorf("Order = %q, want DESC (browsing default)", opts.Order)
	}
	if opts.PKValues != "42" {
		t.Errorf("PKValues = %q, want 42", opts.PKValues)
	}
	if opts.EventType == nil || *opts.EventType != parser.EventUpdate {
		t.Error("EventType not parsed to UPDATE")
	}

	capped, _ := s.buildOptions(filterParams{Schema: "app", Limit: 99999}, 100, 1000)
	if capped.Limit != 1000 {
		t.Errorf("Limit = %d, want capped 1000", capped.Limit)
	}

	asc, _ := s.buildOptions(filterParams{Order: "asc"}, 100, 1000)
	if asc.Order != "ASC" {
		t.Errorf("Order = %q, want ASC", asc.Order)
	}
}

// TestRecoverIsReadOnly asserts the read-only invariant: the recover handler
// fetches with a single SELECT and generates SQL text — it never executes any
// statement. The sqlmock registers ONLY an ExpectQuery; any write the handler
// attempted would fail (no matching expectation) and break the clean 200.
func TestRecoverIsReadOnly(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cols := []string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
		"changed_columns", "row_before", "row_after", "schema_version",
	}
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	// An INSERT event (event_type=1); its reversal is a DELETE built from
	// row_after. schema_version=0 keeps recovery on the default resolver (nil),
	// so no per-row resolver query touches the DB.
	resultRows := sqlmock.NewRows(cols).AddRow(
		int64(1), "bin.000001", int64(4), int64(40), ts,
		nil, nil, "app", "users", int64(parser.EventInsert), "42",
		nil, nil, []byte(`{"id":42,"email":"a@x"}`), int64(0),
	)
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(resultRows)

	s := &Server{
		db:        db,
		engine:    query.New(db),
		dbName:    "", // empty disables the planner → no archive_state query
		noArchive: true,
		resolver:  nil,
		token:     "t",
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/recover", strings.NewReader(`{"schema":"app","table":"users"}`))
	s.handleRecover(rec, req)

	if rec.Code != 200 {
		t.Fatalf("recover status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var resp recoverResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v (body=%s)", err, rec.Body.String())
	}
	if !strings.Contains(resp.SQL, "DELETE FROM") {
		t.Errorf("expected a DELETE in the undo SQL, got:\n%s", resp.SQL)
	}
	if !strings.Contains(resp.SQL, "BEGIN;") || !strings.Contains(resp.SQL, "COMMIT;") {
		t.Errorf("expected a transaction-wrapped script, got:\n%s", resp.SQL)
	}
	if resp.StatementCount != 1 {
		t.Errorf("StatementCount = %d, want 1", resp.StatementCount)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("read-only invariant violated — unexpected DB interaction: %v", err)
	}
}

// TestRecoverRequiresSchema ensures recover refuses to undo the whole index.
func TestRecoverRequiresSchema(t *testing.T) {
	s := &Server{token: "t"}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/recover", strings.NewReader(`{}`))
	s.handleRecover(rec, req)
	if rec.Code != 400 {
		t.Errorf("recover without schema: code = %d, want 400", rec.Code)
	}
}
