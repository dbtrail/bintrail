package forensics

import (
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestEnrichThreadsEmptyIDs(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	_, err = EnrichThreads(t.Context(), db, nil)
	if err == nil || !strings.Contains(err.Error(), "must not be empty") {
		t.Errorf("expected 'must not be empty' error, got %v", err)
	}
}

func TestEnrichThreadsTooManyIDs(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	ids := make([]int64, maxEnrichThreadIDs+1)
	_, err = EnrichThreads(t.Context(), db, ids)
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Errorf("expected limit error mentioning 500, got %v", err)
	}
}

func TestEnrichThreadsLiveLookup(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// Thread 42 is live (with NULL db/state to exercise the NullString paths);
	// thread 77 is gone.
	mock.ExpectQuery("FROM performance_schema.threads").
		WithArgs(int64(42), int64(77)).
		WillReturnRows(sqlmock.NewRows([]string{
			"PROCESSLIST_ID", "PROCESSLIST_USER", "PROCESSLIST_HOST",
			"PROCESSLIST_DB", "PROCESSLIST_COMMAND", "PROCESSLIST_STATE",
		}).AddRow(42, "root", "localhost", nil, "Query", nil))
	mock.ExpectQuery("FROM performance_schema.session_connect_attrs").
		WithArgs(int64(42), int64(77)).
		WillReturnRows(sqlmock.NewRows([]string{"PROCESSLIST_ID", "ATTR_NAME", "ATTR_VALUE"}).
			AddRow(42, "_client_name", "libmysql").
			AddRow(42, "program_name", "mysqldump"))

	res, err := EnrichThreads(t.Context(), db, []int64{42, 77})
	if err != nil {
		t.Fatalf("EnrichThreads: %v", err)
	}

	ti, ok := res.Threads["42"]
	if !ok {
		t.Fatalf("thread 42 missing from result: %+v", res.Threads)
	}
	if ti.User != "root" || ti.Host != "localhost" || ti.ConnectionID != 42 {
		t.Errorf("thread 42 = %+v, want root@localhost id=42", ti)
	}
	if ti.ProcesslistDB != nil {
		t.Errorf("ProcesslistDB = %v, want nil for NULL column", *ti.ProcesslistDB)
	}
	if ti.State != "" {
		t.Errorf("State = %q, want empty for NULL column", ti.State)
	}
	if ti.ConnAttrs["_client_name"] != "libmysql" || ti.ConnAttrs["program_name"] != "mysqldump" {
		t.Errorf("ConnAttrs = %v, want client attrs merged in", ti.ConnAttrs)
	}

	if res.Source != "performance_schema" {
		t.Errorf("Source = %q, want performance_schema", res.Source)
	}
	if len(res.NotFound) != 1 || res.NotFound[0] != 77 {
		t.Errorf("NotFound = %v, want [77]", res.NotFound)
	}
	if len(res.FallbackQueries) == 0 {
		t.Fatal("expected fallback queries for the missing thread ID")
	}
	for _, q := range res.FallbackQueries {
		if q.SQL == "" || q.Description == "" {
			t.Errorf("fallback query with empty field: %+v", q)
		}
		if !strings.Contains(q.SQL, "77") {
			t.Errorf("fallback query should reference the missing ID 77: %s", q.SQL)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestEnrichThreadsAttrsOnlyStub pins the SaaS behavior for a session visible
// in session_connect_attrs but not in threads (raced a disconnect): a stub
// entry with just the connection id and attrs is returned, and the ID is not
// reported as missing.
func TestEnrichThreadsAttrsOnlyStub(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("FROM performance_schema.threads").
		WillReturnRows(sqlmock.NewRows([]string{
			"PROCESSLIST_ID", "PROCESSLIST_USER", "PROCESSLIST_HOST",
			"PROCESSLIST_DB", "PROCESSLIST_COMMAND", "PROCESSLIST_STATE",
		}))
	mock.ExpectQuery("FROM performance_schema.session_connect_attrs").
		WillReturnRows(sqlmock.NewRows([]string{"PROCESSLIST_ID", "ATTR_NAME", "ATTR_VALUE"}).
			AddRow(99, "_client_name", "libmysql"))

	res, err := EnrichThreads(t.Context(), db, []int64{99})
	if err != nil {
		t.Fatalf("EnrichThreads: %v", err)
	}
	ti, ok := res.Threads["99"]
	if !ok {
		t.Fatalf("expected stub entry for 99, got %+v", res.Threads)
	}
	if ti.ConnectionID != 99 || ti.ConnAttrs["_client_name"] != "libmysql" {
		t.Errorf("stub = %+v, want id 99 with attrs", ti)
	}
	if len(res.NotFound) != 0 {
		t.Errorf("NotFound = %v, want empty (attrs-only session still counts as found)", res.NotFound)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestEnrichThreadsQueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("FROM performance_schema.threads").
		WillReturnError(errors.New("SELECT command denied"))

	_, err = EnrichThreads(t.Context(), db, []int64{1})
	if err == nil || !strings.Contains(err.Error(), "performance_schema.threads") {
		t.Errorf("expected threads query error, got %v", err)
	}
}

func TestGenerateThreadFallbackQueries(t *testing.T) {
	queries := generateThreadFallbackQueries([]int64{123, 456})

	if len(queries) < 2 {
		t.Fatalf("expected at least 2 fallback queries, got %d", len(queries))
	}

	// Verify the queries contain the thread IDs.
	for _, q := range queries {
		if q.SQL == "" {
			t.Error("fallback query has empty SQL")
		}
		if q.Description == "" {
			t.Error("fallback query has empty description")
		}
		if !strings.Contains(q.SQL, "123") || !strings.Contains(q.SQL, "456") {
			t.Errorf("fallback query should contain thread IDs: %s", q.SQL)
		}
	}
}
