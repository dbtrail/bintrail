package doctor

import (
	"context"
	"errors"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// pkRows is the shape the check scans: one row per table WITHOUT a primary key.
func pkRows(pairs ...[2]string) *sqlmock.Rows {
	r := sqlmock.NewRows([]string{"TABLE_SCHEMA", "TABLE_NAME"})
	for _, p := range pairs {
		r.AddRow(p[0], p[1])
	}
	return r
}

// A table with no primary key is captured and largely unrecoverable, and
// nothing else in the pipeline says so. The check exists to break that
// silence, so the table NAME is the payload: a warning that says "some tables"
// leaves the operator with the same query to write that they had before.
func TestCheckPrimaryKeys_namesTheTables(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("TABLE_CONSTRAINTS").
		WillReturnRows(pkRows([2]string{"shop", "audit_log"}, [2]string{"shop", "sessions"}))

	got := checkPrimaryKeys(context.Background(), db, nil)

	if got.Status != StatusWarn {
		t.Errorf("status = %v, want WARN. A FAIL would refuse to start capture over tables whose "+
			"capture is still worth having; a PASS is the silence this check exists to break", got.Status)
	}
	for _, want := range []string{"shop.audit_log", "shop.sessions"} {
		if !strings.Contains(got.Detail, want) {
			t.Errorf("detail does not name %s: %q", want, got.Detail)
		}
	}
	if !strings.Contains(got.Detail, "2 table") {
		t.Errorf("detail does not count them: %q", got.Detail)
	}
	if !strings.Contains(got.Remediation, "ADD PRIMARY KEY") {
		t.Error("the remediation does not say how to fix it")
	}
}

// Every table has one: the reassuring answer must be reachable, or the check
// would warn on every install and be dismissed everywhere.
func TestCheckPrimaryKeys_passesWhenEveryTableHasOne(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("TABLE_CONSTRAINTS").WillReturnRows(pkRows())

	got := checkPrimaryKeys(context.Background(), db, nil)
	if got.Status != StatusPass {
		t.Errorf("status = %v, want PASS with no keyless tables (detail %q)", got.Status, got.Detail)
	}
}

// A schema converted from MyISAM can have hundreds. The names stop at the cap
// and the COUNT stays exact: an operator sizing the work needs the real number,
// and a truncated list that also truncated the count would understate it.
func TestCheckPrimaryKeys_capsTheNamesButNotTheCount(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var pairs [][2]string
	for i := 0; i < pkNameLimit+7; i++ {
		pairs = append(pairs, [2]string{"shop", "t" + string(rune('a'+i))})
	}
	mock.ExpectQuery("TABLE_CONSTRAINTS").WillReturnRows(pkRows(pairs...))

	got := checkPrimaryKeys(context.Background(), db, nil)

	if !strings.Contains(got.Detail, "17 table") {
		t.Errorf("the count must be the real one, not the number of names printed: %q", got.Detail)
	}
	if !strings.Contains(got.Detail, "and 7 more") {
		t.Errorf("detail does not say the list was cut: %q", got.Detail)
	}
	if n := strings.Count(got.Detail, "shop."); n != pkNameLimit {
		t.Errorf("printed %d names, want the cap of %d: %q", n, pkNameLimit, got.Detail)
	}
}

// A query that fails is NOT "no tables without a primary key". Reporting the
// reassuring answer from an error is the shape this whole check exists to
// avoid, one level up.
func TestCheckPrimaryKeys_aFailedQueryIsNotAPass(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("TABLE_CONSTRAINTS").WillReturnError(errors.New("Error 1142: SELECT command denied to user"))

	got := checkPrimaryKeys(context.Background(), db, nil)
	if got.Status == StatusPass {
		t.Fatal("a query the account could not run reported PASS, which tells the operator every " +
			"table has a primary key on the evidence of having learned nothing")
	}
	if got.Status != StatusFail {
		t.Errorf("status = %v, want FAIL", got.Status)
	}
}

// The scoped form must not silently widen to every schema on the server: an
// operator monitoring one schema would be warned about tables bintrail will
// never capture, and would go fix the wrong database.
func TestCheckPrimaryKeys_scopesToTheGivenSchemas(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// The QUERY TEXT, not just the argument: dropping the IN clause while still
	// passing the argument leaves WithArgs satisfied, so an args-only
	// expectation cannot see the filter go missing.
	mock.ExpectQuery(`TABLE_SCHEMA IN \(\?\)`).
		WithArgs("shop").
		WillReturnRows(pkRows([2]string{"shop", "audit_log"}))

	if got := checkPrimaryKeys(context.Background(), db, []string{"shop"}); got.Status != StatusWarn {
		t.Fatalf("status = %v, detail %q", got.Status, got.Detail)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("the schema filter was not passed as an argument: %v", err)
	}
}

// A driver error that arrives PART WAY through the rows must not be reported
// as the tables seen so far. The reassuring half of this check is "and no
// others", and a truncated scan cannot say it.
func TestCheckPrimaryKeys_anErrorMidScanIsNotAShorterList(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows := pkRows([2]string{"shop", "audit_log"}, [2]string{"shop", "sessions"}).
		RowError(1, errors.New("Error 2013: lost connection during query"))
	mock.ExpectQuery("TABLE_CONSTRAINTS").WillReturnRows(rows)

	got := checkPrimaryKeys(context.Background(), db, nil)

	if got.Status == StatusWarn && strings.Contains(got.Detail, "1 table") {
		t.Fatal("the scan died after one row and the check reported that one as the whole answer, " +
			"so a server with fifty keyless tables would be described as having one")
	}
	if got.Status != StatusFail {
		t.Errorf("status = %v, want FAIL: the question was not answered", got.Status)
	}
}
