package cliapp

import (
	"bytes"
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// mockIndex points connectIndex at a sqlmock database for the test and
// restores it after. The verbs' RunE functions run unchanged.
func mockIndex(t *testing.T) sqlmock.Sqlmock {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	old := connectIndex
	connectIndex = func(string) (*sql.DB, error) { return db, nil }
	t.Cleanup(func() { connectIndex = old })
	return mock
}

// TestRemoveOfMissingExitsZero pins the command-line contract for the three
// remove verbs: a row that was not there is a printed line and a nil error
// (exit 0), because the state asked for is the state there is. Driven
// through the real RunE functions, with the shared package reporting
// RowsAffected 0.
func TestRemoveOfMissingExitsZero(t *testing.T) {
	ctx := context.Background()
	t.Run("flag remove", func(t *testing.T) {
		mock := mockIndex(t)
		mock.ExpectExec("DELETE FROM table_flags").WithArgs("app", "customers", "email", "pii").
			WillReturnResult(sqlmock.NewResult(0, 0))
		old := []string{flgIndexDSN, flgSchema, flgTable, flgColumn}
		t.Cleanup(func() { flgIndexDSN, flgSchema, flgTable, flgColumn = old[0], old[1], old[2], old[3] })
		flgIndexDSN, flgSchema, flgTable, flgColumn = "mock", "app", "customers", "email "
		var out bytes.Buffer
		flagRemoveCmd.SetOut(&out)
		t.Cleanup(func() { flagRemoveCmd.SetOut(nil) })
		flagRemoveCmd.SetContext(ctx)
		if err := runFlagRemove(flagRemoveCmd, []string{" pii"}); err != nil {
			t.Fatalf("a missing flag must be exit 0, got %v", err)
		}
		if got := out.String(); got != "Flag \"pii\" not found on app.customers (email)\n" {
			t.Errorf("output = %q", got)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
	t.Run("profile remove", func(t *testing.T) {
		mock := mockIndex(t)
		mock.ExpectExec("DELETE FROM profiles").WithArgs("ghost").WillReturnResult(sqlmock.NewResult(0, 0))
		old := proIndexDSN
		t.Cleanup(func() { proIndexDSN = old })
		proIndexDSN = "mock"
		var out bytes.Buffer
		profileRemoveCmd.SetOut(&out)
		t.Cleanup(func() { profileRemoveCmd.SetOut(nil) })
		profileRemoveCmd.SetContext(ctx)
		if err := runProfileRemove(profileRemoveCmd, []string{"ghost "}); err != nil {
			t.Fatalf("a missing profile must be exit 0, got %v", err)
		}
		if got := out.String(); got != "Profile \"ghost\" not found.\n" {
			t.Errorf("output = %q", got)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
	t.Run("access remove", func(t *testing.T) {
		mock := mockIndex(t)
		mock.ExpectExec("DELETE ar FROM access_rules").WithArgs("marketing", "pii").WillReturnResult(sqlmock.NewResult(0, 0))
		old := []string{aclIndexDSN, aclProfile, aclFlag}
		t.Cleanup(func() { aclIndexDSN, aclProfile, aclFlag = old[0], old[1], old[2] })
		aclIndexDSN, aclProfile, aclFlag = "mock", " marketing", "pii "
		var out bytes.Buffer
		accessRemoveCmd.SetOut(&out)
		t.Cleanup(func() { accessRemoveCmd.SetOut(nil) })
		accessRemoveCmd.SetContext(ctx)
		if err := runAccessRemove(accessRemoveCmd, nil); err != nil {
			t.Fatalf("a missing rule must be exit 0, got %v", err)
		}
		if got := out.String(); got != "Access rule not found: profile=\"marketing\" flag=\"pii\"\n" {
			t.Errorf("output = %q", got)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
}

// TestCLIRefusesLongAndCaseCollidingNames: the command line gets the same
// refusals as the console for a name past its column width (never a raw
// 1406 from the database) and for a profile spelled like an existing one.
func TestCLIRefusesLongAndCaseCollidingNames(t *testing.T) {
	ctx := context.Background()
	t.Run("flag add with a long schema", func(t *testing.T) {
		mock := mockIndex(t)
		old := []string{flgIndexDSN, flgSchema, flgTable, flgColumn}
		t.Cleanup(func() { flgIndexDSN, flgSchema, flgTable, flgColumn = old[0], old[1], old[2], old[3] })
		flgIndexDSN, flgSchema, flgTable, flgColumn = "mock", strings.Repeat("s", 65), "t", ""
		flagAddCmd.SetContext(ctx)
		err := runFlagAdd(flagAddCmd, []string{"pii"})
		if err == nil || err.Error() != "schema is too long (65 characters); the limit is 64 characters" {
			t.Errorf("got %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
	t.Run("profile add spelled like an existing one", func(t *testing.T) {
		mock := mockIndex(t)
		mock.ExpectQuery("SELECT name FROM profiles").WithArgs("Marketing").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("marketing"))
		old := []string{proIndexDSN, proDescription}
		t.Cleanup(func() { proIndexDSN, proDescription = old[0], old[1] })
		proIndexDSN, proDescription = "mock", ""
		profileAddCmd.SetContext(ctx)
		err := runProfileAdd(profileAddCmd, []string{"Marketing"})
		if err == nil || err.Error() != `a profile named "marketing" already exists (the index compares names without regard to case or accents)` {
			t.Errorf("got %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
	t.Run("flag add spelled like an existing one", func(t *testing.T) {
		mock := mockIndex(t)
		mock.ExpectQuery("FROM table_flags WHERE").WithArgs("app", "customers", "email", "PII").
			WillReturnRows(sqlmock.NewRows([]string{"schema_name", "table_name", "column_name", "flag"}).
				AddRow("app", "customers", "email", "pii"))
		old := []string{flgIndexDSN, flgSchema, flgTable, flgColumn}
		t.Cleanup(func() { flgIndexDSN, flgSchema, flgTable, flgColumn = old[0], old[1], old[2], old[3] })
		flgIndexDSN, flgSchema, flgTable, flgColumn = "mock", "app", "customers", "email"
		flagAddCmd.SetContext(ctx)
		err := runFlagAdd(flagAddCmd, []string{"PII"})
		if err == nil || err.Error() != `flag "pii" already exists on app.customers (email) (the index compares names without regard to case or accents)` {
			t.Errorf("got %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
}
