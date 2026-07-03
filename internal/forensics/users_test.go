package forensics

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func userRows(users ...string) *sqlmock.Rows {
	r := sqlmock.NewRows([]string{"User"})
	for _, u := range users {
		r.AddRow(u)
	}
	return r
}

func TestListUsersMergesAndDeduplicates(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT DISTINCT User FROM mysql.user").
		WillReturnRows(userRows("app", "root"))
	mock.ExpectQuery("FROM performance_schema.accounts").
		WillReturnRows(userRows("ghost", "root"))

	users, err := ListUsers(t.Context(), db)
	if err != nil {
		t.Fatalf("ListUsers: %v", err)
	}
	want := []string{"app", "root", "ghost"} // first-seen order, deduped
	if !slices.Equal(users, want) {
		t.Errorf("users = %v, want %v", users, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestListUsersSurvivesMysqlUserDenied(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT DISTINCT User FROM mysql.user").
		WillReturnError(errors.New("SELECT command denied"))
	mock.ExpectQuery("FROM performance_schema.accounts").
		WillReturnRows(userRows("app"))

	users, err := ListUsers(t.Context(), db)
	if err != nil {
		t.Fatalf("ListUsers must degrade gracefully when one source fails: %v", err)
	}
	if !slices.Equal(users, []string{"app"}) {
		t.Errorf("users = %v, want [app]", users)
	}
}

func TestListUsersErrorsWhenBothSourcesFail(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT DISTINCT User FROM mysql.user").
		WillReturnError(errors.New("denied one"))
	mock.ExpectQuery("FROM performance_schema.accounts").
		WillReturnError(errors.New("denied two"))

	_, err = ListUsers(t.Context(), db)
	if err == nil {
		t.Fatal("expected error when both sources fail")
	}
	for _, want := range []string{"could not query MySQL user accounts", "denied one", "denied two"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing %q", err.Error(), want)
		}
	}
}
