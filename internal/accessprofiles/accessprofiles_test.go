package accessprofiles

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func newMock(t *testing.T) (sqlmock.Sqlmock, DBExecer) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return mock, db
}

// TestClassification pins IsRefusal / IsNotFound / IsConflict: the console
// picks a status from them and the CLI its exit code, so a new typed error
// that lands in the wrong bucket is a 500 (or an exit 1) for a user
// mistake.
func TestClassification(t *testing.T) {
	cases := []struct {
		err                         error
		refusal, notFound, conflict bool
	}{
		{&MissingFieldError{Field: "schema"}, true, false, false},
		{&InvalidPermissionError{Got: "rw"}, true, false, false},
		{&TooLongError{Field: "schema", Got: 65, Max: 64, Unit: "characters"}, true, false, false},
		{&ProfileExistsError{Existing: "marketing", Requested: "Marketing"}, true, false, true},
		{&FlagExistsError{Existing: Flag{Name: "pii", Schema: "s", Table: "t"}, Requested: Flag{Name: "PII", Schema: "s", Table: "t"}}, true, false, true},
		{&ProfileNotFoundError{Name: "ghost"}, true, true, false},
		{&FlagNotFoundError{Flag: Flag{Name: "pii", Schema: "s", Table: "t"}}, true, true, false},
		{&RuleNotFoundError{Profile: "p", Flag: "f"}, true, true, false},
		{fmt.Errorf("wrapped: %w", &RuleNotFoundError{Profile: "p", Flag: "f"}), true, true, false},
		{errors.New("driver: bad connection"), false, false, false},
		{nil, false, false, false},
	}
	for _, tc := range cases {
		if got := IsRefusal(tc.err); got != tc.refusal {
			t.Errorf("IsRefusal(%v) = %v, want %v", tc.err, got, tc.refusal)
		}
		if got := IsNotFound(tc.err); got != tc.notFound {
			t.Errorf("IsNotFound(%v) = %v, want %v", tc.err, got, tc.notFound)
		}
		if got := IsConflict(tc.err); got != tc.conflict {
			t.Errorf("IsConflict(%v) = %v, want %v", tc.err, got, tc.conflict)
		}
	}
}

// TestFlagNotFoundErrorNamesTheColumn: the column suffix appears only for a
// column-level flag, so the message names the row that was looked for.
func TestFlagNotFoundErrorNamesTheColumn(t *testing.T) {
	table := &FlagNotFoundError{Flag: Flag{Name: "pii", Schema: "app", Table: "customers"}}
	if got := table.Error(); got != `flag "pii" not found on app.customers` {
		t.Errorf("table-level = %q", got)
	}
	column := &FlagNotFoundError{Flag: Flag{Name: "pii", Schema: "app", Table: "customers", Column: "email"}}
	if got := column.Error(); got != `flag "pii" not found on app.customers (email)` {
		t.Errorf("column-level = %q", got)
	}
}

// TestValidateRuleOrder: the permission is checked before the required
// fields, so the CLI's first refusal (cobra already enforces the fields'
// presence, not the permission's value) stays what it always was.
func TestValidateRuleOrder(t *testing.T) {
	var bad *InvalidPermissionError
	if err := ValidateRule(Rule{Permission: "rw"}); !errors.As(err, &bad) || bad.Got != "rw" {
		t.Errorf("empty profile and flag with a bad permission: got %v, want the permission refusal first", err)
	}
	var missing *MissingFieldError
	if err := ValidateRule(Rule{Permission: PermissionDeny, Flag: "f"}); !errors.As(err, &missing) || missing.Field != "profile" {
		t.Errorf("missing profile: got %v", err)
	}
	if err := ValidateRule(Rule{Permission: PermissionDeny, Profile: "p"}); !errors.As(err, &missing) || missing.Field != "flag" {
		t.Errorf("missing flag: got %v", err)
	}
	if err := ValidateRule(Rule{Permission: PermissionAllow, Profile: "p", Flag: "f"}); err != nil {
		t.Errorf("valid rule refused: %v", err)
	}
}

// TestLengthBounds: every name is refused past its column's width, with the
// limit in the message, before any SQL runs (the mock has nothing queued).
func TestLengthBounds(t *testing.T) {
	long := func(n int) string { return strings.Repeat("x", n) }
	// Multi-byte: the VARCHAR widths are in characters, so 64 two-byte
	// runes fit and 65 do not.
	wide := func(n int) string { return strings.Repeat("é", n) }
	mock, db := newMock(t)
	ctx := context.Background()
	cases := []struct {
		name string
		call func() error
		want *TooLongError
	}{
		{"flag name", func() error { return AddFlag(ctx, db, Flag{Name: long(256), Schema: "s", Table: "t"}) },
			&TooLongError{Field: "flag name", Got: 256, Max: MaxFlagLen, Unit: "characters"}},
		{"schema", func() error { return AddFlag(ctx, db, Flag{Name: "f", Schema: long(65), Table: "t"}) },
			&TooLongError{Field: "schema", Got: 65, Max: MaxIdentifierLen, Unit: "characters"}},
		{"table", func() error { return RemoveFlag(ctx, db, Flag{Name: "f", Schema: "s", Table: long(65)}) },
			&TooLongError{Field: "table", Got: 65, Max: MaxIdentifierLen, Unit: "characters"}},
		{"column", func() error { return AddFlag(ctx, db, Flag{Name: "f", Schema: "s", Table: "t", Column: wide(65)}) },
			&TooLongError{Field: "column", Got: 65, Max: MaxIdentifierLen, Unit: "characters"}},
		{"profile name on add", func() error { return AddProfile(ctx, db, Profile{Name: long(256)}) },
			&TooLongError{Field: "profile name", Got: 256, Max: MaxProfileNameLen, Unit: "characters"}},
		{"profile name on remove", func() error { return RemoveProfile(ctx, db, long(256)) },
			&TooLongError{Field: "profile name", Got: 256, Max: MaxProfileNameLen, Unit: "characters"}},
		{"description", func() error { return AddProfile(ctx, db, Profile{Name: "p", Description: long(65536)}) },
			&TooLongError{Field: "description", Got: 65536, Max: MaxDescriptionLen, Unit: "bytes"}},
		{"rule profile", func() error { return AddRule(ctx, db, Rule{Profile: long(256), Flag: "f", Permission: PermissionDeny}) },
			&TooLongError{Field: "profile", Got: 256, Max: MaxProfileNameLen, Unit: "characters"}},
		{"rule flag", func() error { return AddRule(ctx, db, Rule{Profile: "p", Flag: long(256), Permission: PermissionDeny}) },
			&TooLongError{Field: "flag", Got: 256, Max: MaxFlagLen, Unit: "characters"}},
		{"rule remove flag", func() error { return RemoveRule(ctx, db, "p", long(256)) },
			&TooLongError{Field: "flag", Got: 256, Max: MaxFlagLen, Unit: "characters"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			var tl *TooLongError
			if !errors.As(err, &tl) || *tl != *tc.want {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			if !strings.Contains(err.Error(), fmt.Sprintf("the limit is %d %s", tc.want.Max, tc.want.Unit)) {
				t.Errorf("message %q does not say the limit", err.Error())
			}
		})
	}
	// A description of exactly the limit and a column of exactly 64 wide
	// runes are not refused: the check is on the width, not under it.
	if err := AddProfile(ctx, db, Profile{Name: "p", Description: long(MaxDescriptionLen)}); err != nil {
		var tl *TooLongError
		if errors.As(err, &tl) {
			t.Errorf("a description at the limit was refused: %v", err)
		}
	}
	mock.ExpectQuery("FROM table_flags WHERE").WithArgs("s", "t", wide(64), "f").
		WillReturnRows(sqlmock.NewRows([]string{"schema_name", "table_name", "column_name", "flag"}))
	mock.ExpectExec("INSERT INTO table_flags").WithArgs("s", "t", wide(64), "f").WillReturnResult(sqlmock.NewResult(1, 1))
	if err := AddFlag(ctx, db, Flag{Name: "f", Schema: "s", Table: "t", Column: wide(64)}); err != nil {
		t.Errorf("a 64-character column name was refused: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestTrimming: surrounding whitespace never reaches the database and a
// blank value is the missing-field refusal, so "marketing " is marketing
// on both surfaces.
func TestTrimming(t *testing.T) {
	ctx := context.Background()
	t.Run("flag", func(t *testing.T) {
		mock, db := newMock(t)
		mock.ExpectQuery("FROM table_flags WHERE").WithArgs("app", "customers", "email", "pii").
			WillReturnRows(sqlmock.NewRows([]string{"schema_name", "table_name", "column_name", "flag"}))
		mock.ExpectExec("INSERT INTO table_flags").WithArgs("app", "customers", "email", "pii").WillReturnResult(sqlmock.NewResult(1, 1))
		if err := AddFlag(ctx, db, Flag{Name: " pii", Schema: "app ", Table: "\tcustomers", Column: "email \n"}); err != nil {
			t.Fatal(err)
		}
		mock.ExpectExec("DELETE FROM table_flags").WithArgs("app", "customers", "", "pii").WillReturnResult(sqlmock.NewResult(0, 0))
		err := RemoveFlag(ctx, db, Flag{Name: "pii ", Schema: " app", Table: "customers ", Column: "  "})
		var nf *FlagNotFoundError
		if !errors.As(err, &nf) || nf.Flag != (Flag{Name: "pii", Schema: "app", Table: "customers"}) {
			t.Errorf("not-found carries the untrimmed key: %v", err)
		}
		var missing *MissingFieldError
		if err := AddFlag(ctx, db, Flag{Name: "pii", Schema: "app", Table: "   "}); !errors.As(err, &missing) || missing.Field != "table" {
			t.Errorf("blank table: got %v, want the missing-field refusal", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
	t.Run("profile", func(t *testing.T) {
		mock, db := newMock(t)
		mock.ExpectQuery("SELECT name FROM profiles").WithArgs("marketing").WillReturnRows(sqlmock.NewRows([]string{"name"}))
		mock.ExpectExec("INSERT INTO profiles").WithArgs("marketing", "Marketing analysts").WillReturnResult(sqlmock.NewResult(1, 1))
		if err := AddProfile(ctx, db, Profile{Name: "marketing ", Description: " Marketing analysts "}); err != nil {
			t.Fatal(err)
		}
		mock.ExpectExec("DELETE FROM profiles").WithArgs("marketing").WillReturnResult(sqlmock.NewResult(0, 1))
		if err := RemoveProfile(ctx, db, " marketing "); err != nil {
			t.Fatal(err)
		}
		var missing *MissingFieldError
		if err := AddProfile(ctx, db, Profile{Name: " "}); !errors.As(err, &missing) || missing.Field != "profile name" {
			t.Errorf("blank name: got %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
	t.Run("rule", func(t *testing.T) {
		mock, db := newMock(t)
		mock.ExpectQuery("SELECT id FROM profiles").WithArgs("marketing").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(3)))
		mock.ExpectExec("INSERT INTO access_rules").WithArgs(int64(3), "pii", "deny").WillReturnResult(sqlmock.NewResult(1, 1))
		if err := AddRule(ctx, db, Rule{Profile: " marketing", Flag: "pii ", Permission: " deny "}); err != nil {
			t.Fatal(err)
		}
		mock.ExpectExec("DELETE ar FROM access_rules").WithArgs("marketing", "pii").WillReturnResult(sqlmock.NewResult(0, 1))
		if err := RemoveRule(ctx, db, "marketing ", " pii"); err != nil {
			t.Fatal(err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
	t.Run("list filters", func(t *testing.T) {
		mock, db := newMock(t)
		mock.ExpectQuery("FROM table_flags WHERE schema_name = \\? AND table_name = \\?").WithArgs("app", "customers").
			WillReturnRows(sqlmock.NewRows([]string{"schema_name", "table_name", "column_name", "flag", "created_at"}))
		if _, err := ListFlags(ctx, db, " app", "customers "); err != nil {
			t.Fatal(err)
		}
		mock.ExpectQuery("WHERE p.name = \\?").WithArgs("marketing").
			WillReturnRows(sqlmock.NewRows([]string{"name", "flag", "permission", "created_at"}))
		if _, err := ListRules(ctx, db, " marketing "); err != nil {
			t.Fatal(err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
}

// TestAddProfileRefusesACaseCollision: the unique key is case-insensitive,
// so "Marketing" beside "marketing" is one row. Adding the other spelling
// is refused, naming the row that exists, and writes nothing; the exact
// spelling still re-describes.
func TestAddProfileRefusesACaseCollision(t *testing.T) {
	ctx := context.Background()
	mock, db := newMock(t)
	mock.ExpectQuery("SELECT name FROM profiles").WithArgs("Marketing").WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("marketing"))
	err := AddProfile(ctx, db, Profile{Name: "Marketing"})
	var exists *ProfileExistsError
	if !errors.As(err, &exists) || exists.Existing != "marketing" || exists.Requested != "Marketing" {
		t.Fatalf("got %v, want ProfileExistsError{marketing, Marketing}", err)
	}
	if got := err.Error(); got != `a profile named "marketing" already exists (the index compares names without regard to case or accents)` {
		t.Errorf("message = %q", got)
	}
	// Same spelling: an update of the description, as before.
	mock.ExpectQuery("SELECT name FROM profiles").WithArgs("marketing").WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("marketing"))
	mock.ExpectExec("INSERT INTO profiles").WithArgs("marketing", "new words").WillReturnResult(sqlmock.NewResult(0, 2))
	if err := AddProfile(ctx, db, Profile{Name: "marketing", Description: "new words"}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestRemoveOfMissingIsTheNotFoundType: each remove verb reports a row that
// was not there with its typed not-found error (RowsAffected 0), the type
// the CLI maps to a printed line and exit 0 and the console to 404.
func TestRemoveOfMissingIsTheNotFoundType(t *testing.T) {
	ctx := context.Background()
	mock, db := newMock(t)
	mock.ExpectExec("DELETE FROM table_flags").WillReturnResult(sqlmock.NewResult(0, 0))
	var fnf *FlagNotFoundError
	if err := RemoveFlag(ctx, db, Flag{Name: "pii", Schema: "s", Table: "t", Column: "c"}); !errors.As(err, &fnf) || !IsNotFound(err) {
		t.Errorf("flag: got %v", err)
	}
	mock.ExpectExec("DELETE FROM profiles").WillReturnResult(sqlmock.NewResult(0, 0))
	var pnf *ProfileNotFoundError
	if err := RemoveProfile(ctx, db, "ghost"); !errors.As(err, &pnf) || pnf.Name != "ghost" || !IsNotFound(err) {
		t.Errorf("profile: got %v", err)
	}
	mock.ExpectExec("DELETE ar FROM access_rules").WillReturnResult(sqlmock.NewResult(0, 0))
	var rnf *RuleNotFoundError
	if err := RemoveRule(ctx, db, "p", "f"); !errors.As(err, &rnf) || rnf.Profile != "p" || rnf.Flag != "f" || !IsNotFound(err) {
		t.Errorf("rule: got %v", err)
	}
	// A rule for a profile that does not exist is the profile's not-found,
	// before any INSERT.
	mock.ExpectQuery("SELECT id FROM profiles").WillReturnRows(sqlmock.NewRows([]string{"id"}))
	if err := AddRule(ctx, db, Rule{Profile: "ghost", Flag: "f", Permission: PermissionDeny}); !errors.As(err, &pnf) || pnf.Name != "ghost" {
		t.Errorf("rule for an unknown profile: got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestAddFlagRefusesACollision: the unique key on table_flags folds case
// and accents on all four columns, so "PII" on a table carrying "pii" (or
// "pii" on "Customers" when the row says "customers") would have been kept
// under the stored spelling while reporting success. It is refused, naming
// the stored row, and writes nothing; the exact spelling is the no-op it
// always was.
func TestAddFlagRefusesACollision(t *testing.T) {
	ctx := context.Background()
	mock, db := newMock(t)
	cols := []string{"schema_name", "table_name", "column_name", "flag"}
	stored := Flag{Schema: "app", Table: "customers", Column: "email", Name: "pii"}
	for _, req := range []Flag{
		{Schema: "app", Table: "customers", Column: "email", Name: "PII"},
		{Schema: "APP", Table: "customers", Column: "email", Name: "pii"},
		{Schema: "app", Table: "customers", Column: "Email", Name: "pii"},
	} {
		mock.ExpectQuery("FROM table_flags WHERE").WithArgs(req.Schema, req.Table, req.Column, req.Name).
			WillReturnRows(sqlmock.NewRows(cols).AddRow(stored.Schema, stored.Table, stored.Column, stored.Name))
		err := AddFlag(ctx, db, req)
		var exists *FlagExistsError
		if !errors.As(err, &exists) || exists.Existing != stored || exists.Requested != req || !IsConflict(err) {
			t.Fatalf("%+v: got %v, want FlagExistsError naming the stored row", req, err)
		}
		if got := err.Error(); got != `flag "pii" already exists on app.customers (email) (the index compares names without regard to case or accents)` {
			t.Errorf("message = %q", got)
		}
	}
	// Table-level: no column suffix.
	mock.ExpectQuery("FROM table_flags WHERE").WithArgs("app", "invoices", "", "BILLING").
		WillReturnRows(sqlmock.NewRows(cols).AddRow("app", "invoices", "", "billing"))
	err := AddFlag(ctx, db, Flag{Schema: "app", Table: "invoices", Name: "BILLING"})
	if err == nil || err.Error() != `flag "billing" already exists on app.invoices (the index compares names without regard to case or accents)` {
		t.Errorf("table-level message = %v", err)
	}
	// Exact spelling: the INSERT runs and is the no-op it was.
	mock.ExpectQuery("FROM table_flags WHERE").WithArgs("app", "customers", "email", "pii").
		WillReturnRows(sqlmock.NewRows(cols).AddRow(stored.Schema, stored.Table, stored.Column, stored.Name))
	mock.ExpectExec("INSERT INTO table_flags").WithArgs("app", "customers", "email", "pii").WillReturnResult(sqlmock.NewResult(0, 0))
	if err := AddFlag(ctx, db, stored); err != nil {
		t.Errorf("exact spelling refused: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
