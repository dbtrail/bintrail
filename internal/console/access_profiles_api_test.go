package console

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/accessprofiles"
	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/query"
)

// expectProfileLookup queues the SELECT AddProfile runs before its INSERT
// (the collision check); existing == "" is "no such row".
func expectProfileLookup(mock sqlmock.Sqlmock, existing string) {
	rows := sqlmock.NewRows([]string{"name"})
	if existing != "" {
		rows.AddRow(existing)
	}
	mock.ExpectQuery("SELECT name FROM profiles").WillReturnRows(rows)
}

// expectFlagLookup queues the SELECT AddFlag runs before its INSERT; a nil
// existing is "no such row".
func expectFlagLookup(mock sqlmock.Sqlmock, existing *accessprofiles.Flag) {
	rows := sqlmock.NewRows([]string{"schema_name", "table_name", "column_name", "flag"})
	if existing != nil {
		rows.AddRow(existing.Schema, existing.Table, existing.Column, existing.Name)
	}
	mock.ExpectQuery("FROM table_flags WHERE").WillReturnRows(rows)
}

// accessRoutes is every route of the surface: the GET and the six verbs,
// with a body that passes validation so a refusal seen is the gate's.
var accessRoutes = []struct{ method, path, body string }{
	{"GET", "/api/access-profiles", ""},
	{"POST", "/api/access-profiles/flags", `{"flag":"pii","schema":"app","table":"users"}`},
	{"POST", "/api/access-profiles/flags/remove", `{"flag":"pii","schema":"app","table":"users"}`},
	{"POST", "/api/access-profiles/profiles", `{"name":"marketing"}`},
	{"POST", "/api/access-profiles/profiles/remove", `{"name":"marketing"}`},
	{"POST", "/api/access-profiles/rules", `{"profile":"marketing","flag":"pii","permission":"deny"}`},
	{"POST", "/api/access-profiles/rules/remove", `{"profile":"marketing","flag":"pii"}`},
}

// expectAccessDoc queues the three SELECTs loadAccessProfilesDoc runs (the
// readback every mutation answers with), all empty.
func expectAccessDoc(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("FROM table_flags").WillReturnRows(sqlmock.NewRows(
		[]string{"schema_name", "table_name", "column_name", "flag", "created_at"}))
	mock.ExpectQuery("FROM profiles").WillReturnRows(sqlmock.NewRows(
		[]string{"name", "description", "created_at"}))
	mock.ExpectQuery("FROM access_rules").WillReturnRows(sqlmock.NewRows(
		[]string{"name", "flag", "permission", "created_at"}))
}

// driveAccess calls one access-profile handler directly (no middleware, so
// the request is a static-token one) with a JSON body.
func driveAccess(t *testing.T, s *Server, h func(*Server, http.ResponseWriter, *http.Request), body string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	h(s, w, httptest.NewRequest("POST", "/api/access-profiles/x", strings.NewReader(body)))
	return w
}

func decodeErr(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("non-JSON error body %q: %v", w.Body.String(), err)
	}
	return body["error"]
}

// TestAccessProfilesGet renders the three tables as one document, with the
// column-level and table-level flags told apart by the column field.
func TestAccessProfilesGet(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	ts := time.Date(2026, 8, 29, 10, 0, 0, 0, time.UTC)
	mock.ExpectQuery("FROM table_flags").WillReturnRows(sqlmock.NewRows(
		[]string{"schema_name", "table_name", "column_name", "flag", "created_at"}).
		AddRow("app", "customers", "", "billing", ts).
		AddRow("app", "customers", "email", "pii", ts))
	mock.ExpectQuery("FROM profiles").WillReturnRows(sqlmock.NewRows(
		[]string{"name", "description", "created_at"}).AddRow("marketing", "Marketing analysts", ts))
	mock.ExpectQuery("FROM access_rules").WillReturnRows(sqlmock.NewRows(
		[]string{"name", "flag", "permission", "created_at"}).AddRow("marketing", "pii", "deny", ts))

	s := newBootServer(db)
	w := httptest.NewRecorder()
	s.handleAccessProfilesGet(w, httptest.NewRequest("GET", "/api/access-profiles", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
	}
	var doc accessProfilesDoc
	if err := json.Unmarshal(w.Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Flags) != 2 || doc.Flags[0].Column != "" || doc.Flags[1].Column != "email" || doc.Flags[1].Flag != "pii" {
		t.Errorf("flags = %+v", doc.Flags)
	}
	if len(doc.Profiles) != 1 || doc.Profiles[0].Name != "marketing" || doc.Profiles[0].Description != "Marketing analysts" {
		t.Errorf("profiles = %+v", doc.Profiles)
	}
	if len(doc.Rules) != 1 || doc.Rules[0].Profile != "marketing" || doc.Rules[0].Permission != "deny" {
		t.Errorf("rules = %+v", doc.Rules)
	}
	if doc.Flags[0].CreatedAt != "2026-08-29 10:00:00" {
		t.Errorf("created_at = %q, want the console's UTC wire format", doc.Flags[0].CreatedAt)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestAccessProfilesGetEmptyIsLists pins the empty document: three empty
// lists, never null, so the page can render "none yet" without a guard.
func TestAccessProfilesGetEmptyIsLists(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	expectAccessDoc(mock)
	s := newBootServer(db)
	w := httptest.NewRecorder()
	s.handleAccessProfilesGet(w, httptest.NewRequest("GET", "/api/access-profiles", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
	}
	if got := strings.TrimSpace(w.Body.String()); got != `{"flags":[],"profiles":[],"rules":[]}` {
		t.Errorf("empty document = %s", got)
	}
}

// TestAccessProfilesRefusalsAreTheSharedMessages pins that the console's
// 4xx bodies are the shared package's own words (the words the CLI refuses
// with), and that a refusal reaches no Exec: sqlmock has nothing queued, so
// a write here fails the test as an unexpected call. The table is every
// verb crossed with each of its required fields, plus the column widths.
func TestAccessProfilesRefusalsAreTheSharedMessages(t *testing.T) {
	long := func(n int) string { return strings.Repeat("x", n) }
	cases := []struct {
		name    string
		handler func(*Server, http.ResponseWriter, *http.Request)
		body    string
		want    error
	}{
		// flag add
		{"flag add without name", (*Server).handleAccessFlagAdd, `{"schema":"s","table":"t"}`,
			&accessprofiles.MissingFieldError{Field: "flag name"}},
		{"flag add without schema", (*Server).handleAccessFlagAdd, `{"flag":"pii","table":"t"}`,
			&accessprofiles.MissingFieldError{Field: "schema"}},
		{"flag add without table", (*Server).handleAccessFlagAdd, `{"flag":"pii","schema":"s"}`,
			&accessprofiles.MissingFieldError{Field: "table"}},
		{"flag add with a blank table", (*Server).handleAccessFlagAdd, `{"flag":"pii","schema":"s","table":"   "}`,
			&accessprofiles.MissingFieldError{Field: "table"}},
		// flag remove
		{"flag remove without name", (*Server).handleAccessFlagRemove, `{"schema":"s","table":"t"}`,
			&accessprofiles.MissingFieldError{Field: "flag name"}},
		{"flag remove without schema", (*Server).handleAccessFlagRemove, `{"flag":"pii","table":"t"}`,
			&accessprofiles.MissingFieldError{Field: "schema"}},
		{"flag remove without table", (*Server).handleAccessFlagRemove, `{"flag":"pii","schema":"s"}`,
			&accessprofiles.MissingFieldError{Field: "table"}},
		// profile add / remove
		{"profile add without name", (*Server).handleAccessProfileAdd, `{"description":"x"}`,
			&accessprofiles.MissingFieldError{Field: "profile name"}},
		{"profile remove without name", (*Server).handleAccessProfileRemove, `{}`,
			&accessprofiles.MissingFieldError{Field: "profile name"}},
		// rule add (the permission is checked first, so the field cases
		// carry a valid one)
		{"rule add with a bad permission", (*Server).handleAccessRuleAdd, `{"profile":"p","flag":"f","permission":"readwrite"}`,
			&accessprofiles.InvalidPermissionError{Got: "readwrite"}},
		{"rule add without permission", (*Server).handleAccessRuleAdd, `{"profile":"p","flag":"f"}`,
			&accessprofiles.InvalidPermissionError{Got: ""}},
		{"rule add without profile", (*Server).handleAccessRuleAdd, `{"flag":"f","permission":"deny"}`,
			&accessprofiles.MissingFieldError{Field: "profile"}},
		{"rule add without flag", (*Server).handleAccessRuleAdd, `{"profile":"p","permission":"deny"}`,
			&accessprofiles.MissingFieldError{Field: "flag"}},
		// rule remove
		{"rule remove without profile", (*Server).handleAccessRuleRemove, `{"flag":"f"}`,
			&accessprofiles.MissingFieldError{Field: "profile"}},
		{"rule remove without flag", (*Server).handleAccessRuleRemove, `{"profile":"p"}`,
			&accessprofiles.MissingFieldError{Field: "flag"}},
		// column widths: refused here, never as a raw 1406 from the database
		{"flag add with a long schema", (*Server).handleAccessFlagAdd, `{"flag":"pii","schema":"` + long(65) + `","table":"t"}`,
			&accessprofiles.TooLongError{Field: "schema", Got: 65, Max: 64, Unit: "characters"}},
		{"flag add with a long flag name", (*Server).handleAccessFlagAdd, `{"flag":"` + long(256) + `","schema":"s","table":"t"}`,
			&accessprofiles.TooLongError{Field: "flag name", Got: 256, Max: 255, Unit: "characters"}},
		{"flag remove with a long column", (*Server).handleAccessFlagRemove, `{"flag":"pii","schema":"s","table":"t","column":"` + long(65) + `"}`,
			&accessprofiles.TooLongError{Field: "column", Got: 65, Max: 64, Unit: "characters"}},
		{"profile add with a long name", (*Server).handleAccessProfileAdd, `{"name":"` + long(256) + `"}`,
			&accessprofiles.TooLongError{Field: "profile name", Got: 256, Max: 255, Unit: "characters"}},
		{"profile add with a long description", (*Server).handleAccessProfileAdd, `{"name":"p","description":"` + long(65536) + `"}`,
			&accessprofiles.TooLongError{Field: "description", Got: 65536, Max: 65535, Unit: "bytes"}},
		{"rule add with a long flag", (*Server).handleAccessRuleAdd, `{"profile":"p","flag":"` + long(256) + `","permission":"deny"}`,
			&accessprofiles.TooLongError{Field: "flag", Got: 256, Max: 255, Unit: "characters"}},
		{"rule remove with a long profile", (*Server).handleAccessRuleRemove, `{"profile":"` + long(256) + `","flag":"f"}`,
			&accessprofiles.TooLongError{Field: "profile", Got: 256, Max: 255, Unit: "characters"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, closeDB := newSQLMock(t)
			defer closeDB()
			w := driveAccess(t, newBootServer(db), tc.handler, tc.body)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("code=%d body=%s, want 400", w.Code, w.Body.String())
			}
			if got := decodeErr(t, w); got != tc.want.Error() {
				t.Errorf("error = %q, want the shared message %q", got, tc.want.Error())
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

// TestAccessProfilesTrimsNames: the page trims its inputs, and so does the
// API, so "marketing " typed into another client is the same profile, and
// a flag on "customers " is a flag on customers. Pinned on the arguments
// that reach the database AND on the audit event: the writer trims, so the
// event must name the row as stored, not the bytes as typed.
func TestAccessProfilesTrimsNames(t *testing.T) {
	rec := audittest.Install(t)
	oneEvent := func(t *testing.T, action string) ext.AuditEvent {
		t.Helper()
		evs := rec.Events()
		if len(evs) != 1 || evs[0].Action != action {
			t.Fatalf("audit events = %+v, want one %s", evs, action)
		}
		return evs[0]
	}
	t.Run("flag add", func(t *testing.T) {
		rec.Reset()
		db, mock, closeDB := newSQLMock(t)
		defer closeDB()
		expectFlagLookup(mock, nil)
		mock.ExpectExec("INSERT INTO table_flags").WithArgs("app", "customers", "email", "pii").
			WillReturnResult(sqlmock.NewResult(1, 1))
		expectAccessDoc(mock)
		w := driveAccess(t, newBootServer(db), (*Server).handleAccessFlagAdd,
			`{"flag":" pii ","schema":"app ","table":" customers","column":"email\t"}`)
		if w.Code != http.StatusOK {
			t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
		}
		ev := oneEvent(t, "flag.add")
		if ev.Schema != "app" || ev.Table != "customers" || ev.Detail["flag"] != "pii" || ev.Detail["column"] != "email" {
			t.Errorf("audit names the untrimmed input: schema=%q table=%q detail=%v", ev.Schema, ev.Table, ev.Detail)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
	t.Run("flag remove", func(t *testing.T) {
		rec.Reset()
		db, mock, closeDB := newSQLMock(t)
		defer closeDB()
		mock.ExpectExec("DELETE FROM table_flags").WithArgs("app", "customers", "", "pii").
			WillReturnResult(sqlmock.NewResult(0, 1))
		expectAccessDoc(mock)
		w := driveAccess(t, newBootServer(db), (*Server).handleAccessFlagRemove,
			`{"flag":"pii ","schema":" app","table":"customers "}`)
		if w.Code != http.StatusOK {
			t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
		}
		ev := oneEvent(t, "flag.remove")
		if ev.Schema != "app" || ev.Table != "customers" || ev.Detail["flag"] != "pii" {
			t.Errorf("audit names the untrimmed input: schema=%q table=%q detail=%v", ev.Schema, ev.Table, ev.Detail)
		}
	})
	t.Run("profile add", func(t *testing.T) {
		rec.Reset()
		db, mock, closeDB := newSQLMock(t)
		defer closeDB()
		mock.ExpectQuery("SELECT name FROM profiles").WithArgs("marketing").
			WillReturnRows(sqlmock.NewRows([]string{"name"}))
		mock.ExpectExec("INSERT INTO profiles").WithArgs("marketing", "Marketing analysts").
			WillReturnResult(sqlmock.NewResult(1, 1))
		expectAccessDoc(mock)
		w := driveAccess(t, newBootServer(db), (*Server).handleAccessProfileAdd,
			`{"name":"marketing ","description":" Marketing analysts "}`)
		if w.Code != http.StatusOK {
			t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
		}
		if ev := oneEvent(t, "profile.add"); ev.Detail["profile"] != "marketing" {
			t.Errorf("audit names the untrimmed input: %v", ev.Detail)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
	t.Run("profile remove", func(t *testing.T) {
		rec.Reset()
		db, mock, closeDB := newSQLMock(t)
		defer closeDB()
		mock.ExpectExec("DELETE FROM profiles").WithArgs("marketing").WillReturnResult(sqlmock.NewResult(0, 1))
		expectAccessDoc(mock)
		w := driveAccess(t, newBootServer(db), (*Server).handleAccessProfileRemove, `{"name":" marketing "}`)
		if w.Code != http.StatusOK {
			t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
		}
		if ev := oneEvent(t, "profile.remove"); ev.Detail["profile"] != "marketing" {
			t.Errorf("audit names the untrimmed input: %v", ev.Detail)
		}
	})
	t.Run("rule add", func(t *testing.T) {
		rec.Reset()
		db, mock, closeDB := newSQLMock(t)
		defer closeDB()
		mock.ExpectQuery("SELECT id FROM profiles").WithArgs("marketing").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(7)))
		mock.ExpectExec("INSERT INTO access_rules").WithArgs(int64(7), "pii", "deny").WillReturnResult(sqlmock.NewResult(1, 1))
		expectAccessDoc(mock)
		w := driveAccess(t, newBootServer(db), (*Server).handleAccessRuleAdd,
			`{"profile":"marketing ","flag":" pii","permission":" deny "}`)
		if w.Code != http.StatusOK {
			t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
		}
		ev := oneEvent(t, "access.add")
		if ev.Detail["profile"] != "marketing" || ev.Detail["flag"] != "pii" || ev.Detail["permission"] != "deny" {
			t.Errorf("audit names the untrimmed input: %v", ev.Detail)
		}
	})
	t.Run("rule remove", func(t *testing.T) {
		rec.Reset()
		db, mock, closeDB := newSQLMock(t)
		defer closeDB()
		mock.ExpectExec("DELETE ar FROM access_rules").WithArgs("marketing", "pii").
			WillReturnResult(sqlmock.NewResult(0, 1))
		expectAccessDoc(mock)
		w := driveAccess(t, newBootServer(db), (*Server).handleAccessRuleRemove, `{"profile":" marketing","flag":"pii "}`)
		if w.Code != http.StatusOK {
			t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
		}
		ev := oneEvent(t, "access.remove")
		if ev.Detail["profile"] != "marketing" || ev.Detail["flag"] != "pii" {
			t.Errorf("audit names the untrimmed input: %v", ev.Detail)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
}

// TestAccessProfilesCaseCollisionIs409: the unique keys fold case and
// accents, so adding "Marketing" beside "marketing" would have updated the
// existing row and answered as if a profile had been added, and "PII" on a
// table that carries "pii" would have kept the stored spelling and
// answered as if a flag had been added. Both are refused, naming the row
// that is there, nothing is written and nothing is audited.
func TestAccessProfilesCaseCollisionIs409(t *testing.T) {
	rec := audittest.Install(t)
	t.Run("profile", func(t *testing.T) {
		rec.Reset()
		db, mock, closeDB := newSQLMock(t)
		defer closeDB()
		expectProfileLookup(mock, "marketing")
		w := driveAccess(t, newBootServer(db), (*Server).handleAccessProfileAdd, `{"name":"Marketing"}`)
		if w.Code != http.StatusConflict {
			t.Fatalf("code=%d body=%s, want 409", w.Code, w.Body.String())
		}
		if got := decodeErr(t, w); got != `a profile named "marketing" already exists (the index compares names without regard to case or accents)` {
			t.Errorf("error = %q", got)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
	t.Run("flag", func(t *testing.T) {
		rec.Reset()
		db, mock, closeDB := newSQLMock(t)
		defer closeDB()
		expectFlagLookup(mock, &accessprofiles.Flag{Schema: "app", Table: "customers", Column: "email", Name: "pii"})
		w := driveAccess(t, newBootServer(db), (*Server).handleAccessFlagAdd,
			`{"flag":"PII","schema":"app","table":"customers","column":"email"}`)
		if w.Code != http.StatusConflict {
			t.Fatalf("code=%d body=%s, want 409", w.Code, w.Body.String())
		}
		if got := decodeErr(t, w); got != `flag "pii" already exists on app.customers (email) (the index compares names without regard to case or accents)` {
			t.Errorf("error = %q", got)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
	if evs := rec.Events(); len(evs) != 0 {
		t.Errorf("a refused add audited %+v", evs)
	}
}

// TestAccessProfilesNotFoundIs404 covers the rows that were not there: a
// remove that deleted nothing, and a rule for a profile that does not
// exist. The message is the shared one in each case.
func TestAccessProfilesNotFoundIs404(t *testing.T) {
	t.Run("flag remove", func(t *testing.T) {
		db, mock, closeDB := newSQLMock(t)
		defer closeDB()
		mock.ExpectExec("DELETE FROM table_flags").WillReturnResult(sqlmock.NewResult(0, 0))
		w := driveAccess(t, newBootServer(db), (*Server).handleAccessFlagRemove,
			`{"flag":"pii","schema":"app","table":"users","column":"email"}`)
		if w.Code != http.StatusNotFound {
			t.Fatalf("code=%d body=%s, want 404", w.Code, w.Body.String())
		}
		if got := decodeErr(t, w); got != `flag "pii" not found on app.users (email)` {
			t.Errorf("error = %q", got)
		}
	})
	t.Run("profile remove", func(t *testing.T) {
		db, mock, closeDB := newSQLMock(t)
		defer closeDB()
		mock.ExpectExec("DELETE FROM profiles").WillReturnResult(sqlmock.NewResult(0, 0))
		w := driveAccess(t, newBootServer(db), (*Server).handleAccessProfileRemove, `{"name":"ghost"}`)
		if w.Code != http.StatusNotFound {
			t.Fatalf("code=%d body=%s, want 404", w.Code, w.Body.String())
		}
		if got := decodeErr(t, w); got != `profile "ghost" not found` {
			t.Errorf("error = %q", got)
		}
	})
	t.Run("rule add for an unknown profile", func(t *testing.T) {
		db, mock, closeDB := newSQLMock(t)
		defer closeDB()
		mock.ExpectQuery("SELECT id FROM profiles").WillReturnRows(sqlmock.NewRows([]string{"id"}))
		w := driveAccess(t, newBootServer(db), (*Server).handleAccessRuleAdd,
			`{"profile":"ghost","flag":"pii","permission":"deny"}`)
		if w.Code != http.StatusNotFound {
			t.Fatalf("code=%d body=%s, want 404", w.Code, w.Body.String())
		}
		if got := decodeErr(t, w); got != `profile "ghost" not found` {
			t.Errorf("error = %q", got)
		}
	})
	t.Run("rule remove", func(t *testing.T) {
		db, mock, closeDB := newSQLMock(t)
		defer closeDB()
		mock.ExpectExec("DELETE ar FROM access_rules").WillReturnResult(sqlmock.NewResult(0, 0))
		w := driveAccess(t, newBootServer(db), (*Server).handleAccessRuleRemove, `{"profile":"p","flag":"f"}`)
		if w.Code != http.StatusNotFound {
			t.Fatalf("code=%d body=%s, want 404", w.Code, w.Body.String())
		}
		if got := decodeErr(t, w); got != `access rule not found: profile="p" flag="f"` {
			t.Errorf("error = %q", got)
		}
	})
}

// TestAccessProfilesLegacyIndexIs422: an index created before the RBAC
// tables existed answers MySQL 1146; the page must say the tables are
// missing rather than show a raw database error as a 500.
func TestAccessProfilesLegacyIndexIs422(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	noTable := &mysql.MySQLError{Number: 1146, Message: "Table 'idx.table_flags' doesn't exist"}
	mock.ExpectQuery("FROM table_flags").WillReturnError(noTable)
	s := newBootServer(db)
	w := httptest.NewRecorder()
	s.handleAccessProfilesGet(w, httptest.NewRequest("GET", "/api/access-profiles", nil))
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("code=%d body=%s, want 422", w.Code, w.Body.String())
	}
	if got := decodeErr(t, w); !strings.Contains(got, "no access profile tables") {
		t.Errorf("error = %q", got)
	}
}

// TestAccessProfilesMalformedBodyIs400 pins the decode path and that a
// refused body touches no database.
func TestAccessProfilesMalformedBodyIs400(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	w := driveAccess(t, newBootServer(db), (*Server).handleAccessFlagAdd, `{bad`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("code=%d body=%s, want 400", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestAccessProfilesInvalidatesSessionProfileCache: a scoped session's
// profile rules are cached per server for 30 seconds. A mutation must drop
// that server's entries, or a deny rule removed from the page keeps
// redacting (or, the other way, a deny rule added keeps NOT redacting) for
// up to half a minute for every profiled session.
func TestAccessProfilesInvalidatesSessionProfileCache(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	expectProfileLookup(mock, "")
	mock.ExpectExec("INSERT INTO profiles").WillReturnResult(sqlmock.NewResult(1, 1))
	expectAccessDoc(mock)
	s := newBootServer(db)
	// Seed a cached resolution for the boot server under the key shape
	// applySessionProfile uses (server id, NUL, profile).
	key := s.cm.defaultID() + "\x00marketing"
	s.sessionProfiles.m[key] = profileRuleEntry{exists: true, loadedAt: time.Now(),
		redact: []query.SchemaTableColumn{{Schema: "app", Table: "customers", Column: "email"}}}
	other := "other-server\x00marketing"
	s.sessionProfiles.m[other] = profileRuleEntry{exists: true, loadedAt: time.Now()}

	w := driveAccess(t, s, (*Server).handleAccessProfileAdd, `{"name":"marketing"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
	}
	if _, still := s.sessionProfiles.m[key]; still {
		t.Error("the selected server's cached profile rules survived a mutation")
	}
	if _, kept := s.sessionProfiles.m[other]; !kept {
		t.Error("another server's cached rules were dropped; invalidation must be per server")
	}
}

// newAccessPolicyServer builds a full Server (middleware included) over a
// sqlmock boot index, for the RBAC cases below.
func newAccessPolicyServer(t *testing.T) (*Server, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, closeDB := newSQLMock(t)
	t.Cleanup(closeDB)
	srv, err := New(Config{
		DB: db, DBName: "idx",
		Listen: "127.0.0.1:8090", Token: "static-tok", NoArchive: true,
		AuthPath: filepath.Join(t.TempDir(), "auth.yaml"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return srv, mock
}

func accessReq(t *testing.T, srv *Server, method, path, bearer, body string) *httptest.ResponseRecorder {
	t.Helper()
	var r *strings.Reader
	if body != "" {
		r = strings.NewReader(body)
	} else {
		r = strings.NewReader("")
	}
	req := httptest.NewRequest(method, "http://127.0.0.1:8090"+path, r)
	req.Host = "127.0.0.1:8090"
	req.Header.Set("Authorization", "Bearer "+bearer)
	if method != http.MethodGet {
		req.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	return w
}

// TestAccessProfilesRBAC drives the real middleware chain: a session with
// settings:read reads the document (200) and is refused every mutation
// (403 naming settings:write, with nothing written); a session with
// settings:write authors.
func TestAccessProfilesRBAC(t *testing.T) {
	srv, mock := newAccessPolicyServer(t)
	viewer, _, err := srv.sessions.IssueWithPolicy("auditor@example.com",
		&ext.AccessPolicy{Permissions: []ext.Permission{ext.PermSettingsRead}})
	if err != nil {
		t.Fatal(err)
	}
	admin, _, err := srv.sessions.IssueWithPolicy("admin@example.com",
		&ext.AccessPolicy{Permissions: []ext.Permission{ext.PermSettingsRead, ext.PermSettingsWrite}})
	if err != nil {
		t.Fatal(err)
	}

	expectAccessDoc(mock)
	if w := accessReq(t, srv, "GET", "/api/access-profiles", viewer, ""); w.Code != http.StatusOK {
		t.Fatalf("viewer GET = %d body=%s, want 200", w.Code, w.Body.String())
	}
	mutations := []struct{ path, body string }{
		{"/api/access-profiles/flags", `{"flag":"pii","schema":"app","table":"users"}`},
		{"/api/access-profiles/flags/remove", `{"flag":"pii","schema":"app","table":"users"}`},
		{"/api/access-profiles/profiles", `{"name":"marketing"}`},
		{"/api/access-profiles/profiles/remove", `{"name":"marketing"}`},
		{"/api/access-profiles/rules", `{"profile":"marketing","flag":"pii","permission":"deny"}`},
		{"/api/access-profiles/rules/remove", `{"profile":"marketing","flag":"pii"}`},
	}
	for _, m := range mutations {
		w := accessReq(t, srv, "POST", m.path, viewer, m.body)
		if w.Code != http.StatusForbidden {
			t.Errorf("viewer POST %s = %d body=%s, want 403", m.path, w.Code, w.Body.String())
			continue
		}
		if got := decodeErr(t, w); !strings.Contains(got, string(ext.PermSettingsWrite)) {
			t.Errorf("viewer POST %s error = %q, want it to name settings:write", m.path, got)
		}
	}
	// Nothing queued was consumed by a refused write.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}

	expectProfileLookup(mock, "")
	mock.ExpectExec("INSERT INTO profiles").WillReturnResult(sqlmock.NewResult(1, 1))
	expectAccessDoc(mock)
	if w := accessReq(t, srv, "POST", "/api/access-profiles/profiles", admin, `{"name":"marketing"}`); w.Code != http.StatusOK {
		t.Fatalf("admin POST profiles = %d body=%s, want 200", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestAccessProfilesRefusesDataProfileSession: a session that carries a
// data profile is refused every route of the surface, the GET included,
// even when its permissions include settings:write (a mutation could lift
// its own redaction; the GET lists the very tables and columns the profile
// withholds). Each refusal is audited as exactly one profile.denied, and
// nothing reaches the database: sqlmock has nothing queued.
func TestAccessProfilesRefusesDataProfileSession(t *testing.T) {
	rec := audittest.Install(t)
	srv, mock := newAccessPolicyServer(t)
	profiled, _, err := srv.sessions.IssueWithPolicy("sam@example.com",
		&ext.AccessPolicy{Permissions: ext.AllPermissions(), Profile: "marketing"})
	if err != nil {
		t.Fatal(err)
	}
	for _, rt := range accessRoutes {
		t.Run(rt.method+" "+rt.path, func(t *testing.T) {
			rec.Reset()
			w := accessReq(t, srv, rt.method, rt.path, profiled, rt.body)
			if w.Code != http.StatusForbidden {
				t.Fatalf("profiled %s %s = %d body=%s, want 403", rt.method, rt.path, w.Code, w.Body.String())
			}
			if got := decodeErr(t, w); got != accessProfilesRefusal {
				t.Errorf("error = %q", got)
			}
			evs := rec.Events()
			if len(evs) != 1 || evs[0].Action != "profile.denied" || evs[0].Actor != "sam@example.com" ||
				evs[0].Detail["surface_gate"] != "access_profiles" {
				t.Errorf("audit events = %+v, want one profile.denied for sam@example.com on access_profiles", evs)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

// TestAccessProfilesRefusedUnderStartupProfile: a console started under
// --profile refuses the whole surface too. The floor is the NAMED profile
// (profileActiveFor), not the resolved rules: a profile with no rules yet
// is a profile all the same, and a fresh index under `serve --profile` is
// exactly where the first rule would be authored, so the zero-rule case is
// the one that matters. No session is involved, so nothing is audited and
// the message names the startup profile, not a session.
func TestAccessProfilesRefusedUnderStartupProfile(t *testing.T) {
	rec := audittest.Install(t)
	cases := []struct {
		name          string
		deny          []query.SchemaTable
		profileActive bool
	}{
		{"named profile with no rules", nil, true},
		{"rules without the flag (older callers)", []query.SchemaTable{{Schema: "app", Table: "invoices"}}, false},
		{"named profile with rules", []query.SchemaTable{{Schema: "app", Table: "invoices"}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec.Reset()
			db, mock, closeDB := newSQLMock(t)
			defer closeDB()
			srv, err := New(Config{
				DB: db, DBName: "idx",
				Listen: "127.0.0.1:8090", Token: "static-tok", NoArchive: true,
				AuthPath:      filepath.Join(t.TempDir(), "auth.yaml"),
				DenyTables:    tc.deny,
				ProfileActive: tc.profileActive,
			})
			if err != nil {
				t.Fatal(err)
			}
			for _, rt := range accessRoutes {
				w := accessReq(t, srv, rt.method, rt.path, "static-tok", rt.body)
				if w.Code != http.StatusForbidden {
					t.Errorf("%s %s under a startup profile = %d body=%s, want 403", rt.method, rt.path, w.Code, w.Body.String())
					continue
				}
				if got := decodeErr(t, w); got != accessProfilesStartupRefusal {
					t.Errorf("%s %s error = %q", rt.method, rt.path, got)
				}
			}
			if evs := rec.Events(); len(evs) != 0 {
				t.Errorf("a startup-profile refusal audited %+v; only a session refusal is a profile.denied", evs)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

// TestAccessProfilesReadbackFailureSaysSaved: the write landed and was
// audited, then the readback failed. The body must say the change is in, so
// the operator reloads rather than repeats it (and the audit trail keeps
// the one event, not two).
func TestAccessProfilesReadbackFailureSaysSaved(t *testing.T) {
	rec := audittest.Install(t)
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	expectProfileLookup(mock, "")
	mock.ExpectExec("INSERT INTO profiles").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery("FROM table_flags").WillReturnError(errors.New("connection reset by peer"))
	w := driveAccess(t, newBootServer(db), (*Server).handleAccessProfileAdd, `{"name":"marketing"}`)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("code=%d body=%s, want 500", w.Code, w.Body.String())
	}
	got := decodeErr(t, w)
	if !strings.HasPrefix(got, accessReadbackFailedPrefix) || !strings.Contains(got, "connection reset by peer") {
		t.Errorf("error = %q, want it to open with %q and carry the readback error", got, accessReadbackFailedPrefix)
	}
	if evs := rec.Events(); len(evs) != 1 || evs[0].Action != "profile.add" {
		t.Errorf("audit events = %+v, want the one profile.add (the write is in)", evs)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestAccessProfilesAuditNamesTheServer: the audit detail always names the
// index the change landed on. Without the selection header the shared
// recorder writes no server at all, and a change made under the default
// selection is a change to a real index; with the header it is that id.
func TestAccessProfilesAuditNamesTheServer(t *testing.T) {
	rec := audittest.Install(t)
	for _, header := range []string{"", bootServerID} {
		t.Run("header="+header, func(t *testing.T) {
			rec.Reset()
			db, mock, closeDB := newSQLMock(t)
			defer closeDB()
			mock.ExpectExec("DELETE ar FROM access_rules").WillReturnResult(sqlmock.NewResult(0, 1))
			expectAccessDoc(mock)
			s := newBootServer(db)
			req := httptest.NewRequest("POST", "/api/access-profiles/rules/remove", strings.NewReader(`{"profile":"marketing","flag":"pii"}`))
			if header != "" {
				req.Header.Set(serverHeader, header)
			}
			w := httptest.NewRecorder()
			s.handleAccessRuleRemove(w, req)
			if w.Code != http.StatusOK {
				t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
			}
			evs := rec.Events()
			if len(evs) != 1 {
				t.Fatalf("audit events = %+v, want one access.remove", evs)
			}
			if want := s.cm.defaultID(); want == "" || evs[0].Detail["server"] != want {
				t.Errorf("Detail[server] = %q, want %q", evs[0].Detail["server"], want)
			}
		})
	}
}
