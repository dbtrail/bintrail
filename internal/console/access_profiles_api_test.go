package console

import (
	"encoding/json"
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
// a write here fails the test as an unexpected call.
func TestAccessProfilesRefusalsAreTheSharedMessages(t *testing.T) {
	cases := []struct {
		name    string
		handler func(*Server, http.ResponseWriter, *http.Request)
		body    string
		want    error
	}{
		{"flag without schema", (*Server).handleAccessFlagAdd, `{"flag":"pii","table":"t"}`,
			&accessprofiles.MissingFieldError{Field: "schema"}},
		{"flag without name", (*Server).handleAccessFlagAdd, `{"schema":"s","table":"t"}`,
			&accessprofiles.MissingFieldError{Field: "flag name"}},
		{"flag remove without table", (*Server).handleAccessFlagRemove, `{"flag":"pii","schema":"s"}`,
			&accessprofiles.MissingFieldError{Field: "table"}},
		{"profile without name", (*Server).handleAccessProfileAdd, `{"description":"x"}`,
			&accessprofiles.MissingFieldError{Field: "profile name"}},
		{"rule with a bad permission", (*Server).handleAccessRuleAdd, `{"profile":"p","flag":"f","permission":"readwrite"}`,
			&accessprofiles.InvalidPermissionError{Got: "readwrite"}},
		{"rule without flag", (*Server).handleAccessRuleAdd, `{"profile":"p","permission":"deny"}`,
			&accessprofiles.MissingFieldError{Field: "flag"}},
		{"rule remove without profile", (*Server).handleAccessRuleRemove, `{"flag":"f"}`,
			&accessprofiles.MissingFieldError{Field: "profile"}},
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
// data profile is refused every mutation even when its permissions include
// settings:write (it could lift its own redaction), the refusal is audited
// as profile.denied, nothing is written, and the read still works (the
// configuration is not row data).
func TestAccessProfilesRefusesDataProfileSession(t *testing.T) {
	rec := audittest.Install(t)
	srv, mock := newAccessPolicyServer(t)
	profiled, _, err := srv.sessions.IssueWithPolicy("sam@example.com",
		&ext.AccessPolicy{Permissions: ext.AllPermissions(), Profile: "marketing"})
	if err != nil {
		t.Fatal(err)
	}
	w := accessReq(t, srv, "POST", "/api/access-profiles/rules/remove", profiled,
		`{"profile":"marketing","flag":"pii"}`)
	if w.Code != http.StatusForbidden {
		t.Fatalf("profiled POST = %d body=%s, want 403", w.Code, w.Body.String())
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

	expectAccessDoc(mock)
	if w := accessReq(t, srv, "GET", "/api/access-profiles", profiled, ""); w.Code != http.StatusOK {
		t.Errorf("profiled GET = %d body=%s, want 200 (configuration, not row data)", w.Code, w.Body.String())
	}
}
