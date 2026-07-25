package console

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/parser"
)

// auditEventRow feeds one indexed UPDATE through a sqlmock index — enough for
// the events and recover handlers to produce a real response (and therefore
// reach their audit emission) without a live MySQL.
func auditEventRow(t *testing.T) *sqlmock.Rows {
	t.Helper()
	cols := []string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
		"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
	}
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	return sqlmock.NewRows(cols).AddRow(
		int64(1), "bin.000001", int64(4), int64(40), ts,
		nil, nil, "app", "users", int64(parser.EventInsert), "42",
		nil, nil, []byte(`{"id":42,"email":"a@x"}`), int64(0), nil, nil,
	)
}

// TestAuditContract_ConsoleUnit is the console half of the #945 audit
// contract that needs no live index: every endpoint that returns row images
// or a reversal script emits on the audit seam, attributed to the request's
// authenticated identity — plus the authorization denial the console already
// recorded before #945.
//
// Behavioural on purpose: each case drives the real handler with a recording
// sink installed, so an emission that is moved onto an unreachable branch, or
// deleted, fails here. Handlers are driven in-process (no listener) because
// ext's sink is a process-wide variable — see audittest.Install.
func TestAuditContract_ConsoleUnit(t *testing.T) {
	rec := audittest.Install(t)

	cases := []struct {
		name       string
		action     string
		wantActor  string
		wantSchema string
		wantTable  string
		call       func(t *testing.T)
	}{
		{
			name:       "events",
			action:     "query.run",
			wantActor:  tokenActor,
			wantSchema: "app",
			wantTable:  "users",
			call: func(t *testing.T) {
				db, mock, closeDB := newSQLMock(t)
				defer closeDB()
				mock.ExpectQuery("FROM binlog_events").WillReturnRows(auditEventRow(t))
				s := newBootServer(db)
				w := httptest.NewRecorder()
				s.handleEvents(w, httptest.NewRequest("GET", "/api/events?schema=app&table=users", nil))
				if w.Code != http.StatusOK {
					t.Fatalf("events: code=%d body=%s", w.Code, w.Body.String())
				}
			},
		},
		{
			name:       "recover",
			action:     "recover.generate",
			wantActor:  tokenActor,
			wantSchema: "app",
			wantTable:  "users",
			call: func(t *testing.T) {
				db, mock, closeDB := newSQLMock(t)
				defer closeDB()
				mock.ExpectQuery("FROM binlog_events").WillReturnRows(auditEventRow(t))
				s := newBootServer(db)
				w := httptest.NewRecorder()
				s.handleRecover(w, httptest.NewRequest("POST", "/api/recover",
					strings.NewReader(`{"schema":"app","table":"users"}`)))
				if w.Code != http.StatusOK {
					t.Fatalf("recover: code=%d body=%s", w.Code, w.Body.String())
				}
			},
		},
		{
			name:       "verify explain",
			action:     "verify.explain",
			wantActor:  tokenActor,
			wantSchema: "wp",
			wantTable:  "posts",
			call: func(t *testing.T) {
				srv, ctrl := newVerifyTriggerServer(t)
				ctrl.explain = &VerifyExplanation{Schema: "wp", Table: "posts", Total: 1}
				id := addVerifyEntry(t, srv, "u:p@tcp(127.0.0.1:3306)/", "s3://b/baselines/", "")
				w, body := doServersReq(t, srv, "GET",
					"/api/servers/"+id+"/verify/explain?schema=wp&table=posts", "")
				if w.Code != http.StatusOK {
					t.Fatalf("verify explain: code=%d body=%s", w.Code, body)
				}
			},
		},
		{
			name:      "authz denial",
			action:    "authz.denied",
			wantActor: "ana@example.com",
			call: func(t *testing.T) {
				srv, err := New(Config{
					Listen: "127.0.0.1:8090", Token: "static-tok",
					AuthPath: filepath.Join(t.TempDir(), "auth.yaml"),
				})
				if err != nil {
					t.Fatal(err)
				}
				viewer := &ext.AccessPolicy{Permissions: []ext.Permission{ext.PermStatusRead}}
				tok, _, err := srv.sessions.IssueWithPolicy("ana@example.com", viewer)
				if err != nil {
					t.Fatal(err)
				}
				if w := getPath(t, srv, "127.0.0.1:8090", "/api/events", tok); w.Code != http.StatusForbidden {
					t.Fatalf("scoped GET /api/events = %d, want 403", w.Code)
				}
			},
		},
		{
			name:      "profile gate denial",
			action:    "profile.denied",
			wantActor: "sam@example.com",
			call: func(t *testing.T) {
				srv, err := New(Config{
					Listen: "127.0.0.1:8090", Token: "static-tok",
					AuthPath: filepath.Join(t.TempDir(), "auth.yaml"),
				})
				if err != nil {
					t.Fatal(err)
				}
				// A session carrying a data profile: cascade synthesis cannot
				// honor redaction, so its gate refuses before the request ever
				// reaches an index — no fixture needed.
				tok, _, err := srv.sessions.IssueWithPolicy("sam@example.com",
					&ext.AccessPolicy{Permissions: ext.AllPermissions(), Profile: "sensitive"})
				if err != nil {
					t.Fatal(err)
				}
				w := postJSON(t, srv, "/api/recover-cascade", tok, `{"schema":"app","table":"t","pk":"1"}`)
				if w.Code != http.StatusForbidden {
					t.Fatalf("profiled POST /api/recover-cascade = %d, want 403", w.Code)
				}
			},
		},
	}

	var observed []audittest.Pair
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec.Reset()
			tc.call(t)
			evs := rec.Events()
			if len(evs) != 1 {
				t.Fatalf("recorded %d audit events, want exactly 1: %+v", len(evs), evs)
			}
			ev := evs[0]
			if ev.Surface != "console" || ev.Action != tc.action {
				t.Errorf("event = %s/%s, want console/%s", ev.Surface, ev.Action, tc.action)
			}
			// The console authenticates its callers, so the actor is the
			// session identity (or the shared-token sentinel) — never the
			// daemon's OS owner, which says nothing about who asked.
			if ev.Actor != tc.wantActor {
				t.Errorf("actor = %q, want %q", ev.Actor, tc.wantActor)
			}
			if tc.wantSchema != "" && (ev.Schema != tc.wantSchema || ev.Table != tc.wantTable) {
				t.Errorf("schema/table = %q/%q, want %q/%q", ev.Schema, ev.Table, tc.wantSchema, tc.wantTable)
			}
			observed = append(observed, audittest.Pair{Surface: ev.Surface, Action: ev.Action})
		})
	}

	audittest.CheckCoverage(t, audittest.OwnerConsoleUnit, observed)
}

// TestAuditContract_ConsoleSilentOnRefusal pins the failure semantics: a
// request the console refuses returns no rows and no script, so it must not
// be recorded as a data access (the authz middleware's own denial event is a
// separate action, and this request never reaches the middleware).
func TestAuditContract_ConsoleSilentOnRefusal(t *testing.T) {
	rec := audittest.Install(t)
	s := newBootServer(nil)

	w := httptest.NewRecorder()
	s.handleRecover(w, httptest.NewRequest("POST", "/api/recover", strings.NewReader(`{bad`)))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("malformed body: code=%d, want 400", w.Code)
	}
	if evs := rec.Events(); len(evs) != 0 {
		t.Errorf("a refused request recorded %d audit events, want 0: %+v", len(evs), evs)
	}
}

// TestAuditContract_ConsoleActorFallsBackToToken documents the identity
// vocabulary: a session request records its verified login, a static-token
// request records the token sentinel — never "".
func TestAuditContract_ConsoleActorFallsBackToToken(t *testing.T) {
	r := httptest.NewRequest("GET", "/api/events", nil)
	if got := consoleActor(r); got != tokenActor {
		t.Errorf("consoleActor(no session) = %q, want %q", got, tokenActor)
	}
	withID := r.WithContext(context.WithValue(r.Context(), identityCtxKey{}, "ana@example.com"))
	if got := consoleActor(withID); got != "ana@example.com" {
		t.Errorf("consoleActor(session) = %q, want the verified identity", got)
	}
}
