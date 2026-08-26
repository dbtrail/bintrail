package console

import (
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/audittest"
)

// TestSQLPanel_registryReadFailure: with a baseline present and the archive
// registry unreadable, the panel still serves the half it can build (a state_*
// query is fully answerable) but never quietly: a success carries the note as
// a warning, and a failure on the missing events view carries it ahead of the
// engine's message, so it is not read as a typo in the operator's SQL.
func TestSQLPanel_registryReadFailure(t *testing.T) {
	dir, _ := writeSQLPanelBaseline(t) // a REAL Parquet: the session opens the state views
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for range 2 {
		mock.ExpectQuery("FROM archive_state").WillReturnError(
			&mysql.MySQLError{Number: 1142, Message: "SELECT command denied"})
	}
	srv := newSQLPanelServer(t, dir, true)
	srv.cm.boot.db = db
	audit := audittest.Install(t)

	rec, body := doServersReq(t, srv, "POST", "/api/sql", `{"sql":"SELECT 1"}`)
	if rec.Code != 200 {
		t.Fatalf("state-only query: code = %d, body = %s; want 200", rec.Code, body)
	}
	if !strings.Contains(string(body), `"warnings":[`) || !strings.Contains(string(body), "archive_state") {
		t.Errorf("success does not warn about the missing events view: %s", body)
	}
	if strings.Contains(string(body), "SELECT command denied") {
		t.Errorf("raw registry error reached the client: %s", body)
	}

	rec, body = doServersReq(t, srv, "POST", "/api/sql", `{"sql":"SELECT count(*) FROM events"}`)
	// 422, not 502: a missing catalog entry is an engine (statement) error,
	// which is the branch that carries the note. A reclassification to the
	// server-fault branch would drop the note and must fail here loudly.
	if rec.Code != 422 {
		t.Fatalf("query on the missing events view: code = %d, body = %s; want 422", rec.Code, body)
	}
	s := string(body)
	noteAt, engineAt := strings.Index(s, ". Note: the archive registry"), strings.Index(s, "events")
	if noteAt < 0 || engineAt < 0 || noteAt < engineAt {
		// The note FOLLOWS the engine message: *sqlUserError is the whole
		// user-error class (timeouts, policy refusals), and a note leading
		// the message would assert a cause for refusals that never touched
		// the events view.
		t.Errorf("note must follow the engine message: %s", s)
	}
	// The audit detail is the engine message alone: the same claim that
	// would mislabel a policy refusal in the body would mislabel it forever
	// in the audit log.
	var refused int
	for _, ev := range audit.Events() {
		if ev.Action != "sql.run" || ev.Detail["outcome"] != "refused" {
			continue
		}
		refused++
		if strings.Contains(ev.Detail["error"], "archive_state") {
			t.Errorf("audit detail carries the registry note: %q", ev.Detail["error"])
		}
	}
	if refused != 1 {
		t.Errorf("refused sql.run audit events = %d, want 1", refused)
	}
}
