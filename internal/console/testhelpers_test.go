package console

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/query"
)

// newBootServer builds a Server whose ephemeral boot entry wraps db — the
// multi-server equivalent of the pre-registry single-DB construction these
// tests used (&Server{db: ..., engine: ...}). The boot bundle is reachable as
// s.cm.boot for per-test tweaks (baseline gates, resolver, ...). noArchive is
// true, matching the old tests' default (no planner / archive discovery).
func newBootServer(db *sql.DB) *Server {
	s := &Server{token: "t", cm: newConnManager(nil, false)}
	s.cm.boot = &bundle{db: db, engine: query.New(db), noArchive: true, activity: newActivityCache()}
	s.mux = s.buildHandler()
	return s
}

// newSQLMock wraps sqlmock.New with the usual fatal-on-error + closer.
func newSQLMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	return db, mock, func() { db.Close() }
}
