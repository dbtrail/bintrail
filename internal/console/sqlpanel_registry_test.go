package console

import (
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

// TestSQLPanel_registryReadFailure: with a baseline present and the archive
// registry unreadable, the panel would otherwise open a session without the
// events view it advertises, and every query on it would fail as a catalog
// error blamed on the operator's SQL. It refuses with the cause instead.
func TestSQLPanel_registryReadFailure(t *testing.T) {
	dir := t.TempDir()
	writeBaselineFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("FROM archive_state").WillReturnError(
		&mysql.MySQLError{Number: 1142, Message: "SELECT command denied"})

	srv := newSQLPanelServer(t, dir, true)
	srv.cm.boot.db = db
	rec, body := doServersReq(t, srv, "POST", "/api/sql", `{"sql":"SELECT 1"}`)
	if rec.Code != 502 {
		t.Fatalf("code = %d, body = %s; want 502", rec.Code, body)
	}
	if !strings.Contains(string(body), "archive_state") {
		t.Errorf("refusal does not name the registry: %s", body)
	}
}
