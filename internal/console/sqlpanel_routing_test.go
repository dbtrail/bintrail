package console

import (
	"path/filepath"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// TestSQLPanel_readsLocalCopyWhenS3AlsoRegistered pins the panel's routing
// (#1456): it runs in the daemon, so an archive that is both on this host and
// in S3 is read from the local copy, like every other console read. Switching
// the panel to the portable routing would send this query to a bucket that
// does not exist and fail it on credentials.
func TestSQLPanel_readsLocalCopyWhenS3AlsoRegistered(t *testing.T) {
	archiveRoot := t.TempDir()
	const id = "11111111-2222-3333-4444-555555555555"
	writeSQLPanelArchive(t, archiveRoot, id)
	baselineRoot, _ := writeSQLPanelBaseline(t)

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cols := []string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}
	mock.ExpectQuery("FROM archive_state").WillReturnRows(sqlmock.NewRows(cols).
		AddRow(id, filepath.Join(archiveRoot, "bintrail_id="+id, "event_date=2026-05-01", "event_hour=03", "events.parquet"),
			"no-such-bucket-1456", "events/bintrail_id="+id+"/f.parquet"))

	srv := newSQLPanelServer(t, baselineRoot, true)
	srv.cm.boot.db = db
	rec, body := doServersReq(t, srv, "POST", "/api/sql", `{"sql":"SELECT count(*) AS n FROM events"}`)
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s; want the local copy served", rec.Code, body)
	}
	if !strings.Contains(string(body), `"rows":[[1]]`) {
		t.Errorf("unexpected result over the local archive: %s", body)
	}
}
