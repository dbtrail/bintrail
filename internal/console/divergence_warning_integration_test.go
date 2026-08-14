//go:build integration

package console

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/buffer"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The unit tests prove MergeResultsReport counts a diverging duplicate and
// appendDivergenceWarning renders it. This proves the count reaches the WIRE
// (#1325): the real handlers are driven end to end against a live index plus
// a registered Parquet archive holding (a) the live row's event_id with a
// DISAGREEING row image and (b) one archive-only event. (b) is the non-vacuous
// precondition: Count must include it, proving the archive was actually read
// and merged — without it, a silently-unread archive would let the warning
// assertions pass or fail for the wrong reason (the #1321 lesson).
//
// Mutating fetchRestricted to drop the diverged count, or either handler to
// skip appendDivergenceWarning, turns this red while the unit tests stay green.
func TestIntegrationMergeDivergenceReachesResponseWarnings(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.InsertEvent(t, db, "bin.000001", 4, 40, "2026-06-01 12:00:00", nil,
		"app", "users", 1 /*INSERT*/, "1", nil, nil, []byte(`{"id":1,"name":"alice"}`))
	var eventID uint64
	if err := db.QueryRow(`SELECT event_id FROM binlog_events LIMIT 1`).Scan(&eventID); err != nil {
		t.Fatalf("read event_id: %v", err)
	}

	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken})
	if err != nil {
		t.Fatal(err)
	}

	getEvents := func(t *testing.T) (int, []string) {
		t.Helper()
		r := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/events?schema=app&table=users", nil)
		r.Host = "127.0.0.1:8090"
		w := httptest.NewRecorder()
		srv.handleEvents(w, r)
		if w.Code != 200 {
			t.Fatalf("events code = %d, body = %s", w.Code, w.Body.String())
		}
		var resp struct {
			Count    int      `json:"count"`
			Warnings []string `json:"warnings"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v\n%s", err, w.Body.String())
		}
		return resp.Count, resp.Warnings
	}

	// Cry-wolf control: no archive registered → nothing merged, no warning.
	if _, warnings := getEvents(t); strings.Contains(strings.Join(warnings, "\n"), "disagreed") {
		t.Errorf("no archive → no divergence warning, got %#v", warnings)
	}

	// A valid archive: the live event's id with a mutated row image, plus an
	// archive-only sibling.
	ts := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	diverging := query.ResultRow{
		EventID: eventID, BinlogFile: "bin.000001", StartPos: 4, EndPos: 40,
		EventTimestamp: ts, SchemaName: "app", TableName: "users",
		EventType: 1, PKValues: "1",
		RowAfter: map[string]any{"id": 1, "name": "MUTATED"},
	}
	archiveOnly := query.ResultRow{
		EventID: eventID + 1000, BinlogFile: "bin.000001", StartPos: 41, EndPos: 80,
		EventTimestamp: ts.Add(time.Minute), SchemaName: "app", TableName: "users",
		EventType: 1, PKValues: "2",
		RowAfter: map[string]any{"id": 2, "name": "bob-from-archive"},
	}
	base := filepath.Join(t.TempDir(), "bintrail_id=div-test")
	hourDir := filepath.Join(base, "date=2026-06-01", "hour=12")
	if err := os.MkdirAll(hourDir, 0o755); err != nil {
		t.Fatal(err)
	}
	pq := filepath.Join(hourDir, "events.parquet")
	if _, err := buffer.WriteParquet([]query.ResultRow{diverging, archiveOnly}, pq, "none"); err != nil {
		t.Fatalf("WriteParquet: %v", err)
	}
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count, s3_bucket, s3_key, s3_uploaded_at)
		VALUES ('p_2026060112', 'div-test', ?, 2, NULL, NULL, NULL)`, pq)

	// Events browser: the divergence must land in the response warnings.
	count, warnings := getEvents(t)
	if count != 2 {
		t.Fatalf("count = %d, want 2 (live row + archive-only row) — the archive was not merged, so the warning assertion below would be vacuous; warnings: %#v", count, warnings)
	}
	got := strings.Join(warnings, "\n")
	if !strings.Contains(got, "1 duplicate event(s) disagreed between the live index and an archive copy") {
		t.Errorf("events response does not carry the divergence warning: %#v", warnings)
	}

	// Recover: the same finding is sharpest here — the kept copy's row images
	// become the reversal SQL the operator is reviewing.
	{
		body := strings.NewReader(`{"schema":"app","table":"users"}`)
		r := httptest.NewRequest("POST", "http://127.0.0.1:8090/api/recover", body)
		r.Host = "127.0.0.1:8090"
		w := httptest.NewRecorder()
		srv.handleRecover(w, r)
		if w.Code != 200 {
			t.Fatalf("recover code = %d, body = %s", w.Code, w.Body.String())
		}
		var resp struct {
			SQL      string   `json:"sql"`
			RowCount int      `json:"row_count"`
			Warnings []string `json:"warnings"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode recover: %v\n%s", err, w.Body.String())
		}
		if resp.RowCount != 2 || resp.SQL == "" {
			t.Fatalf("recover did not process the merged rows (row_count=%d, sql empty=%v) — the warning assertion below would be vacuous", resp.RowCount, resp.SQL == "")
		}
		if !strings.Contains(strings.Join(resp.Warnings, "\n"), "disagreed between the live index and an archive copy") {
			t.Errorf("recover response does not carry the divergence warning: %#v", resp.Warnings)
		}
	}
}
