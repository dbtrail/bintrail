//go:build integration

package console

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// seedAckConsole is seedConsoleData without the event rows: this test needs the
// server AND the index handle, and no events at all.
func seedAckConsole(t *testing.T) (*Server, *sql.DB) {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, NoArchive: true})
	if err != nil {
		t.Fatal(err)
	}
	return srv, db
}

// TestIntegrationCaptureSkipsAck drives the acknowledge endpoint (#1314) against
// a real index: the write lands, /api/status reports it, the stale-render guard
// refuses a tab that saw a smaller count, and a later skip re-arms the alarm.
//
// It goes through srv.Handler() rather than calling the handler directly so the
// route registration and the authz table are exercised too — a handler nobody
// can reach is the failure mode a direct call cannot see.
func TestIntegrationCaptureSkipsAck(t *testing.T) {
	srv, db := seedAckConsole(t)
	ctx := context.Background()

	// Nothing recorded: refused, and NOT stamped — an acknowledgement written
	// over an empty ledger would pre-acknowledge the next skip.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO stream_state (id, mode, server_id, last_checkpoint, capture_skips)
		 VALUES (1, 'gtid', 7, UTC_TIMESTAMP(), '{}')`); err != nil {
		t.Fatalf("seed clean ledger: %v", err)
	}
	if rec, body := doReq(t, srv, "POST", "/api/capture-skips/ack", `{"seen_total":0}`); rec.Code != 400 {
		t.Fatalf("acknowledging a clean ledger: code = %d, body = %s", rec.Code, body)
	}

	if _, err := db.ExecContext(ctx,
		`UPDATE stream_state SET capture_skips = '{"column_count_mismatch":{"count":3,"last_at":"2026-08-04T10:00:00Z"}}' WHERE id = 1`); err != nil {
		t.Fatalf("seed skips: %v", err)
	}

	captureHealth := func(t *testing.T) map[string]any {
		t.Helper()
		rec, body := doReq(t, srv, "GET", "/api/status", "")
		if rec.Code != 200 {
			t.Fatalf("status code = %d, body = %s", rec.Code, body)
		}
		var parsed struct {
			Stream struct {
				CaptureHealth map[string]any `json:"capture_health"`
			} `json:"stream"`
		}
		if err := json.Unmarshal(body, &parsed); err != nil {
			t.Fatalf("status JSON: %v\n%s", err, body)
		}
		return parsed.Stream.CaptureHealth
	}

	if ch := captureHealth(t); ch["acknowledged"] == true {
		t.Fatalf("a fresh tally reported as acknowledged: %v", ch)
	}

	// The stale-render guard: this tab rendered 2, the index holds 3.
	rec, body := doReq(t, srv, "POST", "/api/capture-skips/ack", `{"seen_total":2}`)
	if rec.Code != 409 {
		t.Fatalf("a stale view must be refused with 409, got %d: %s", rec.Code, body)
	}
	if !strings.Contains(string(body), "reload") {
		t.Errorf("the 409 must tell the operator what to do, got: %s", body)
	}
	if ch := captureHealth(t); ch["acknowledged"] == true {
		t.Fatal("a refused acknowledgement was stamped anyway")
	}

	if rec, body := doReq(t, srv, "POST", "/api/capture-skips/ack", `{"seen_total":3}`); rec.Code != 200 {
		t.Fatalf("acknowledge code = %d, body = %s", rec.Code, body)
	}
	ch := captureHealth(t)
	if ch["acknowledged"] != true {
		t.Fatalf("capture health did not report the acknowledgement: %v", ch)
	}
	if at, _ := ch["acknowledged_at"].(string); at == "" {
		t.Errorf("acknowledged with no timestamp — the console has nothing to show: %v", ch)
	}
	// The verdict stays "degraded": the events are still missing, and a
	// consumer keying on the status string must not read a human's "seen it"
	// as the loss being undone.
	if ch["status"] != "degraded" {
		t.Errorf("acknowledging changed the verdict to %v; it must stay degraded", ch["status"])
	}
	if ch["total_skipped"] != float64(3) {
		t.Errorf("the tally changed on acknowledgement: %v", ch["total_skipped"])
	}

	// A later skip re-arms it with no operator action — the property that makes
	// a one-click "Mark as read" safe to offer at all.
	if _, err := db.ExecContext(ctx,
		`UPDATE stream_state SET capture_skips = '{"column_count_mismatch":{"count":4,"last_at":"2026-08-12T10:00:00Z"}}' WHERE id = 1`); err != nil {
		t.Fatalf("record a later skip: %v", err)
	}
	if ch := captureHealth(t); ch["acknowledged"] == true {
		t.Error("a skip AFTER the acknowledgement stayed acknowledged — the console would keep hiding new loss")
	}
}
