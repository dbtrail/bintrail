//go:build integration

package console

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationActivityLiveRetentionWindow drives the real handler against
// real MySQL and pins the #1352 design end to end:
//   - the window IS the live retention: since equals the oldest dated live
//     partition's hour, and the label says so;
//   - the read is single-tier: archive_state is DROPPED before the request,
//     and the Overview aggregate does not care — the pre-#1352 completeness
//     pass read it on every request;
//   - the response stamps refreshed_at (the freshness the tile renders).
func TestIntegrationActivityLiveRetentionWindow(t *testing.T) {
	srv, _ := seedConsoleData(t)
	db := srv.cm.boot.db

	// Give the seeded events (2026-06-01 12:00 / 12:05) a dated live partition
	// so the live floor is derivable, exactly as rotation maintains in
	// production.
	testutil.MustExec(t, db, `ALTER TABLE binlog_events REORGANIZE PARTITION p_future INTO (
		PARTITION p_2026060112 VALUES LESS THAN (TO_SECONDS('2026-06-01 13:00:00')),
		PARTITION p_future VALUES LESS THAN MAXVALUE)`)

	// The Overview must not read the archive tier at all (#1352 point 2): with
	// the table gone, a reintroduced archive_state read fails the request.
	testutil.MustExec(t, db, `DROP TABLE archive_state`)

	rec, body := doReq(t, srv, "GET", "/api/activity", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var got activityResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Since != "2026-06-01 12:00:00" {
		t.Errorf("since = %q, want the oldest live partition hour 2026-06-01 12:00:00", got.Since)
	}
	if !strings.HasPrefix(got.Label, "live retention · ~") {
		t.Errorf("label = %q, want a live-retention label", got.Label)
	}
	if got.Total != 2 || got.Inserts != 1 || got.Updates != 1 {
		t.Errorf("counts = total %d ins %d upd %d, want 2/1/1 (both seeded events inside the live window)",
			got.Total, got.Inserts, got.Updates)
	}
	if got.RefreshedAt == "" {
		t.Error("refreshed_at is empty — the materialization must disclose when it was computed")
	}
	if !got.Complete || len(got.Notes) != 0 {
		t.Errorf("complete = %v notes = %v; the live-retention window has nothing to caveat", got.Complete, got.Notes)
	}
}

// TestIntegrationActivityIsMaterialized pins the cache against real MySQL: a
// row inserted AFTER the first request must NOT appear in the second response
// — the numbers are a materialization, and what keeps that honest is the
// unchanged refreshed_at the tile renders.
func TestIntegrationActivityIsMaterialized(t *testing.T) {
	srv, _ := seedConsoleData(t)
	db := srv.cm.boot.db

	rec, body := doReq(t, srv, "GET", "/api/activity", "")
	if rec.Code != 200 {
		t.Fatalf("first code = %d, body = %s", rec.Code, body)
	}
	var first activityResponse
	if err := json.Unmarshal(body, &first); err != nil {
		t.Fatal(err)
	}

	// New activity lands after the materialization.
	testutil.InsertEvent(t, db, "bin.000001", 80, 120, "2026-06-01 12:10:00", nil,
		"app", "users", 3 /*DELETE*/, "1",
		nil, []byte(`{"id":1,"name":"alicia"}`), nil)

	rec, body = doReq(t, srv, "GET", "/api/activity", "")
	if rec.Code != 200 {
		t.Fatalf("second code = %d, body = %s", rec.Code, body)
	}
	var second activityResponse
	if err := json.Unmarshal(body, &second); err != nil {
		t.Fatal(err)
	}
	if second.Total != first.Total || second.Deletes != first.Deletes {
		t.Errorf("second response (total %d, deletes %d) re-scanned instead of serving the materialization (total %d, deletes %d)",
			second.Total, second.Deletes, first.Total, first.Deletes)
	}
	if second.RefreshedAt != first.RefreshedAt {
		t.Errorf("refreshed_at changed (%q → %q) without a recompute window elapsing", first.RefreshedAt, second.RefreshedAt)
	}
}
