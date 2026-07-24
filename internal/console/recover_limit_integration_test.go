//go:build integration

package console

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// seedRecoverLimitConsole seeds 4 chained UPDATEs on one row (v0→v1→v2→v3→v4),
// mirroring internal/cli/recover_limit_integration_test.go's seedRecoverUpdates
// so the two suites exercise the identical scenario across CLI and console.
func seedRecoverLimitConsole(t *testing.T) (*Server, string) {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	for i := 1; i <= 4; i++ {
		ts := h.Add(time.Duration(i) * time.Minute).Format("2006-01-02 15:04:05")
		before := fmt.Sprintf(`{"id":1,"v":"v%d"}`, i-1)
		after := fmt.Sprintf(`{"id":1,"v":"v%d"}`, i)
		testutil.InsertEvent(t, db, "binlog.000001", uint64(i*100), uint64(i*100+50), ts, nil,
			dbName, "orders", 2 /*UPDATE*/, "1",
			[]byte(`["v"]`), []byte(before), []byte(after))
	}

	srv, err := New(Config{
		DB:        db,
		DBName:    dbName,
		Listen:    "127.0.0.1:8090",
		Token:     intToken,
		NoArchive: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return srv, dbName
}

// TestIntegrationRecover_limitKeepsNewestEvents is the console counterpart of
// the CLI's TestRecover_limitKeepsNewestEvents (#785) for #981: POST
// /api/recover with a truncating limit must reverse the NEWEST events of the
// matched window, not the oldest, and must surface a truncation warning so a
// partial-window recover is never presented as complete.
func TestIntegrationRecover_limitKeepsNewestEvents(t *testing.T) {
	srv, dbName := seedRecoverLimitConsole(t)

	rec, body := doReq(t, srv, "POST", "/api/recover",
		`{"schema":"`+dbName+`","table":"orders","limit":2}`)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, body)
	}
	var resp recoverResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, body)
	}

	if !strings.Contains(resp.SQL, `'v3'`) || !strings.Contains(resp.SQL, `'v2'`) {
		t.Errorf("expected reversals of the two NEWEST updates (v2→v3, v3→v4), got:\n%s", resp.SQL)
	}
	// v0/v1 exist only in the oldest two events — any occurrence means the
	// truncation kept the oldest prefix (the #981 bug).
	if strings.Contains(resp.SQL, `'v0'`) || strings.Contains(resp.SQL, `'v1'`) {
		t.Errorf("SQL reversed the OLDEST events; limit must keep the newest suffix of the window:\n%s", resp.SQL)
	}
	// Most-recent undone first: the reverse of v3→v4 (its WHERE carries 'v4')
	// must precede the reverse of v2→v3 (its SET carries 'v2').
	i4, i2 := strings.Index(resp.SQL, `'v4'`), strings.Index(resp.SQL, `'v2'`)
	if i4 == -1 || i2 == -1 || i4 > i2 {
		t.Errorf("expected the newest event's reversal first (most-recent undone first), got:\n%s", resp.SQL)
	}

	foundTruncationWarning := false
	for _, w := range resp.Warnings {
		if strings.Contains(strings.ToLower(w), "truncat") {
			foundTruncationWarning = true
			break
		}
	}
	if !foundTruncationWarning {
		t.Errorf("expected a truncation warning in the response, got warnings=%v", resp.Warnings)
	}
}

// TestIntegrationRecover_limitNotTruncatedNoWarning pins the negative case: a
// limit that comfortably covers the whole matched window must NOT emit a
// truncation warning.
func TestIntegrationRecover_limitNotTruncatedNoWarning(t *testing.T) {
	srv, dbName := seedRecoverLimitConsole(t)

	rec, body := doReq(t, srv, "POST", "/api/recover",
		`{"schema":"`+dbName+`","table":"orders","limit":1000}`)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, body)
	}
	var resp recoverResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, body)
	}

	if !strings.Contains(resp.SQL, `'v0'`) || !strings.Contains(resp.SQL, `'v1'`) {
		t.Errorf("expected the full window (including the oldest updates) when not truncated, got:\n%s", resp.SQL)
	}
	for _, w := range resp.Warnings {
		if strings.Contains(strings.ToLower(w), "truncat") {
			t.Errorf("did not expect a truncation warning when the limit exceeds the matched window, got warnings=%v", resp.Warnings)
		}
	}
}
