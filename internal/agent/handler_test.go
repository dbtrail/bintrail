package agent

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/buffer"
	"github.com/dbtrail/bintrail/internal/parser"
)

// makeRecoverEvent builds a single INSERT-style parser.Event for the buffer.
// HandleRecover generates a reversing DELETE for an INSERT; that DELETE
// contains the PK column value, which the assertions match against.
func makeRecoverEvent(now time.Time, pk string, gtid string) parser.Event {
	return parser.Event{
		BinlogFile:    "binlog.000001",
		StartPos:      uint64(100),
		EndPos:        uint64(200),
		Timestamp:     now,
		GTID:          gtid,
		Schema:        "shop",
		Table:         "orders",
		EventType:     parser.EventInsert,
		PKValues:      pk,
		RowAfter:      map[string]any{"id": pk, "amount": "100"},
		SchemaVersion: 0,
	}
}

// TestHandleRecover_filterByGTID is the regression test for
// nethalo/dbtrail#1512.  The SaaS side forwards ``params.gtid`` in the
// WebSocket recover payload; this test confirms the BYOS agent honours
// it as the precise scope and ignores other events in the same table.
//
// Before the fix, RecoverRequest had no GTID field, so the field was
// silently dropped during JSON unmarshal and the agent fell back to
// time-only filtering — producing reversal SQL for unrelated events.
func TestHandleRecover_filterByGTID(t *testing.T) {
	now := time.Now().UTC()

	// Three events on the same table at the same timestamp.  Only the
	// middle one carries the requested GTID; the other two must NOT
	// appear in the recovery SQL even though they overlap the time
	// window.
	buf := buffer.New(buffer.Config{MaxAge: time.Hour})
	buf.Insert([]parser.Event{
		makeRecoverEvent(now, "1", "abc:1"),
		makeRecoverEvent(now, "42", "abc:42"),
		makeRecoverEvent(now, "99", "abc:99"),
	})

	h := &DefaultHandler{Buffer: buf}

	sql, err := h.HandleRecover(context.Background(), RecoverRequest{
		Schema: "shop",
		Table:  "orders",
		GTID:   "abc:42",
		// TimeStart / TimeEnd intentionally zero — the SaaS side skips
		// the 24h clamp on gtid-only calls (see #1512), so the agent
		// receives Go zero-time here and must NOT use it as a filter.
	})
	if err != nil {
		t.Fatalf("HandleRecover returned error: %v", err)
	}

	// The reversing DELETE for the matching INSERT must contain pk "42".
	// PK "1" and "99" come from non-matching GTIDs — their presence
	// means the GTID filter was ignored (the original #1512 bug).
	if !strings.Contains(sql, "= '42'") && !strings.Contains(sql, "=42") {
		t.Errorf("expected reversal SQL to reference pk=42, got:\n%s", sql)
	}
	if strings.Contains(sql, "= '1'") || strings.Contains(sql, "= '99'") {
		t.Errorf("non-matching GTID events leaked into recovery SQL:\n%s", sql)
	}
}

// TestHandleRecover_gtidNoMatch confirms that a GTID that matches no
// events returns empty SQL instead of falling back to the full table.
// Pre-fix, the GTID field was ignored and the handler returned reversal
// SQL for whatever events fell in the (possibly zero) time window.
func TestHandleRecover_gtidNoMatch(t *testing.T) {
	now := time.Now().UTC()
	buf := buffer.New(buffer.Config{MaxAge: time.Hour})
	buf.Insert([]parser.Event{
		makeRecoverEvent(now, "1", "abc:1"),
		makeRecoverEvent(now, "2", "abc:2"),
	})

	h := &DefaultHandler{Buffer: buf}

	sql, err := h.HandleRecover(context.Background(), RecoverRequest{
		Schema: "shop",
		Table:  "orders",
		GTID:   "does-not-exist:9999",
	})
	if err != nil {
		t.Fatalf("HandleRecover returned error: %v", err)
	}

	// No statements should reference the table — only the surrounding
	// transactional shell (BEGIN/COMMIT) and SET statements are
	// permitted.  In particular, DELETE / INSERT / UPDATE against the
	// real table must NOT appear.
	for _, stmt := range []string{"DELETE FROM `shop`", "INSERT INTO `shop`", "UPDATE `shop`"} {
		if strings.Contains(sql, stmt) {
			t.Errorf("expected no recovery statements for non-matching GTID, got %q in:\n%s", stmt, sql)
		}
	}
}

// TestRecoverRequestJSON_gtid pins the wire format: the SaaS side sends
// ``"gtid"`` (lowercase) and the agent must decode it into the GTID
// field.  A typo in the JSON tag would silently drop the field again.
func TestRecoverRequestJSON_gtid(t *testing.T) {
	raw := `{"schema":"shop","table":"orders","pk_hashes":["x"],"time_start":"0001-01-01T00:00:00Z","time_end":"0001-01-01T00:00:00Z","gtid":"abc:42"}`
	var req RecoverRequest
	if err := json.Unmarshal([]byte(raw), &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if req.GTID != "abc:42" {
		t.Errorf("GTID = %q, want %q (json tag regression — saas#1512 will reproduce)", req.GTID, "abc:42")
	}
	// Zero-time TimeStart/TimeEnd are the natural shape from a SaaS
	// gtid-only call.  Pin that they decode as time.Time{} so the
	// handler's IsZero() check works as designed.
	if !req.TimeStart.IsZero() || !req.TimeEnd.IsZero() {
		t.Errorf("expected zero-time bounds, got TimeStart=%v TimeEnd=%v", req.TimeStart, req.TimeEnd)
	}
}

// TestRecoverRequestJSON_backwardCompat confirms a payload WITHOUT the
// ``gtid`` key (the pre-fix SaaS shape) still decodes cleanly.  This is
// the forward/backward-compat guarantee — an older SaaS can still talk
// to a newer agent.
func TestRecoverRequestJSON_backwardCompat(t *testing.T) {
	raw := `{"schema":"shop","table":"orders","pk_hashes":["x"],"time_start":"2026-01-01T00:00:00Z","time_end":"2026-01-02T00:00:00Z"}`
	var req RecoverRequest
	if err := json.Unmarshal([]byte(raw), &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if req.GTID != "" {
		t.Errorf("GTID = %q, want \"\" (missing key → zero value)", req.GTID)
	}
	if req.TimeStart.IsZero() {
		t.Errorf("TimeStart should be parsed from JSON")
	}
}
