package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/buffer"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
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
// nethalo/dbtrail#1512.  The SaaS side forwards “params.gtid“ in the
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

// TestHandleRecover_rejectsEmptyScope pins the fail-loud guard added
// alongside #1512: a request with empty GTID and zero time bounds must
// return an error rather than fall back to recovering the last 1000
// events in the index (the silent shape that surfaced the original bug).
func TestHandleRecover_rejectsEmptyScope(t *testing.T) {
	buf := buffer.New(buffer.Config{MaxAge: time.Hour})
	h := &DefaultHandler{Buffer: buf}

	_, err := h.HandleRecover(context.Background(), RecoverRequest{
		Schema: "shop",
		Table:  "orders",
		// GTID, TimeStart, TimeEnd all zero — the unscoped shape.
	})
	if err == nil {
		t.Fatalf("expected error for unscoped recover, got nil")
	}
	if !strings.Contains(err.Error(), "recover requires gtid or time bounds") {
		t.Errorf("expected guard error message, got: %v", err)
	}
}

// TestHandleRecover_gtidNoMatch_largeFallback strengthens the no-match
// guarantee in TestHandleRecover_gtidNoMatch with a buffer larger than the
// historical Limit=1000 cap inside HandleRecover. If the GTID filter were
// dropped on the floor again the handler would now return reversal SQL for
// up to 1000 unrelated events; this test fails immediately in that case.
func TestHandleRecover_gtidNoMatch_largeFallback(t *testing.T) {
	now := time.Now().UTC()
	buf := buffer.New(buffer.Config{MaxAge: time.Hour})
	events := make([]parser.Event, 0, 1500)
	for i := 0; i < 1500; i++ {
		events = append(events, makeRecoverEvent(now, fmt.Sprintf("%d", i), fmt.Sprintf("abc:%d", i)))
	}
	buf.Insert(events)

	h := &DefaultHandler{Buffer: buf}

	// Broad time window covers every event in the buffer; only the GTID
	// filter stands between an unscoped fallback and an empty result.
	sql, err := h.HandleRecover(context.Background(), RecoverRequest{
		Schema:    "shop",
		Table:     "orders",
		GTID:      "non-matching:9999999",
		TimeStart: now.Add(-time.Hour),
		TimeEnd:   now.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("HandleRecover returned error: %v", err)
	}

	// No reversal statements should reference the table. A regression that
	// drops the GTID filter would return up to 1000 statements here.
	for _, stmt := range []string{"DELETE FROM `shop`", "INSERT INTO `shop`", "UPDATE `shop`"} {
		if strings.Contains(sql, stmt) {
			t.Errorf("expected no recovery statements for non-matching GTID on 1500-event buffer, got %q in:\n%s", stmt, sql)
		}
	}
}

// TestHandleRecover_rejectsOverCap is the regression test for #763: a
// recover scope matching more than recoverEventLimit events must be
// rejected outright instead of silently emitting reversal SQL for only the
// first recoverEventLimit of them. The prior behavior produced a script
// that looked complete (its header even reported the truncated count) while
// leaving the remaining matched rows half-reverted.
func TestHandleRecover_rejectsOverCap(t *testing.T) {
	now := time.Now().UTC()
	buf := buffer.New(buffer.Config{MaxAge: time.Hour})
	events := make([]parser.Event, 0, recoverEventLimit+1)
	for i := 0; i < recoverEventLimit+1; i++ {
		events = append(events, makeRecoverEvent(now, fmt.Sprintf("%d", i), fmt.Sprintf("abc:%d", i)))
	}
	buf.Insert(events)

	h := &DefaultHandler{Buffer: buf}

	_, err := h.HandleRecover(context.Background(), RecoverRequest{
		Schema:    "shop",
		Table:     "orders",
		TimeStart: now.Add(-time.Hour),
		TimeEnd:   now.Add(time.Hour),
	})
	if err == nil {
		t.Fatalf("expected error for over-cap recover scope, got nil")
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("more than %d events", recoverEventLimit)) {
		t.Errorf("expected cap-exceeded error, got: %v", err)
	}
}

// TestHandleRecover_pkHashesOverCap pins a deliberate trade-off: the cap
// check runs on the raw scope fetch, BEFORE the client-side PKHashes
// filter (handler.go applies PKHashes after MergeResults). So a
// PK-targeted recover ("reverse these 3 rows") over a busy time window
// with >recoverEventLimit total table events is rejected too, even though
// only a handful of rows are actually wanted — checking after the PK
// filter would let a wanted PK whose events fall past the cap boundary
// silently get a truncated reversal, which is the exact failure mode #763
// exists to prevent. Callers hitting this on a busy table should narrow
// the time range instead of relying on PKHashes to narrow it for them.
func TestHandleRecover_pkHashesOverCap(t *testing.T) {
	now := time.Now().UTC()
	buf := buffer.New(buffer.Config{MaxAge: time.Hour})
	events := make([]parser.Event, 0, recoverEventLimit+1)
	for i := 0; i < recoverEventLimit+1; i++ {
		events = append(events, makeRecoverEvent(now, fmt.Sprintf("%d", i), fmt.Sprintf("abc:%d", i)))
	}
	buf.Insert(events)

	h := &DefaultHandler{Buffer: buf}

	_, err := h.HandleRecover(context.Background(), RecoverRequest{
		Schema:    "shop",
		Table:     "orders",
		TimeStart: now.Add(-time.Hour),
		TimeEnd:   now.Add(time.Hour),
		PKHashes:  []string{byosPKHash("1")},
	})
	if err == nil {
		t.Fatalf("expected error for over-cap scope even with a narrow PKHashes filter, got nil")
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("more than %d events", recoverEventLimit)) {
		t.Errorf("expected cap-exceeded error, got: %v", err)
	}
}

// TestHandleRecover_atCapSucceeds confirms the cap check does not
// misfire on a scope of exactly recoverEventLimit events — only scopes
// that exceed the cap should be rejected.
func TestHandleRecover_atCapSucceeds(t *testing.T) {
	now := time.Now().UTC()
	buf := buffer.New(buffer.Config{MaxAge: time.Hour})
	events := make([]parser.Event, 0, recoverEventLimit)
	for i := 0; i < recoverEventLimit; i++ {
		events = append(events, makeRecoverEvent(now, fmt.Sprintf("%d", i), fmt.Sprintf("abc:%d", i)))
	}
	buf.Insert(events)

	h := &DefaultHandler{Buffer: buf}

	sql, err := h.HandleRecover(context.Background(), RecoverRequest{
		Schema:    "shop",
		Table:     "orders",
		TimeStart: now.Add(-time.Hour),
		TimeEnd:   now.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("HandleRecover returned error for exactly-at-cap scope: %v", err)
	}
	if !strings.Contains(sql, "DELETE FROM `shop`") {
		t.Errorf("expected reversal statements for the at-cap scope, got:\n%s", sql)
	}
}

// TestRecoverRequestJSON_gtid pins the wire format: the SaaS side sends
// “"gtid"“ (lowercase) and the agent must decode it into the GTID
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
// “gtid“ key (the pre-fix SaaS shape) still decodes cleanly.  This is
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

// TestHandleResolvePK_archiveFetchedOncePerTable is the regression test for
// #818: the archive fallback used to fetch the ENTIRE archived table once
// per batch item (per source). It must fetch each (source, schema, table)
// exactly once per resolve_pk command and reuse the hash index for every
// item in the batch.
func TestHandleResolvePK_archiveFetchedOncePerTable(t *testing.T) {
	calls := map[string]int{}
	h := &DefaultHandler{
		ArchiveSources: []string{"srcA"},
		ArchiveFetcher: func(ctx context.Context, opts query.Options, source string) ([]query.ResultRow, error) {
			calls[source+"|"+opts.Schema+"|"+opts.Table]++
			if opts.Schema == "shop" && opts.Table == "orders" {
				return []query.ResultRow{
					{PKValues: "1"},
					{PKValues: "1"}, // duplicate pk across events
					{PKValues: "2"},
				}, nil
			}
			return nil, nil
		},
	}

	req := ResolvePKRequest{Items: []PKItem{
		{PKHash: byosPKHash("1"), Schema: "shop", Table: "orders"},
		{PKHash: byosPKHash("2"), Schema: "shop", Table: "orders"},
		{PKHash: byosPKHash("999"), Schema: "shop", Table: "orders"}, // miss
		{PKHash: byosPKHash("u1"), Schema: "shop", Table: "users"},   // different table
	}}
	results, err := h.HandleResolvePK(context.Background(), req)
	if err != nil {
		t.Fatalf("HandleResolvePK: %v", err)
	}

	if got := calls["srcA|shop|orders"]; got != 1 {
		t.Errorf("shop.orders fetched %d times, want exactly 1 for the whole batch", got)
	}
	if got := calls["srcA|shop|users"]; got != 1 {
		t.Errorf("shop.users fetched %d times, want 1", got)
	}

	want := []PKResult{
		{PKHash: byosPKHash("1"), PKValues: "1", Found: true},
		{PKHash: byosPKHash("2"), PKValues: "2", Found: true},
		{PKHash: byosPKHash("999")},
		{PKHash: byosPKHash("u1")},
	}
	for i, w := range want {
		if results[i] != w {
			t.Errorf("results[%d] = %+v, want %+v", i, results[i], w)
		}
	}
}

// TestHandleResolvePK_archiveErrorNotCached: a failing source is skipped for
// the current item (warn-and-continue, unchanged behavior) and the failure is
// NOT memoized — the next item retries it, and a later healthy source still
// resolves the hash.
func TestHandleResolvePK_archiveErrorNotCached(t *testing.T) {
	fetches := map[string]int{}
	h := &DefaultHandler{
		ArchiveSources: []string{"bad", "good"},
		ArchiveFetcher: func(ctx context.Context, opts query.Options, source string) ([]query.ResultRow, error) {
			fetches[source]++
			if source == "bad" {
				return nil, fmt.Errorf("boom")
			}
			return []query.ResultRow{{PKValues: "42"}}, nil
		},
	}

	req := ResolvePKRequest{Items: []PKItem{
		{PKHash: byosPKHash("42"), Schema: "shop", Table: "orders"},
		{PKHash: byosPKHash("nope"), Schema: "shop", Table: "orders"},
	}}
	results, err := h.HandleResolvePK(context.Background(), req)
	if err != nil {
		t.Fatalf("HandleResolvePK: %v", err)
	}

	if !results[0].Found || results[0].PKValues != "42" {
		t.Errorf("results[0] = %+v, want found via healthy source", results[0])
	}
	if results[1].Found {
		t.Errorf("results[1] = %+v, want not found", results[1])
	}
	if got := fetches["bad"]; got != 2 {
		t.Errorf("failing source fetched %d times, want 2 (errors must not be cached)", got)
	}
	if got := fetches["good"]; got != 1 {
		t.Errorf("healthy source fetched %d times, want 1 (memoized)", got)
	}
}
