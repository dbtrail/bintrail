package forensics

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// ─── fixtures ─────────────────────────────────────────────────────────────────

// wcTime parses a "15:04:05"-style clock on a fixed test day.
func wcTime(t *testing.T, clock string) time.Time {
	t.Helper()
	ts, err := time.Parse("2006-01-02 15:04:05", "2026-06-15 "+clock)
	if err != nil {
		t.Fatalf("parse fixture time %q: %v", clock, err)
	}
	return ts
}

// wcEvent builds a binlog event fixture. connID 0 means "no connection id".
func wcEvent(t *testing.T, id uint64, clock string, connID uint32) query.ResultRow {
	t.Helper()
	r := query.ResultRow{
		EventID:        id,
		EventTimestamp: wcTime(t, clock),
		SchemaName:     "shop",
		TableName:      "orders",
		EventType:      event.EventUpdate,
		PKValues:       "42",
	}
	if connID != 0 {
		c := connID
		r.ConnectionID = &c
	}
	return r
}

// auditRec builds a normalised audit event fixture (timestamps in the same
// "2006-01-02 15:04:05" layout parseFlexTimestamp accepts).
func auditRec(clock, eventType, user, host string, connID int64, sqlText string) AuditEvent {
	return AuditEvent{
		Timestamp:    "2026-06-15 " + clock,
		EventType:    eventType,
		User:         user,
		Host:         host,
		ConnectionID: connID,
		SQLText:      sqlText,
	}
}

// ─── the lifetime-bounding fix (the required scenario) ────────────────────────

// TestAttributeFromAudit_ConnectionIDReuse proves the improvement over the
// SaaS implementation (forensics.py:271-285): when a connection id is reused
// within the scan window (pool churn), first-match-wins attributes BOTH events
// to whichever identity appears first in the log — here alice. Lifetime
// bounding must attribute each event to the session that actually contained
// it.
func TestAttributeFromAudit_ConnectionIDReuse(t *testing.T) {
	// Connection id 42 is used by two sessions inside one window:
	//   alice@app1:   CONNECT 10:00:00 .. DISCONNECT 10:00:30
	//   mallory@app2: CONNECT 10:00:40 .. DISCONNECT 10:01:10
	audit := []AuditEvent{
		auditRec("10:00:00", "CONNECT", "alice", "app1", 42, ""),
		auditRec("10:00:30", "DISCONNECT", "alice", "app1", 42, ""),
		auditRec("10:00:40", "CONNECT", "mallory", "app2", 42, ""),
		auditRec("10:01:10", "DISCONNECT", "mallory", "app2", 42, ""),
	}
	events := []query.ResultRow{
		wcEvent(t, 1, "10:00:10", 42), // inside alice's session
		wcEvent(t, 2, "10:00:50", 42), // inside mallory's session
	}

	got := attributeFromAudit(events, audit, false)

	a1, ok := got[0]
	if !ok {
		t.Fatal("event inside alice's session was not attributed")
	}
	if a1.User != "alice" || a1.Host != "app1" {
		t.Errorf("event 1 attributed to %s@%s, want alice@app1", a1.User, a1.Host)
	}
	if a1.Confidence != ConfidenceExact {
		t.Errorf("event 1 confidence = %q, want %q (lifetime-bounded)", a1.Confidence, ConfidenceExact)
	}

	a2, ok := got[1]
	if !ok {
		t.Fatal("event inside mallory's session was not attributed")
	}
	// The misattribution the SaaS first-match-wins produced: alice is the
	// first audit record for id 42 in the window, so the second event — which
	// happened during MALLORY's session — was blamed on alice.
	if a2.User == "alice" {
		t.Fatalf("event 2 attributed to alice — first-match-wins misattribution; lifetime bounding must pick the session containing the event")
	}
	if a2.User != "mallory" || a2.Host != "app2" {
		t.Errorf("event 2 attributed to %s@%s, want mallory@app2", a2.User, a2.Host)
	}
	if a2.Confidence != ConfidenceExact {
		t.Errorf("event 2 confidence = %q, want %q", a2.Confidence, ConfidenceExact)
	}
	for i, a := range got {
		if a.Source != AttributionSourceAuditLog {
			t.Errorf("event %d source = %q, want %q", i, a.Source, AttributionSourceAuditLog)
		}
	}
}

// TestAttributeFromAudit_NoBracket_NearestRecordCorroborated covers log
// truncation: a long-lived session whose CONNECT predates the audit window
// has no lifetime brackets, so the nearest identity-bearing record matches —
// downgraded to corroborated, carrying that record's SQL as evidence.
func TestAttributeFromAudit_NoBracket_NearestRecordCorroborated(t *testing.T) {
	audit := []AuditEvent{
		auditRec("10:00:00", "QUERY", "bob", "web1", 77, "UPDATE orders SET status='old'"),
		auditRec("10:05:00", "QUERY", "bob", "web1", 77, "UPDATE orders SET status='new'"),
	}
	events := []query.ResultRow{wcEvent(t, 1, "10:04:00", 77)}

	got := attributeFromAudit(events, audit, false)
	a, ok := got[0]
	if !ok {
		t.Fatal("no-bracket event was not attributed via nearest record")
	}
	if a.User != "bob" || a.Host != "web1" {
		t.Errorf("attributed to %s@%s, want bob@web1", a.User, a.Host)
	}
	if a.Confidence != ConfidenceCorroborated {
		t.Errorf("confidence = %q, want %q (lifetime unverified)", a.Confidence, ConfidenceCorroborated)
	}
	if want := "UPDATE orders SET status='new'"; a.AuditSQL != want {
		t.Errorf("AuditSQL = %q, want the NEAREST record's SQL %q", a.AuditSQL, want)
	}
}

// TestAttributeFromAudit_BracketsExcludeEvent: when the id's known lifetimes
// positively exclude the event, the audit tier must NOT guess — not even from
// in-window QUERY evidence — and leaves the event for the next tier.
func TestAttributeFromAudit_BracketsExcludeEvent(t *testing.T) {
	audit := []AuditEvent{
		auditRec("10:00:00", "CONNECT", "alice", "app1", 42, ""),
		auditRec("10:00:05", "QUERY", "alice", "app1", 42, "SELECT 1"),
		auditRec("10:00:30", "DISCONNECT", "alice", "app1", 42, ""),
	}
	events := []query.ResultRow{wcEvent(t, 1, "10:02:00", 42)} // after the disconnect

	got := attributeFromAudit(events, audit, false)
	if a, ok := got[0]; ok {
		t.Fatalf("event outside all known lifetimes was attributed to %s@%s; must stay unresolved", a.User, a.Host)
	}
}

// TestAttributeFromAudit_OpenEndedIntervals: a CONNECT without DISCONNECT
// covers everything after it; a DISCONNECT without CONNECT (head truncation)
// covers everything before it. Both are exact — the bound that exists is
// positively known.
func TestAttributeFromAudit_OpenEndedIntervals(t *testing.T) {
	audit := []AuditEvent{
		auditRec("10:00:00", "CONNECT", "dave", "batch1", 10, ""),  // never disconnects
		auditRec("10:10:00", "DISCONNECT", "erin", "web2", 20, ""), // connect predates log
	}
	events := []query.ResultRow{
		wcEvent(t, 1, "10:30:00", 10), // long after dave connected
		wcEvent(t, 2, "10:05:00", 20), // before erin disconnected
		wcEvent(t, 3, "10:15:00", 20), // AFTER erin disconnected — excluded
	}

	got := attributeFromAudit(events, audit, false)
	if a := got[0]; a.User != "dave" || a.Confidence != ConfidenceExact {
		t.Errorf("open-ended CONNECT: got %+v, want dave/exact", a)
	}
	if a := got[1]; a.User != "erin" || a.Confidence != ConfidenceExact {
		t.Errorf("truncated-head DISCONNECT: got %+v, want erin/exact", a)
	}
	if a, ok := got[2]; ok {
		t.Errorf("event after erin's disconnect was attributed to %s; want unresolved", a.User)
	}
}

// TestAttributeFromAudit_AmbiguousBoundaryHeuristic: two lifetimes abut inside
// the log's one-second granularity and both contain the event timestamp. The
// engine picks the latest-starting session but grades the guess heuristic.
func TestAttributeFromAudit_AmbiguousBoundaryHeuristic(t *testing.T) {
	audit := []AuditEvent{
		auditRec("10:00:00", "CONNECT", "alice", "app1", 42, ""),
		auditRec("10:00:30", "DISCONNECT", "alice", "app1", 42, ""),
		auditRec("10:00:30", "CONNECT", "mallory", "app2", 42, ""),
		auditRec("10:01:00", "DISCONNECT", "mallory", "app2", 42, ""),
	}
	events := []query.ResultRow{wcEvent(t, 1, "10:00:30", 42)}

	got := attributeFromAudit(events, audit, false)
	a, ok := got[0]
	if !ok {
		t.Fatal("boundary event was not attributed")
	}
	if a.User != "mallory" {
		t.Errorf("boundary event attributed to %q, want mallory (latest-starting session)", a.User)
	}
	if a.Confidence != ConfidenceHeuristic {
		t.Errorf("confidence = %q, want %q for an ambiguous boundary", a.Confidence, ConfidenceHeuristic)
	}
}

// TestAttributeFromAudit_ImplicitCloseOnReconnect: a second CONNECT on the
// same id implies the first session ended unlogged; the first identity must
// not leak past the reconnect.
func TestAttributeFromAudit_ImplicitCloseOnReconnect(t *testing.T) {
	audit := []AuditEvent{
		auditRec("10:00:00", "CONNECT", "alice", "app1", 42, ""),
		auditRec("10:00:40", "CONNECT", "mallory", "app2", 42, ""), // no DISCONNECT between
	}
	events := []query.ResultRow{
		wcEvent(t, 1, "10:00:20", 42),
		wcEvent(t, 2, "10:00:50", 42),
	}

	got := attributeFromAudit(events, audit, false)
	if a := got[0]; a.User != "alice" {
		t.Errorf("pre-reconnect event attributed to %q, want alice", a.User)
	}
	if a := got[1]; a.User != "mallory" {
		t.Errorf("post-reconnect event attributed to %q, want mallory", a.User)
	}
}

func TestClassifyAuditEventType(t *testing.T) {
	cases := []struct {
		in   string
		want auditRecordKind
	}{
		{"CONNECT", kindConnect},            // MariaDB / RDS / Aurora
		{"Connect", kindConnect},            // Percona CSV, MySQL Enterprise XML
		{"connection/connect", kindConnect}, // JSON class/event composite
		{"DISCONNECT", kindDisconnect},      // MariaDB family
		{"connection/disconnect", kindDisconnect},
		{"Quit", kindDisconnect},              // Percona CSV, Enterprise XML
		{"FAILED_CONNECT", kindOther},         // opens no session
		{"connection/change_user", kindOther}, // not a connect despite the class name
		{"QUERY", kindOther},
		{"Query", kindOther},
		{"general/status", kindOther},
		{"CREATE_TABLE", kindOther},
	}
	for _, c := range cases {
		if got := classifyAuditEventType(c.in); got != c.want {
			t.Errorf("classifyAuditEventType(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

// ─── WhoChanged flow (fixture fetch, no databases) ────────────────────────────

func TestWhoChanged_Validation(t *testing.T) {
	ctx := context.Background()
	fetch := func(context.Context, query.Options) ([]query.ResultRow, error) { return nil, nil }

	if _, err := WhoChanged(ctx, WhoChangedDeps{Fetch: fetch}, WhoChangedParams{Table: "orders"}); err == nil {
		t.Error("missing schema: want error")
	}
	if _, err := WhoChanged(ctx, WhoChangedDeps{Fetch: fetch}, WhoChangedParams{Schema: "shop"}); err == nil {
		t.Error("missing table: want error")
	}
	if _, err := WhoChanged(ctx, WhoChangedDeps{}, WhoChangedParams{Schema: "shop", Table: "orders"}); err == nil {
		t.Error("nil Fetch: want error")
	}
}

func TestWhoChanged_FetchErrorWrapped(t *testing.T) {
	boom := errors.New("index unreachable")
	fetch := func(context.Context, query.Options) ([]query.ResultRow, error) { return nil, boom }
	_, err := WhoChanged(context.Background(), WhoChangedDeps{Fetch: fetch},
		WhoChangedParams{Schema: "shop", Table: "orders"})
	if !errors.Is(err, boom) {
		t.Errorf("fetch error not wrapped: %v", err)
	}
}

// TestWhoChanged_DefaultWindow pins the honest-empty-results nuance ported
// from the SaaS: an unbounded call gets a 24h window, the result records that
// it was applied, and the fetch actually receives the bounds (partition
// pruning depends on them).
func TestWhoChanged_DefaultWindow(t *testing.T) {
	var gotOpts query.Options
	fetch := func(_ context.Context, opts query.Options) ([]query.ResultRow, error) {
		gotOpts = opts
		return nil, nil
	}

	res, err := WhoChanged(context.Background(), WhoChangedDeps{Fetch: fetch},
		WhoChangedParams{Schema: "shop", Table: "orders"})
	if err != nil {
		t.Fatalf("WhoChanged: %v", err)
	}
	if !res.AppliedDefaultWindow {
		t.Error("AppliedDefaultWindow = false, want true for an unbounded call")
	}
	if gotOpts.Since == nil || gotOpts.Until == nil {
		t.Fatal("fetch did not receive the default window bounds")
	}
	if span := gotOpts.Until.Sub(*gotOpts.Since); span != whoChangedDefaultWindow {
		t.Errorf("default window span = %v, want %v", span, whoChangedDefaultWindow)
	}
	if gotOpts.Limit != whoChangedDefaultLimit {
		t.Errorf("default limit = %d, want %d", gotOpts.Limit, whoChangedDefaultLimit)
	}
	// The empty result must say the default window was in play.
	if len(res.Notes) == 0 || !strings.Contains(res.Notes[0], "default window") {
		t.Errorf("empty default-window result must carry the honest note, got %v", res.Notes)
	}

	// Explicit bounds: no default window, bounds pass through unchanged.
	since := wcTime(t, "10:00:00")
	res, err = WhoChanged(context.Background(), WhoChangedDeps{Fetch: fetch},
		WhoChangedParams{Schema: "shop", Table: "orders", Since: &since})
	if err != nil {
		t.Fatalf("WhoChanged with explicit since: %v", err)
	}
	if res.AppliedDefaultWindow {
		t.Error("AppliedDefaultWindow = true despite an explicit since")
	}
	if gotOpts.Since == nil || !gotOpts.Since.Equal(since) {
		t.Errorf("explicit since not passed through: %v", gotOpts.Since)
	}
	if gotOpts.Until != nil {
		t.Errorf("until fabricated for an explicit-since call: %v", gotOpts.Until)
	}
	if len(res.Notes) == 0 || !strings.Contains(res.Notes[0], "specified time range") {
		t.Errorf("empty explicit-window result note wrong: %v", res.Notes)
	}
}

// TestWhoChanged_BinlogOnlyDegradation: with no source and no index database,
// events still come back — binlog data plus an explanatory note and fallback
// SQL, never an error.
func TestWhoChanged_BinlogOnlyDegradation(t *testing.T) {
	qt := "UPDATE orders SET status = 'shipped' WHERE id = 42"
	rows := []query.ResultRow{
		wcEvent(t, 1, "10:00:10", 42),
		wcEvent(t, 2, "10:00:50", 0), // no connection id at all
	}
	rows[0].QueryText = &qt
	fetch := func(context.Context, query.Options) ([]query.ResultRow, error) { return rows, nil }

	res, err := WhoChanged(context.Background(), WhoChangedDeps{Fetch: fetch},
		WhoChangedParams{Schema: "shop", Table: "orders"})
	if err != nil {
		t.Fatalf("binlog-only degradation must not error: %v", err)
	}
	if res.TotalCount != 2 || len(res.Events) != 2 {
		t.Fatalf("got %d events (total %d), want 2", len(res.Events), res.TotalCount)
	}
	for i, ev := range res.Events {
		if ev.Attribution != nil {
			t.Errorf("event %d attributed with no data sources: %+v", i, ev.Attribution)
		}
	}
	// #712: the captured statement is surfaced directly on the event.
	if res.Events[0].QueryText == nil || *res.Events[0].QueryText != qt {
		t.Errorf("query_text not surfaced: %v", res.Events[0].QueryText)
	}
	if res.Events[0].EventType != "UPDATE" {
		t.Errorf("event type = %q, want UPDATE", res.Events[0].EventType)
	}
	// The unattributed note names the count; fallback SQL covers the known id.
	joined := strings.Join(res.Notes, "\n")
	if !strings.Contains(joined, "2 of 2") {
		t.Errorf("binlog-only note missing the unattributed count, got %v", res.Notes)
	}
	if len(res.FallbackQueries) == 0 {
		t.Error("no fallback queries for the unresolved connection id")
	}
	var sawID bool
	for _, fq := range res.FallbackQueries {
		if strings.Contains(fq.SQL, "42") {
			sawID = true
		}
	}
	if !sawID {
		t.Errorf("fallback SQL does not target connection id 42: %+v", res.FallbackQueries)
	}
	// No attribution happened, so the pooler/replica/spoof caveats are
	// irrelevant noise here.
	if strings.Contains(joined, "pooler") || strings.Contains(joined, "ProxySQL") {
		t.Errorf("pooler caveat emitted with zero attributions: %v", res.Notes)
	}
}

// TestAssembleResult_CaveatsOnceAndMapped drives the assembly step directly
// with a fake attribution map: every standard caveat appears exactly once,
// and per-event attributions land on the right events.
func TestAssembleResult_CaveatsOnceAndMapped(t *testing.T) {
	rows := []query.ResultRow{
		wcEvent(t, 1, "10:00:10", 42),
		wcEvent(t, 2, "10:00:50", 42),
		wcEvent(t, 3, "10:01:30", 99),
	}
	attr := map[int]Attribution{
		0: {User: "alice", Host: "app1", Source: AttributionSourceAuditLog, Confidence: ConfidenceExact},
		1: {User: "mallory", Host: "app2", Source: AttributionSourceConnCache, Confidence: ConfidenceCorroborated},
	}

	res := assembleResult(rows, attr, true, 100, nil)

	if res.Events[0].Attribution == nil || res.Events[0].Attribution.User != "alice" {
		t.Errorf("event 0 attribution wrong: %+v", res.Events[0].Attribution)
	}
	if res.Events[1].Attribution == nil || res.Events[1].Attribution.User != "mallory" {
		t.Errorf("event 1 attribution wrong: %+v", res.Events[1].Attribution)
	}
	if res.Events[2].Attribution != nil {
		t.Errorf("event 2 must stay unattributed, got %+v", res.Events[2].Attribution)
	}

	for _, want := range []string{notePooler, noteReplica, noteSpoof} {
		if n := strings.Count(strings.Join(res.Notes, "\n"), want); n != 1 {
			t.Errorf("caveat %.40q... appears %d times, want exactly 1", want, n)
		}
	}
	joined := strings.Join(res.Notes, "\n")
	if !strings.Contains(joined, "default window") {
		t.Errorf("default-window note missing: %v", res.Notes)
	}
	if !strings.Contains(joined, "1 of 3") {
		t.Errorf("unattributed note missing '1 of 3': %v", res.Notes)
	}
	// Fallback SQL only for the UNRESOLVED id (99), not the attributed 42.
	if len(res.FallbackQueries) == 0 {
		t.Fatal("no fallback queries for unresolved id 99")
	}
	for _, fq := range res.FallbackQueries {
		if strings.Contains(fq.SQL, "42") {
			t.Errorf("fallback SQL targets the already-attributed id 42: %s", fq.SQL)
		}
	}
}

// TestAssembleResult_NoConnectionIDsNote: events indexed before connection-id
// capture get the dedicated explanation, not the generic unattributed count.
func TestAssembleResult_NoConnectionIDsNote(t *testing.T) {
	rows := []query.ResultRow{wcEvent(t, 1, "10:00:10", 0)}
	res := assembleResult(rows, map[int]Attribution{}, false, 100, nil)
	joined := strings.Join(res.Notes, "\n")
	if !strings.Contains(joined, "no connection id") {
		t.Errorf("missing the no-connection-id explanation: %v", res.Notes)
	}
	if len(res.FallbackQueries) != 0 {
		t.Errorf("fallback queries fabricated with no connection ids: %+v", res.FallbackQueries)
	}
}

// ─── unresolved-id helpers ────────────────────────────────────────────────────

func TestUnresolvedConnIDs(t *testing.T) {
	rows := []query.ResultRow{
		wcEvent(t, 1, "10:00:00", 42),
		wcEvent(t, 2, "10:00:01", 42), // duplicate id
		wcEvent(t, 3, "10:00:02", 7),
		wcEvent(t, 4, "10:00:03", 0), // no id
	}
	ids := unresolvedConnIDs(rows, map[int]Attribution{0: {User: "a"}})
	// Event 0 resolved, but id 42 still appears via event 1; sorted output.
	if len(ids) != 2 || ids[0] != 7 || ids[1] != 42 {
		t.Errorf("unresolvedConnIDs = %v, want [7 42]", ids)
	}
}

// ─── review-hardening: read completeness must shape confidence and notes ─────

// TestAuditReadOptionsFor pins the audit read shape: a FULL scan (the
// recent-history tail auto-mode would silently read only the last ~2.5MB of
// each file and drop older events' brackets), rotated files included, and the
// ±pad window derived from the events' span.
func TestAuditReadOptionsFor(t *testing.T) {
	rows := []query.ResultRow{
		wcEvent(t, 1, "10:00:10", 42),
		wcEvent(t, 2, "09:00:00", 42), // earliest
		wcEvent(t, 3, "11:30:00", 42), // latest
	}
	opts := auditReadOptionsFor(rows)

	if opts.TailLines >= 0 {
		t.Errorf("TailLines = %d, want < 0 (full scan): tail auto-mode silently drops older brackets", opts.TailLines)
	}
	if !opts.IncludeRotated {
		t.Error("IncludeRotated = false; a 24h window routinely spans a rotation")
	}
	if opts.Limit != auditMaxLimit {
		t.Errorf("Limit = %d, want %d", opts.Limit, auditMaxLimit)
	}
	if want := wcTime(t, "09:00:00").Add(-auditWindowPad); !opts.Since.Equal(want) {
		t.Errorf("Since = %v, want %v", opts.Since, want)
	}
	if want := wcTime(t, "11:30:00").Add(auditWindowPad); !opts.Until.Equal(want) {
		t.Errorf("Until = %v, want %v", opts.Until, want)
	}
}

// TestAttributeFromAudit_TruncatedRead: when the audit read stopped at its
// record cap, an interval with an unbounded side may only look unbounded
// because its closing record was dropped — matches against it downgrade to
// corroborated. Fully-bounded intervals keep exact (sessions on one id are
// serial; nothing dropped fits between two real endpoint records).
func TestAttributeFromAudit_TruncatedRead(t *testing.T) {
	audit := []AuditEvent{
		auditRec("10:00:00", "CONNECT", "alice", "app1", 42, ""),
		auditRec("10:00:30", "DISCONNECT", "alice", "app1", 42, ""), // bounded
		auditRec("10:05:00", "CONNECT", "dave", "batch1", 77, ""),   // open tail
	}
	events := []query.ResultRow{
		wcEvent(t, 1, "10:00:10", 42), // inside the bounded lifetime
		wcEvent(t, 2, "10:30:00", 77), // inside the open lifetime
	}

	got := attributeFromAudit(events, audit, true)
	if a := got[0]; a.Confidence != ConfidenceExact {
		t.Errorf("bounded-interval match under truncation = %q, want %q", a.Confidence, ConfidenceExact)
	}
	if a := got[1]; a.User != "dave" || a.Confidence != ConfidenceCorroborated {
		t.Errorf("open-interval match under truncation = %+v, want dave/corroborated", a)
	}

	// Without truncation the open interval is trustworthy (the log positively
	// showed no disconnect) and stays exact.
	got = attributeFromAudit(events, audit, false)
	if a := got[1]; a.Confidence != ConfidenceExact {
		t.Errorf("open-interval match without truncation = %q, want %q", a.Confidence, ConfidenceExact)
	}
}

// TestAssembleResult_LimitAndTierNotes: hitting the fetch limit and any
// tier-degradation notes must reach the structured result — a JSON consumer
// sees only the payload, never stderr or logs.
func TestAssembleResult_LimitAndTierNotes(t *testing.T) {
	rows := []query.ResultRow{
		wcEvent(t, 1, "10:00:10", 42),
		wcEvent(t, 2, "10:00:50", 43),
	}
	tierNote := "The source server was unreachable, so nothing source-side was consulted."
	res := assembleResult(rows, map[int]Attribution{}, false, 2, []string{tierNote})

	joined := strings.Join(res.Notes, "\n")
	if !strings.Contains(joined, "truncated at the limit of 2") {
		t.Errorf("limit-truncation note missing when len(rows) == limit: %v", res.Notes)
	}
	if !strings.Contains(joined, tierNote) {
		t.Errorf("tier note not surfaced in the result: %v", res.Notes)
	}

	// Under the limit: no truncation note.
	res = assembleResult(rows, map[int]Attribution{}, false, 100, nil)
	if strings.Contains(strings.Join(res.Notes, "\n"), "truncated at the limit") {
		t.Errorf("truncation note fabricated below the limit: %v", res.Notes)
	}
}
