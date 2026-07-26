package parser

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
)

// ─── Capture-skip counters (#1034) ────────────────────────────────────────────
//
// SkipCounters make sustained event skipping visible: per-reason monotonic
// tallies persisted with the stream checkpoint (so `status` can render a
// Capture health verdict) plus a single escalation ERROR after
// SkipEscalationThreshold consecutive skips.

func TestSkipCounters_snapshotSeedRoundTrip(t *testing.T) {
	c := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	c.RecordSkip(SkipColumnCountMismatch)
	c.RecordSkip(SkipColumnCountMismatch)
	c.RecordSkip(SkipStatementFormatDML)

	snap, err := c.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	for _, want := range []string{SkipColumnCountMismatch, SkipStatementFormatDML, `"count":2`, `"count":1`} {
		if !strings.Contains(snap, want) {
			t.Errorf("snapshot missing %q: %s", want, snap)
		}
	}

	// The restart path: a fresh counter set seeded from the persisted document
	// resumes the monotonic tallies instead of zeroing them.
	restarted := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	if err := restarted.Seed(snap); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	if got := restarted.Total(); got != 3 {
		t.Fatalf("Total after seed = %d, want 3", got)
	}
	restarted.RecordSkip(SkipColumnCountMismatch)
	if got := restarted.Total(); got != 4 {
		t.Fatalf("Total after seed+skip = %d, want 4 (counters must stay monotonic across restarts)", got)
	}
}

func TestSkipCounters_seedEmptyAndInvalid(t *testing.T) {
	c := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	if err := c.Seed(""); err != nil {
		t.Fatalf("Seed(\"\") must be a no-op, got %v", err)
	}
	c.RecordSkip(SkipNoResolver)
	if err := c.Seed("{not json"); err == nil {
		t.Fatal("Seed with invalid JSON must return an error")
	}
	if got := c.Total(); got != 1 {
		t.Fatalf("a failed Seed must leave counters unchanged; Total = %d, want 1", got)
	}
	// A nil counter set snapshots to the evaluated-and-clean marker.
	var nilC *SkipCounters
	if snap, err := nilC.Snapshot(); err != nil || snap != "{}" {
		t.Fatalf("nil Snapshot = (%q, %v), want ({}, nil)", snap, err)
	}
}

// The escalation contract: exactly ONE ERROR per degraded episode, emitted at
// SkipEscalationThreshold consecutive skips, re-armed only by a captured event.
func TestSkipCounters_escalatesOncePerEpisode(t *testing.T) {
	var buf bytes.Buffer
	c := NewSkipCounters(newTestLogger(&buf))

	for i := 0; i < SkipEscalationThreshold-1; i++ {
		c.RecordSkip(SkipColumnCountMismatch)
	}
	if strings.Contains(buf.String(), "level=ERROR") {
		t.Fatalf("ERROR emitted before the threshold:\n%s", buf.String())
	}

	c.RecordSkip(SkipColumnCountMismatch)
	out := buf.String()
	if got := strings.Count(out, "level=ERROR"); got != 1 {
		t.Fatalf("expected exactly 1 ERROR at the threshold, got %d:\n%s", got, out)
	}
	for _, want := range []string{"sustained event skipping", "bintrail snapshot", SkipColumnCountMismatch} {
		if !strings.Contains(out, want) {
			t.Errorf("escalation ERROR missing %q:\n%s", want, out)
		}
	}

	// More skips in the SAME episode must not re-emit.
	for i := 0; i < 2*SkipEscalationThreshold; i++ {
		c.RecordSkip(SkipColumnCountMismatch)
	}
	if got := strings.Count(buf.String(), "level=ERROR"); got != 1 {
		t.Fatalf("ERROR re-emitted within one episode: %d", got)
	}

	// A captured event breaks the run and re-arms the escalation.
	c.RecordCaptured()
	for i := 0; i < SkipEscalationThreshold; i++ {
		c.RecordSkip(SkipStatementFormatDML)
	}
	if got := strings.Count(buf.String(), "level=ERROR"); got != 2 {
		t.Fatalf("expected a second ERROR after capture re-armed the escalation, got %d", got)
	}
	if !strings.Contains(buf.String(), "binlog_format=ROW") {
		t.Errorf("statement-format escalation must carry the ROW-format remediation:\n%s", buf.String())
	}
}

func TestSkipCounters_capturedDoesNotResetTallies(t *testing.T) {
	c := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	c.RecordSkip(SkipColumnCountMismatch)
	c.RecordCaptured()
	if got := c.Total(); got != 1 {
		t.Fatalf("RecordCaptured must not reset the monotonic tallies; Total = %d, want 1", got)
	}
}

// ─── The production counting sites ────────────────────────────────────────────
//
// Each site is pinned through the real code path (repo lesson: mutate each
// site) — reverting a RecordSkip call fails the corresponding test.

// mismatchRowsEvent builds a WRITE_ROWS event for shop.orders whose TABLE_MAP
// column count (3) diverges from the driftResolver snapshot (2) — the #700
// guard's skip, the exact failure mode of #1034.
func mismatchRowsEvent() *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.WRITE_ROWS_EVENTv2,
			LogPos:    200,
			EventSize: 100,
		},
		Event: &replication.RowsEvent{
			Table: &replication.TableMapEvent{
				Schema:      []byte("shop"),
				Table:       []byte("orders"),
				ColumnCount: 3,
			},
			Rows: [][]any{{int64(1), int64(10), int64(99)}},
		},
	}
}

func TestHandleRows_columnCountMismatchCountsSkip(t *testing.T) {
	skips := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	binlogEv := mismatchRowsEvent()
	rowsEv := binlogEv.Event.(*replication.RowsEvent)
	out := make(chan Event, 4)
	logBuf := &bytes.Buffer{}
	err := handleRows(context.Background(), newTestLogger(logBuf), driftResolver(), &Filters{}, binlogEv, rowsEv, "binlog.000001", "", 0, 0, "", 9, out, nil, skips)
	if err != nil {
		t.Fatalf("a column-count mismatch is warn-and-skip on the stream path, got error: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("mismatch must emit nothing, got %d events", len(out))
	}
	snap, _ := skips.Snapshot()
	if !strings.Contains(snap, SkipColumnCountMismatch) || skips.Total() != 1 {
		t.Fatalf("column-count skip not counted (total=%d): %s", skips.Total(), snap)
	}
	if !strings.Contains(logBuf.String(), "column count mismatch") {
		t.Errorf("the per-event WARN must be kept alongside the counter:\n%s", logBuf.String())
	}
}

func TestHandleRows_tableNotInSnapshotCountsSkip(t *testing.T) {
	skips := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	binlogEv := driftRowsEvent(nil)
	rowsEv := binlogEv.Event.(*replication.RowsEvent)
	rowsEv.Table.Schema = []byte("unknowndb")
	out := make(chan Event, 4)
	err := handleRows(context.Background(), newTestLogger(&bytes.Buffer{}), driftResolver(), &Filters{}, binlogEv, rowsEv, "binlog.000001", "", 0, 0, "", 9, out, nil, skips)
	if err != nil {
		t.Fatalf("handleRows: %v", err)
	}
	snap, _ := skips.Snapshot()
	if !strings.Contains(snap, SkipTableNotInSnapshot) {
		t.Fatalf("table-not-in-snapshot skip not counted: %s", snap)
	}
}

// A snapshot-excluded system schema (e.g. RDS's periodic mysql.rds_heartbeat2
// UPDATEs) is a routine, permanent skip: counting it would mark every RDS
// install DEGRADED forever.
func TestHandleRows_excludedSchemaSkipNotCounted(t *testing.T) {
	skips := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	binlogEv := driftRowsEvent(nil)
	rowsEv := binlogEv.Event.(*replication.RowsEvent)
	rowsEv.Table.Schema = []byte("mysql")
	rowsEv.Table.Table = []byte("rds_heartbeat2")
	out := make(chan Event, 4)
	err := handleRows(context.Background(), newTestLogger(&bytes.Buffer{}), driftResolver(), &Filters{}, binlogEv, rowsEv, "binlog.000001", "", 0, 0, "", 9, out, nil, skips)
	if err != nil {
		t.Fatalf("handleRows: %v", err)
	}
	if got := skips.Total(); got != 0 {
		t.Fatalf("a snapshot-excluded schema skip must NOT count toward capture health, got total=%d", got)
	}
}

// A successfully captured event must break the consecutive-skip run (the
// RecordCaptured site at the emit dispatch).
func TestHandleRows_captureBreaksEscalationRun(t *testing.T) {
	var buf bytes.Buffer
	skips := NewSkipCounters(newTestLogger(&buf))
	mismatch := mismatchRowsEvent()
	good := driftRowsEvent([]string{"id", "amount"})

	run := func(binlogEv *replication.BinlogEvent) {
		t.Helper()
		rowsEv := binlogEv.Event.(*replication.RowsEvent)
		out := make(chan Event, 4)
		if err := handleRows(context.Background(), newTestLogger(&bytes.Buffer{}), driftResolver(), &Filters{}, binlogEv, rowsEv, "binlog.000001", "", 0, 0, "", 9, out, nil, skips); err != nil {
			t.Fatalf("handleRows: %v", err)
		}
	}

	// Interleave a captured event into every threshold-sized run of skips:
	// the escalation must never fire.
	for i := 0; i < 2*SkipEscalationThreshold; i++ {
		run(mismatch)
		if i%(SkipEscalationThreshold-1) == 0 {
			run(good)
		}
	}
	if strings.Contains(buf.String(), "level=ERROR") {
		t.Fatalf("captured events must reset the consecutive-skip run:\n%s", buf.String())
	}
}

// The #999 statement-format DML drop site inside StreamParser.Run.
func TestStreamParser_statementDMLCountsSkip(t *testing.T) {
	skips := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	sp := NewStreamParser(makeOrdersResolver(), Filters{}, newTestLogger(&bytes.Buffer{}))
	sp.SetSkipCounters(skips)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel, makeQueryEvent("UPDATE shop.orders SET amount = 1"))
	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	snap, _ := skips.Snapshot()
	if !strings.Contains(snap, SkipStatementFormatDML) || skips.Total() != 1 {
		t.Fatalf("statement-format DML drop not counted (total=%d): %s", skips.Total(), snap)
	}
}

// LastAt must be stamped with a real recent time so `status` can render
// "last <ts>".
func TestSkipCounters_lastAtStamped(t *testing.T) {
	c := NewSkipCounters(newTestLogger(&bytes.Buffer{}))
	before := time.Now().UTC().Add(-time.Minute)
	c.RecordSkip(SkipColumnCountMismatch)
	snap, err := c.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	m := map[string]SkipStat{}
	if err := json.Unmarshal([]byte(snap), &m); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	st := m[SkipColumnCountMismatch]
	if !st.LastAt.After(before) || st.LastAt.After(time.Now().UTC().Add(time.Minute)) {
		t.Fatalf("last_at = %v, want a recent UTC timestamp", st.LastAt)
	}
}
