package parser

import (
	"context"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// ─── #1117: post-reconnect FillZeroLogPos overshoot correction ────────────────
//
// The fixture below is the EXACT event sequence captured live from MariaDB
// 11.4.12 for a position-mode resume landing mid-transaction (a legal #775
// statement-boundary checkpoint at offset 1264, inside a 3-statement
// transaction whose statements truly end at 1332/1385/1428 with the XID at
// 1459): the server honors the offset, re-sends the file's FDE with LogPos
// zeroed, FillZeroLogPos fills it to 1264+252=1516, and the transaction
// tail's cache-buffered events arrive filled with a constant +252 overshoot
// (1584/1637/1680) until the genuine XID (1459) snaps back. Without the
// corrector, the tail row would be stored at [1637,1680] instead of
// [1385,1428] — a value that is not an event boundary, so a checkpoint
// persisting it turns into a fatal server error 1236 on the next restart —
// and the genuine XID would read as a same-file backward jump, tripping the
// wraparound guard on every such resume.

// artificialRotate builds the connect-time fake rotate the server sends at
// (re)connect: ARTIFICIAL flag set, Position naming the resume offset.
func artificialRotate(nextFile string, position uint64) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.ROTATE_EVENT,
			Flags:     replication.LOG_EVENT_ARTIFICIAL_F,
		},
		Event: &replication.RotateEvent{NextLogName: []byte(nextFile), Position: position},
	}
}

func TestStreamParser_midTransactionResumeCorrectsFilledPositions(t *testing.T) {
	sp := NewStreamParser(makeOrdersResolver(), Filters{}, nil)
	sp.SetFlavor("mariadb")
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 16)

	fde := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.FORMAT_DESCRIPTION_EVENT,
			LogPos:    1516, // filled: resume 1264 + FDE size 252
			EventSize: 252,
		},
		Event: &replication.FormatDescriptionEvent{Version: 4},
	}
	annotate := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.MARIADB_ANNOTATE_ROWS_EVENT,
			LogPos:    1584, // filled; true end 1332
			EventSize: 68,
		},
		Event: &replication.MariadbAnnotateRowsEvent{Query: []byte("INSERT INTO orders (amount) VALUES (30)")},
	}
	tableMap := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.TABLE_MAP_EVENT,
			LogPos:    1637, // filled; true end 1385
			EventSize: 53,
		},
		Event: &replication.TableMapEvent{Schema: []byte("shop"), Table: []byte("orders"), ColumnCount: 2},
	}
	tailRows := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.WRITE_ROWS_EVENTv1,
			LogPos:    1680, // filled; true [1385,1428]
			EventSize: 43,
			Flags:     0,
		},
		Event: &replication.RowsEvent{
			Table: &replication.TableMapEvent{Schema: []byte("shop"), Table: []byte("orders"), ColumnCount: 2},
			Rows:  [][]any{{int64(4), int64(30)}},
			Flags: replication.RowsEventStmtEndFlag,
		},
	}
	xid := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.XID_EVENT, LogPos: 1459, EventSize: 31},
		Event:  &replication.XIDEvent{XID: 7},
	}
	nextGTID := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.MARIADB_GTID_EVENT, LogPos: 1501, EventSize: 42},
		Event: &replication.MariadbGTIDEvent{
			GTID: mysql.MariadbGTID{DomainID: 0, ServerID: 2, SequenceNumber: 8},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		artificialRotate("mariadb-bin.000002", 1264),
		fde, annotate, tableMap, tailRows, xid, nextGTID,
	)

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: unexpected error on a mid-transaction resume (wraparound false-trip?): %v", err)
	}
	close(out)

	var rows []Event
	for ev := range out {
		if ev.EventType == EventInsert {
			rows = append(rows, ev)
		}
	}
	if len(rows) != 1 {
		t.Fatalf("expected the transaction-tail row to be emitted, got %d row events", len(rows))
	}
	// The corrected TRUE offsets, not the +252-inflated filled values.
	if rows[0].StartPos != 1385 || rows[0].EndPos != 1428 {
		t.Errorf("tail row positions = [%d, %d], want corrected true offsets [1385, 1428]",
			rows[0].StartPos, rows[0].EndPos)
	}
}

// TestResumeFillCorrector_genuineBoundaryResumeUntouched pins the common
// case: a resume at a transaction boundary has a directly-written GTID event
// (genuine LogPos) right after the ghost FDE, so the corrector must disarm
// without rewriting anything (live capture: resume 938 → FDE filled 1190 →
// GTID genuine 980 = 938+42).
func TestResumeFillCorrector_genuineBoundaryResumeUntouched(t *testing.T) {
	var c resumeFillCorrector

	rot := artificialRotate("mariadb-bin.000002", 938)
	fde := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.FORMAT_DESCRIPTION_EVENT, LogPos: 1190, EventSize: 252},
		Event:  &replication.FormatDescriptionEvent{Version: 4},
	}
	gtid := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.MARIADB_GTID_EVENT, LogPos: 980, EventSize: 42},
		Event:  &replication.MariadbGTIDEvent{GTID: mysql.MariadbGTID{DomainID: 0, ServerID: 2, SequenceNumber: 5}},
	}

	for _, ev := range []*replication.BinlogEvent{rot, fde, gtid} {
		if c.Observe(ev) {
			t.Fatalf("Observe rewrote %s — a genuine-boundary resume must stay untouched", ev.Header.EventType)
		}
	}
	if gtid.Header.LogPos != 980 {
		t.Errorf("genuine GTID LogPos changed to %d, want 980 untouched", gtid.Header.LogPos)
	}
	if c.adjust != 0 || c.armed {
		t.Errorf("corrector should be fully disarmed after a genuine first event, got %+v", c)
	}
}

// TestResumeFillCorrector_zeroLogPosLeftForBelt pins fail-open: without
// FillZeroLogPos (or a non-MariaDB source), post-FDE cache-buffered events
// arrive with LogPos=0 — the corrector must NOT invent positions for them
// (that is the handleRows belt's job to reject, and the file parser's job to
// fill for files); it disarms and leaves the header untouched.
func TestResumeFillCorrector_zeroLogPosLeftForBelt(t *testing.T) {
	var c resumeFillCorrector

	c.Observe(artificialRotate("mariadb-bin.000002", 1264))
	c.Observe(&replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.FORMAT_DESCRIPTION_EVENT, LogPos: 1516, EventSize: 252},
		Event:  &replication.FormatDescriptionEvent{Version: 4},
	})
	zeroRows := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv1, LogPos: 0, EventSize: 43},
		Event:  &replication.RowsEvent{Table: &replication.TableMapEvent{Schema: []byte("shop"), Table: []byte("orders"), ColumnCount: 2}},
	}
	if c.Observe(zeroRows) {
		t.Fatal("Observe rewrote a zero-LogPos event — must fail open to the underflow belt")
	}
	if zeroRows.Header.LogPos != 0 {
		t.Errorf("zero LogPos was rewritten to %d, want 0 untouched", zeroRows.Header.LogPos)
	}
}
