package parser

import (
	"context"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
)

// ─── Query-text capture (#699) ────────────────────────────────────────────────
//
// These tests pin the statement-scoped query-text state machine in
// StreamParser.Run: a ROWS_QUERY_EVENT (MySQL, binlog_rows_query_log_events=ON)
// or ANNOTATE_ROWS event (MariaDB, binlog_annotate_row_events=ON) sets the text
// for the rows events that follow it, and QUERY/GTID/XID boundaries clear it so
// text can never leak onto a later statement that logged none.

// makeRowsQueryEvent builds the MySQL ROWS_QUERY_EVENT carrying the original
// statement text.
func makeRowsQueryEvent(query string) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.ROWS_QUERY_EVENT},
		Event:  &replication.RowsQueryEvent{Query: []byte(query)},
	}
}

// makeAnnotateRowsEvent builds the MariaDB ANNOTATE_ROWS event, the positional
// sibling of MySQL's ROWS_QUERY_EVENT.
func makeAnnotateRowsEvent(query string) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.MARIADB_ANNOTATE_ROWS_EVENT},
		Event:  &replication.MariadbAnnotateRowsEvent{Query: []byte(query)},
	}
}

// makeOrdersInsertEvent builds a decodable WRITE_ROWS event for shop.orders
// (matching makeOrdersResolver) so handleRows clears every guard and emits.
func makeOrdersInsertEvent(id, amount int64) *replication.BinlogEvent {
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
				ColumnCount: 2,
			},
			Rows: [][]any{{id, amount}},
		},
	}
}

// makeOrdersInsertEventFlags is makeOrdersInsertEvent with explicit RowsEvent
// flags (STMT_END_F marks the last rows event of a statement).
func makeOrdersInsertEventFlags(id, amount int64, flags uint16) *replication.BinlogEvent {
	ev := makeOrdersInsertEvent(id, amount)
	ev.Event.(*replication.RowsEvent).Flags = flags
	return ev
}

// dmlOf filters the emitted stream down to row-DML events (drops the GTID
// tracking and commit boundary events that ride along).
func dmlOf(evs []Event) []Event {
	var dml []Event
	for _, ev := range evs {
		switch ev.EventType {
		case EventInsert, EventUpdate, EventDelete:
			dml = append(dml, ev)
		}
	}
	return dml
}

// TestStreamParser_rowsQueryTextAttachedToRows: the text from a ROWS_QUERY
// event lands on the statement's row events, and a later transaction that
// logs NO rows-query text emits rows with an empty QueryText (no stale leak
// across the XID/GTID boundary).
func TestStreamParser_rowsQueryTextAttachedToRows(t *testing.T) {
	sp := NewStreamParser(makeOrdersResolver(), Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 16)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeRotate("binlog.000001"),
		// Transaction 1: statement text logged.
		makeGTIDEvent(1),
		makeQueryEvent("BEGIN"),
		makeRowsQueryEvent("INSERT INTO shop.orders VALUES (1, 10)"),
		makeOrdersInsertEvent(1, 10),
		makeXIDEvent(300),
		// Transaction 2: variable toggled off — no ROWS_QUERY event.
		makeGTIDEvent(2),
		makeQueryEvent("BEGIN"),
		makeOrdersInsertEvent(2, 20),
		makeXIDEvent(600),
	)

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	dml := dmlOf(drainAll(out))
	if len(dml) != 2 {
		t.Fatalf("expected 2 DML events, got %d", len(dml))
	}
	if got, want := dml[0].QueryText, "INSERT INTO shop.orders VALUES (1, 10)"; got != want {
		t.Errorf("trx1 QueryText = %q, want %q", got, want)
	}
	if dml[1].QueryText != "" {
		t.Errorf("trx2 QueryText = %q, want empty (no ROWS_QUERY logged; stale text must not leak)", dml[1].QueryText)
	}
}

// TestStreamParser_perStatementQueryTextScoping: a multi-statement transaction
// emits one ROWS_QUERY event per statement; each statement's rows must carry
// their OWN text, not the transaction's first.
func TestStreamParser_perStatementQueryTextScoping(t *testing.T) {
	sp := NewStreamParser(makeOrdersResolver(), Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 16)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeRotate("binlog.000001"),
		makeGTIDEvent(1),
		makeQueryEvent("BEGIN"),
		makeRowsQueryEvent("INSERT INTO shop.orders VALUES (1, 10)"),
		makeOrdersInsertEvent(1, 10),
		makeRowsQueryEvent("INSERT INTO shop.orders VALUES (2, 20)"),
		makeOrdersInsertEvent(2, 20),
		makeXIDEvent(900),
	)

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	dml := dmlOf(drainAll(out))
	if len(dml) != 2 {
		t.Fatalf("expected 2 DML events, got %d", len(dml))
	}
	if got, want := dml[0].QueryText, "INSERT INTO shop.orders VALUES (1, 10)"; got != want {
		t.Errorf("statement 1 QueryText = %q, want %q", got, want)
	}
	if got, want := dml[1].QueryText, "INSERT INTO shop.orders VALUES (2, 20)"; got != want {
		t.Errorf("statement 2 QueryText = %q, want %q", got, want)
	}
}

// TestStreamParser_annotateRowsCarriesQueryText: the MariaDB ANNOTATE_ROWS
// event feeds the same QueryText field as MySQL's ROWS_QUERY_EVENT.
func TestStreamParser_annotateRowsCarriesQueryText(t *testing.T) {
	sp := NewStreamParser(makeOrdersResolver(), Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 16)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeRotate("mariadb-bin.000001"),
		makeMariadbGTIDEvent(0, 1, 100),
		makeQueryEvent("BEGIN"),
		makeAnnotateRowsEvent("DELETE FROM shop.orders WHERE id = 1"),
		makeOrdersInsertEvent(1, 10), // event type is irrelevant to the text plumbing
		makeXIDEvent(400),
	)

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	dml := dmlOf(drainAll(out))
	if len(dml) != 1 {
		t.Fatalf("expected 1 DML event, got %d", len(dml))
	}
	if got, want := dml[0].QueryText, "DELETE FROM shop.orders WHERE id = 1"; got != want {
		t.Errorf("QueryText = %q, want %q", got, want)
	}
}

// TestStreamParser_queryTextInsideTransactionPayload: with
// binlog_transaction_compression=ON the ROWS_QUERY event arrives INSIDE the
// Transaction_payload envelope (like the BEGIN that carries connection_id) —
// the recursive dispatch must capture it before the inner rows decode.
func TestStreamParser_queryTextInsideTransactionPayload(t *testing.T) {
	sp := NewStreamParser(makeOrdersResolver(), Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 16)

	innerBegin := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.QUERY_EVENT, LogPos: 0},
		Event:  &replication.QueryEvent{Query: []byte("BEGIN"), SlaveProxyID: 42},
	}
	innerRowsQuery := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.ROWS_QUERY_EVENT, LogPos: 0},
		Event:  &replication.RowsQueryEvent{Query: []byte("INSERT INTO shop.orders VALUES (1, 10)")},
	}
	innerInsert := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.WRITE_ROWS_EVENTv2,
			LogPos:    0,
			EventSize: 500,
		},
		Event: &replication.RowsEvent{
			Table: &replication.TableMapEvent{
				Schema:      []byte("shop"),
				Table:       []byte("orders"),
				ColumnCount: 2,
			},
			Rows: [][]any{{int64(1), int64(10)}},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeGTIDEvent(7),
		makePayloadEvent(5000, 900, innerBegin, innerRowsQuery, innerInsert),
	)

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	dml := dmlOf(drainAll(out))
	if len(dml) != 1 {
		t.Fatalf("expected 1 DML event from payload inner rows, got %d", len(dml))
	}
	if got, want := dml[0].QueryText, "INSERT INTO shop.orders VALUES (1, 10)"; got != want {
		t.Errorf("QueryText = %q, want %q (from inner ROWS_QUERY)", got, want)
	}
}

// TestStreamParser_queryTextClearedAtStatementEnd pins the STMT_END_F clear:
// MySQL allows SET SESSION binlog_rows_query_log_events=OFF INSIDE an open
// transaction, so a later statement in the SAME transaction can emit rows with
// no ROWS_QUERY of its own — it must NOT inherit the previous statement's
// text (there is no GTID/QUERY boundary in between to clear it). The last
// rows event of a statement carries STMT_END_F; that is the boundary.
func TestStreamParser_queryTextClearedAtStatementEnd(t *testing.T) {
	sp := NewStreamParser(makeOrdersResolver(), Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 16)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeRotate("binlog.000001"),
		makeGTIDEvent(1),
		makeQueryEvent("BEGIN"),
		// Statement 1: logged, spans TWO chained rows events — only the
		// second carries STMT_END_F; both must share the text.
		makeRowsQueryEvent("INSERT INTO shop.orders VALUES (1, 10), (2, 20)"),
		makeOrdersInsertEventFlags(1, 10, 0),
		makeOrdersInsertEventFlags(2, 20, replication.RowsEventStmtEndFlag),
		// Statement 2, SAME transaction: variable toggled off — no
		// ROWS_QUERY. Its rows must carry NO text.
		makeOrdersInsertEventFlags(3, 30, replication.RowsEventStmtEndFlag),
		makeXIDEvent(900),
	)

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	dml := dmlOf(drainAll(out))
	if len(dml) != 3 {
		t.Fatalf("expected 3 DML events, got %d", len(dml))
	}
	want := "INSERT INTO shop.orders VALUES (1, 10), (2, 20)"
	if dml[0].QueryText != want || dml[1].QueryText != want {
		t.Errorf("chained rows events of one statement must share its text, got %q / %q", dml[0].QueryText, dml[1].QueryText)
	}
	if dml[2].QueryText != "" {
		t.Errorf("statement after mid-transaction toggle must carry NO text, got %q (stale attribution)", dml[2].QueryText)
	}
}
