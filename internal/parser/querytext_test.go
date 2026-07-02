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
