package parser

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
)

// ─── Microsecond commit timestamps (#18) ─────────────────────────────────────
//
// MySQL 8.0.1+ writes immediate_commit_timestamp — MICROSECONDS since epoch —
// into every GTID event, while the common header every other event carries
// resolves to one SECOND. These tests pin the transaction-scoped state machine
// that carries the microsecond value onto row events, and the two ways it must
// come back as "unknown" rather than as a wrong-but-precise number.

// makeGTIDEventAt is makeGTIDEvent with an explicit immediate_commit_timestamp.
func makeGTIDEventAt(gno int64, commitTsUS uint64) *replication.BinlogEvent {
	ev := makeGTIDEvent(gno)
	ev.Event.(*replication.GTIDEvent).ImmediateCommitTimestamp = commitTsUS
	return ev
}

// TestStreamParser_commitTsAttachedToRows: the GTID event's microsecond
// timestamp lands on the transaction's row events, and the NEXT transaction's
// rows carry that transaction's own value — the failure that matters is a
// stale carry-over, which would read as a precise time for the wrong commit.
func TestStreamParser_commitTsAttachedToRows(t *testing.T) {
	const (
		trx1US = uint64(1767225600_123456)
		trx2US = uint64(1767225600_987654)
	)

	sp := NewStreamParser(makeOrdersResolver(), Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 16)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeRotate("binlog.000001"),
		makeGTIDEventAt(1, trx1US),
		makeQueryEvent("BEGIN"),
		makeOrdersInsertEvent(1, 10),
		makeXIDEvent(200),
		makeGTIDEventAt(2, trx2US),
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
	if dml[0].CommitTsUS != trx1US {
		t.Errorf("trx1 CommitTsUS = %d, want %d", dml[0].CommitTsUS, trx1US)
	}
	if dml[1].CommitTsUS != trx2US {
		t.Errorf("trx2 CommitTsUS = %d, want %d (the second transaction's own stamp, not the first's)",
			dml[1].CommitTsUS, trx2US)
	}
}

// TestStreamParser_commitTsUnknownSources: both ways the value is genuinely
// unknown must produce zero, never a carried-over stamp from the previous
// transaction. A stale microsecond timestamp is worse than none: it reads as
// precise, so any consumer correlating against it would silently trust it.
func TestStreamParser_commitTsUnknownSources(t *testing.T) {
	const trx1US = uint64(1767225600_123456)

	tests := []struct {
		name string
		// second transaction's opening event, replacing a MySQL 8.0.1+ GTID
		// event that would carry a timestamp
		opener *replication.BinlogEvent
	}{
		// MariaDB's GTID event has no commit timestamp at all.
		{name: "mariadb gtid event", opener: makeMariadbGTIDEvent(0, 1, 100)},
		// MySQL older than 8.0.1: a GTID event whose timestamp field is zero.
		{name: "pre-8.0.1 mysql gtid event", opener: makeGTIDEventAt(2, 0)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sp := NewStreamParser(makeOrdersResolver(), Filters{}, nil)
			streamer := replication.NewBinlogStreamer()
			out := make(chan Event, 16)

			ctx, cancel := context.WithCancel(context.Background())
			feedThenCancel(t, streamer, cancel,
				makeRotate("binlog.000001"),
				// A first transaction WITH a stamp, so a leak has something to leak.
				makeGTIDEventAt(1, trx1US),
				makeQueryEvent("BEGIN"),
				makeOrdersInsertEvent(1, 10),
				makeXIDEvent(200),
				tc.opener,
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
			if dml[0].CommitTsUS != trx1US {
				t.Errorf("trx1 CommitTsUS = %d, want %d", dml[0].CommitTsUS, trx1US)
			}
			if dml[1].CommitTsUS != 0 {
				t.Errorf("trx2 CommitTsUS = %d, want 0 — this source wrote no commit timestamp, "+
					"and the previous transaction's must not carry over", dml[1].CommitTsUS)
			}
		})
	}
}

// TestStreamParser_unhandledEventTypeIsLogged pins the hygiene half of #18: an
// event type the switch does not name is dropped, and until now it was dropped
// in complete silence. ROWS_QUERY_EVENT carried the originating SQL for years
// and nobody could see the parser was ignoring it — the log line is the only
// thing that makes "the parser has no case for this" discoverable without
// reading the source.
func TestStreamParser_unhandledEventTypeIsLogged(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	sp := NewStreamParser(makeOrdersResolver(), Filters{}, logger)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 8)

	unhandled := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.FORMAT_DESCRIPTION_EVENT, LogPos: 123},
		Event:  &replication.FormatDescriptionEvent{Version: 4},
	}

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel, makeRotate("binlog.000001"), unhandled)

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}

	logged := buf.String()
	if !strings.Contains(logged, "not handled by the stream parser") {
		t.Errorf("an unhandled event type produced no log line; got:\n%s", logged)
	}
	if !strings.Contains(logged, replication.FORMAT_DESCRIPTION_EVENT.String()) {
		t.Errorf("the log line does not name the event type (%s), which is the only useful part; got:\n%s",
			replication.FORMAT_DESCRIPTION_EVENT, logged)
	}
}
