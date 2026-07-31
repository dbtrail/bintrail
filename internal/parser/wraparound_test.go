package parser

import (
	"context"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// ─── #845: binlog position wraparound detection ───────────────────────────────
//
// resolveStartForFlavor's resume-time guard (internal/streamrun) checks
// saved.binlogPos > math.MaxUint32 before casting to uint32 — but every writer
// of that saved value already derives it from replication.EventHeader.LogPos,
// itself a uint32 wire field, so the guard can never actually fire through this
// codebase's own capture path. The real corruption happens upstream: MySQL's
// end_log_pos is a 4-byte field in every event's wire header, so a single
// oversized transaction that delays rotation past 4GiB makes the SOURCE itself
// wrap the position it reports, with no signal. These tests pin the guard that
// actually catches it — live, inside StreamParser.Run, during streaming — by
// exercising the real capture-time code path (AddEventToStreamer + Run), not a
// hand-built streamState struct.

// TestStreamParser_positionWraparoundFailsLoud simulates the issue's own
// scenario: a transaction whose commit lands near the uint32 ceiling, followed
// (same file, no RotateEvent) by a transaction whose commit lands at a small
// value — the unmistakable signature of the source having wrapped LogPos after
// crossing 4GiB. Run must fail loud, pointing at GTID mode, instead of silently
// letting the checkpoint advance under the wrapped position.
func TestStreamParser_positionWraparoundFailsLoud(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 16)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeRotate("binlog.000009"),
		makeGTIDEvent(1),
		makeQueryEvent("BEGIN"),
		makeXIDEvent(4_294_967_290), // near math.MaxUint32 — the oversized transaction's commit
		makeGTIDEvent(2),
		makeQueryEvent("BEGIN"),
		makeXIDEvent(1000), // wrapped: smaller than the prior commit, same file, no rotate
	)

	err := sp.Run(ctx, streamer, out)
	if err == nil {
		t.Fatal("expected a wraparound error, got nil")
	}
	if !strings.Contains(err.Error(), "wraparound") {
		t.Errorf("error should name the wraparound, got: %v", err)
	}
	if !strings.Contains(err.Error(), "binlog.000009") {
		t.Errorf("error should name the file, got: %v", err)
	}
	if !strings.Contains(err.Error(), "GTID") {
		t.Errorf("error should direct the operator to GTID mode, got: %v", err)
	}
}

// TestStreamParser_positionWraparound_mariadbHint pins that the remediation
// text names MariaDB's gtid_binlog_pos (not MySQL's gtid_executed, which
// errors as an unknown system variable on MariaDB) when the stream is
// configured for the mariadb flavor via SetFlavor.
func TestStreamParser_positionWraparound_mariadbHint(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	sp.SetFlavor("mariadb")
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 16)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeRotate("binlog.000009"),
		makeGTIDEvent(1),
		makeQueryEvent("BEGIN"),
		makeXIDEvent(4_294_967_290),
		makeGTIDEvent(2),
		makeQueryEvent("BEGIN"),
		makeXIDEvent(1000),
	)

	err := sp.Run(ctx, streamer, out)
	if err == nil {
		t.Fatal("expected a wraparound error, got nil")
	}
	if !strings.Contains(err.Error(), "gtid_binlog_pos") {
		t.Errorf("MariaDB error should hint gtid_binlog_pos, got: %v", err)
	}
	if strings.Contains(err.Error(), "gtid_executed") {
		t.Errorf("MariaDB error must not suggest MySQL's gtid_executed (unknown system variable on MariaDB), got: %v", err)
	}
}

// TestStreamParser_filledFDEDoesNotFalseTripWraparound pins the #1117 guard
// reconciliation: on a mid-file (re)connect the server re-sends the file's
// FORMAT_DESCRIPTION event with LogPos zeroed on the wire, and go-mysql's
// FillZeroLogPos (enabled for MariaDB 11.4+ sources) fills that zero to
// resumePos+EventSize — a synthetic value that overshoots the next
// transaction's real positions. Sequence verified live against MariaDB
// 11.4.12: resume at 938 → fake rotate → FDE filled to 1190 → GTID event
// real 980. Before the fix, the guard read 1190→980 as a same-file backward
// jump and killed the stream; the FDE must neither trip the guard nor
// advance its high-water mark.
func TestStreamParser_filledFDEDoesNotFalseTripWraparound(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	sp.SetFlavor("mariadb")
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 16)

	fde := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.FORMAT_DESCRIPTION_EVENT,
			LogPos:    1190, // filled: resume pos 938 + EventSize 252
			EventSize: 252,
		},
		Event: &replication.FormatDescriptionEvent{Version: 4},
	}
	gtid := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.MARIADB_GTID_EVENT, LogPos: 980},
		Event: &replication.MariadbGTIDEvent{
			GTID: mysql.MariadbGTID{DomainID: 0, ServerID: 2, SequenceNumber: 42},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeRotate("mariadb-bin.000002"), // fake resume rotate
		fde,
		gtid,
		makeQueryEvent("BEGIN"),
		makeXIDEvent(1175), // real commit position — below the filled FDE value
	)

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: unexpected error after a filled connect-time FDE: %v", err)
	}
}

// TestStreamParser_zeroLogPosRowFailsLoud pins the #1117 fail-loud belt: a row
// event whose LogPos is below its EventSize (MariaDB 11.4+ sends
// cache-buffered events with end_log_pos=0 when FillZeroLogPos is absent)
// must abort the stream instead of being indexed with an underflowed
// start_pos = 2^64-EventSize — a value the resume-time dedup treats as beyond
// every checkpoint, deleting the whole file's rows on every restart.
func TestStreamParser_zeroLogPosRowFailsLoud(t *testing.T) {
	sp := NewStreamParser(makeOrdersResolver(), Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 16)

	zeroPosRows := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.WRITE_ROWS_EVENTv1,
			LogPos:    0, // as MariaDB 11.4+ writes it for cache-buffered events
			EventSize: 53,
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
		makeRotate("mariadb-bin.000002"),
		zeroPosRows,
	)

	err := sp.Run(ctx, streamer, out)
	if err == nil {
		t.Fatal("expected a fail-loud error for a zero-LogPos row event, got nil")
	}
	if !strings.Contains(err.Error(), "could not be established") {
		t.Errorf("error should say the position could not be established, got: %v", err)
	}
	if !strings.Contains(err.Error(), "shop.orders") {
		t.Errorf("error should name the table, got: %v", err)
	}
}

// TestStreamParser_positionResetByRealRotate proves the detector does NOT
// false-positive on the ordinary case a same-file backward jump is meant to
// distinguish from: position legitimately restarting small in a genuinely new
// file, signaled by an intervening RotateEvent.
func TestStreamParser_positionResetByRealRotate(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 16)

	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeRotate("binlog.000009"),
		makeGTIDEvent(1),
		makeQueryEvent("BEGIN"),
		makeXIDEvent(4_294_967_290), // high position near end of binlog.000009
		makeRotate("binlog.000010"), // legitimate rotation to a new file
		makeGTIDEvent(2),
		makeQueryEvent("BEGIN"),
		makeXIDEvent(500), // small position — fine, it's a fresh file
	)

	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: unexpected error across a real rotation: %v", err)
	}
}
