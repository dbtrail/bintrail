package parser

import (
	"context"
	"strings"
	"testing"

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
