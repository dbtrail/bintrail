package status

import (
	"database/sql"
	"testing"
	"time"
)

func skip(count int64) CaptureSkipStat               { return CaptureSkipStat{Count: count, LastAt: time.Now()} }
func ackAt(count int64, at time.Time) CaptureSkipAck { return CaptureSkipAck{Count: count, At: at} }

// TestCaptureSkipsAcknowledged pins the verdict, and in particular the one
// property that makes it safe for the console to go quiet on an acknowledged
// record: an acknowledgement covers a COUNT, so anything skipped afterwards
// un-acknowledges it with no operator action.
func TestCaptureSkipsAcknowledged(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name  string
		skips map[string]CaptureSkipStat
		ack   map[string]CaptureSkipAck
		want  bool
	}{
		{
			name:  "exact count acknowledged",
			skips: map[string]CaptureSkipStat{"column_count_mismatch": skip(3)},
			ack:   map[string]CaptureSkipAck{"column_count_mismatch": ackAt(3, now)},
			want:  true,
		},
		{
			// THE load-bearing case: three were acknowledged, a fourth was
			// skipped. If this ever returns true the feature is a mute button
			// on future data loss.
			name:  "a later skip re-arms the alarm",
			skips: map[string]CaptureSkipStat{"column_count_mismatch": skip(4)},
			ack:   map[string]CaptureSkipAck{"column_count_mismatch": ackAt(3, now)},
			want:  false,
		},
		{
			// A reason that did not exist when the operator acknowledged is
			// unacknowledged by construction: ack[missing].Count is zero and
			// the live count is not.
			name:  "a new reason is not covered by an older acknowledgement",
			skips: map[string]CaptureSkipStat{"column_count_mismatch": skip(3), "no_resolver": skip(1)},
			ack:   map[string]CaptureSkipAck{"column_count_mismatch": ackAt(3, now)},
			want:  false,
		},
		{
			name:  "all reasons must be covered",
			skips: map[string]CaptureSkipStat{"column_count_mismatch": skip(3), "no_resolver": skip(1)},
			ack:   map[string]CaptureSkipAck{"column_count_mismatch": ackAt(3, now), "no_resolver": ackAt(1, now)},
			want:  true,
		},
		{
			// An ack higher than the tally is not a fault worth alarming on
			// (it cannot happen through either surface — both stamp what they
			// read — and treating it as unacknowledged would strand the
			// operator with an alarm they already retired).
			name:  "an acknowledgement above the tally still counts",
			skips: map[string]CaptureSkipStat{"column_count_mismatch": skip(2)},
			ack:   map[string]CaptureSkipAck{"column_count_mismatch": ackAt(9, now)},
			want:  true,
		},
		{
			// "Acknowledged" is a statement about a record. With no record
			// there is nothing to say, and a true here would let a caller
			// render "acknowledged" over a perfectly clean ledger.
			name:  "a clean ledger is not acknowledged",
			skips: map[string]CaptureSkipStat{},
			ack:   map[string]CaptureSkipAck{"column_count_mismatch": ackAt(3, now)},
			want:  false,
		},
		{
			// A zero-count entry is not an active reason (activeReasons skips
			// it), so it must not demand an acknowledgement of its own.
			name:  "a zero-count reason needs no acknowledgement",
			skips: map[string]CaptureSkipStat{"column_count_mismatch": skip(3), "no_resolver": skip(0)},
			ack:   map[string]CaptureSkipAck{"column_count_mismatch": ackAt(3, now)},
			want:  true,
		},
		{
			name:  "no acknowledgement at all",
			skips: map[string]CaptureSkipStat{"column_count_mismatch": skip(3)},
			ack:   nil,
			want:  false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := CaptureSkipsAcknowledged(tc.skips, tc.ack); got != tc.want {
				t.Errorf("CaptureSkipsAcknowledged = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestCaptureSkipsAcknowledgedAtIsNewest pins that the reported moment is when
// the operator had seen ALL of it — acknowledging one reason on Monday and
// another on Friday makes Friday the answer, not Monday.
func TestCaptureSkipsAcknowledgedAtIsNewest(t *testing.T) {
	mon := time.Date(2026, 8, 3, 9, 0, 0, 0, time.UTC)
	fri := time.Date(2026, 8, 7, 9, 0, 0, 0, time.UTC)
	skips := map[string]CaptureSkipStat{"column_count_mismatch": skip(3), "no_resolver": skip(1)}
	ack := map[string]CaptureSkipAck{"column_count_mismatch": ackAt(3, mon), "no_resolver": ackAt(1, fri)}
	if got := CaptureSkipsAcknowledgedAt(skips, ack); !got.Equal(fri) {
		t.Errorf("acknowledged at %s, want the newest stamp %s", got, fri)
	}
	// Not fully acknowledged ⇒ zero, so no caller can print a stamp for a
	// record that is still alarming.
	partial := map[string]CaptureSkipAck{"column_count_mismatch": ackAt(3, mon)}
	if got := CaptureSkipsAcknowledgedAt(skips, partial); !got.IsZero() {
		t.Errorf("partially acknowledged reported %s, want zero", got)
	}
}

// TestParseCaptureSkipsAckFailsUnacknowledged pins the tolerance DIRECTION: a
// missing column, empty value or corrupt payload must all read as
// unacknowledged, so a decode failure can only ever leave the alarm up. The
// opposite (defaulting to acknowledged) would let one bad write silence a
// permanent-loss record.
func TestParseCaptureSkipsAckFailsUnacknowledged(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  sql.NullString
	}{
		{"legacy index without the column", sql.NullString{}},
		{"empty value", sql.NullString{Valid: true, String: "  "}},
		{"corrupt payload", sql.NullString{Valid: true, String: "{not json"}},
		{"wrong shape", sql.NullString{Valid: true, String: `{"column_count_mismatch":42}`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := StreamStateInfo{CaptureSkipsAck: tc.raw}
			ack := s.ParseCaptureSkipsAck()
			skips := map[string]CaptureSkipStat{"column_count_mismatch": skip(1)}
			if CaptureSkipsAcknowledged(skips, ack) {
				t.Error("an undecodable acknowledgement read as acknowledged; the alarm would be silenced by bad data")
			}
		})
	}
}

// TestParseCaptureSkipsAckRoundTrip pins the persisted shape, since it is
// written by one surface and read by another (and by whatever reads the column
// directly during an incident).
func TestParseCaptureSkipsAckRoundTrip(t *testing.T) {
	s := StreamStateInfo{CaptureSkipsAck: sql.NullString{Valid: true,
		String: `{"column_count_mismatch":{"count":7,"at":"2026-08-11T20:14:00Z"}}`}}
	ack := s.ParseCaptureSkipsAck()
	got, ok := ack["column_count_mismatch"]
	if !ok {
		t.Fatalf("reason missing from decoded acknowledgement: %#v", ack)
	}
	if got.Count != 7 {
		t.Errorf("count = %d, want 7", got.Count)
	}
	if want := time.Date(2026, 8, 11, 20, 14, 0, 0, time.UTC); !got.At.Equal(want) {
		t.Errorf("at = %s, want %s", got.At, want)
	}
}
