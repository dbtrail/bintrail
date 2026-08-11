// Package status — this file implements the operator acknowledgement of a
// capture-skip tally (#1314).
//
// The problem it solves: stream_state.capture_skips is MONOTONIC. It counts
// events that were read and dropped, not events still being dropped, so it
// reads identically before and after a successful fix and never goes away. The
// only escape the product used to document was "stop the daemon and clear
// stream_state.capture_skips" — impossible from the console, and it DESTROYS
// the loss record, which is the one thing that should survive. An operator who
// cannot retire the alarm eventually deletes --fail-on-gap from cron, and then
// the next real loss is silent.
//
// Two decisions carry the design:
//
//   - The acknowledgement lives in its own column. capture_skips is rewritten
//     from the daemon's in-memory tally on every checkpoint (streamrun's
//     saveCheckpoint is its only writer), so an ack stored inside it would be
//     overwritten within seconds.
//   - An acknowledgement records a COUNT, not a fact: "I have seen these N".
//     A later skip pushes the tally above the acknowledged number and the
//     alarm comes back on its own. That is what makes it safe for the console
//     to go quiet — an acknowledgement can retire a record, it cannot hide a
//     fresh incident. A fact-shaped ack (a bare "acknowledged: true", or
//     clearing the tally) would suppress the next loss too, which is exactly
//     the failure the capture-health verdict exists to prevent.
//
// Nothing is erased. capture_skips keeps its counts, `status` keeps reporting
// total/reasons/last-skip, and this adds a timestamp saying a human saw it.
package status

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// CaptureSkipAck is one reason's acknowledgement, as persisted in
// stream_state.capture_skips_ack.
type CaptureSkipAck struct {
	// Count is the skip count that was acknowledged. The reason reads as
	// acknowledged while the live tally has not risen above it.
	Count int64 `json:"count"`
	// At is when the acknowledgement was made — the only new fact this feature
	// records, and the one the console shows instead of the alarm.
	At time.Time `json:"at"`
}

// Sentinels for the two refusals a caller must distinguish. Everything else is
// a plain database error.
var (
	// ErrNothingToAcknowledge means there is no skip tally to acknowledge:
	// either no skip-aware daemon has written the ledger, or it is clean.
	// Acknowledging nothing would write a record that suppresses the NEXT
	// skip, so it is refused rather than treated as a no-op.
	ErrNothingToAcknowledge = errors.New("no capture skips to acknowledge")
	// ErrAcknowledgeStale means the live tally is higher than the total the
	// caller says it saw — events were skipped between the caller reading and
	// acting. Acknowledging here would retire a record nobody looked at, so
	// the caller is sent back to re-read.
	ErrAcknowledgeStale = errors.New("capture skips rose since they were read")
	// ErrAckColumnMissing means this index predates capture_skips_ack. It is
	// its own sentinel because the console cannot fix it: the console never
	// runs DDL on a registry index (see connManager), so it must tell the
	// operator which CLI command migrates instead of failing opaquely.
	ErrAckColumnMissing = errors.New("this index has no capture_skips_ack column")
)

// Acknowledgement is what an acknowledgement wrote, for the caller to report.
type Acknowledgement struct {
	Total   int64     // events acknowledged, summed across reasons
	Reasons []string  // the reasons acknowledged, most events first
	At      time.Time // the stamp written into every reason's entry
}

// ParseCaptureSkipsAck decodes the persisted acknowledgement. A missing column,
// an empty value or an unparseable payload all yield nil — which reads as
// UNACKNOWLEDGED, so a decode failure can only ever leave the alarm up. This is
// deliberately the opposite tolerance from a verdict that could go quiet on bad
// data.
func (s *StreamStateInfo) ParseCaptureSkipsAck() map[string]CaptureSkipAck {
	if !s.CaptureSkipsAck.Valid || strings.TrimSpace(s.CaptureSkipsAck.String) == "" {
		return nil
	}
	m := map[string]CaptureSkipAck{}
	if err := json.Unmarshal([]byte(s.CaptureSkipsAck.String), &m); err != nil {
		slog.Warn("could not parse capture_skips_ack; capture skips read as unacknowledged", "error", err)
		return nil
	}
	return m
}

// CaptureSkipsAcknowledged reports whether EVERY reason with a live count has
// been acknowledged at or above that count.
//
// False when there is nothing skipped: "acknowledged" is a statement about a
// record, and with no record there is nothing to say. Callers asking "should I
// stay quiet?" must check the tally first — a clean ledger is already quiet.
func CaptureSkipsAcknowledged(skips map[string]CaptureSkipStat, ack map[string]CaptureSkipAck) bool {
	active := activeReasons(skips)
	if len(active) == 0 {
		return false
	}
	for _, r := range active {
		if ack[r].Count < skips[r].Count {
			return false
		}
	}
	return true
}

// CaptureSkipsAcknowledgedAt returns the NEWEST acknowledgement stamp among the
// active reasons — the moment the whole record became acknowledged. Zero when
// it is not fully acknowledged.
//
// Newest, not oldest: acknowledging three reasons on Monday and a fourth on
// Friday makes Friday the moment an operator had seen all of it.
func CaptureSkipsAcknowledgedAt(skips map[string]CaptureSkipStat, ack map[string]CaptureSkipAck) time.Time {
	if !CaptureSkipsAcknowledged(skips, ack) {
		return time.Time{}
	}
	var newest time.Time
	for _, r := range activeReasons(skips) {
		if at := ack[r].At; at.After(newest) {
			newest = at
		}
	}
	return newest
}

// AcknowledgeCaptureSkips records that an operator has seen the current tally.
//
// expectTotal is the total the caller saw, and it is the stale-render guard: a
// console tab rendered ten minutes ago must not be able to acknowledge skips
// that happened since. Pass a negative value to acknowledge whatever is there
// (the CLI's default — it reads and writes in the same breath, so there is no
// stale view to protect against).
//
// The read and the write share one transaction with SELECT ... FOR UPDATE. The
// capture daemon's checkpoint upsert touches the same single row, so without
// the lock a checkpoint landing between our read and our write would leave us
// acknowledging a count we never saw — the precise thing expectTotal exists to
// prevent, arriving by another door.
func AcknowledgeCaptureSkips(ctx context.Context, db *sql.DB, expectTotal int64, now time.Time) (Acknowledgement, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Acknowledgement{}, err
	}
	defer tx.Rollback() //nolint:errcheck // rolled back only when commit did not happen

	var raw sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT capture_skips FROM stream_state WHERE id = 1 FOR UPDATE`).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return Acknowledgement{}, ErrNothingToAcknowledge
	}
	if err != nil {
		return Acknowledgement{}, err
	}
	state := StreamStateInfo{CaptureSkips: raw}
	skips, ok := state.ParseCaptureSkips()
	if !ok {
		// An unreadable ledger is NOT acknowledgeable: it may be hiding a loss
		// count, and stamping an ack over it would retire a number nobody —
		// including us — could read.
		return Acknowledgement{}, ErrNothingToAcknowledge
	}
	active := activeReasons(skips)
	if len(active) == 0 {
		return Acknowledgement{}, ErrNothingToAcknowledge
	}
	total := totalCaptureSkips(skips)
	if expectTotal >= 0 && total > expectTotal {
		return Acknowledgement{}, fmt.Errorf("%w: %d now, %d when read", ErrAcknowledgeStale, total, expectTotal)
	}

	at := now.UTC().Truncate(time.Second)
	ack := make(map[string]CaptureSkipAck, len(active))
	for _, r := range active {
		ack[r] = CaptureSkipAck{Count: skips[r].Count, At: at}
	}
	payload, err := json.Marshal(ack)
	if err != nil {
		return Acknowledgement{}, err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE stream_state SET capture_skips_ack = ? WHERE id = 1`, string(payload)); err != nil {
		if isUnknownColumnErr(err) {
			return Acknowledgement{}, ErrAckColumnMissing
		}
		return Acknowledgement{}, err
	}
	if err := tx.Commit(); err != nil {
		return Acknowledgement{}, err
	}
	return Acknowledgement{Total: total, Reasons: active, At: at}, nil
}
