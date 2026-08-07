package status

import (
	"fmt"
	"io"
	"time"
)

// The freshness verdict vocabulary (#1226), deliberately shaped like the
// continuity one above it: named symbols so a consumer branches on a constant,
// and two verdicts that are NOT claims about health so a consumer cannot fold
// "could not evaluate" into a pass.
//
//	"current"     — the daemon is checkpointing AND recent events are indexed
//	"idle"        — checkpointing, but nothing recent was indexed. See the note
//	                on FreshnessStatus: this is genuinely ambiguous offline.
//	"stalled"     — the checkpoint itself is stale: the daemon is dead, wedged,
//	                or cannot write stream_state. This is the alertable state.
//	"unknown"     — a stream row with no checkpoint to judge; never a false
//	                "current" asserted from absent data.
//	"unavailable" — stream_state could not be read at all.
//	"none"        — no stream row (a file-mode index): no capture ran, so there
//	                is no freshness to claim. A genuine no-claim, not a fault.
const (
	FreshnessCurrent     = "current"
	FreshnessIdle        = "idle"
	FreshnessStalled     = "stalled"
	FreshnessUnknown     = "unknown"
	FreshnessUnavailable = "unavailable"
	FreshnessNone        = "none"
)

// DefaultCheckpointStale is how long stream_state may go un-updated before the
// verdict reads "stalled". The checkpoint ticker runs with or WITHOUT traffic,
// so this is a liveness bound on the daemon, not on the workload.
//
// It is 30× the default --checkpoint interval (10s) on purpose. The tighter
// value that "should" work turns every restart, network blip, or long flush
// into a red verdict on a healthy system, and an alert that cries wolf gets
// muted — at which point it protects nothing. An operator running a deliberately
// long --checkpoint should raise this via the CLI's own threshold rather than
// have the default second-guess them.
const DefaultCheckpointStale = 5 * time.Minute

// FreshnessStatus is the single rule for the machine-readable freshness verdict
// STRINGS, so `bintrail status` and the console (#1227) can never disagree —
// the same discipline ContinuityStatus follows.
//
// It answers a different question from ContinuityStatus. Continuity is "did I
// lose events inside the range I captured?", and is explicitly NOT a liveness
// claim. This is the liveness half: "is capture still keeping up right now?".
// Both are needed — a perfectly contiguous index can be three days stale, and a
// perfectly fresh one can have a hole in the middle.
//
// The hard limit of an OFFLINE verdict, and the reason "idle" exists as its own
// state instead of being called healthy or unhealthy: a source with no writes
// and a source whose capture is an hour behind look IDENTICAL from the index —
// fresh checkpoint, old newest-event. Nothing in stream_state separates them.
// The daemon can tell (that is exactly what index_commit_latency measures,
// #1225); a `status` run against the index cannot, and must not pretend to.
// Callers rendering "idle" should say so rather than imply either reading.
//
// eventsRecentWithin bounds "recent events are indexed". staleAfter bounds the
// checkpoint. Passing zero for either uses DefaultCheckpointStale, so a caller
// cannot accidentally get an always-stalled verdict from an unset field.
func FreshnessStatus(stream *StreamStateInfo, streamErr error, now time.Time, staleAfter, eventsRecentWithin time.Duration) string {
	if staleAfter <= 0 {
		staleAfter = DefaultCheckpointStale
	}
	if eventsRecentWithin <= 0 {
		eventsRecentWithin = DefaultCheckpointStale
	}
	switch {
	case stream == nil && streamErr != nil:
		return FreshnessUnavailable
	case stream == nil:
		return FreshnessNone
	case stream.LastCheckpoint.IsZero():
		// A stream row exists but never recorded a checkpoint. Judging it
		// against `now` would date it to the Unix epoch and report a permanent
		// "stalled" — report the absence instead of inventing a verdict.
		return FreshnessUnknown
	case now.Sub(stream.LastCheckpoint) > staleAfter:
		return FreshnessStalled
	case !stream.LastEventTime.Valid || now.Sub(stream.LastEventTime.Time) > eventsRecentWithin:
		return FreshnessIdle
	default:
		return FreshnessCurrent
	}
}

// FreshnessEvaluated reports whether a verdict is an actual claim about capture
// keeping up. The three that are not — unknown, unavailable, none — must never
// be treated as a pass by an alerting consumer; that fold is the single mistake
// this helper exists to prevent (see the continuity vocabulary's identical
// warning).
func FreshnessEvaluated(verdict string) bool {
	switch verdict {
	case FreshnessCurrent, FreshnessIdle, FreshnessStalled:
		return true
	default:
		return false
	}
}

// CheckpointAge is how long ago the daemon last wrote stream_state, and whether
// that is knowable at all. Callers render the age; the false return is what
// keeps a missing checkpoint from printing as a confident "0s ago".
func CheckpointAge(stream *StreamStateInfo, now time.Time) (time.Duration, bool) {
	if stream == nil || stream.LastCheckpoint.IsZero() {
		return 0, false
	}
	age := now.Sub(stream.LastCheckpoint)
	if age < 0 {
		age = 0 // clock skew between the daemon's host and this one
	}
	return age, true
}

// NewestEventAge is how long ago the newest indexed event was COMMITTED AT THE
// SOURCE — so it crosses the source's clock and ours, and is approximate for
// the same reason availability_lag is (#1225). Negative values clamp to 0.
func NewestEventAge(stream *StreamStateInfo, now time.Time) (time.Duration, bool) {
	if stream == nil || !stream.LastEventTime.Valid {
		return 0, false
	}
	age := now.Sub(stream.LastEventTime.Time)
	if age < 0 {
		age = 0
	}
	return age, true
}

// writeFreshness renders the Stream section's freshness line (#1226). Wording,
// not policy: the verdict comes from FreshnessStatus so the text, the JSON and
// the console cannot drift apart.
//
// "idle" is spelled out rather than abbreviated because it is the one verdict an
// operator will misread. It does NOT mean healthy and it does NOT mean behind —
// offline, those two are the same observation (see FreshnessStatus).
func writeFreshness(w io.Writer, stream *StreamStateInfo, streamErr error, now time.Time) {
	verdict := FreshnessStatus(stream, streamErr, now, 0, 0)
	switch verdict {
	case FreshnessStalled:
		age, _ := CheckpointAge(stream, now)
		fmt.Fprintf(w, "  Freshness:       ⚠ STALLED — no checkpoint for %s (the ticker runs even with no traffic,\n", roundAge(age))
		fmt.Fprintln(w, "                   so this is the daemon, not the workload: check that it is running)")
	case FreshnessIdle:
		if age, ok := NewestEventAge(stream, now); ok {
			fmt.Fprintf(w, "  Freshness:       idle — checkpointing, newest indexed event is %s old\n", roundAge(age))
		} else {
			fmt.Fprintln(w, "  Freshness:       idle — checkpointing, but no event has been indexed yet")
		}
		fmt.Fprintln(w, "                   (no traffic and capture falling behind look identical from the index;")
		fmt.Fprintln(w, "                   bintrail_stream_index_commit_latency_seconds on the daemon tells them apart)")
	case FreshnessCurrent:
		age, _ := NewestEventAge(stream, now)
		fmt.Fprintf(w, "  Freshness:       current — newest indexed event is %s old\n", roundAge(age))
	case FreshnessUnknown:
		fmt.Fprintln(w, "  Freshness:       not evaluated (a stream row with no checkpoint recorded yet)")
	}
	// FreshnessNone and FreshnessUnavailable print nothing here: this block only
	// runs with a stream row in hand, and the surrounding renderer already
	// reports an unreadable stream_state loudly.
}

// roundAge trims a duration to something an operator reads at a glance without
// implying more precision than the one-second source resolution supports.
func roundAge(d time.Duration) time.Duration {
	if d >= time.Hour {
		return d.Round(time.Minute)
	}
	return d.Round(time.Second)
}
