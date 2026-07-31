package parser

import (
	"github.com/go-mysql-org/go-mysql/replication"
)

// resumeFillCorrector undoes the constant position overshoot go-mysql's
// FillZeroLogPos introduces right after a mid-file (re)connect (#1117).
//
// At connect the server sends an artificial RotateEvent naming the resume
// offset R, then re-sends the file's FORMAT_DESCRIPTION event. On a mid-file
// connect (R > 4) that FDE is a GHOST: it is not physically located at R, and
// the server zeroes its end_log_pos on the wire. FillZeroLogPos (MariaDB
// 11.4+ compensation) cannot know that and fills it to R+len(FDE), advancing
// the library's internal running position past the resume point. Every
// subsequent cache-buffered event (LogPos=0 on the wire: ANNOTATE, TABLE_MAP,
// row events) is then filled len(FDE) bytes beyond its true offset, until the
// first directly-written event (GTID, XID — genuine wire LogPos) snaps the
// running position back. A resume that lands INSIDE a transaction — a #775
// statement-boundary checkpoint, or go-mysql's own auto-resync after a
// network error, which resumes from its internal per-event position — would
// therefore store the transaction tail's rows with positions inflated by
// exactly len(FDE) (values a persisted checkpoint would turn into a fatal
// server error 1236 on the next restart, since they are not event
// boundaries), and the genuine snap-back would read as a same-file backward
// jump to the wraparound guard, killing the stream. Verified live against
// MariaDB 11.4.12: resume at 1264 → FDE filled to 1516 → tail filled
// 1584/1637/1680 (true offsets 1332/1385/1428) → XID genuine 1459.
//
// The corrector detects that signature and rewrites the filled headers back
// to their true offsets. It is pure arithmetic on known quantities: the
// artificial rotate names R; a binlog file is contiguous, so the first
// post-FDE event's true end is R+size and each following one's is
// prevTrue+size; an observed LogPos equal to true+len(FDE) is the poisoned
// fill, equal to true is a genuine value (which also self-corrects the
// library's running position, closing the window). Anything else is
// unexpected: the corrector disarms WITHOUT rewriting — the wraparound guard
// downstream stays the fail-loud backstop, and the handleRows underflow belt
// stays the zero-position backstop.
//
// Cases that reduce to no-ops by construction:
//   - MySQL flavor: FillZeroLogPos is library-gated to MariaDB, the ghost FDE
//     keeps LogPos=0 on the wire, and the first real event matches its true
//     offset → disarmed, nothing rewritten.
//   - Connect at a transaction boundary (the common case): the first
//     post-FDE event is a directly-written GTID event at its true offset →
//     disarmed immediately.
//   - Connect at a file start (R <= 4): the FDE is physically present and its
//     (genuine or filled) LogPos equals the true offset → never armed as a
//     ghost.
//   - Transaction_payload inner events: their headers are rewritten to the
//     outer event's coordinates before recursing, and the payload path is
//     MySQL-only, where no window ever opens.
type resumeFillCorrector struct {
	// armed is true between an artificial rotate and the verdict on the first
	// position-bearing event after the (ghost) FDE.
	armed bool
	// base is the resume offset R named by the artificial rotate.
	base uint32
	// ghost is the re-sent FDE's EventSize, once seen while armed; the
	// overshoot magnitude if the window turns out to be poisoned.
	ghost uint32
	// adjust is the confirmed overshoot being subtracted; 0 = no open window.
	adjust uint32
	// prevTrue is the corrected end offset of the previous event in the window.
	prevTrue uint32
}

// Observe inspects one streamed event and, when inside a confirmed poisoned
// fill window, rewrites its header LogPos back to the true file offset.
// Returns true when it rewrote the header.
func (c *resumeFillCorrector) Observe(ev *replication.BinlogEvent) bool {
	hdr := ev.Header
	if rot, isRotate := ev.Event.(*replication.RotateEvent); isRotate {
		if hdr.Flags&replication.LOG_EVENT_ARTIFICIAL_F != 0 {
			// Connect-time fake rotate: arm at the resume offset it names.
			*c = resumeFillCorrector{armed: true, base: uint32(rot.Position)}
		} else {
			// Real rotation: fresh file, the next FDE is physically present.
			*c = resumeFillCorrector{}
		}
		return false
	}
	if hdr.Flags&replication.LOG_EVENT_ARTIFICIAL_F != 0 {
		return false // heartbeats etc. — not file events, not filled
	}
	if _, isFDE := ev.Event.(*replication.FormatDescriptionEvent); isFDE {
		if c.armed && c.base > 4 {
			c.ghost = hdr.EventSize // mid-file connect: this FDE is a ghost
		} else {
			*c = resumeFillCorrector{} // physically-present FDE: nothing to correct
		}
		return false
	}
	if c.armed {
		// First position-bearing event after the connect sequence.
		armedGhost := c.ghost
		base := c.base
		*c = resumeFillCorrector{}
		if armedGhost == 0 {
			return false // no ghost FDE seen: nothing to correct
		}
		expected := base + hdr.EventSize
		switch hdr.LogPos {
		case expected + armedGhost: // the poisoned-fill signature
			hdr.LogPos = expected
			c.adjust, c.prevTrue = armedGhost, expected
			return true
		default: // genuine value (== expected), or unexpected: leave untouched
			return false
		}
	}
	if c.adjust != 0 {
		expected := c.prevTrue + hdr.EventSize
		switch hdr.LogPos {
		case expected + c.adjust: // still inside the poisoned window
			hdr.LogPos = expected
			c.prevTrue = expected
			return true
		default:
			// Genuine value (== expected — the library self-corrected) or an
			// unexpected shape: close the window either way; the guard is the
			// backstop for the latter.
			*c = resumeFillCorrector{}
			return false
		}
	}
	return false
}
