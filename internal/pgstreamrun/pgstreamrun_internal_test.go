package pgstreamrun

import (
	"log/slog"
	"testing"
)

func TestResolveStartLSN(t *testing.T) {
	log := slog.New(slog.DiscardHandler)

	// A saved checkpoint wins over the flag (idempotent resume).
	if got := resolveStartLSN(&pgStreamState{lsn: 500}, 999, log); got != 500 {
		t.Errorf("saved checkpoint: got %d, want 500", got)
	}
	// No checkpoint → the explicit flag.
	if got := resolveStartLSN(nil, 999, log); got != 999 {
		t.Errorf("flag start: got %d, want 999", got)
	}
	// No checkpoint, no flag → 0: first run, the capturer starts from the slot's
	// ConsistentPoint (this must NOT be an error).
	if got := resolveStartLSN(nil, 0, log); got != 0 {
		t.Errorf("first run: got %d, want 0", got)
	}
}
