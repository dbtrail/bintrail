package shim

import "context"

// Gate caps how many full-table time-travel reconstructions run
// concurrently across every connection of one shim process (#823).
// Full-table queries are the shim's heaviest path — each buffers up to
// FullTableRowCap rows post-merge (multiplied pre-merge by the number
// of archive sources) plus DuckDB/S3 fetch state — so an ORM retry
// loop stacking them unbounded can OOM the daemon or saturate the
// index DB shared with the streamer. Excess queries WAIT for a slot
// rather than failing immediately; the per-query context (QueryTimeout
// / client disconnect) bounds the wait, so abandoned waiters are
// reaped instead of queuing forever.
//
// A nil *Gate admits everything — all methods are nil-receiver safe so
// call sites stay unconditional.
type Gate struct {
	slots chan struct{}
}

// NewGate returns a Gate admitting at most n concurrent holders.
// n <= 0 returns nil (unlimited).
func NewGate(n int) *Gate {
	if n <= 0 {
		return nil
	}
	return &Gate{slots: make(chan struct{}, n)}
}

// Acquire blocks until a slot is free or ctx is done, returning
// ctx.Err() in the latter case. Every successful Acquire must be
// paired with exactly one Release.
func (g *Gate) Acquire(ctx context.Context) error {
	if g == nil {
		return nil
	}
	select {
	case g.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release frees a slot taken by a successful Acquire.
func (g *Gate) Release() {
	if g == nil {
		return
	}
	<-g.slots
}

// Cap reports the gate's concurrency limit (0 for the nil, unlimited
// gate). Used to build actionable saturation error messages.
func (g *Gate) Cap() int {
	if g == nil {
		return 0
	}
	return cap(g.slots)
}
