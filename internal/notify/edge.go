package notify

import (
	"sync"
	"time"
)

// DefaultRepeatEvery is how long an Edge stays quiet about a still-active
// condition before re-notifying — long enough that a persistent problem
// produces one reminder a day, not one per polling cycle.
const DefaultRepeatEvery = 24 * time.Hour

// Edge tracks per-key condition state so callers notify on the *transition*
// into a bad state, re-notify while it persists only after repeatEvery, and
// notify exactly once on recovery. Keys are caller-defined, e.g.
// "verify:<server-id>".
type Edge struct {
	repeatEvery time.Duration
	now         func() time.Time // injectable for tests

	mu     sync.Mutex
	active map[string]time.Time // key → last notified
}

// NewEdge builds an Edge; repeatEvery <= 0 uses DefaultRepeatEvery.
func NewEdge(repeatEvery time.Duration) *Edge {
	if repeatEvery <= 0 {
		repeatEvery = DefaultRepeatEvery
	}
	return &Edge{repeatEvery: repeatEvery, now: time.Now, active: make(map[string]time.Time)}
}

// Fire reports whether the caller should notify for key being in a bad state
// now: true on the transition into it, and again each repeatEvery while it
// persists.
func (e *Edge) Fire(key string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.now()
	last, ok := e.active[key]
	if ok && now.Sub(last) < e.repeatEvery {
		return false
	}
	e.active[key] = now
	return true
}

// Resolve reports whether the caller should send a recovery notification:
// true only if key was active (fired at least once and not yet resolved).
func (e *Edge) Resolve(key string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.active[key]; !ok {
		return false
	}
	delete(e.active, key)
	return true
}
