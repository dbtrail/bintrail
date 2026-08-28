package consoleapp

import "time"

// crashLoopPolicy is the restart policy shared by the supervised registry
// sources (monitorSupervisor.run) and the daemon's main source
// (runMainStreamWithWriteDeadlineRetry): exponential backoff plus a circuit
// breaker, in ONE implementation so the two cannot drift apart. It holds no
// clock of its own — callers pass the run's start and the current time — so the
// arithmetic is testable at real constants without sleeping through them.
type crashLoopPolicy struct {
	attempt int
	// since is the first FAILURE of the current loop; zero means not looping.
	since time.Time
}

// failed records one failed run and returns how long to wait before restarting,
// how long the current loop has been going, and whether to stop retrying.
func (p *crashLoopPolicy) failed(started, now time.Time) (delay, looping time.Duration, giveUp bool) {
	// A run that streamed for longer than monitorHealthyReset was not part of a
	// crash loop, so it clears both the backoff and the breaker clock.
	//
	// A source that flaps SLOWLY therefore restarts indefinitely. That is
	// deliberate, not an oversight, and TestMonitorRun_healthyRunResetsBreaker
	// pins it: across such a cycle capture is up for all but the backoff, and
	// stopping capture outright would be worse than the flap. The breaker exists
	// for a FAST crash loop, where restarting achieves nothing.
	if now.Sub(started) > monitorHealthyReset {
		p.attempt = 0
		p.since = time.Time{}
	}
	if p.since.IsZero() {
		// The FAILURE time, never the run's start.
		//
		// Charging a healthy run's uptime to the breaker made it trip on failure
		// number ONE of any daemon that had been up longer than
		// monitorGiveUpAfter, which is the normal state of a capture daemon: a
		// process up 35 hours reported "gave up after 35h of crash-looping" on its
		// first stall, having restarted nothing. The restart policy therefore did
		// not engage at all in the incident it exists to handle (#1482), and a
		// registry source hit the same wall as a permanent `failed` (#1031).
		p.since = now
	}
	looping = now.Sub(p.since)
	// >= so that a zero threshold still means "trip on the first failure", which
	// is how the breaker's own tests express "immediately".
	if looping >= monitorGiveUpAfter {
		return 0, looping, true
	}
	// Advance the backoff only while it is still growing. Past the cap another
	// shift changes nothing, and left unbounded it overflows int64 (at attempt 30
	// for a 15s base) where min() would pick the NEGATIVE value and the backoff
	// would silently become no delay at all.
	shifted := monitorBackoffBase << p.attempt
	if shifted <= 0 || shifted >= monitorBackoffCap {
		return monitorBackoffCap, looping, false
	}
	p.attempt++
	return shifted, looping, false
}
