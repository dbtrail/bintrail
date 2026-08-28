package consoleapp

import (
	"testing"
	"time"
)

// The breaker must measure CRASH-LOOPING, not uptime.
//
// Assigning the breaker clock the start of the run that just failed charges a
// daemon's entire healthy lifetime to it. Any daemon up longer than
// monitorGiveUpAfter, which is the normal state of a capture daemon, then gives
// up on failure number ONE having restarted nothing, and logs a crash loop that
// never happened. That defeats the restart policy in exactly the incident it
// exists to handle (#1482) and marks a registry source permanently failed after
// one blip (#1031).
//
// Real constants, fake clock: no sleeping through six hours.
func TestCrashLoopPolicy_longHealthyRunDoesNotTripTheBreaker(t *testing.T) {
	var p crashLoopPolicy
	start := time.Now()

	// A daemon that streamed for 35 hours, then failed once.
	delay, looping, giveUp := p.failed(start, start.Add(35*time.Hour))

	if giveUp {
		t.Fatalf("gave up on the first failure of a 35h-healthy daemon (looping reported as %s); "+
			"the breaker is measuring uptime, not crash-looping", looping)
	}
	if looping != 0 {
		t.Errorf("looping = %s on the first failure; want 0, the loop has just begun", looping)
	}
	if delay != monitorBackoffBase {
		t.Errorf("delay = %s, want the base %s on the first restart", delay, monitorBackoffBase)
	}
}

// The breaker must still fire on a genuine fast crash loop, or the change above
// would trade one broken direction for another.
func TestCrashLoopPolicy_fastCrashLoopStillGivesUp(t *testing.T) {
	var p crashLoopPolicy
	now := time.Now()

	// Runs that fail after a second each, well under monitorHealthyReset, so
	// nothing resets. Walk a fake clock until the breaker trips.
	for i := 0; i < 100000; i++ {
		started := now
		now = now.Add(time.Second) // the run
		delay, looping, giveUp := p.failed(started, now)
		if giveUp {
			if looping < monitorGiveUpAfter {
				t.Fatalf("gave up after only %s of looping, before the %s threshold", looping, monitorGiveUpAfter)
			}
			return
		}
		now = now.Add(delay) // the backoff
	}
	t.Fatalf("a fast crash loop never tripped the %s breaker", monitorGiveUpAfter)
}

// The backoff must never hand back a non-positive delay. Left unbounded the
// shift overflows int64 (attempt 30 at a 15s base), min() picks the NEGATIVE
// value, and the "backoff" becomes an unthrottled hammer against the resource
// that is already failing.
func TestCrashLoopPolicy_backoffNeverGoesNonPositive(t *testing.T) {
	var p crashLoopPolicy
	now := time.Now()

	for i := 0; i < 200; i++ {
		started := now
		now = now.Add(time.Millisecond)
		delay, _, giveUp := p.failed(started, now)
		if giveUp {
			// Re-arm: this test is about the delay, not the breaker.
			p = crashLoopPolicy{}
			continue
		}
		if delay <= 0 {
			t.Fatalf("iteration %d: delay = %s; a non-positive backoff is an unthrottled retry loop", i, delay)
		}
		if delay > monitorBackoffCap {
			t.Fatalf("iteration %d: delay = %s exceeds the %s cap", i, delay, monitorBackoffCap)
		}
		now = now.Add(delay)
	}
}
