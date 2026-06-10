package console

import (
	"fmt"
	"testing"
	"time"
)

func fakeLimiter(t *testing.T) (*loginLimiter, *time.Time) {
	t.Helper()
	now := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	l := newLoginLimiter()
	l.now = func() time.Time { return now }
	return l, &now
}

func TestLimiterPerIPShortWindow(t *testing.T) {
	l, _ := fakeLimiter(t)
	for i := 0; i < ipShortMax; i++ {
		if ok, _ := l.Allow("10.0.0.1"); !ok {
			t.Fatalf("attempt %d denied early", i)
		}
		l.Fail("10.0.0.1")
	}
	ok, retry := l.Allow("10.0.0.1")
	if ok {
		t.Fatalf("%dth failure within a minute should trip the limiter", ipShortMax+1)
	}
	if retry <= 0 || retry > ipShortWindow {
		t.Errorf("retryAfter = %v, want within (0, %v]", retry, ipShortWindow)
	}
	// Another IP is unaffected.
	if ok, _ := l.Allow("10.0.0.2"); !ok {
		t.Error("per-IP limit leaked across IPs")
	}
}

func TestLimiterShortWindowResets(t *testing.T) {
	l, now := fakeLimiter(t)
	for i := 0; i < ipShortMax; i++ {
		l.Fail("10.0.0.1")
	}
	*now = now.Add(ipShortWindow + time.Second)
	if ok, _ := l.Allow("10.0.0.1"); !ok {
		t.Error("short window did not reset after its width elapsed")
	}
}

func TestLimiterPerIPLongWindow(t *testing.T) {
	l, now := fakeLimiter(t)
	// Spread failures so the short window never trips: 4 per minute.
	for i := 0; i < ipLongMax; i++ {
		if i%4 == 0 {
			*now = now.Add(time.Minute + time.Second)
		}
		if ok, _ := l.Allow("10.0.0.1"); !ok {
			t.Fatalf("attempt %d denied before the long window should trip", i)
		}
		l.Fail("10.0.0.1")
	}
	if ok, _ := l.Allow("10.0.0.1"); ok {
		t.Errorf("%dth failure in 15min should trip the long window", ipLongMax+1)
	}
}

func TestLimiterGlobalWindow(t *testing.T) {
	l, _ := fakeLimiter(t)
	// Distinct IPs each stay under their per-IP caps; the global cap trips.
	for i := 0; i < globalMax; i++ {
		ip := fmt.Sprintf("10.0.%d.%d", i/250, i%250)
		if ok, _ := l.Allow(ip); !ok {
			t.Fatalf("attempt %d denied early", i)
		}
		l.Fail(ip)
	}
	if ok, _ := l.Allow("192.168.1.1"); ok {
		t.Error("global window did not trip across rotating IPs")
	}
}

func TestLimiterSuccessClearsIP(t *testing.T) {
	l, _ := fakeLimiter(t)
	for i := 0; i < ipShortMax; i++ {
		l.Fail("10.0.0.1")
	}
	l.Success("10.0.0.1")
	if ok, _ := l.Allow("10.0.0.1"); !ok {
		t.Error("successful login did not clear the IP's counters")
	}
}

func TestLimiterMapCapPrunesExpired(t *testing.T) {
	l, now := fakeLimiter(t)
	for i := 0; i < maxTrackedIPs; i++ {
		l.Fail(fmt.Sprintf("ip-%d", i))
	}
	if len(l.perIP) != maxTrackedIPs {
		t.Fatalf("tracked IPs = %d, want %d", len(l.perIP), maxTrackedIPs)
	}
	// At the cap with nothing expired: a brand-new IP is not tracked (the
	// global window absorbs the flood) — the map must not grow.
	l.Fail("fresh-ip")
	if len(l.perIP) > maxTrackedIPs {
		t.Error("map grew past its cap")
	}
	// Once the long window elapses, pruning makes room again.
	*now = now.Add(ipLongWindow + time.Second)
	l.Fail("fresh-ip-2")
	if _, ok := l.perIP["fresh-ip-2"]; !ok {
		t.Error("expired entries were not pruned to admit a new IP")
	}
}

func TestLimiterNilFailsClosed(t *testing.T) {
	var l *loginLimiter
	// A nil limiter DENIES — New always populates loginLimiter, so a nil here
	// is a construction bug that must surface as "throttled", not as silently
	// disabled brute-force defense.
	if ok, _ := l.Allow("10.0.0.1"); ok {
		t.Error("nil limiter allowed — must fail closed")
	}
	l.Fail("10.0.0.1")    // must not panic
	l.Success("10.0.0.1") // must not panic
}

func TestLimiterGlobalWindowResets(t *testing.T) {
	l, now := fakeLimiter(t)
	for i := 0; i < globalMax; i++ {
		l.Fail(fmt.Sprintf("10.0.%d.%d", i/250, i%250))
	}
	if ok, _ := l.Allow("192.168.1.1"); ok {
		t.Fatal("global window should be tripped")
	}
	// After the window elapses it must reset — otherwise 30 global failures
	// brick logins for everyone forever (the self-DoS the throttle forbids).
	*now = now.Add(globalWindow + time.Second)
	if ok, _ := l.Allow("192.168.1.1"); !ok {
		t.Error("global window did not reset after its width elapsed")
	}
}
