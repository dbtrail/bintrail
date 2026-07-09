package console

import (
	"log/slog"
	"net"
	"sync"
	"time"
)

// Brute-force throttling for the two bcrypt-verifying endpoints (login and
// change-password — the latter is the privilege-escalation path for a stolen
// session token). Fixed windows counting FAILED attempts only; a successful
// login clears its IP. Throttle-only, never a lockout: locking the single
// user out would hand an attacker a trivial DoS against the operator.
const (
	ipShortWindow = time.Minute
	ipShortMax    = 5
	ipLongWindow  = 15 * time.Minute
	ipLongMax     = 20
	globalWindow  = time.Minute
	globalMax     = 30 // caps attacker-driven bcrypt CPU at ~30×250ms/min
	maxTrackedIPs = 4096
)

type failWindow struct {
	start time.Time
	count int
}

// hit returns the count within the current window, resetting it when the
// window has elapsed.
func (w *failWindow) observe(now time.Time, width time.Duration) int {
	if now.Sub(w.start) >= width {
		w.start, w.count = now, 0
	}
	return w.count
}

func (w *failWindow) retryAfter(now time.Time, width time.Duration) time.Duration {
	return w.start.Add(width).Sub(now)
}

type ipWindows struct {
	short failWindow
	long  failWindow
}

// loginLimiter tracks login failures per client IP plus one global window.
// Client identity is the host part of r.RemoteAddr ONLY — X-Forwarded-For is
// spoofable and never trusted. Behind a reverse proxy all clients therefore
// share one bucket; operators who care rate-limit at the proxy (documented).
// Nil-receiver-safe: a nil limiter denies nothing (used only by partially
// constructed test Servers, which cannot reach the login handler anyway).
type loginLimiter struct {
	mu     sync.Mutex
	perIP  map[string]*ipWindows
	global failWindow
	now    func() time.Time // injectable for tests

	// lastLogged throttles the rejection log line to once per window — a
	// sustained brute force must not flood the operator's logs at the exact
	// moment they need them readable.
	lastLogged time.Time
}

func newLoginLimiter() *loginLimiter {
	return &loginLimiter{perIP: make(map[string]*ipWindows), now: time.Now}
}

// Allow reports whether ip may attempt a login now. When denied it returns
// the duration after which the binding window resets (for Retry-After).
// Checks run BEFORE any file read or bcrypt work.
//
// A nil limiter DENIES (fail closed), unlike Fail/Success which no-op: New is
// the sole constructor and always populates loginLimiter, so a nil here means
// a future construction path forgot it — failing closed surfaces that as
// "logins are throttled" rather than silently disabling brute-force defense.
func (l *loginLimiter) Allow(ip string) (bool, time.Duration) {
	if l == nil {
		return false, ipShortWindow
	}
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()

	// The global window caps attacker-driven bcrypt CPU, but it must never
	// lock out the operator (the file's throttle-only invariant). A loopback
	// peer is the operator on the same host — ip is the real socket peer from
	// r.RemoteAddr (never a spoofable X-Forwarded-For), so exempt it from the
	// global gate. The per-IP gate below still applies to loopback callers.
	if !isLoopbackIP(ip) && l.global.observe(now, globalWindow) >= globalMax {
		l.logThrottledLocked(now, "global")
		return false, l.global.retryAfter(now, globalWindow)
	}
	w := l.perIP[ip]
	if w == nil {
		return true, 0
	}
	if w.short.observe(now, ipShortWindow) >= ipShortMax {
		l.logThrottledLocked(now, ip)
		return false, w.short.retryAfter(now, ipShortWindow)
	}
	if w.long.observe(now, ipLongWindow) >= ipLongMax {
		l.logThrottledLocked(now, ip)
		return false, w.long.retryAfter(now, ipLongWindow)
	}
	return true, 0
}

// isLoopbackIP reports whether ip (the host part of r.RemoteAddr) is a
// loopback address. A non-parseable value (never a real socket peer) is not
// treated as loopback, so it stays subject to the global gate.
func isLoopbackIP(ip string) bool {
	parsed := net.ParseIP(ip)
	return parsed != nil && parsed.IsLoopback()
}

// Fail records a failed attempt from ip.
func (l *loginLimiter) Fail(ip string) {
	if l == nil {
		return
	}
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()

	l.global.observe(now, globalWindow)
	l.global.count++

	w := l.perIP[ip]
	if w == nil {
		if len(l.perIP) >= maxTrackedIPs {
			l.pruneLocked(now)
		}
		if len(l.perIP) >= maxTrackedIPs {
			// Still saturated: don't grow the map. The global window is
			// already absorbing the flood; new IPs simply aren't tracked
			// individually until pressure drops.
			return
		}
		w = &ipWindows{}
		l.perIP[ip] = w
	}
	w.short.observe(now, ipShortWindow)
	w.short.count++
	w.long.observe(now, ipLongWindow)
	w.long.count++
}

// Success clears ip's failure counters (a legitimate operator who fat-fingered
// the password a few times starts fresh).
func (l *loginLimiter) Success(ip string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.perIP, ip)
}

// pruneLocked drops IPs whose long window has fully elapsed.
func (l *loginLimiter) pruneLocked(now time.Time) {
	for ip, w := range l.perIP {
		if now.Sub(w.long.start) >= ipLongWindow {
			delete(l.perIP, ip)
		}
	}
}

func (l *loginLimiter) logThrottledLocked(now time.Time, scope string) {
	if now.Sub(l.lastLogged) < globalWindow {
		return
	}
	l.lastLogged = now
	slog.Warn("console login throttled", "scope", scope)
}
