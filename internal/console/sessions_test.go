package console

import (
	"strings"
	"testing"
	"time"
)

// fakeClock returns a sessionStore with a controllable clock.
func fakeClock(t *testing.T) (*sessionStore, *time.Time) {
	t.Helper()
	now := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	s := newSessionStore()
	s.now = func() time.Time { return now }
	return s, &now
}

func TestSessionIssueAndValidate(t *testing.T) {
	s, _ := fakeClock(t)
	tok, expires, err := s.Issue()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(tok, sessionPrefix) || len(tok) != len(sessionPrefix)+64 {
		t.Errorf("token shape = %q, want %s + 64 hex", tok, sessionPrefix)
	}
	if want := s.now().Add(sessionAbsoluteTTL); !expires.Equal(want) {
		t.Errorf("expires = %v, want %v", expires, want)
	}
	if !s.Validate(tok) {
		t.Error("fresh session did not validate")
	}
	if s.Validate(tok + "x") {
		t.Error("mangled token validated")
	}
	if s.Validate("") {
		t.Error("empty token validated")
	}
}

func TestSessionAbsoluteExpiry(t *testing.T) {
	s, now := fakeClock(t)
	tok, _, _ := s.Issue()
	// Keep it active so only the absolute TTL can kill it.
	for i := 0; i < 25; i++ {
		*now = now.Add(time.Hour)
		s.Validate(tok)
	}
	if s.Validate(tok) {
		t.Error("session alive past the 24h absolute TTL despite activity")
	}
}

func TestSessionIdleExpiry(t *testing.T) {
	s, now := fakeClock(t)
	tok, _, _ := s.Issue()
	*now = now.Add(sessionIdleTTL - time.Minute)
	if !s.Validate(tok) {
		t.Fatal("session idle-expired early")
	}
	// The validate above refreshed lastSeen — another near-idle wait survives.
	*now = now.Add(sessionIdleTTL - time.Minute)
	if !s.Validate(tok) {
		t.Fatal("lastSeen refresh did not extend the idle window")
	}
	*now = now.Add(sessionIdleTTL + time.Minute)
	if s.Validate(tok) {
		t.Error("session alive past the idle TTL")
	}
}

func TestSessionLastSeenThrottle(t *testing.T) {
	s, now := fakeClock(t)
	tok, _, _ := s.Issue()
	created := s.m[sessionKey(tok)].lastSeen

	// A Validate within lastSeenGranularity must NOT rewrite lastSeen (the
	// throttle that keeps the hot path off the write churn).
	*now = now.Add(30 * time.Second)
	s.Validate(tok)
	if !s.m[sessionKey(tok)].lastSeen.Equal(created) {
		t.Error("lastSeen advanced within the granularity window — throttle broken")
	}
	// Past the granularity it does advance.
	*now = now.Add(2 * time.Minute)
	s.Validate(tok)
	if !s.m[sessionKey(tok)].lastSeen.Equal(*now) {
		t.Error("lastSeen did not advance past the granularity window")
	}
}

func TestSessionEvictionUsesIdleDeadline(t *testing.T) {
	// The eviction comparator is min(absolute, idle); since idleTTL (8h) <
	// absoluteTTL (24h), the idle deadline governs. So refreshing the
	// oldest-created session must PROTECT it: a later-created but un-refreshed
	// session becomes the earliest-expiring and is evicted instead.
	s, now := fakeClock(t)
	a, _, _ := s.Issue() // created first
	var later []string
	for i := 1; i < maxSessions; i++ {
		*now = now.Add(time.Minute)
		tok, _, _ := s.Issue()
		later = append(later, tok)
	}
	// Refresh A: its idle deadline becomes the latest of all.
	*now = now.Add(20 * time.Minute)
	s.Validate(a)
	// Overflow eviction now targets later[0] (earliest idle deadline), not A.
	*now = now.Add(time.Minute)
	overflow, _, _ := s.Issue()
	if !s.Validate(a) {
		t.Error("a refreshed session was evicted despite the latest idle deadline")
	}
	if s.Validate(later[0]) {
		t.Error("the earliest-idle session was not the eviction victim")
	}
	if !s.Validate(overflow) {
		t.Error("the new session was not admitted")
	}
}

func TestSessionRevocation(t *testing.T) {
	s, _ := fakeClock(t)
	t1, _, _ := s.Issue()
	t2, _, _ := s.Issue()
	s.Revoke(t1)
	if s.Validate(t1) {
		t.Error("revoked session validated")
	}
	if !s.Validate(t2) {
		t.Error("Revoke killed an unrelated session")
	}
	s.RevokeAll()
	if s.Validate(t2) {
		t.Error("RevokeAll left a live session")
	}
}

func TestSessionCapEvictsEarliestExpiring(t *testing.T) {
	s, now := fakeClock(t)
	first, _, _ := s.Issue()
	var rest []string
	for i := 1; i < maxSessions; i++ {
		*now = now.Add(time.Second)
		tok, _, _ := s.Issue()
		rest = append(rest, tok)
	}
	// Store is full; the next Issue must evict `first` (earliest-expiring).
	*now = now.Add(time.Second)
	overflow, _, _ := s.Issue()
	if s.Validate(first) {
		t.Error("cap overflow did not evict the earliest-expiring session")
	}
	if !s.Validate(overflow) || !s.Validate(rest[len(rest)-1]) {
		t.Error("eviction removed the wrong session")
	}
}

func TestSessionNilStoreFailsClosed(t *testing.T) {
	var s *sessionStore
	if s.Validate("bcs_anything") {
		t.Error("nil store validated a token")
	}
	s.Revoke("bcs_anything") // must not panic
	s.RevokeAll()            // must not panic
}
