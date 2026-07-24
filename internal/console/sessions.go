package console

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/ext"
)

// Session lifetime policy. Unexported constants on purpose — no knobs in v1
// (every knob is a misconfiguration vector; flag-ifying later is
// compat-safe). Absolute: a session dies 24h after login no matter what.
// Idle: it also dies 8h after its last authenticated request.
const (
	sessionAbsoluteTTL = 24 * time.Hour
	sessionIdleTTL     = 8 * time.Hour
	// maxSessions bounds both memory and how long a stolen-but-unnoticed
	// credential set can accumulate; overflow evicts the earliest-expiring.
	maxSessions = 16
	// lastSeenGranularity throttles lastSeen rewrites so the hot request
	// path isn't taking the write lock to bump a timestamp on every call.
	lastSeenGranularity = time.Minute
)

// sessionPrefix makes issued tokens visually distinct from the 32-hex static
// token and friendly to secret scanners. The middleware never branches on it.
const sessionPrefix = "bcs_"

type session struct {
	createdAt time.Time
	lastSeen  time.Time
	// policy is the session's access scope, or nil for a full-access session
	// (what the password login and the static token mint). Set only when an
	// external auth provider passed one to IssueWithPolicy — i.e. an EE build.
	policy *ext.AccessPolicy
	// identity is the verified login identity this session was minted for
	// (the auth-file username, an SSO email, a credential-backend username),
	// or "" when unknown. Display/audit only — authorization is the policy.
	identity string
}

// sessionStore holds the in-memory login sessions. Keys are
// sha256(presented-token) — the raw token never lives server-side, so a heap
// dump or a careless debug log cannot leak a usable credential. Sessions are
// deliberately NOT persisted: restart-logs-you-out is a security-positive
// non-event for a single-operator tool, and nothing session-shaped lands on
// disk.
//
// All methods are nil-receiver-safe and fail closed, so partially
// constructed Servers in tests (&Server{token: "x"}) deny session auth
// instead of panicking.
type sessionStore struct {
	mu  sync.Mutex
	m   map[[32]byte]*session
	now func() time.Time // injectable for tests
}

func newSessionStore() *sessionStore {
	return &sessionStore{m: make(map[[32]byte]*session), now: time.Now}
}

// sessionKey is the map key for a presented token: its SHA-256. Keeping the
// raw token out of the map means a heap dump yields hashes, not credentials.
func sessionKey(token string) [32]byte { return sha256.Sum256([]byte(token)) }

// Issue mints a new full-access (policy-less) session with no recorded
// identity — the anonymous built-in mints. See IssueWithPolicy.
func (s *sessionStore) Issue() (token string, expiresAt time.Time, err error) {
	return s.IssueWithPolicy("", nil)
}

// IssueWithPolicy mints a new session token: sessionPrefix + 64 hex chars (256
// bits of crypto/rand entropy), recording the verified identity (display/audit
// only; "" = unknown) and carrying policy as its access scope (nil = full
// access). It sweeps expired entries and, if the store is still at capacity,
// evicts the earliest-expiring session.
func (s *sessionStore) IssueWithPolicy(identity string, policy *ext.AccessPolicy) (token string, expiresAt time.Time, err error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", time.Time{}, err
	}
	token = sessionPrefix + hex.EncodeToString(b)
	now := s.now()

	s.mu.Lock()
	defer s.mu.Unlock()
	s.sweepLocked(now)
	if len(s.m) >= maxSessions {
		s.evictEarliestLocked()
	}
	s.m[sessionKey(token)] = &session{createdAt: now, lastSeen: now, policy: policy, identity: identity}
	return token, now.Add(sessionAbsoluteTTL), nil
}

// Validate reports whether the presented token is a live session, refreshing
// its idle timer (coarsely) on success. Expired entries are deleted lazily.
func (s *sessionStore) Validate(token string) bool {
	_, _, ok := s.Lookup(token)
	return ok
}

// Lookup validates the presented token and, on success, returns its recorded
// identity ("" when unknown) and access policy (nil for a full-access session).
// It refreshes the idle timer (coarsely) and deletes an expired entry lazily —
// the same behavior Validate had; Validate is now a thin wrapper.
// Nil-receiver-safe and fail-closed.
func (s *sessionStore) Lookup(token string) (identity string, policy *ext.AccessPolicy, ok bool) {
	if s == nil || token == "" {
		return "", nil, false
	}
	key := sessionKey(token)
	now := s.now()

	s.mu.Lock()
	defer s.mu.Unlock()
	sess, found := s.m[key]
	if !found {
		return "", nil, false
	}
	if s.expiredLocked(sess, now) {
		delete(s.m, key)
		return "", nil, false
	}
	if now.Sub(sess.lastSeen) > lastSeenGranularity {
		sess.lastSeen = now
	}
	return sess.identity, sess.policy, true
}

// Revoke deletes the presented session, if any. Idempotent.
func (s *sessionStore) Revoke(token string) {
	if s == nil {
		return
	}
	key := sessionKey(token)
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, key)
}

// RevokeAll deletes every session (password change, removal).
func (s *sessionStore) RevokeAll() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.m)
}

func (s *sessionStore) expiredLocked(sess *session, now time.Time) bool {
	return now.After(sess.createdAt.Add(sessionAbsoluteTTL)) ||
		now.After(sess.lastSeen.Add(sessionIdleTTL))
}

func (s *sessionStore) sweepLocked(now time.Time) {
	for k, sess := range s.m {
		if s.expiredLocked(sess, now) {
			delete(s.m, k)
		}
	}
}

// evictEarliestLocked removes the session that would expire soonest
// (min of its absolute and idle deadlines).
func (s *sessionStore) evictEarliestLocked() {
	var victim [32]byte
	var earliest time.Time
	found := false
	for k, sess := range s.m {
		deadline := sess.createdAt.Add(sessionAbsoluteTTL)
		if idle := sess.lastSeen.Add(sessionIdleTTL); idle.Before(deadline) {
			deadline = idle
		}
		if !found || deadline.Before(earliest) {
			victim, earliest, found = k, deadline, true
		}
	}
	if found {
		delete(s.m, victim)
	}
}
