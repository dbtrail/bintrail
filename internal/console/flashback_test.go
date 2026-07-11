package console

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

// newFlashbackServer builds the minimal Server the flashback routing/auth code
// touches: a connManager over reg plus a token. It bypasses New() (no HTTP mux,
// no boot bundle) because flashbackTarget/flashbackCreds only read s.cm + s.token.
func newFlashbackServer(t *testing.T, token string) (*Server, *Registry) {
	t.Helper()
	reg, err := LoadRegistry("")
	if err != nil {
		t.Fatal(err)
	}
	return &Server{cm: newConnManager(reg, false), token: token}, reg
}

// TestFlashbackTarget: a username resolves to a canonical server id by registry
// ID, by display Name, or "default" for a visible boot entry — and to nothing
// for an empty/unknown selector or a hidden boot.
func TestFlashbackTarget(t *testing.T) {
	s, reg := newFlashbackServer(t, "tok")
	a, err := reg.Add(ServerEntry{Name: "alpha", DSN: "u:p@tcp(h:3306)/idxA"})
	if err != nil {
		t.Fatal(err)
	}
	if id, ok := s.flashbackTarget(a.ID); !ok || id != a.ID {
		t.Fatalf("by id: got (%q,%v), want (%q,true)", id, ok, a.ID)
	}
	if id, ok := s.flashbackTarget("alpha"); !ok || id != a.ID {
		t.Fatalf("by name: got (%q,%v), want (%q,true)", id, ok, a.ID)
	}
	if _, ok := s.flashbackTarget("nope"); ok {
		t.Fatal("unknown selector must not resolve")
	}
	if _, ok := s.flashbackTarget(""); ok {
		t.Fatal("empty selector must not resolve")
	}

	// Boot: not a target until it exists AND is visible.
	if _, ok := s.flashbackTarget(bootServerID); ok {
		t.Fatal("absent boot must not resolve")
	}
	s.cm.boot = &bundle{}
	if id, ok := s.flashbackTarget(bootServerID); !ok || id != bootServerID {
		t.Fatalf("visible boot: got (%q,%v), want (%q,true)", id, ok, bootServerID)
	}
	s.cm.hideBoot = true
	if _, ok := s.flashbackTarget(bootServerID); ok {
		t.Fatal("hidden boot (source-less watch anchor) must not be a flashback target")
	}
}

// TestFlashbackCreds: auth is the token alone. Every username — known or not —
// gets the same token, so the handshake error code cannot leak which usernames
// name a real server (validity is checked post-auth in bindFlashbackHandler).
// An empty token uniformly refuses credentials (defence in depth behind the
// ServeFlashback startup guard).
func TestFlashbackCreds(t *testing.T) {
	s, reg := newFlashbackServer(t, "sekret")
	a, err := reg.Add(ServerEntry{Name: "alpha", DSN: "u:p@tcp(h:3306)/idxA"})
	if err != nil {
		t.Fatal(err)
	}
	creds := flashbackCreds{s: s}

	// A known username and an unknown one are indistinguishable at the handshake.
	for _, u := range []string{a.ID, "alpha", "ghost", ""} {
		if ok, _ := creds.CheckUsername(u); !ok {
			t.Fatalf("CheckUsername(%q) = false, want true (token-only auth)", u)
		}
		if pw, found, _ := creds.GetCredential(u); !found || pw != "sekret" {
			t.Fatalf("GetCredential(%q) = (%q,%v), want (sekret,true)", u, pw, found)
		}
	}

	// No token: every connection is denied, including a valid server username.
	s.token = ""
	if ok, _ := creds.CheckUsername(a.ID); ok {
		t.Fatal("no token: CheckUsername must deny")
	}
	if _, found, _ := creds.GetCredential(a.ID); found {
		t.Fatal("no token must never authorise a passwordless handshake")
	}
}

// TestSplitBaselineSource: a resolved baseline source maps to the shim's dir/S3
// fields by scheme.
func TestSplitBaselineSource(t *testing.T) {
	for _, tc := range []struct {
		src, dir, s3 string
	}{
		{"", "", ""},
		{"/var/lib/baselines", "/var/lib/baselines", ""},
		{"s3://bucket/prefix/", "", "s3://bucket/prefix/"},
	} {
		dir, s3 := splitBaselineSource(tc.src)
		if dir != tc.dir || s3 != tc.s3 {
			t.Errorf("splitBaselineSource(%q) = (%q,%q), want (%q,%q)", tc.src, dir, s3, tc.dir, tc.s3)
		}
	}
}

// TestRoutingHandlerPendingDB: a UseDB before inner is bound (the go-mysql
// handshake path) is stashed and does not fail the connection.
func TestRoutingHandlerPendingDB(t *testing.T) {
	r := &routingHandler{}
	if err := r.UseDB("shopdb"); err != nil {
		t.Fatalf("pre-bind UseDB must not error (handshake would abort): %v", err)
	}
	if r.pendingDB != "shopdb" {
		t.Fatalf("pendingDB = %q, want shopdb", r.pendingDB)
	}
}

// TestFlashbackDefaultSchema derives the USE-less default schema from a
// registry entry's SourceDSN; empty for a source-less entry or the boot entry.
func TestFlashbackDefaultSchema(t *testing.T) {
	s, reg := newFlashbackServer(t, "tok")
	a, err := reg.Add(ServerEntry{Name: "alpha", DSN: "u:p@tcp(h:3306)/idxA", SourceDSN: "r:pw@tcp(src:3306)/shopdb"})
	if err != nil {
		t.Fatal(err)
	}
	if got := s.flashbackDefaultSchema(a.ID); got != "shopdb" {
		t.Fatalf("default schema = %q, want shopdb", got)
	}
	b, err := reg.Add(ServerEntry{Name: "beta", DSN: "u:p@tcp(h:3306)/idxB"})
	if err != nil {
		t.Fatal(err)
	}
	if got := s.flashbackDefaultSchema(b.ID); got != "" {
		t.Fatalf("source-less entry: default schema = %q, want empty", got)
	}
	if got := s.flashbackDefaultSchema(bootServerID); got != "" {
		t.Fatalf("boot: default schema = %q, want empty", got)
	}
}

// TestRoutingHandlerUnresolved: with no bound handler, HandleQuery errors
// generically; once routing has set a fail, BOTH verbs return it verbatim.
func TestRoutingHandlerUnresolved(t *testing.T) {
	r := &routingHandler{}
	if _, err := r.HandleQuery("SELECT 1"); err == nil {
		t.Fatal("HandleQuery with nil inner must error")
	}

	want := gomysql.NewError(gomysql.ER_BAD_DB_ERROR, "boom")
	r.fail = want
	if _, err := r.HandleQuery("SELECT 1"); err != want {
		t.Fatalf("HandleQuery returned %v, want the stored fail", err)
	}
	if err := r.UseDB("x"); err != want {
		t.Fatalf("UseDB returned %v, want the stored fail (unresolvable connection must reject USE too)", err)
	}
}

// TestServeFlashbackRequiresToken: the port refuses to start without a token,
// since MySQL-protocol auth cannot be driven by the bcrypt password store.
func TestServeFlashbackRequiresToken(t *testing.T) {
	s, _ := newFlashbackServer(t, "")
	err := s.ServeFlashback(context.Background(), nil, FlashbackConfig{})
	if err == nil || !strings.Contains(err.Error(), "token") {
		t.Fatalf("empty token: err = %v, want a token-required error", err)
	}
}

// TestServeFlashbackDrainsActiveConn: ServeFlashback returns only after an
// IN-FLIGHT connection has drained when ctx is cancelled — pinning the
// wg.Add/wg.Wait drain (a regression dropping wg.Add would let ServeFlashback
// return while a handler goroutine still runs, and this would still pass a
// naive no-connection test).
func TestServeFlashbackDrainsActiveConn(t *testing.T) {
	s, _ := newFlashbackServer(t, "tok")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.ServeFlashback(ctx, ln, FlashbackConfig{}) }()

	// Open a raw connection and read the server's handshake greeting: that
	// confirms the accept loop has spawned an in-flight handler goroutine
	// (wg > 0) before we cancel, so the drain is actually exercised.
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err != nil {
		t.Fatalf("expected a handshake greeting from the flashback port: %v", err)
	}

	cancel() // daemon shutdown must drain the in-flight connection
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ServeFlashback did not drain the in-flight connection on ctx cancel")
	}
}

func TestNextFlashbackBackoff(t *testing.T) {
	if got := nextFlashbackBackoff(0); got != initialFlashbackBackoff {
		t.Fatalf("seed = %v, want %v", got, initialFlashbackBackoff)
	}
	if got := nextFlashbackBackoff(initialFlashbackBackoff); got != 2*initialFlashbackBackoff {
		t.Fatalf("double = %v, want %v", got, 2*initialFlashbackBackoff)
	}
	if got := nextFlashbackBackoff(maxFlashbackBackoff); got != maxFlashbackBackoff {
		t.Fatalf("at cap = %v, want %v", got, maxFlashbackBackoff)
	}
	if got := nextFlashbackBackoff(4 * time.Second); got != maxFlashbackBackoff {
		t.Fatalf("past cap = %v, want %v", got, maxFlashbackBackoff)
	}
}
