package consoleapp

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/console"
)

// This file mutates up* package globals via save-and-restore. DO NOT add
// t.Parallel() here — see watch_test.go's note.

// newFlashbackConsole builds a console.Server with the given token for the
// serving-layer tests (creds, serveFlashback). A loopback listen keeps a
// no-token server legal (first-run setup).
func newFlashbackConsole(t *testing.T, token string) *console.Server {
	t.Helper()
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: token})
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

// TestFlashbackCreds: auth is the token alone. Every username — known or not —
// gets the same token, so the handshake error code cannot leak which usernames
// name a real server. An empty token uniformly denies.
func TestFlashbackCreds(t *testing.T) {
	creds := flashbackCreds{srv: newFlashbackConsole(t, "sekret")}
	for _, u := range []string{"someid", "alpha", "", "ghost"} {
		if ok, _ := creds.CheckUsername(u); !ok {
			t.Fatalf("CheckUsername(%q) = false, want true (token-only auth)", u)
		}
		if pw, found, _ := creds.GetCredential(u); !found || pw != "sekret" {
			t.Fatalf("GetCredential(%q) = (%q,%v), want (sekret,true)", u, pw, found)
		}
	}
	credsNoTok := flashbackCreds{srv: newFlashbackConsole(t, "")}
	if ok, _ := credsNoTok.CheckUsername("x"); ok {
		t.Fatal("no token: CheckUsername must deny")
	}
	if _, found, _ := credsNoTok.GetCredential("x"); found {
		t.Fatal("no token must never authorise a passwordless handshake")
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

// TestFlashbackConfigWithDefaults pins the zero→default substitution — notably
// QueryTimeout, which must never reach the query path as 0 (the only runaway
// backstop) — and that explicit values are preserved.
func TestFlashbackConfigWithDefaults(t *testing.T) {
	got := flashbackConfig{}.withDefaults()
	if got.QueryTimeout != defaultFlashbackQueryTimeout {
		t.Fatalf("QueryTimeout default = %v, want %v", got.QueryTimeout, defaultFlashbackQueryTimeout)
	}
	if got.MaxFullTable != defaultFlashbackMaxFullTable {
		t.Fatalf("MaxFullTable default = %d, want %d", got.MaxFullTable, defaultFlashbackMaxFullTable)
	}
	custom := flashbackConfig{QueryTimeout: time.Second, MaxFullTable: 9}.withDefaults()
	if custom.QueryTimeout != time.Second || custom.MaxFullTable != 9 {
		t.Fatalf("explicit values overwritten: %+v", custom)
	}
}

// TestServeFlashbackRequiresToken: the port refuses to start without a token,
// since MySQL-protocol auth cannot be driven by the bcrypt password store.
func TestServeFlashbackRequiresToken(t *testing.T) {
	err := serveFlashback(context.Background(), newFlashbackConsole(t, ""), nil, flashbackConfig{})
	if err == nil || !strings.Contains(err.Error(), "token") {
		t.Fatalf("empty token: err = %v, want a token-required error", err)
	}
}

// TestServeFlashbackDrainsActiveConn: serveFlashback returns only after an
// IN-FLIGHT connection has drained when ctx is cancelled — pinning the
// wg.Add/wg.Wait drain (a naive no-connection test would pass even without it).
func TestServeFlashbackDrainsActiveConn(t *testing.T) {
	srv := newFlashbackConsole(t, "tok")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveFlashback(ctx, srv, ln, flashbackConfig{}) }()

	// Read the server's handshake greeting to confirm the accept loop spawned an
	// in-flight handler goroutine (wg > 0) before cancelling.
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err != nil {
		t.Fatalf("expected a handshake greeting from the flashback port: %v", err)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("serveFlashback did not drain the in-flight connection on ctx cancel")
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

// TestStartFlashbackPortDisabled: an empty --flashback-listen is a no-op that
// returns a usable drain func (callers defer it unconditionally).
func TestStartFlashbackPortDisabled(t *testing.T) {
	saved := upConsoleFlashbackListen
	t.Cleanup(func() { upConsoleFlashbackListen = saved })

	upConsoleFlashbackListen = ""
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok"})
	if err != nil {
		t.Fatal(err)
	}
	stop, err := startFlashbackPort(context.Background(), srv)
	if err != nil {
		t.Fatalf("disabled port must not error: %v", err)
	}
	stop() // must be safe to call
}

// TestStartFlashbackPortRequiresToken: enabling the port without a console token
// fails fast (MySQL-protocol auth cannot use the bcrypt password store).
func TestStartFlashbackPortRequiresToken(t *testing.T) {
	saved := upConsoleFlashbackListen
	t.Cleanup(func() { upConsoleFlashbackListen = saved })

	upConsoleFlashbackListen = "127.0.0.1:0"
	// A loopback bind with no token is legal for the console (first-run setup),
	// so New succeeds — but the flashback port must refuse it.
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	if srv.Token() != "" {
		t.Fatal("precondition: expected an empty token")
	}
	_, err = startFlashbackPort(context.Background(), srv)
	if err == nil || !strings.Contains(err.Error(), "token") {
		t.Fatalf("no token: err = %v, want a token-required error", err)
	}
}

// TestStartFlashbackPortBindAndDrain: the enabled port binds, serves on the
// daemon context, and the returned drain func returns once ctx is cancelled —
// pinning the shutdown-ordering contract (drain before the deferred db.Close)
// against a regression that would hang the daemon.
func TestStartFlashbackPortBindAndDrain(t *testing.T) {
	saved := upConsoleFlashbackListen
	t.Cleanup(func() { upConsoleFlashbackListen = saved })

	upConsoleFlashbackListen = "127.0.0.1:0" // ephemeral
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stop, err := startFlashbackPort(ctx, srv)
	if err != nil {
		t.Fatalf("bind failed: %v", err)
	}

	cancel() // daemon shutdown → ServeFlashback closes the listener and returns
	drained := make(chan struct{})
	go func() { stop(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal("flashback drain did not return after ctx cancel (shutdown would hang)")
	}
}

// TestResolveFlashbackEnv locks the --flashback-listen env fallback and its
// flag > env precedence, guarding the Changed("flashback-listen") string against
// a rename that would silently let the env override an explicit flag.
func TestResolveFlashbackEnv(t *testing.T) {
	saved := upConsoleFlashbackListen
	t.Cleanup(func() { upConsoleFlashbackListen = saved })

	if watchCmd.Flags().Lookup("flashback-listen") == nil {
		t.Fatal("flag --flashback-listen not registered on watchCmd; resolveUpConsoleEnv's Changed would always be false")
	}

	newCmd := func() *cobra.Command {
		cmd := &cobra.Command{}
		cmd.Flags().StringVar(&upConsoleFlashbackListen, "flashback-listen", "", "")
		return cmd
	}

	t.Setenv("BINTRAIL_CONSOLE_FLASHBACK_LISTEN", "127.0.0.1:3308")

	// No flag set → env applies.
	upConsoleFlashbackListen = ""
	resolveUpConsoleEnv(newCmd())
	if upConsoleFlashbackListen != "127.0.0.1:3308" {
		t.Fatalf("env fallback: got %q, want 127.0.0.1:3308", upConsoleFlashbackListen)
	}

	// Explicit flag wins over env.
	upConsoleFlashbackListen = "127.0.0.1:9000"
	cmd := newCmd()
	if err := cmd.Flags().Set("flashback-listen", "127.0.0.1:9000"); err != nil {
		t.Fatal(err)
	}
	resolveUpConsoleEnv(cmd)
	if upConsoleFlashbackListen != "127.0.0.1:9000" {
		t.Fatalf("flag precedence: got %q, want 127.0.0.1:9000 (env leaked over an explicit flag)", upConsoleFlashbackListen)
	}
}
