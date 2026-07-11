package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/console"
)

// This file mutates up* package globals via save-and-restore. DO NOT add
// t.Parallel() here — see watch_test.go's note.

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
