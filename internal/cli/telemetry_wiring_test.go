package cli

import (
	"errors"
	"testing"

	"github.com/spf13/cobra"
)

func TestCommandPath(t *testing.T) {
	root := &cobra.Command{Use: "bintrail"}
	status := &cobra.Command{Use: "status"}
	archive := &cobra.Command{Use: "archive"}
	reconcile := &cobra.Command{Use: "reconcile"}
	root.AddCommand(status, archive)
	archive.AddCommand(reconcile)

	if got := commandPath(status); got != "status" {
		t.Errorf("commandPath(status) = %q, want status", got)
	}
	// Sibling subcommands must stay distinguishable: a bare leaf name would
	// bucket every "reconcile"-like subcommand together.
	if got := commandPath(reconcile); got != "archive-reconcile" {
		t.Errorf("commandPath(archive reconcile) = %q, want archive-reconcile", got)
	}
	if got := commandPath(root); got != "root" {
		t.Errorf("commandPath(root) = %q, want root", got)
	}
}

// TestTelemetrySubtreeIsNotInstrumented: initialising telemetry while running
// `telemetry off` would drain and deliver the spooled backlog moments before
// the command discards it — an operator opting out would watch the tool phone
// home on its way out.
func TestTelemetrySubtreeIsNotInstrumented(t *testing.T) {
	root := &cobra.Command{Use: "bintrail"}
	AddTelemetryCommand(root)

	for _, cmd := range []*cobra.Command{telemetryCmd, telemetryStatusCmd, telemetryOffCmd, telemetryShowCmd} {
		var h TelemetryHook
		h.Start(cmd)
		if h.client != nil || h.span != nil {
			t.Errorf("%q was instrumented; the telemetry control surface must not be", commandPath(cmd))
		}
	}

	// A normal command still is (there is no endpoint compiled in during tests,
	// so the client is inert — but it must exist).
	other := &cobra.Command{Use: "status"}
	root.AddCommand(other)
	var h TelemetryHook
	h.Start(other)
	if h.client == nil {
		t.Error("an ordinary command was not instrumented")
	}
}

// TestCompletionMachineryIsNotInstrumented: cobra's __complete is an ordinary
// child of root, so the root hook fires for it. Recording it would spool an
// event on every TAB press, attempt a POST, and pay the shutdown wait on a
// keystroke — and since sanitizeCommand rejects underscores it would all land
// in "other", making that the largest bucket in the dataset.
func TestCompletionMachineryIsNotInstrumented(t *testing.T) {
	root := &cobra.Command{Use: "bintrail"}
	// Names cobra generates itself, plus the ones it adds on demand.
	for _, use := range []string{"__complete", "__completeNoDesc", "completion", "help"} {
		cmd := &cobra.Command{Use: use}
		root.AddCommand(cmd)
		var h TelemetryHook
		h.Start(cmd)
		if h.client != nil || h.span != nil {
			t.Errorf("%q was instrumented; it is not user work", use)
		}
		// Nested (e.g. `completion zsh`) must be exempt too.
		sub := &cobra.Command{Use: "zsh"}
		cmd.AddCommand(sub)
		var hs TelemetryHook
		hs.Start(sub)
		if hs.client != nil {
			t.Errorf("%q zsh was instrumented", use)
		}
	}
}

func TestTelemetryHookReturnsCommandErrorUnchanged(t *testing.T) {
	want := errors.New("index unreachable")
	root := &cobra.Command{
		Use:           "x",
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE:          func(*cobra.Command, []string) error { return want },
	}
	root.SetArgs(nil)
	var h TelemetryHook
	if got := h.Execute(root); !errors.Is(got, want) {
		t.Errorf("Execute returned %v, want the command's own error", got)
	}
}

// TestTelemetryHookRepanics: a crash must reach the user byte-for-byte as it
// would without telemetry. Recording it must not turn a panic into a silent
// exit, and the panic VALUE must not be swallowed or rewritten.
func TestTelemetryHookRepanics(t *testing.T) {
	root := &cobra.Command{
		Use:  "x",
		RunE: func(*cobra.Command, []string) error { panic("boom: /var/lib/mysql/binlog.000042") },
	}
	root.SetArgs(nil)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("telemetry swallowed a panic")
		}
		if s, _ := r.(string); s != "boom: /var/lib/mysql/binlog.000042" {
			t.Errorf("panic value was altered: %v", r)
		}
	}()

	var h TelemetryHook // span is nil — Execute must be nil-safe on this path too
	_ = h.Execute(root)
}
