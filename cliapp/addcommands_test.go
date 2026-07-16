package cliapp

import (
	"testing"

	"github.com/spf13/cobra"
)

func TestAddCommandsRegistersOnRoot(t *testing.T) {
	ran := false
	cmd := &cobra.Command{Use: "ext-test-cmd", Run: func(*cobra.Command, []string) { ran = true }}
	AddCommands(cmd)
	t.Cleanup(func() {
		rootCmd.RemoveCommand(cmd)
		rootCmd.SetArgs(nil) // restore cobra's default os.Args handling
	})

	found, _, err := rootCmd.Find([]string{"ext-test-cmd"})
	if err != nil || found != cmd {
		t.Fatalf("rootCmd.Find(ext-test-cmd) = (%v, %v), want the injected command", found, err)
	}

	// Execute through the real root so the injected command runs under the
	// actual PersistentPreRun chain (log setup) — AddCommands must dispatch,
	// not merely register.
	rootCmd.SetArgs([]string{"ext-test-cmd"})
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("rootCmd.Execute() = %v", err)
	}
	if !ran {
		t.Fatal("injected command did not run through rootCmd.Execute")
	}
}
