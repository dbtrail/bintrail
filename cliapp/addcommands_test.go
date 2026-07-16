package cliapp

import (
	"testing"

	"github.com/spf13/cobra"
)

func TestAddCommandsRegistersOnRoot(t *testing.T) {
	cmd := &cobra.Command{Use: "ext-test-cmd", Run: func(*cobra.Command, []string) {}}
	AddCommands(cmd)
	t.Cleanup(func() { rootCmd.RemoveCommand(cmd) })

	found, _, err := rootCmd.Find([]string{"ext-test-cmd"})
	if err != nil || found != cmd {
		t.Fatalf("rootCmd.Find(ext-test-cmd) = (%v, %v), want the injected command", found, err)
	}
}
