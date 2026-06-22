package cli

import (
	"testing"

	"github.com/spf13/cobra"
)

func TestRecoverCascadeFlagsRegistered(t *testing.T) {
	for _, name := range []string{
		"index-dsn", "schema", "table", "pk", "pks", "since", "until",
		"output", "dry-run", "format", "lookback", "max-depth", "limit", "allow-incomplete",
	} {
		if recoverCascadeCmd.Flags().Lookup(name) == nil {
			t.Errorf("flag --%s not registered on recoverCascadeCmd", name)
		}
	}
	for _, name := range []string{"index-dsn", "schema", "table"} {
		f := recoverCascadeCmd.Flags().Lookup(name)
		if f == nil || f.Annotations[cobra.BashCompOneRequiredFlag] == nil {
			t.Errorf("flag --%s should be required", name)
		}
	}
}

func TestRecoverCascadeRegistered(t *testing.T) {
	root := &cobra.Command{Use: "bintrail"}
	AddReadCommands(root)
	var found bool
	for _, c := range root.Commands() {
		if c.Name() == "recover-cascade" {
			found = true
			break
		}
	}
	if !found {
		t.Error("recover-cascade not registered by AddReadCommands")
	}
}
