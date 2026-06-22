package cli

import (
	"context"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// resetCascadeFlags sets every recover-cascade global to a valid baseline so a
// validation test can flip exactly one into the bad state it exercises.
func resetCascadeFlags() {
	rcIndexDSN = "root:x@tcp(127.0.0.1:1)/idx"
	rcSchema, rcTable = "app", "parent"
	rcPK, rcPKs, rcSince, rcUntil = "", nil, "", ""
	rcOutput, rcDryRun, rcFormat = "", true, "text"
	rcLookback, rcMaxDepth, rcLimit, rcAllowIncomplete = "30d", 5, 1000, false
}

// TestRunRecoverCascade_validation pins the pre-DB validation branches — they
// all return before config.Connect, so no MySQL is needed.
func TestRunRecoverCascade_validation(t *testing.T) {
	cases := []struct {
		name   string
		mutate func()
		want   string
	}{
		{"invalid format", func() { rcFormat = "xml" }, "invalid --format"},
		{"no output and no dry-run", func() { rcDryRun = false; rcOutput = "" }, "--output or --dry-run"},
		{"pk and pks", func() { rcPK = "1"; rcPKs = []string{"2"} }, "mutually exclusive"},
		{"max-depth zero", func() { rcMaxDepth = 0 }, "--max-depth"},
		{"limit zero", func() { rcLimit = 0 }, "--limit"},
		{"bad lookback", func() { rcLookback = "bogus" }, "--lookback"},
		{"bad since", func() { rcSince = "not-a-time" }, "--since"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resetCascadeFlags()
			c.mutate()
			cmd := &cobra.Command{}
			cmd.SetContext(context.Background())
			err := runRecoverCascade(cmd, nil)
			if err == nil {
				t.Fatalf("expected an error for %q, got nil", c.name)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("error %q should contain %q", err.Error(), c.want)
			}
		})
	}
	resetCascadeFlags() // leave globals clean for other tests
}

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
