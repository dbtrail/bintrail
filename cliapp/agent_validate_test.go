package cliapp

import "testing"

func TestValidateFlagRegistration(t *testing.T) {
	f := agentCmd.Flag("validate")
	if f == nil {
		t.Fatal("--validate flag not registered on agent command")
	}
	if f.DefValue != "false" {
		t.Errorf("--validate default = %q, want false", f.DefValue)
	}
}

func TestPrintCheckFormat(t *testing.T) {
	// Verify printCheck and printSkip don't panic.
	// Output goes to stdout which we don't capture, but we verify no crash.
	printCheck("test check", "detail", nil)
	printCheck("test check", "", nil)
	printCheck("test check", "", &testError{"something broke"})
	printSkip("test check", "reason")
}
