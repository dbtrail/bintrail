package cli

import (
	"errors"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/recovery"
)

func TestLimitWarning(t *testing.T) {
	cases := []struct {
		limit    int
		wantWarn bool
	}{
		{100, false},
		{1, false},
		{0, true},
		{-1, true},
	}
	for _, tc := range cases {
		got := limitWarning(tc.limit)
		if (got != "") != tc.wantWarn {
			t.Fatalf("limitWarning(%d) = %q, wantWarn=%v", tc.limit, got, tc.wantWarn)
		}
		if tc.wantWarn && !strings.Contains(got, "--limit 0") {
			t.Errorf("limitWarning(%d) should mention --limit 0: %q", tc.limit, got)
		}
	}
}

func TestWrapScriptBudget(t *testing.T) {
	t.Run("budget error gets CLI hint", func(t *testing.T) {
		base := &recovery.ScriptBudgetError{EstimatedBytes: 3 << 30, Budget: 2 << 30}
		got := wrapScriptBudget(base)
		// The typed error must remain unwrappable for callers/tests.
		var be *recovery.ScriptBudgetError
		if !errors.As(got, &be) {
			t.Fatalf("wrapped error no longer unwraps to *ScriptBudgetError: %v", got)
		}
		for _, want := range []string{"--since", "--pk", "--limit", "--max-script-bytes", "BINTRAIL_RECOVER_MAX_BYTES"} {
			if !strings.Contains(got.Error(), want) {
				t.Errorf("CLI hint missing %q: %s", want, got.Error())
			}
		}
	})

	t.Run("other errors pass through unchanged", func(t *testing.T) {
		base := errors.New("some other failure")
		if got := wrapScriptBudget(base); got != base {
			t.Fatalf("non-budget error should pass through unchanged, got %v", got)
		}
	})

	t.Run("nil passes through", func(t *testing.T) {
		if got := wrapScriptBudget(nil); got != nil {
			t.Fatalf("nil should stay nil, got %v", got)
		}
	})
}

// TestMemGuardFlagDefaults pins the break-nothing defaults so a future edit that
// silently zeroes them (which would disable the guards) fails the build (#654).
func TestMemGuardFlagDefaults(t *testing.T) {
	if f := recoverCmd.Flags().Lookup("max-script-bytes"); f == nil {
		t.Fatal("recover: --max-script-bytes flag is missing")
	} else if f.DefValue != "2GB" {
		t.Errorf("recover --max-script-bytes default = %q, want 2GB", f.DefValue)
	}
	if f := reconstructCmd.Flags().Lookup("warn-event-threshold"); f == nil {
		t.Fatal("reconstruct: --warn-event-threshold flag is missing")
	} else if f.DefValue != "5000000" {
		t.Errorf("reconstruct --warn-event-threshold default = %q, want 5000000", f.DefValue)
	}
}
