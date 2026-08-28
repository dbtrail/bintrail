package serverid

import (
	"errors"
	"fmt"
	"testing"

	"github.com/dbtrail/dbtrail/internal/telemetry"
)

// TestErrConflictStaysASentinelAndIsClassed: ErrConflict became a typed value
// in #1503 so it can name its telemetry class; callers keep using errors.Is.
func TestErrConflictStaysASentinelAndIsClassed(t *testing.T) {
	wrapped := fmt.Errorf("cannot stream: %w", ErrConflict)
	if !errors.Is(wrapped, ErrConflict) {
		t.Fatal("errors.Is(wrapped, ErrConflict) = false")
	}
	if errors.Is(errors.New("server identity conflict"), ErrConflict) {
		t.Fatal("an unrelated error with similar text must not match ErrConflict")
	}
	if got := telemetry.ClassifyError(wrapped); got != telemetry.ClassConfigInvalid {
		t.Errorf("ClassifyError(ErrConflict) = %q, want %q", got, telemetry.ClassConfigInvalid)
	}
}
