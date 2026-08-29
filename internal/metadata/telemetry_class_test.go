package metadata

import (
	"errors"
	"fmt"
	"testing"

	"github.com/dbtrail/dbtrail/internal/telemetry"
)

// TestErrNoSnapshotsStaysASentinelAndIsClassed: ErrNoSnapshots became a typed
// value in #1503 so it can name its telemetry class; callers keep using
// errors.Is.
func TestErrNoSnapshotsStaysASentinelAndIsClassed(t *testing.T) {
	wrapped := fmt.Errorf("load resolver: %w", ErrNoSnapshots)
	if !errors.Is(wrapped, ErrNoSnapshots) {
		t.Fatal("errors.Is(wrapped, ErrNoSnapshots) = false")
	}
	if got := telemetry.ClassifyError(wrapped); got != telemetry.ClassNotFound {
		t.Errorf("ClassifyError(ErrNoSnapshots) = %q, want %q", got, telemetry.ClassNotFound)
	}
}
