package parser

import (
	"errors"
	"testing"

	"github.com/dbtrail/dbtrail/internal/telemetry"
)

// TestHandleRows_driftErrorIsSchemaDriftError: the #700 hard error is a
// *SchemaDriftError so usage telemetry can report it as schema_mismatch
// instead of unknown (#1503). Driven through the real handleRows path, not a
// hand-built value, so a refactor that goes back to fmt.Errorf fails here.
func TestHandleRows_driftErrorIsSchemaDriftError(t *testing.T) {
	_, err := runHandleRows(t, driftRowsEvent([]string{"id", "total"}))
	if err == nil {
		t.Fatal("post-snapshot drift must hard-error")
	}
	var de *SchemaDriftError
	if !errors.As(err, &de) {
		t.Fatalf("drift error is %T, want *SchemaDriftError", err)
	}
	if got := telemetry.ClassifyError(err); got != telemetry.ClassSchemaMismatch {
		t.Errorf("ClassifyError(drift) = %q, want %q", got, telemetry.ClassSchemaMismatch)
	}
}
