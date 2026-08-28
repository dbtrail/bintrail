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

// TestSchemaGapTrackerErrIsClassed: the file-mode "schema gap" verdict is
// typed like its drift sibling, so `bintrail index` reports it as
// schema_mismatch instead of unknown (#1503 review).
func TestSchemaGapTrackerErrIsClassed(t *testing.T) {
	if err := (&schemaGapTracker{}).err("binlog.000001"); err != nil {
		t.Fatalf("no gaps must be nil, got %v", err)
	}
	g := &schemaGapTracker{}
	g.record("shop.orders at binlog.000001:4")
	err := g.err("binlog.000001")
	var gap *SchemaGapError
	if !errors.As(err, &gap) {
		t.Fatalf("got %T (%v), want *SchemaGapError", err, err)
	}
	if got := telemetry.ClassifyError(err); got != telemetry.ClassSchemaMismatch {
		t.Errorf("ClassifyError = %q, want %q", got, telemetry.ClassSchemaMismatch)
	}
}
