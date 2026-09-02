package cli

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// The snapshot views.sql (#1583) is published through hooks internal/views
// arms from its init() — arming rides the import graph, so nothing goes red
// at compile time if a refactor stops linking the generator into a binary.
// This package is imported by both capture binaries (bintrail and
// bintrail-pg), so a green run here proves both of them publish the file.
func TestSnapshotViewsHooksArmed(t *testing.T) {
	if !baseline.SnapshotViewsWriterArmed() {
		t.Fatal("the snapshot views.sql writer is not armed: completing snapshots will publish " +
			"no views file, silently — internal/views' init() no longer reaches this binary")
	}
	if !baseline.SnapshotViewsRespellerArmed() {
		t.Fatal("the snapshot views.sql respeller is not armed: S3 uploads will skip the views " +
			"file (with only a per-upload warning) — internal/views' init() no longer reaches this binary")
	}
}
