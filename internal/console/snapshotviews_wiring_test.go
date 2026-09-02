package console

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// Same wiring probe as internal/cli's: the console daemon runs the dump and
// refresh pipelines in-process, so its binary must arm the #1583 hooks too —
// its snapshots publish their views.sql, and its uploads respell it.
func TestSnapshotViewsHooksArmed(t *testing.T) {
	if !baseline.SnapshotViewsWriterArmed() {
		t.Fatal("the snapshot views.sql writer is not armed in the console binary — " +
			"internal/views' init() no longer reaches it")
	}
	if !baseline.SnapshotViewsRespellerArmed() {
		t.Fatal("the snapshot views.sql respeller is not armed in the console binary — " +
			"internal/views' init() no longer reaches it")
	}
}
