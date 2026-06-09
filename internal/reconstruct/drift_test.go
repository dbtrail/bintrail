package reconstruct

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/query"
)

// TestApplyEvent_driftRowPreservesState pins the dbtrail/bintrail#318 guard:
// rows with EventType==0 (the zero value, produced by defensive scanRows
// when the row's event_type column is NULL) must NOT mutate reconstructed
// state. The case is wired explicitly so a future refactor that changes the
// default-branch semantics (e.g. to "treat unknown as a delete") cannot
// silently produce wrong PITR state for drift rows.
func TestApplyEvent_driftRowPreservesState(t *testing.T) {
	initial := map[string]any{"id": 1, "name": "Alice"}
	drift := query.ResultRow{EventID: 99, EventType: 0}

	got := applyEvent(initial, drift)
	if len(got) != len(initial) {
		t.Fatalf("expected state preserved, got %d keys vs %d initial", len(got), len(initial))
	}
	for k, v := range initial {
		if got[k] != v {
			t.Errorf("key %q: got %v, want %v", k, got[k], v)
		}
	}
}
