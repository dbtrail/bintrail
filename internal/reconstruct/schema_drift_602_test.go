package reconstruct

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

// TestReconstruct602_columnAddedAfterBaselineFailsLoud is the regression for
// #602. A column ADDed to the source table after the baseline snapshot lives
// only in the delta events' row_after (the baseline Parquet predates it). The
// mydumper writer projects every row onto the baseline column set, so that
// column's value used to be dropped silently from the dump. The fix refuses
// the run loudly instead — and must do so BEFORE writing any chunk file, so
// no partial output is left on disk.
func TestReconstruct602_columnAddedAfterBaselineFailsLoud(t *testing.T) {
	baselinePath := writeTestBaseline(t, [][]string{
		{"1", "new"},
		{"2", "paid"},
	})
	outDir := t.TempDir()

	// id=2 is UPDATEd after a `note` column was ADDed. The event row_after
	// carries id, status AND note. note ∉ baseline columns (id, status).
	changes := map[string]*query.ResultRow{
		pkStrForInt(2): {
			EventType: parser.EventUpdate,
			PKValues:  pkStrForInt(2),
			RowBefore: map[string]any{"id": float64(2), "status": "paid"},
			RowAfter:  map[string]any{"id": float64(2), "status": "shipped", "note": "gift-wrap"},
		},
	}

	rep := &TableReport{}
	err := mergeBaselineIntoWriter(context.Background(), mergeInput{
		LocalBaselinePath: baselinePath,
		CreateTableSQL:    "-- test",
		Schema:            "mydb",
		Table:             "orders",
		PKCols:            pkColsIntID(),
		Changes:           changes,
		OutputDir:         outDir,
		ChunkSize:         0,
	}, rep)

	if err == nil {
		t.Fatalf("expected a fail-loud error for the post-baseline 'note' column, got nil")
	}
	if !strings.Contains(err.Error(), "note") {
		t.Errorf("error should name the dropped column %q, got: %v", "note", err)
	}

	// No partial output may be left on disk: the guard fires before the writer
	// opens, so the output dir must contain no .sql chunk files.
	entries, derr := os.ReadDir(outDir)
	if derr != nil {
		t.Fatalf("read output dir: %v", derr)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sql") {
			t.Errorf("partial output left on disk after failed run: %s", e.Name())
		}
	}
}

// TestPostBaselineColumns covers the pure detection helper directly.
func TestPostBaselineColumns(t *testing.T) {
	baseCols := []string{"id", "status"}

	cases := []struct {
		name    string
		changes map[string]*query.ResultRow
		want    []string
	}{
		{
			name:    "no events",
			changes: map[string]*query.ResultRow{},
			want:    nil,
		},
		{
			name: "no drift",
			changes: map[string]*query.ResultRow{
				"2": {EventType: event.EventUpdate, RowAfter: map[string]any{"id": 2, "status": "x"}},
			},
			want: nil,
		},
		{
			name: "one added column",
			changes: map[string]*query.ResultRow{
				"2": {EventType: event.EventUpdate, RowAfter: map[string]any{"id": 2, "status": "x", "note": "n"}},
			},
			want: []string{"note"},
		},
		{
			name: "multiple added columns sorted and deduped across events",
			changes: map[string]*query.ResultRow{
				"2": {EventType: event.EventUpdate, RowAfter: map[string]any{"id": 2, "zeta": "z", "note": "n"}},
				"3": {EventType: event.EventInsert, RowAfter: map[string]any{"id": 3, "note": "n2", "alpha": "a"}},
			},
			want: []string{"alpha", "note", "zeta"},
		},
		{
			name: "DELETE events ignored (no row_after)",
			changes: map[string]*query.ResultRow{
				"2": {EventType: event.EventDelete, RowBefore: map[string]any{"id": 2, "ghost": "g"}},
			},
			want: nil,
		},
		{
			name: "nil row_after ignored",
			changes: map[string]*query.ResultRow{
				"2": {EventType: event.EventInsert, RowAfter: nil},
			},
			want: nil,
		},
		{
			name: "nil event entry ignored",
			changes: map[string]*query.ResultRow{
				"2": nil,
				"3": {EventType: event.EventUpdate, RowAfter: map[string]any{"id": 3, "note": "n"}},
			},
			want: []string{"note"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := postBaselineColumns(c.changes, baseCols)
			if len(got) != len(c.want) {
				t.Fatalf("postBaselineColumns = %v, want %v", got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Fatalf("postBaselineColumns = %v, want %v", got, c.want)
				}
			}
		})
	}
}
