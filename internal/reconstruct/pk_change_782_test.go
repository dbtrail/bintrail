package reconstruct

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// pkColsIntTwo returns a composite PK descriptor (a INT, b INT) for the helper
// test's composite case.
func pkColsIntTwo() []metadata.ColumnMeta {
	return []metadata.ColumnMeta{
		{Name: "a", OrdinalPosition: 1, IsPK: true, DataType: "int"},
		{Name: "b", OrdinalPosition: 2, IsPK: true, DataType: "int"},
	}
}

// TestPKChangingUpdate covers the pure detection helper directly.
func TestPKChangingUpdate(t *testing.T) {
	cases := []struct {
		name       string
		pkCols     []metadata.ColumnMeta
		changes    map[string]*query.ResultRow
		wantFound  bool
		wantBefore string
		wantAfter  string
	}{
		{
			name:    "empty",
			pkCols:  pkColsIntID(),
			changes: map[string]*query.ResultRow{},
		},
		{
			name:   "non-pk-changing update ignored",
			pkCols: pkColsIntID(),
			changes: map[string]*query.ResultRow{
				pkStrForInt(1): {
					EventType: event.EventUpdate,
					RowBefore: map[string]any{"id": float64(1), "status": "new"},
					RowAfter:  map[string]any{"id": float64(1), "status": "paid"},
				},
			},
		},
		{
			name:   "insert ignored (nil before)",
			pkCols: pkColsIntID(),
			changes: map[string]*query.ResultRow{
				pkStrForInt(9): {
					EventType: event.EventInsert,
					RowAfter:  map[string]any{"id": float64(9), "status": "new"},
				},
			},
		},
		{
			name:   "delete ignored (nil after)",
			pkCols: pkColsIntID(),
			changes: map[string]*query.ResultRow{
				pkStrForInt(9): {
					EventType: event.EventDelete,
					RowBefore: map[string]any{"id": float64(9), "status": "gone"},
				},
			},
		},
		{
			name:   "pk-changing update detected",
			pkCols: pkColsIntID(),
			changes: map[string]*query.ResultRow{
				pkStrForInt(1): {
					EventType: event.EventUpdate,
					RowBefore: map[string]any{"id": float64(1), "status": "new"},
					RowAfter:  map[string]any{"id": float64(2), "status": "moved"},
				},
			},
			wantFound:  true,
			wantBefore: "1",
			wantAfter:  "2",
		},
		{
			name:   "composite pk partial change detected",
			pkCols: pkColsIntTwo(),
			changes: map[string]*query.ResultRow{
				"1|5": {
					EventType: event.EventUpdate,
					RowBefore: map[string]any{"a": float64(1), "b": float64(5)},
					RowAfter:  map[string]any{"a": float64(1), "b": float64(6)},
				},
			},
			wantFound:  true,
			wantBefore: "1|5",
			wantAfter:  "1|6",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, a, found := pkChangingUpdate(tc.changes, tc.pkCols)
			if found != tc.wantFound {
				t.Fatalf("found = %v, want %v (before=%q after=%q)", found, tc.wantFound, b, a)
			}
			if found && (b != tc.wantBefore || a != tc.wantAfter) {
				t.Errorf("before/after = %q/%q, want %q/%q", b, a, tc.wantBefore, tc.wantAfter)
			}
		})
	}
}

// TestMergeBaseline_pkChangingUpdateRefused_scenarioA is #782 scenario A
// (resurrection): `UPDATE pk 1→2; DELETE pk=2`. The change map is keyed by the
// before-image PK, so without the guard the baseline row pk=1 would match the
// UPDATE and emit pk=2 — a row the DELETE actually removed. The guard must
// refuse loudly, naming both PKs, before any chunk file is written.
func TestMergeBaseline_pkChangingUpdateRefused_scenarioA(t *testing.T) {
	outDir := t.TempDir()

	changes := map[string]*query.ResultRow{
		pkStrForInt(1): {
			EventType: event.EventUpdate,
			PKValues:  pkStrForInt(1),
			RowBefore: map[string]any{"id": float64(1), "status": "new"},
			RowAfter:  map[string]any{"id": float64(2), "status": "moved"},
		},
		pkStrForInt(2): {
			EventType: event.EventDelete,
			PKValues:  pkStrForInt(2),
			RowBefore: map[string]any{"id": float64(2), "status": "moved"},
		},
	}

	assertPKChangeRefusal(t, outDir, changes)
}

// TestMergeBaseline_pkChangingUpdateRefused_scenarioB is #782 scenario B
// (duplication): `UPDATE pk 1→2; UPDATE pk=2`. pk=2 would be emitted twice (as
// the first UPDATE's after-image under key 1, and by the second UPDATE under
// key 2), a 1062 that only surfaces at restore time.
func TestMergeBaseline_pkChangingUpdateRefused_scenarioB(t *testing.T) {
	outDir := t.TempDir()

	changes := map[string]*query.ResultRow{
		pkStrForInt(1): {
			EventType: event.EventUpdate,
			PKValues:  pkStrForInt(1),
			RowBefore: map[string]any{"id": float64(1), "status": "new"},
			RowAfter:  map[string]any{"id": float64(2), "status": "renamed"},
		},
		pkStrForInt(2): {
			EventType: event.EventUpdate,
			PKValues:  pkStrForInt(2),
			RowBefore: map[string]any{"id": float64(2), "status": "renamed"},
			RowAfter:  map[string]any{"id": float64(2), "status": "final"},
		},
	}

	assertPKChangeRefusal(t, outDir, changes)
}

// assertPKChangeRefusal runs mergeBaselineIntoWriter with the given change map
// and asserts the #782 refusal: a loud error naming the schema.table and the
// PK transition, and no partial output left on disk (the guard fires before the
// writer opens).
func assertPKChangeRefusal(t *testing.T, outDir string, changes map[string]*query.ResultRow) {
	t.Helper()
	baselinePath := writeTestBaseline(t, [][]string{{"1", "new"}})

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
		t.Fatal("expected a fail-loud error for a PK-changing UPDATE, got nil")
	}
	for _, want := range []string{"PK-changing UPDATE", "mydb.orders", `"1"`, `"2"`, "bintrail baseline"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q:\n%s", want, err)
		}
	}
	entries, derr := os.ReadDir(outDir)
	if derr != nil {
		t.Fatalf("read output dir: %v", derr)
	}
	if len(entries) != 0 {
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		t.Errorf("refusal left partial output on disk: %v", names)
	}
}

// TestSnapshotFullTableImages_pkChangingUpdateRefused pins the #782 guard on the
// shim/verify full-table _snapshot entry point INDEPENDENTLY of the mydumper
// path. BaselinePath deliberately points nowhere and emit records invocations:
// the refusal must fire BEFORE baseline materialization (in production an S3
// download) and before a single row is emitted.
func TestSnapshotFullTableImages_pkChangingUpdateRefused(t *testing.T) {
	emitted := 0
	err := SnapshotFullTableImages(context.Background(), SnapshotFullTableInput{
		BaselinePath: "/nonexistent/never-touched/baseline.parquet",
		Schema:       "mydb",
		Table:        "orders",
		PKCols:       pkColsIntID(),
		Changes: map[string]*query.ResultRow{
			pkStrForInt(1): {
				EventType: event.EventUpdate,
				PKValues:  pkStrForInt(1),
				RowBefore: map[string]any{"id": float64(1), "status": "new"},
				RowAfter:  map[string]any{"id": float64(2), "status": "moved"},
			},
		},
	}, func(map[string]any) error {
		emitted++
		return nil
	})
	if err == nil {
		t.Fatal("expected a loud error for a PK-changing UPDATE")
	}
	if !strings.Contains(err.Error(), "PK-changing UPDATE") {
		t.Errorf("error should name the PK-changing UPDATE, got: %v", err)
	}
	if strings.Contains(err.Error(), "materialize baseline") {
		t.Errorf("refusal must fire BEFORE baseline materialization, got: %v", err)
	}
	if emitted != 0 {
		t.Errorf("refusal must emit no rows, emitted %d", emitted)
	}
}

// TestWriteBinlogOnlyChanges_pkChangingUpdateRefused covers the binlog-only
// fallback path (no baseline, #766): the change map is still keyed by the
// before-image PK, so a PK-changing UPDATE duplicates a row. Refuse loudly with
// no partial output.
func TestWriteBinlogOnlyChanges_pkChangingUpdateRefused(t *testing.T) {
	outDir := t.TempDir()
	colNames := []string{"id", "status"}

	changes := map[string]*query.ResultRow{
		pkStrForInt(1): {
			EventType: event.EventUpdate,
			PKValues:  pkStrForInt(1),
			RowBefore: map[string]any{"id": float64(1), "status": "new"},
			RowAfter:  map[string]any{"id": float64(2), "status": "moved"},
		},
	}

	rep := &TableReport{Schema: "mydb", Table: "orders"}
	err := writeBinlogOnlyChanges(outDir, "mydb", "orders", pkColsIntID(), colNames, 0,
		binlogOnlySchemaPlaceholder("mydb", "orders"), changes, rep)
	if err == nil {
		t.Fatal("expected a fail-loud error for a PK-changing UPDATE, got nil")
	}
	if !strings.Contains(err.Error(), "PK-changing UPDATE") {
		t.Errorf("error should name the PK-changing UPDATE, got: %v", err)
	}
	entries, derr := os.ReadDir(outDir)
	if derr != nil {
		t.Fatalf("read output dir: %v", derr)
	}
	if len(entries) != 0 {
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		t.Errorf("refusal left partial output on disk: %v", names)
	}
}
