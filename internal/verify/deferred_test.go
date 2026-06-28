package verify

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

func TestDeferredReprChanged(t *testing.T) {
	intCol := metadata.ColumnMeta{Name: "n", DataType: "int"}
	strCol := metadata.ColumnMeta{Name: "s", DataType: "varchar"}
	enumCol := metadata.ColumnMeta{Name: "kind", DataType: "enum"}
	withEnum := []metadata.ColumnMeta{intCol, strCol, enumCol}
	noDeferred := []metadata.ColumnMeta{intCol, strCol}

	ch := func(rows ...*query.ResultRow) map[string]*query.ResultRow {
		m := map[string]*query.ResultRow{}
		for i, r := range rows {
			m[string(rune('a'+i))] = r
		}
		return m
	}
	ins := &query.ResultRow{EventType: event.EventInsert}
	updEnum := &query.ResultRow{EventType: event.EventUpdate, ChangedColumns: []string{"kind"}}
	updStr := &query.ResultRow{EventType: event.EventUpdate, ChangedColumns: []string{"s"}}
	del := &query.ResultRow{EventType: event.EventDelete}

	cases := []struct {
		name    string
		cols    []metadata.ColumnMeta
		changes map[string]*query.ResultRow
		want    bool
	}{
		{"no deferred column at all", noDeferred, ch(ins), false},
		{"insert with a deferred column present", withEnum, ch(ins), true},
		{"update touches the deferred column", withEnum, ch(updEnum), true},
		// The load-bearing case: an update touching ONLY a non-deferred column,
		// on a table that merely contains an unchanged ENUM column, must NOT
		// trigger the deferred downgrade — otherwise a real divergence on the
		// non-deferred column would be masked inconclusive instead of mismatch.
		{"update touches only a non-deferred column", withEnum, ch(updStr), false},
		{"delete only", withEnum, ch(del), false},
		{"no changes", withEnum, ch(), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := deferredReprChanged(tc.cols, tc.changes); got != tc.want {
				t.Errorf("deferredReprChanged = %v, want %v", got, tc.want)
			}
		})
	}
}
