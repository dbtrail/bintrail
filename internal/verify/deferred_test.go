package verify

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// TestIsDeferredType pins the case list, including the #672 decision: TEXT
// family types are decoded by DecodeEventBinaries (same as BLOB) but are NOT
// deferred — once decoded, TEXT is directly comparable to the baseline/source
// text, unlike ENUM/JSON/binary, so deferring it would mask genuine
// divergences instead of guarding against an unresolved representation gap.
func TestIsDeferredType(t *testing.T) {
	deferred := []string{
		"enum", "set", "json",
		"binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob", "bit",
		// #793: spatial family + VECTOR — binary (WKB / packed floats) in the
		// event image, same base64-vs-raw gap as BLOB, so they must defer to
		// Inconclusive rather than report a conclusive false-MISMATCH.
		"geometry", "point", "linestring", "polygon",
		"multipoint", "multilinestring", "multipolygon",
		"geometrycollection", "geomcollection", // MySQL 8.0.11+ reports the latter
		"vector",
		"BLOB", "Enum", "GEOMETRY", // case-insensitive
	}
	for _, dt := range deferred {
		if !isDeferredType(dt) {
			t.Errorf("isDeferredType(%q) = false, want true", dt)
		}
	}
	notDeferred := []string{
		"int", "varchar", "char", "datetime", "double", "decimal",
		"text", "tinytext", "mediumtext", "longtext", "TEXT",
	}
	for _, dt := range notDeferred {
		if isDeferredType(dt) {
			t.Errorf("isDeferredType(%q) = true, want false", dt)
		}
	}
}

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
