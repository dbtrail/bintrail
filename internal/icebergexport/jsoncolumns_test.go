package icebergexport

import (
	"errors"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// TestApplyJSONColumns pins where the delta path learns which string columns
// are JSON (#1508): the table's own property first, the schema snapshot as a
// cross-check and, for a table loaded before the property existed, as the
// fallback.
func TestApplyJSONColumns(t *testing.T) {
	fresh := func() []column {
		return []column{
			{Name: "id", Kind: kindInt32, FieldID: 1, PK: true},
			{Name: "Meta", Kind: kindString, FieldID: 2},
			{Name: "note", Kind: kindString, FieldID: 3},
		}
	}
	snapshot := func(metaType, noteType string) *metadata.TableMeta {
		return &metadata.TableMeta{Columns: []metadata.ColumnMeta{
			{Name: "id", DataType: "int"}, {Name: "Meta", DataType: metaType}, {Name: "note", DataType: noteType},
		}}
	}
	jsonFlags := func(cols []column) string {
		var b strings.Builder
		for _, c := range cols {
			if c.isJSON() {
				b.WriteString("1")
			} else {
				b.WriteString("0")
			}
		}
		return b.String()
	}

	t.Run("recorded property marks the column, case-insensitively", func(t *testing.T) {
		cols := fresh()
		if err := applyJSONColumns(cols, iceberg.Properties{propJSONColumns: "meta"}, snapshot("json", "text"), "s", "t"); err != nil {
			t.Fatal(err)
		}
		if got := jsonFlags(cols); got != "010" {
			t.Fatalf("json flags = %s, want 010", got)
		}
	})
	t.Run("recorded property without the column, snapshot says json: refused", func(t *testing.T) {
		cols := fresh()
		err := applyJSONColumns(cols, iceberg.Properties{propJSONColumns: ""}, snapshot("json", "text"), "s", "t")
		if !errors.Is(err, reconstruct.ErrSchemaChanged) || !strings.Contains(err.Error(), "Meta is now json but was exported as text") {
			t.Fatalf("err = %v, want ErrSchemaChanged naming Meta", err)
		}
	})
	t.Run("recorded json column that the snapshot now calls text: refused", func(t *testing.T) {
		cols := fresh()
		err := applyJSONColumns(cols, iceberg.Properties{propJSONColumns: "meta"}, snapshot("longtext", "text"), "s", "t")
		if !errors.Is(err, reconstruct.ErrSchemaChanged) || !strings.Contains(err.Error(), "Meta is now longtext but was exported as json") {
			t.Fatalf("err = %v, want ErrSchemaChanged naming Meta", err)
		}
	})
	t.Run("recorded property, snapshot without data_type: no cross-check, property wins", func(t *testing.T) {
		cols := fresh()
		if err := applyJSONColumns(cols, iceberg.Properties{propJSONColumns: "meta"}, snapshot("", ""), "s", "t"); err != nil {
			t.Fatal(err)
		}
		if got := jsonFlags(cols); got != "010" {
			t.Fatalf("json flags = %s, want 010", got)
		}
	})
	t.Run("no property (older table): the snapshot decides", func(t *testing.T) {
		cols := fresh()
		if err := applyJSONColumns(cols, iceberg.Properties{}, snapshot("json", "text"), "s", "t"); err != nil {
			t.Fatal(err)
		}
		if got := jsonFlags(cols); got != "010" {
			t.Fatalf("json flags = %s, want 010", got)
		}
	})
	t.Run("no property and no data_type: nothing typed, nothing refused", func(t *testing.T) {
		cols := fresh()
		if err := applyJSONColumns(cols, iceberg.Properties{}, snapshot("", ""), "s", "t"); err != nil {
			t.Fatal(err)
		}
		if got := jsonFlags(cols); got != "000" {
			t.Fatalf("json flags = %s, want 000", got)
		}
	})
}

func TestJSONColumnsProperty_roundTrip(t *testing.T) {
	cols := []column{
		{Name: "id", Kind: kindInt32, MySQLType: "int"},
		{Name: "Meta", Kind: kindString, MySQLType: "json"},
		{Name: "note", Kind: kindString, MySQLType: "text"},
		{Name: "extra", Kind: kindString, MySQLType: "json"},
	}
	if got := jsonColumnsProperty(cols); got != "meta,extra" {
		t.Fatalf("property = %q, want meta,extra", got)
	}
	if got := jsonColumnsProperty(cols[:1]); got != "" {
		t.Fatalf("property with no JSON column = %q, want empty (recorded, not absent)", got)
	}
	// Written by the load, read back by the increment: the same columns.
	rebuilt, err := columnsFromSchema(icebergSchema(cols))
	if err != nil {
		t.Fatal(err)
	}
	if err := applyJSONColumns(rebuilt, iceberg.Properties{propJSONColumns: jsonColumnsProperty(cols)}, &metadata.TableMeta{}, "s", "t"); err != nil {
		t.Fatal(err)
	}
	for i, c := range cols {
		if rebuilt[i].isJSON() != c.isJSON() {
			t.Fatalf("column %s: rebuilt isJSON=%v, loaded isJSON=%v", c.Name, rebuilt[i].isJSON(), c.isJSON())
		}
	}
}
