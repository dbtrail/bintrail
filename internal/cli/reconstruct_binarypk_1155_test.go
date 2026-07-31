package cli

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// The #1155 reconciler tests (padFixedBinaryFilter, IndexPKSpelling) moved to
// internal/reconstruct with the reconcilers themselves (#1157) — see
// internal/reconstruct/pk_filter_test.go. Only the CLI-side diagnosis helper
// remains here.

// TestUnsupportedPKType guards the #1155 misdiagnosis fix: the PK-changing-
// UPDATE explanation may only be reached when the lookup was capable of
// resolving the key in the first place.
func TestUnsupportedPKType(t *testing.T) {
	supported := []metadata.ColumnMeta{
		{Name: "k", DataType: "binary", ColumnType: "binary(16)"},
		{Name: "id", DataType: "int"},
	}
	if c := unsupportedPKType(supported); c != nil {
		t.Errorf("unsupportedPKType flagged %q (%s) — the binary family is supported since #1155", c.Name, c.DataType)
	}

	mixed := []metadata.ColumnMeta{
		{Name: "id", DataType: "int"},
		{Name: "flags", DataType: "bit", ColumnType: "bit(8)"},
	}
	c := unsupportedPKType(mixed)
	if c == nil {
		t.Fatal("unsupportedPKType did not flag a BIT primary-key column")
	}
	if c.Name != "flags" {
		t.Errorf("flagged column = %q, want %q", c.Name, "flags")
	}

	if c := unsupportedPKType(nil); c != nil {
		t.Errorf("unsupportedPKType(nil) = %v, want nil — no metadata means no verdict, not a bad verdict", c)
	}

	// A PostgreSQL snapshot leaves data_type AND column_type empty (#533), and
	// single-row reconstruct runs generically for a PG source. Flagging that as
	// "unsupported" would tell every PG operator their schema does not work
	// when it does — worse than the #782 misdiagnosis this branch replaces.
	pg := []metadata.ColumnMeta{{Name: "id", DataType: "", ColumnType: ""}}
	if c := unsupportedPKType(pg); c != nil {
		t.Errorf("unsupportedPKType flagged an empty DataType (%q) — that is the PostgreSQL snapshot signature, not an unsupported type", c.Name)
	}
}
