package icebergexport

import (
	"strings"
	"testing"

	"github.com/apache/iceberg-go"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

func TestColumnKind_mapping(t *testing.T) {
	cases := []struct {
		name     string
		col      baseline.Column
		wantKind kind
		wantP    int
		wantS    int
		wantErr  string
	}{
		{"int", baseline.Column{MySQLType: "int"}, kindInt32, 0, 0, ""},
		{"int unsigned widens", baseline.Column{MySQLType: "int", Unsigned: true}, kindInt64, 0, 0, ""},
		{"tinyint", baseline.Column{MySQLType: "tinyint"}, kindInt32, 0, 0, ""},
		{"bigint", baseline.Column{MySQLType: "bigint"}, kindInt64, 0, 0, ""},
		{"bigint unsigned is decimal(20,0)", baseline.Column{MySQLType: "bigint", Unsigned: true}, kindDecimal, 20, 0, ""},
		{"decimal keeps p,s", baseline.Column{MySQLType: "decimal", DecimalPrecision: 10, DecimalScale: 2}, kindDecimal, 10, 2, ""},
		{"decimal over 38 is text", baseline.Column{MySQLType: "decimal", DecimalPrecision: 65, DecimalScale: 30}, kindString, 0, 0, ""},
		{"datetime", baseline.Column{MySQLType: "datetime"}, kindTimestamp, 0, 0, ""},
		{"timestamp", baseline.Column{MySQLType: "timestamp"}, kindTimestamp, 0, 0, ""},
		{"date", baseline.Column{MySQLType: "date"}, kindDate, 0, 0, ""},
		{"time is text", baseline.Column{MySQLType: "time"}, kindString, 0, 0, ""},
		{"year", baseline.Column{MySQLType: "year"}, kindInt32, 0, 0, ""},
		{"float", baseline.Column{MySQLType: "float"}, kindFloat32, 0, 0, ""},
		{"double", baseline.Column{MySQLType: "double"}, kindFloat64, 0, 0, ""},
		{"varchar", baseline.Column{MySQLType: "varchar"}, kindString, 0, 0, ""},
		{"enum is text (labels)", baseline.Column{MySQLType: "enum"}, kindString, 0, 0, ""},
		{"json is text", baseline.Column{MySQLType: "json"}, kindString, 0, 0, ""},
		{"blob is binary", baseline.Column{MySQLType: "blob"}, kindBinary, 0, 0, ""},
		{"varbinary is binary", baseline.Column{MySQLType: "varbinary"}, kindBinary, 0, 0, ""},
		{"bit refused", baseline.Column{Name: "flags", MySQLType: "bit"}, 0, 0, 0, "BIT columns are not supported"},
		{"case-insensitive", baseline.Column{MySQLType: "DATETIME"}, kindTimestamp, 0, 0, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			k, p, s, err := columnKind(tc.col)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if k != tc.wantKind || p != tc.wantP || s != tc.wantS {
				t.Fatalf("got (%v,%d,%d), want (%v,%d,%d)", k, p, s, tc.wantKind, tc.wantP, tc.wantS)
			}
		})
	}
}

func TestBuildColumns_fieldIDsAndPK(t *testing.T) {
	cols, err := buildColumns([]baseline.Column{
		{Name: "tenant", MySQLType: "int"},
		{Name: "id", MySQLType: "bigint"},
		{Name: "amount", MySQLType: "decimal", DecimalPrecision: 12, DecimalScale: 3},
	}, []string{"tenant", "ID"})
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range cols {
		if c.FieldID != i+1 {
			t.Errorf("column %s field id = %d, want %d (ordinal position)", c.Name, c.FieldID, i+1)
		}
	}
	if !cols[0].PK || !cols[1].PK || cols[2].PK {
		t.Fatalf("PK flags = %v %v %v, want true true false (case-insensitive match)", cols[0].PK, cols[1].PK, cols[2].PK)
	}
	if got := pkFieldIDs(cols); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("pkFieldIDs = %v, want [1 2]", got)
	}

	sc := icebergSchema(cols)
	if got := sc.IdentifierFieldIDs; len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("identifier field ids = %v, want [1 2]", got)
	}
	for _, f := range sc.Fields() {
		wantRequired := f.Name == "tenant" || f.Name == "id"
		if f.Required != wantRequired {
			t.Errorf("field %s required = %v, want %v", f.Name, f.Required, wantRequired)
		}
	}
	if dt, ok := sc.Fields()[2].Type.(iceberg.DecimalType); !ok || dt.Precision() != 12 || dt.Scale() != 3 {
		t.Fatalf("amount type = %v, want decimal(12,3)", sc.Fields()[2].Type)
	}
}

func TestBuildColumns_refusals(t *testing.T) {
	if _, err := buildColumns(nil, []string{"id"}); err == nil {
		t.Fatal("no columns: want error")
	}
	if _, err := buildColumns([]baseline.Column{{Name: "id", MySQLType: "int"}}, []string{"id", "missing"}); err == nil {
		t.Fatal("PK column absent from CREATE TABLE: want error")
	}
	if _, err := buildColumns([]baseline.Column{{Name: "id", MySQLType: "int"}, {Name: "flags", MySQLType: "bit"}}, []string{"id"}); err == nil {
		t.Fatal("BIT column: want error")
	}
}

func TestColumnsFromSchema_roundTrip(t *testing.T) {
	src := []baseline.Column{
		{Name: "id", MySQLType: "bigint"},
		{Name: "amount", MySQLType: "decimal", DecimalPrecision: 10, DecimalScale: 2},
		{Name: "at", MySQLType: "datetime"},
		{Name: "day", MySQLType: "date"},
		{Name: "blob", MySQLType: "blob"},
		{Name: "name", MySQLType: "varchar"},
		{Name: "ratio", MySQLType: "double"},
	}
	cols, err := buildColumns(src, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	back, err := columnsFromSchema(icebergSchema(cols))
	if err != nil {
		t.Fatal(err)
	}
	if len(back) != len(cols) {
		t.Fatalf("len = %d, want %d", len(back), len(cols))
	}
	for i := range cols {
		if back[i].Name != cols[i].Name || back[i].Kind != cols[i].Kind || back[i].FieldID != cols[i].FieldID ||
			back[i].PK != cols[i].PK || back[i].Precision != cols[i].Precision || back[i].Scale != cols[i].Scale {
			t.Errorf("column %d: got %+v, want %+v", i, back[i], cols[i])
		}
	}
}
