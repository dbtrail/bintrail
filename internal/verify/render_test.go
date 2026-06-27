package verify

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

func col(dataType, columnType string) metadata.ColumnMeta {
	return metadata.ColumnMeta{Name: "c", DataType: dataType, ColumnType: columnType}
}

func TestRenderCell_MatchesTextProtocolForm(t *testing.T) {
	ts := time.Date(2021, 1, 1, 0, 0, 0, 123456000, time.UTC) // .123456
	dt0 := time.Date(2022, 6, 15, 12, 30, 45, 0, time.UTC)
	d := time.Date(2021, 3, 4, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		name string
		v    any
		col  metadata.ColumnMeta
		want []byte // nil = SQL NULL
	}{
		{"null", nil, col("varchar", "varchar(64)"), nil},
		{"int64", int64(42), col("int", "int"), []byte("42")},
		{"int32 baseline", int32(7), col("int", "int"), []byte("7")},
		{"uint64 max", uint64(18446744073709551615), col("bigint", "bigint unsigned"), []byte("18446744073709551615")},
		{"json.Number big", json.Number("9007199254740993"), col("bigint", "bigint"), []byte("9007199254740993")},
		{"json.Number decimal", json.Number("1.50"), col("decimal", "decimal(10,2)"), []byte("1.50")},
		{"decimal as string", "1.50", col("decimal", "decimal(10,2)"), []byte("1.50")},
		{"utf8mb4 string", "café", col("varchar", "varchar(64)"), []byte("café")},
		{"empty string", "", col("varchar", "varchar(64)"), []byte("")},
		{"binary bytes", []byte{0x61, 0x00, 0x62}, col("varbinary", "varbinary(16)"), []byte{0x61, 0x00, 0x62}},
		{"datetime(6)", ts, col("datetime", "datetime(6)"), []byte("2021-01-01 00:00:00.123456")},
		{"datetime(0)", dt0, col("datetime", "datetime"), []byte("2022-06-15 12:30:45")},
		{"datetime(3)", ts, col("datetime", "datetime(3)"), []byte("2021-01-01 00:00:00.123")},
		{"date", d, col("date", "date"), []byte("2021-03-04")},
		{"timestamp(0)", dt0, col("timestamp", "timestamp"), []byte("2022-06-15 12:30:45")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := renderCell(tc.v, tc.col)
			if tc.want == nil {
				if got != nil {
					t.Errorf("got %q, want NULL (nil)", got)
				}
				return
			}
			if !bytes.Equal(got, tc.want) {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRenderCell_JSONContainerCompletes(t *testing.T) {
	// A JSON column changed by an event decodes to map[string]any; renderCell
	// must produce deterministic bytes (so the digest completes) rather than
	// erroring. Two equal maps render identically.
	a := renderCell(map[string]any{"b": 2, "a": 1}, col("json", "json"))
	b := renderCell(map[string]any{"a": 1, "b": 2}, col("json", "json"))
	if a == nil || !bytes.Equal(a, b) {
		t.Errorf("JSON container rendering not deterministic: %q vs %q", a, b)
	}
}

func TestTemporalPrecision(t *testing.T) {
	cases := map[string]int{
		"datetime":      0,
		"datetime(6)":   6,
		"timestamp(3)":  3,
		"datetime(0)":   0,
		"int":           0,
		"decimal(10,2)": 0, // multi-arg paren isn't a single precision int → 0 (only temporal types call this)
	}
	for ct, want := range cases {
		if got := temporalPrecision(ct); got != want {
			t.Errorf("temporalPrecision(%q) = %d, want %d", ct, got, want)
		}
	}
}
