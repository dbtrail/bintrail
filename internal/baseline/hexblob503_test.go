package baseline

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// Item 1 (#503): a --hex-blob binary column (0x<hex>) decodes to the real bytes,
// a non-binary column keeps the 0x… literal, and GEOMETRY routes through the
// binary path (item 2 decode). 0x612C6229 = 'a',',','b',')'.
func TestConvertValue_HexBlobDecodesForBinaryFamily(t *testing.T) {
	want := []byte{0x61, 0x2c, 0x62, 0x29}

	binaryTypes := []string{
		"varbinary", "binary", "blob", "tinyblob", "mediumblob", "longblob", "bit",
		"geometry", "point", "linestring", "polygon", "multipolygon",
	}
	for _, typ := range binaryTypes {
		v, err := convertValue(Column{Name: "c", MySQLType: typ}, "0x612C6229")
		if err != nil {
			t.Fatalf("%s: convertValue: %v", typ, err)
		}
		if got := v.ByteArray(); !bytes.Equal(got, want) {
			t.Errorf("%s: stored %q (%x), want real bytes %q (%x) — hex-blob not decoded (#503 item 1)",
				typ, got, got, want, want)
		}
	}

	// A non-binary column keeps 0x… verbatim (there 0x is a legitimate literal,
	// never a hex-blob), so it must NOT be decoded.
	v, err := convertValue(Column{Name: "c", MySQLType: "varchar"}, "0x612C6229")
	if err != nil {
		t.Fatal(err)
	}
	if got := string(v.ByteArray()); got != "0x612C6229" {
		t.Errorf("varchar: 0x literal was decoded (%q); non-binary columns must keep it verbatim", got)
	}
}

func TestDecodeBinaryLiteral_Fallbacks(t *testing.T) {
	cases := []struct {
		in   string
		want []byte
	}{
		{"0x612C6229", []byte{0x61, 0x2c, 0x62, 0x29}}, // even hex → decode
		{"0x", []byte("0x")},                           // no digits → verbatim
		{"0xZZ", []byte("0xZZ")},                       // invalid hex → verbatim
		{"0x612", []byte("0x612")},                     // odd length → verbatim (DecodeString errors)
		{"plain", []byte("plain")},                     // not hex → verbatim
		{"", []byte("")},                               // empty → verbatim
	}
	for _, c := range cases {
		if got := decodeBinaryLiteral(c.in); !bytes.Equal(got, c.want) {
			t.Errorf("decodeBinaryLiteral(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// Item 1 (#503): the reader strips the _binary introducer from `_binary 0x…` and
// yields the 0x… literal (convertValue then decodes it). Pre-fix the whole
// "_binary 0x…" token was captured as the value.
func TestReadSQLFile_BinaryIntroducerHex(t *testing.T) {
	p := filepath.Join(t.TempDir(), "d.sql")
	if err := os.WriteFile(p, []byte("INSERT INTO `t` (`id`,`vb`) VALUES(1,_binary 0x612C6229);\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var got []string
	if err := ReadSQLFile(p, func(values []string, nulls []bool) error {
		got = append([]string(nil), values...)
		return nil
	}); err != nil {
		t.Fatalf("ReadSQLFile: %v", err)
	}
	if len(got) != 2 || got[1] != "0x612C6229" {
		t.Errorf("_binary 0x… → vb=%q, want %q", got, "0x612C6229")
	}
}

// Item 2 (#503): GEOMETRY and its subtypes must map to a binary Parquet leaf
// (like blob), NOT the UTF-8 String default — otherwise non-UTF-8 WKB bytes would
// be written under a STRING annotation. Guards the schema-node half of the fix,
// which the convertValue test cannot see (it builds Columns with no ParquetType).
func TestGeometryMapsToBinaryNode(t *testing.T) {
	blob := MysqlToParquetNode2("blob", false)
	str := MysqlToParquetNode2("varchar", false)
	// Sanity: the assertion below must be able to tell a binary leaf from STRING
	// in this parquet-go version, else it would false-pass.
	if (blob.Type().LogicalType() == nil) == (str.Type().LogicalType() == nil) {
		t.Fatal("cannot distinguish binary leaf from STRING via LogicalType — revisit the assertion")
	}
	for _, typ := range []string{
		"geometry", "point", "linestring", "polygon",
		"multipoint", "multilinestring", "multipolygon",
		"geometrycollection", "geomcollection",
	} {
		node := MysqlToParquetNode2(typ, false)
		if node.Type().Kind() != blob.Type().Kind() {
			t.Errorf("%s: node kind %v, want %v (binary leaf like blob)", typ, node.Type().Kind(), blob.Type().Kind())
		}
		if (node.Type().LogicalType() == nil) != (blob.Type().LogicalType() == nil) {
			t.Errorf("%s: logical-type annotation differs from blob — likely mapped to STRING, storing WKB as UTF-8", typ)
		}
	}
}
