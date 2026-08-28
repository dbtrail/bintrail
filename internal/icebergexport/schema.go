package icebergexport

import (
	"fmt"
	"strings"

	"github.com/apache/iceberg-go"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// kind is the Iceberg-side shape of a column. It decides which Arrow builder
// receives the value and which conversions are legal for it.
type kind int

const (
	kindInt32 kind = iota
	kindInt64
	kindFloat32
	kindFloat64
	kindDecimal
	kindTimestamp
	kindDate
	kindString
	kindBinary
)

// maxDecimalPrecision is the Iceberg (and Arrow decimal128) ceiling. A wider
// MySQL DECIMAL is exported as text, the same carve-out `bintrail views`
// applies when it casts the baseline's text decimals.
const maxDecimalPrecision = 38

// column is one exported column: the MySQL declaration it came from and the
// Iceberg field it maps to. FieldID is assigned by ordinal position at table
// creation and then owned by the table forever.
type column struct {
	Name      string
	MySQLType string
	Unsigned  bool
	Kind      kind
	Precision int
	Scale     int
	FieldID   int
	PK        bool
}

// columnKind maps one MySQL column declaration to its Iceberg shape.
//
// The two value sources this export merges disagree on representation for
// several types (the baseline Parquet stores DECIMAL as text and DATETIME as
// microseconds; the row events store DECIMAL as a JSON number and DATETIME as
// a naive string), so the mapping is chosen per TYPE and both sources are
// converted to it in values.go. A column that is text on one side and a
// number on the other would otherwise split the table in two.
func columnKind(c baseline.Column) (kind, int, int, error) {
	t := strings.ToLower(strings.TrimSpace(c.MySQLType))
	switch t {
	case "bit":
		// The baseline stores BIT as raw bytes and the row events store it as
		// an unsigned integer; nothing in the tree reconciles the two yet, and
		// guessing here would write one representation into the first load and
		// another into every increment.
		return 0, 0, 0, fmt.Errorf("column %q: BIT columns are not supported by the Iceberg export yet", c.Name)
	case "tinyint", "smallint", "mediumint", "year":
		return kindInt32, 0, 0, nil
	case "int", "integer":
		if c.Unsigned {
			return kindInt64, 0, 0, nil
		}
		return kindInt32, 0, 0, nil
	case "bigint":
		if c.Unsigned {
			// No unsigned 64-bit in Iceberg; decimal(20,0) holds every value.
			return kindDecimal, 20, 0, nil
		}
		return kindInt64, 0, 0, nil
	case "float":
		return kindFloat32, 0, 0, nil
	case "double", "real":
		return kindFloat64, 0, 0, nil
	case "decimal", "numeric":
		if c.DecimalPrecision > maxDecimalPrecision {
			return kindString, 0, 0, nil
		}
		return kindDecimal, c.DecimalPrecision, c.DecimalScale, nil
	case "datetime", "timestamp":
		return kindTimestamp, 0, 0, nil
	case "date":
		return kindDate, 0, 0, nil
	}
	if baseline.IsBinaryType(t) {
		return kindBinary, 0, 0, nil
	}
	// time, char, varchar, the text family, enum, set, json and anything
	// unknown: text on both sides already.
	return kindString, 0, 0, nil
}

// buildColumns turns the baseline's parsed CREATE TABLE into the export's
// column list, in declaration order, with field IDs 1..n and the primary key
// flagged by name.
func buildColumns(cols []baseline.Column, pkNames []string) ([]column, error) {
	if len(cols) == 0 {
		return nil, fmt.Errorf("the baseline's CREATE TABLE declares no columns")
	}
	pk := make(map[string]bool, len(pkNames))
	for _, n := range pkNames {
		pk[strings.ToLower(n)] = true
	}
	out := make([]column, 0, len(cols))
	seenPK := 0
	for i, c := range cols {
		k, p, s, err := columnKind(c)
		if err != nil {
			return nil, err
		}
		isPK := pk[strings.ToLower(c.Name)]
		if isPK {
			seenPK++
		}
		out = append(out, column{
			Name:      c.Name,
			MySQLType: strings.ToLower(strings.TrimSpace(c.MySQLType)),
			Unsigned:  c.Unsigned,
			Kind:      k,
			Precision: p,
			Scale:     s,
			FieldID:   i + 1,
			PK:        isPK,
		})
	}
	if seenPK != len(pkNames) {
		return nil, fmt.Errorf("primary key columns %v are not all present in the baseline's CREATE TABLE", pkNames)
	}
	return out, nil
}

// icebergType is the Iceberg type for one column.
func icebergType(c column) iceberg.Type {
	switch c.Kind {
	case kindInt32:
		return iceberg.PrimitiveTypes.Int32
	case kindInt64:
		return iceberg.PrimitiveTypes.Int64
	case kindFloat32:
		return iceberg.PrimitiveTypes.Float32
	case kindFloat64:
		return iceberg.PrimitiveTypes.Float64
	case kindDecimal:
		return iceberg.DecimalTypeOf(c.Precision, c.Scale)
	case kindTimestamp:
		// Naive: MySQL DATETIME has no zone and the capture renders TIMESTAMP
		// in UTC, so both sides are read as UTC wall-clock values.
		return iceberg.PrimitiveTypes.Timestamp
	case kindDate:
		return iceberg.PrimitiveTypes.Date
	case kindBinary:
		return iceberg.PrimitiveTypes.Binary
	default:
		return iceberg.PrimitiveTypes.String
	}
}

// icebergSchema builds the table schema: every column by field ID, the
// primary key columns required and declared as the identifier fields, which
// is what makes an equality delete on them mean "this row".
func icebergSchema(cols []column) *iceberg.Schema {
	fields := make([]iceberg.NestedField, 0, len(cols))
	var ids []int
	for _, c := range cols {
		fields = append(fields, iceberg.NestedField{
			ID:       c.FieldID,
			Name:     c.Name,
			Type:     icebergType(c),
			Required: c.PK,
		})
		if c.PK {
			ids = append(ids, c.FieldID)
		}
	}
	return iceberg.NewSchemaWithIdentifiers(0, ids, fields...)
}

// pkFieldIDs lists the identifier field IDs in column order.
func pkFieldIDs(cols []column) []int {
	var ids []int
	for _, c := range cols {
		if c.PK {
			ids = append(ids, c.FieldID)
		}
	}
	return ids
}

// columnsFromSchema rebuilds the export's column list from an existing
// table's Iceberg schema, for incremental runs where the CREATE TABLE is not
// re-read. The MySQL type is recovered from the current schema snapshot by
// the caller; here only the Iceberg-side shape is needed.
func columnsFromSchema(sc *iceberg.Schema, mysqlTypes map[string]baseline.Column) ([]column, error) {
	ident := make(map[int]bool)
	for _, id := range sc.IdentifierFieldIDs {
		ident[id] = true
	}
	out := make([]column, 0, len(sc.Fields()))
	for _, f := range sc.Fields() {
		c := column{Name: f.Name, FieldID: f.ID, PK: ident[f.ID]}
		if bc, ok := mysqlTypes[strings.ToLower(f.Name)]; ok {
			c.MySQLType = strings.ToLower(strings.TrimSpace(bc.MySQLType))
			c.Unsigned = bc.Unsigned
		}
		switch t := f.Type.(type) {
		case iceberg.Int32Type:
			c.Kind = kindInt32
		case iceberg.Int64Type:
			c.Kind = kindInt64
		case iceberg.Float32Type:
			c.Kind = kindFloat32
		case iceberg.Float64Type:
			c.Kind = kindFloat64
		case iceberg.DecimalType:
			c.Kind = kindDecimal
			c.Precision = t.Precision()
			c.Scale = t.Scale()
		case iceberg.TimestampType:
			c.Kind = kindTimestamp
		case iceberg.DateType:
			c.Kind = kindDate
		case iceberg.BinaryType:
			c.Kind = kindBinary
		case iceberg.StringType:
			c.Kind = kindString
		default:
			return nil, fmt.Errorf("column %q: Iceberg type %s was not written by this export", f.Name, f.Type)
		}
		out = append(out, c)
	}
	return out, nil
}
