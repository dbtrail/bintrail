package baseline

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/parquet-go/parquet-go"
)

// binaryTypeTokens is the single authority for "this MySQL type carries raw
// bytes, not text". Two independent decisions read it and MUST agree, which is
// why it is one map rather than two switch case-lists: the Parquet column node
// (a binary leaf, never the UTF-8 STRING default, #503 item 2) and the value
// conversion (decodeBinaryLiteral, which turns a --hex-blob 0x… literal back
// into bytes). A type present in one list and absent from the other would write
// non-UTF-8 bytes into a string column, or store the ASCII text "0x…" as the
// value.
//
// GEOMETRY and its subtypes carry WKB bytes. MySQL 8.0 canonicalizes
// GEOMETRYCOLLECTION to GEOMCOLLECTION; both spellings are listed because a
// schema file can come from either server generation.
var binaryTypeTokens = map[string]bool{
	"binary": true, "varbinary": true,
	"tinyblob": true, "blob": true, "mediumblob": true, "longblob": true,
	"bit":      true,
	"geometry": true, "point": true, "linestring": true, "polygon": true,
	"multipoint": true, "multilinestring": true, "multipolygon": true,
	"geometrycollection": true, "geomcollection": true,
}

// IsBinaryType reports whether a MySQL type token names a binary-family column
// — one whose Parquet representation is a byte array and whose text rendering
// is the --hex-blob 0x<hex> literal form. Exported for producers that build a
// baseline row's text values themselves rather than reading them out of a
// mydumper dump (full-table reconstruct's Parquet output, #1169).
func IsBinaryType(typeToken string) bool {
	return binaryTypeTokens[strings.ToLower(strings.TrimSpace(typeToken))]
}

// Column describes a single column parsed from a CREATE TABLE statement.
type Column struct {
	Name        string
	MySQLType   string // raw type token e.g. "int", "varchar", "datetime"
	Unsigned    bool   // true when the column carries the UNSIGNED attribute
	ParquetType parquet.Node
	// RawText marks a column whose values are stored VERBATIM as optional
	// Parquet strings, bypassing the MySQL type mapping entirely (both the
	// schema node and convertValue). Used by the PostgreSQL baseline producer
	// (#593): PG values arrive as pgoutput-style text and must round-trip
	// byte-identically so the PK join with the delta path stays an identity
	// string match — no type conversion, ever. MySQL callers never set it,
	// so the mydumper paths are untouched.
	RawText bool
}

// colRe matches a column definition line from mydumper's schema SQL output.
// Groups: 1=name, 2=type token, 3="unsigned" iff present.
// The unsigned attribute is matched only in the tail that immediately follows
// the type token (plus an optional display width like int(10)), so a column
// literally named `is_unsigned` or a COMMENT containing "unsigned" never trips
// it — the name lives in group 1 inside backticks, separate from group 3.
// The unsigned group is case-insensitive (`(?i:...)` inside a capture group, so
// group 3 is preserved): mydumper emits lowercase by contract, but an uppercase
// UNSIGNED from a hand-rolled schema must not silently fall through to signed.
var colRe = regexp.MustCompile("^\\s+`([^`]+)`\\s+(\\w+)(?:\\s*\\([^)]*\\))?\\s*((?i:unsigned))?")

// generatedRe matches a STORED/VIRTUAL/PERSISTENT generated column's defining
// clause: MySQL's canonical "GENERATED ALWAYS AS (`price` * `qty`) STORED",
// and the shorter form MariaDB's SHOW CREATE TABLE may emit without the
// "GENERATED ALWAYS" prefix, e.g. "AS (`price` * `qty`) PERSISTENT"
// (PERSISTENT is MariaDB's legacy alias for STORED). mysqldump and mydumper
// both EXCLUDE generated columns from the INSERT column-list and VALUES
// tuples they emit (see internal/consistency/checksum.go's tableColumns,
// which drops them from the live-source fingerprint for the same reason) —
// so a schema that still lists them shifts every subsequent column's
// positional mapping in WriteRow (issue #767).
//
// Requiring the trailing VIRTUAL/STORED/PERSISTENT keyword (not just "AS (")
// is a deliberate, accepted trade-off: it is possible in principle for a
// COMMENT string to contain " as (...) stored" and false-trip this, but
// unlike the #506 UNSIGNED false-positive (which silently mis-typed a real
// column), the consequence here is the arity check in baseline.go failing
// loud on a column-count mismatch — never silent corruption.
var generatedRe = regexp.MustCompile(`(?i)\bAS\s*\(.*\)\s*(?:VIRTUAL|STORED|PERSISTENT)\b`)

// ParseSchema reads a mydumper <db>.<table>-schema.sql file and returns the
// ordered list of columns with their Parquet type mappings.
func ParseSchema(path string) ([]Column, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open schema file %s: %w", path, err)
	}
	defer f.Close()

	cols, err := parseSchemaFrom(f)
	if err != nil {
		return nil, fmt.Errorf("%w (schema file %s)", err, path)
	}
	return cols, nil
}

// ParseSchemaText parses the same mydumper schema SQL that ParseSchema reads
// from disk, but from an in-memory string.
//
// It exists for the consumers that already hold those exact bytes rather than a
// path: a baseline Parquet's MetaKeyCreateTableSQL metadata is the verbatim
// <db>.<table>-schema.sql that produced it (embedded by Run), so a producer
// deriving a NEW snapshot from an existing one — full-table reconstruct's
// Parquet output (#1169) — can recover the column list and its MySQL types
// without a dump directory on disk.
func ParseSchemaText(createSQL string) ([]Column, error) {
	return parseSchemaFrom(strings.NewReader(createSQL))
}

// parseSchemaFrom is the shared scanner both entry points above drive.
func parseSchemaFrom(r io.Reader) ([]Column, error) {
	var cols []Column
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		// Stop at PRIMARY KEY / KEY / UNIQUE or closing paren lines.
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "PRIMARY") ||
			strings.HasPrefix(trimmed, "UNIQUE") ||
			strings.HasPrefix(trimmed, "KEY") ||
			strings.HasPrefix(trimmed, "CONSTRAINT") ||
			trimmed == ");" || trimmed == ")" {
			break
		}
		m := colRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		if generatedRe.MatchString(line) {
			// STORED/VIRTUAL generated column — mydumper never dumps its value,
			// so it must not occupy a slot in the positional column list either.
			continue
		}
		name := m[1]
		typeToken := strings.ToLower(m[2])
		unsigned := strings.EqualFold(m[3], "unsigned")
		cols = append(cols, Column{
			Name:        name,
			MySQLType:   typeToken,
			Unsigned:    unsigned,
			ParquetType: mysqlToParquetNode(typeToken, unsigned),
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read schema: %w", err)
	}
	if len(cols) == 0 {
		return nil, errors.New("no columns found in schema SQL")
	}
	return cols, nil
}

// BuildParquetSchema converts a slice of Columns into a parquet.Schema.
func BuildParquetSchema(cols []Column) *parquet.Schema {
	group := make(parquet.Group, len(cols))
	for _, c := range cols {
		group[c.Name] = c.ParquetType
	}
	return parquet.NewSchema("row", group)
}

// MysqlToParquetNode maps a MySQL type token to the appropriate parquet-go node,
// treating integer types as signed. Callers with an UNSIGNED column should build
// the column via ParseSchema (which threads the attribute through) or via
// MysqlToParquetNode2 — this signed-only entry point is preserved for external
// callers that pass a bare type token (e.g. internal/archive, internal/byos).
func MysqlToParquetNode(typeToken string) parquet.Node {
	return mysqlToParquetNode(typeToken, false)
}

// MysqlToParquetNode2 is the unsigned-aware entry point for callers that hand-
// build a Column from a bare type token (internal/archive.BinlogEventColumns,
// internal/byos) rather than going through ParseSchema. It must be used for any
// column backing an UNSIGNED MySQL type (e.g. connection_id is INT UNSIGNED):
// the signed-only MysqlToParquetNode would emit an Int(32) column, and a value
// past int32 (a CONNECTION_ID() above 2147483647) would then fail conversion
// against that signed column. Passing unsigned=true widens it the same way
// ParseSchema does (INT UNSIGNED → Int64, BIGINT UNSIGNED → Uint64).
func MysqlToParquetNode2(typeToken string, unsigned bool) parquet.Node {
	return mysqlToParquetNode(typeToken, unsigned)
}

// mysqlToParquetNode maps a MySQL type token (plus its UNSIGNED attribute) to the
// appropriate parquet-go node. All fields are Optional so NULL values can be
// represented. UNSIGNED integers are widened so the full unsigned range
// round-trips without overflow into the signed-NULL fallback (issue #506):
//   - INT/INTEGER UNSIGNED (max 4294967295) → INT64 (holds it as a positive value)
//   - BIGINT UNSIGNED (max 18446744073709551615) → UINT64 (logical unsigned)
//
// TINYINT/SMALLINT/MEDIUMINT UNSIGNED already fit in int32, so they keep Int(32).
func mysqlToParquetNode(typeToken string, unsigned bool) parquet.Node {
	// Binary-family types first, from the shared authority above: storing WKB or
	// BLOB bytes in the STRING default would place non-UTF-8 bytes in a UTF-8
	// column (#503 item 2). The exact mydumper spatial encoding is unverified
	// end-to-end here; the binary type mapping is the safe floor.
	if IsBinaryType(typeToken) {
		return parquet.Optional(parquet.Leaf(parquet.ByteArrayType))
	}
	switch typeToken {
	case "int", "integer":
		if unsigned {
			return parquet.Optional(parquet.Int(64))
		}
		return parquet.Optional(parquet.Int(32))
	case "tinyint", "smallint", "mediumint":
		return parquet.Optional(parquet.Int(32))
	case "bigint":
		if unsigned {
			return parquet.Optional(parquet.Uint(64))
		}
		return parquet.Optional(parquet.Int(64))
	case "float":
		return parquet.Optional(parquet.Leaf(parquet.FloatType))
	case "double", "real":
		return parquet.Optional(parquet.Leaf(parquet.DoubleType))
	case "decimal", "numeric":
		// Preserve as string to avoid precision loss.
		return parquet.Optional(parquet.String())
	case "datetime", "timestamp":
		// Microseconds since Unix epoch (INT64 with timestamp logical type).
		return parquet.Optional(parquet.Timestamp(parquet.Microsecond))
	case "date":
		// Days since Unix epoch (INT32 with date logical type).
		return parquet.Optional(parquet.Date())
	case "time":
		return parquet.Optional(parquet.String())
	case "year":
		return parquet.Optional(parquet.Int(32))
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext",
		"enum", "set", "json":
		return parquet.Optional(parquet.String())
	default:
		// Unknown type — treat as string to avoid data loss.
		return parquet.Optional(parquet.String())
	}
}
