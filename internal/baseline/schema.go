package baseline

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/parquet-go/parquet-go"
)

// Column describes a single column parsed from a CREATE TABLE statement.
type Column struct {
	Name        string
	MySQLType   string // raw type token e.g. "int", "varchar", "datetime"
	Unsigned    bool   // true when the column carries the UNSIGNED attribute
	ParquetType parquet.Node
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

// ParseSchema reads a mydumper <db>.<table>-schema.sql file and returns the
// ordered list of columns with their Parquet type mappings.
func ParseSchema(path string) ([]Column, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open schema file %s: %w", path, err)
	}
	defer f.Close()

	var cols []Column
	scanner := bufio.NewScanner(f)
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
		return nil, fmt.Errorf("read schema file: %w", err)
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("no columns found in schema file %s", path)
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
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob",
		"bit":
		return parquet.Optional(parquet.Leaf(parquet.ByteArrayType))
	default:
		// Unknown type — treat as string to avoid data loss.
		return parquet.Optional(parquet.String())
	}
}
