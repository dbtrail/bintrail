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
	Name       string
	MySQLType  string // raw type token e.g. "int", "varchar", "datetime"
	ParquetType parquet.Node
}

// colRe matches a column definition line from mydumper's schema SQL output.
// Groups: 1=name, 2=type token.
// Handles backtick-quoted names and covers common MySQL type forms.
var colRe = regexp.MustCompile("^\\s+`([^`]+)`\\s+(\\w+)")

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
		cols = append(cols, Column{
			Name:       name,
			MySQLType:  typeToken,
			ParquetType: mysqlToParquetNode(typeToken),
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

// mysqlToParquetNode maps a MySQL type token to the appropriate parquet-go node.
// All fields are Optional so NULL values can be represented.
func mysqlToParquetNode(typeToken string) parquet.Node {
	switch typeToken {
	case "tinyint", "smallint", "mediumint", "int", "integer":
		return parquet.Optional(parquet.Int(32))
	case "bigint":
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
