package query

import (
	"fmt"
	"strings"
)

// ColumnEq is one --column-eq filter entry. See Options.ColumnEq and
// ParseColumnEq for parsing rules.
type ColumnEq struct {
	// Column is the JSON key within row_after / row_before to match on.
	// Must be an allowlisted identifier (see ParseColumnEq).
	Column string
	// Value is the expected string form of the column's value. Ignored when
	// IsNull is true.
	Value string
	// IsNull indicates the user asked to match JSON null via the literal
	// sentinel "NULL". Under this flag the generator emits a JSON_TYPE check
	// and binds no positional arg.
	IsNull bool
}

// ParseColumnEq parses one "column=value" entry.
//
//   - Splits on the FIRST '=' so values may themselves contain '='.
//   - Rejects empty or unsafe column names; the allowlist matches identifier
//     characters (letters, digits, underscore) because the column name is
//     interpolated into a JSON path literal (MySQL does not bind JSON paths).
//   - The literal (case-sensitive) value "NULL" sets IsNull=true — this
//     matches rows where the column is explicitly JSON null. The string
//     "NULL" is reserved as the sentinel; matching a column whose value is
//     the literal four-character string "NULL" is not currently supported.
func ParseColumnEq(entry string) (ColumnEq, error) {
	col, val, ok := strings.Cut(entry, "=")
	if !ok {
		return ColumnEq{}, fmt.Errorf("--column-eq entry %q is missing '='; expected column=value", entry)
	}
	col = strings.TrimSpace(col)
	if col == "" {
		return ColumnEq{}, fmt.Errorf("--column-eq entry %q has empty column name", entry)
	}
	if !IsSafeColumnName(col) {
		return ColumnEq{}, fmt.Errorf("--column-eq column name %q must match [A-Za-z0-9_]+", col)
	}
	eq := ColumnEq{Column: col, Value: val}
	if val == "NULL" {
		// Null-sentinel branch binds no positional arg; clear the value so a
		// future caller that forgets to check IsNull cannot silently bind the
		// literal string "NULL".
		eq.IsNull = true
		eq.Value = ""
	}
	return eq, nil
}

// ParseColumnEqs parses multiple entries; returns the first parse error.
func ParseColumnEqs(entries []string) ([]ColumnEq, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	out := make([]ColumnEq, 0, len(entries))
	for _, e := range entries {
		eq, err := ParseColumnEq(e)
		if err != nil {
			return nil, err
		}
		out = append(out, eq)
	}
	return out, nil
}

// IsSafeColumnName reports whether col matches the JSON-path-safe allowlist
// ([A-Za-z0-9_]+). Exposed for use as a defense-in-depth check at SQL builder
// boundaries — `Options.ColumnEq` crosses package and process boundaries (CLI,
// MCP), so callers other than ParseColumnEq cannot be assumed to have validated.
func IsSafeColumnName(col string) bool {
	if col == "" {
		return false
	}
	for _, r := range col {
		switch {
		case r == '_':
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		default:
			return false
		}
	}
	return true
}
