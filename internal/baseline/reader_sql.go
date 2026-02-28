package baseline

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// ReadSQLFile reads a mydumper SQL (.sql) data file and calls fn for each row.
// The file contains INSERT INTO `table` VALUES(...),(...),...; statements.
// Values are returned as raw strings; NULL is returned as "" with nulls[i]=true.
func ReadSQLFile(path string, fn func(values []string, nulls []bool) error) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open sql file %s: %w", path, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 8<<20), 8<<20) // 8 MB per line
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(strings.ToUpper(trimmed), "INSERT") {
			continue
		}
		// Find VALUES keyword
		valIdx := strings.Index(strings.ToUpper(trimmed), " VALUES")
		if valIdx < 0 {
			continue
		}
		valuesPart := strings.TrimSpace(trimmed[valIdx+7:])
		// Parse all tuples from this line
		if err := parseSQLTuples(valuesPart, fn); err != nil {
			return fmt.Errorf("%s line %d: %w", path, lineNum, err)
		}
	}
	return scanner.Err()
}

// parseSQLTuples parses the values portion of an INSERT: "(v,v,...),(v,v,...);".
func parseSQLTuples(s string, fn func(values []string, nulls []bool) error) error {
	i := 0
	for i < len(s) {
		// Skip whitespace and commas between tuples
		for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == ',' || s[i] == ';') {
			i++
		}
		if i >= len(s) {
			break
		}
		if s[i] != '(' {
			break
		}
		i++ // consume '('

		values, nulls, end, err := parseTuple(s, i)
		if err != nil {
			return err
		}
		i = end
		if err := fn(values, nulls); err != nil {
			return err
		}
	}
	return nil
}

// parseTuple parses a comma-separated list of SQL values starting at pos (after
// the opening '('). Returns values, nulls, and the position after the closing ')'.
func parseTuple(s string, pos int) ([]string, []bool, int, error) {
	var values []string
	var nulls []bool

	i := pos
	for {
		// Skip leading space
		for i < len(s) && s[i] == ' ' {
			i++
		}
		if i >= len(s) {
			return nil, nil, i, fmt.Errorf("unterminated tuple")
		}
		if s[i] == ')' {
			// End of tuple
			return values, nulls, i + 1, nil
		}
		if len(values) > 0 {
			// Expect comma separator
			if s[i] != ',' {
				return nil, nil, i, fmt.Errorf("expected ',' at pos %d, got %q", i, s[i])
			}
			i++
			// Skip space after comma
			for i < len(s) && s[i] == ' ' {
				i++
			}
		}

		val, isNull, end, err := parseSQLValue(s, i)
		if err != nil {
			return nil, nil, end, err
		}
		values = append(values, val)
		nulls = append(nulls, isNull)
		i = end
	}
}

// parseSQLValue parses a single SQL value starting at pos.
// Returns (value string, isNull, next pos, error).
func parseSQLValue(s string, pos int) (string, bool, int, error) {
	if pos >= len(s) {
		return "", false, pos, fmt.Errorf("unexpected end of input")
	}

	switch {
	case strings.HasPrefix(s[pos:], "NULL"):
		return "", true, pos + 4, nil

	case s[pos] == '\'':
		// Single-quoted string with \' and '' escaping
		val, end, err := parseSQLString(s, pos+1)
		return val, false, end, err

	case s[pos] == '"':
		// Double-quoted string (MySQL ANSI mode or JSON values)
		val, end, err := parseSQLDoubleString(s, pos+1)
		return val, false, end, err

	case s[pos] == '0' && pos+1 < len(s) && (s[pos+1] == 'x' || s[pos+1] == 'X'):
		// Hex literal: 0x...
		end := pos + 2
		for end < len(s) && isHexDigit(s[end]) {
			end++
		}
		return s[pos:end], false, end, nil

	default:
		// Number, unquoted keyword, or expression — read until ',' or ')'
		end := pos
		depth := 0
		for end < len(s) {
			c := s[end]
			if c == '(' {
				depth++
			} else if c == ')' {
				if depth == 0 {
					break
				}
				depth--
			} else if c == ',' && depth == 0 {
				break
			}
			end++
		}
		return strings.TrimSpace(s[pos:end]), false, end, nil
	}
}

// parseSQLString parses a single-quoted SQL string starting after the opening '.
// Returns (unescaped value, position after closing quote, error).
func parseSQLString(s string, pos int) (string, int, error) {
	var b strings.Builder
	i := pos
	for i < len(s) {
		if s[i] == '\\' && i+1 < len(s) {
			switch s[i+1] {
			case 'n':
				b.WriteByte('\n')
			case 't':
				b.WriteByte('\t')
			case 'r':
				b.WriteByte('\r')
			case '\\':
				b.WriteByte('\\')
			case '\'':
				b.WriteByte('\'')
			case '"':
				b.WriteByte('"')
			case '0':
				b.WriteByte(0)
			default:
				b.WriteByte(s[i+1])
			}
			i += 2
		} else if s[i] == '\'' {
			if i+1 < len(s) && s[i+1] == '\'' {
				// '' → single quote
				b.WriteByte('\'')
				i += 2
			} else {
				// End of string
				return b.String(), i + 1, nil
			}
		} else {
			b.WriteByte(s[i])
			i++
		}
	}
	return "", i, fmt.Errorf("unterminated string")
}

// parseSQLDoubleString parses a double-quoted SQL string starting after the opening ".
func parseSQLDoubleString(s string, pos int) (string, int, error) {
	var b strings.Builder
	i := pos
	for i < len(s) {
		if s[i] == '\\' && i+1 < len(s) {
			switch s[i+1] {
			case 'n':
				b.WriteByte('\n')
			case 't':
				b.WriteByte('\t')
			case 'r':
				b.WriteByte('\r')
			case '\\':
				b.WriteByte('\\')
			case '"':
				b.WriteByte('"')
			default:
				b.WriteByte(s[i+1])
			}
			i += 2
		} else if s[i] == '"' {
			if i+1 < len(s) && s[i+1] == '"' {
				b.WriteByte('"')
				i += 2
			} else {
				return b.String(), i + 1, nil
			}
		} else {
			b.WriteByte(s[i])
			i++
		}
	}
	return "", i, fmt.Errorf("unterminated double-quoted string")
}

func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}
