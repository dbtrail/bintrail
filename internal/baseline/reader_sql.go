package baseline

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// ReadSQLFile reads a mydumper SQL (.sql) data file and calls fn for each row.
//
// Two INSERT layouts must be handled:
//
//	mysqldump-style, all tuples on one line:
//	    INSERT INTO `t` VALUES (a),(b),(c);
//
//	mydumper >= 1.0, one tuple per physical line with a leading comma:
//	    INSERT INTO `t` (...) VALUES(a)
//	    ,(b)
//	    ,(c);
//
// A naive line-oriented parser that only read the VALUES line would silently
// drop every continuation row of the second layout — the catastrophic data
// loss of issue #495. Instead this carries an "inside a not-yet-terminated
// INSERT" state across lines: continuation lines are parsed as further tuples
// until the ';' statement terminator is consumed.
//
// Splitting on physical lines is safe because mydumper escapes embedded
// newlines inside string values as the two-character sequence `\n`, so a line
// boundary always falls between tuples, never inside a quoted value.
//
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
	inStatement := false // inside an INSERT whose ';' terminator hasn't been seen
	for scanner.Scan() {
		lineNum++
		trimmed := strings.TrimSpace(scanner.Text())
		if trimmed == "" {
			continue
		}

		var fragment string
		if inStatement {
			// Continuation line of a multi-row INSERT: ",(...)" tuples, the
			// last ending in ';'.
			fragment = trimmed
		} else {
			upper := strings.ToUpper(trimmed)
			// mysqldump --replace and mydumper --replace emit REPLACE INTO
			// instead of INSERT INTO; both carry row data the same way. Skipping
			// REPLACE lines silently dropped every such row.
			if !strings.HasPrefix(upper, "INSERT") && !strings.HasPrefix(upper, "REPLACE") {
				continue
			}
			// Find VALUES keyword (after any column list).
			valIdx := strings.Index(upper, " VALUES")
			if valIdx < 0 {
				continue
			}
			fragment = strings.TrimSpace(trimmed[valIdx+7:])
		}

		terminated, err := parseSQLTuples(fragment, fn)
		if err != nil {
			return fmt.Errorf("%s line %d: %w", path, lineNum, err)
		}
		inStatement = !terminated
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	// A file that ends mid-statement (no ';') is truncated: fail loudly rather
	// than report a silently short row count.
	if inStatement {
		return fmt.Errorf("%s: unterminated INSERT statement (missing ';') — dump file may be truncated", path)
	}
	return nil
}

// parseSQLTuples parses a fragment of an INSERT's values portion:
// "(v,v,...),(v,v,...)" optionally ending with ";". A fragment is the part of
// a single physical line that carries tuples — for a multi-line INSERT the same
// statement is fed in across several calls. It returns terminated=true once it
// consumes the ';' statement terminator, so ReadSQLFile knows the (possibly
// multi-line) INSERT is complete.
func parseSQLTuples(s string, fn func(values []string, nulls []bool) error) (bool, error) {
	i := 0
	for i < len(s) {
		// Skip separators between tuples. NOT ';' — that is the statement
		// terminator and must be detected, not skipped.
		for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == ',') {
			i++
		}
		if i >= len(s) {
			break
		}
		if s[i] == ';' {
			return true, nil
		}
		if s[i] != '(' {
			// An unexpected token where a tuple or terminator was expected means
			// malformed or non-mydumper SQL. Fail loud: a lenient skip here would
			// discard the rest of the fragment and — because inStatement now
			// carries across lines — swallow the following statements as bogus
			// continuations, silently dropping their rows. That is the exact
			// silent-loss class #495 closes. (On valid mydumper output this is
			// unreachable: after a tuple only ',', whitespace, or ';' appears,
			// all consumed above.)
			return false, fmt.Errorf("unexpected token %q at offset %d (expected '(' or ';')", s[i], i)
		}
		i++ // consume '('

		values, nulls, end, err := parseTuple(s, i)
		if err != nil {
			return false, err
		}
		i = end
		if err := fn(values, nulls); err != nil {
			return false, err
		}
	}
	return false, nil
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
		// Double-quoted string (MySQL ANSI mode, JSON values, BIT columns)
		val, end, err := parseSQLDoubleString(s, pos+1)
		return val, false, end, err

	case s[pos] == '_':
		// MySQL charset introducer: _binary "…" / _utf8mb4 '…' / etc. mydumper
		// dumps BINARY/VARBINARY/BLOB columns this way (verified v1.0.3); the
		// bytes carry raw ',' and ')' that the default reader below would
		// mis-split into the wrong columns (or abort the file). Parse the
		// introduced quoted string so the stored value is the decoded bytes.
		if val, end, ok, err := parseIntroducedString(s, pos); ok {
			return val, false, end, err
		}
		return parseDefaultValue(s, pos)

	case hasPrefixFold(s[pos:], "CONVERT("):
		// mydumper dumps JSON columns as CONVERT("<json>" USING <charset>).
		// Extract the inner document so the baseline stores the JSON itself,
		// not the literal wrapper-expression text.
		if val, end, ok, err := parseConvertExpr(s, pos); ok {
			return val, false, end, err
		}
		return parseDefaultValue(s, pos)

	case s[pos] == '0' && pos+1 < len(s) && (s[pos+1] == 'x' || s[pos+1] == 'X'):
		// Hex literal: 0x...
		end := pos + 2
		for end < len(s) && isHexDigit(s[end]) {
			end++
		}
		return s[pos:end], false, end, nil

	default:
		return parseDefaultValue(s, pos)
	}
}

// parseDefaultValue reads an unquoted number, keyword, or expression up to a
// top-level ',' or ')'. Parenthesis depth is tracked so a function expression
// like POINT(1 2) is read whole.
func parseDefaultValue(s string, pos int) (string, bool, int, error) {
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

// parseIntroducedString handles a MySQL charset introducer followed by a quoted
// string literal, e.g. `_binary "a,b)"` or `_utf8mb4 'x'`. It returns the
// unescaped value and the position after the closing quote. ok=false (nothing
// consumed) when pos is not an introducer followed by a quote, so the caller can
// fall back to the default reader.
func parseIntroducedString(s string, pos int) (val string, end int, ok bool, err error) {
	i := pos + 1 // past '_'
	for i < len(s) && isIdentByte(s[i]) {
		i++
	}
	if i == pos+1 {
		return "", pos, false, nil // a lone '_' is not an introducer
	}
	for i < len(s) && (s[i] == ' ' || s[i] == '\t') {
		i++
	}
	if i >= len(s) {
		return "", pos, false, nil
	}
	switch s[i] {
	case '\'':
		v, e, perr := parseSQLString(s, i+1)
		return v, e, true, perr
	case '"':
		v, e, perr := parseSQLDoubleString(s, i+1)
		return v, e, true, perr
	default:
		return "", pos, false, nil
	}
}

// parseConvertExpr handles mydumper's JSON encoding CONVERT("<json>" USING
// <charset>), returning the unescaped inner string and the position after the
// wrapper's closing ')'. ok=false when the text after "CONVERT(" is not a quoted
// literal (caller falls back to the default reader).
func parseConvertExpr(s string, pos int) (val string, end int, ok bool, err error) {
	i := pos + len("CONVERT(")
	for i < len(s) && (s[i] == ' ' || s[i] == '\t') {
		i++
	}
	if i >= len(s) {
		return "", pos, false, nil
	}
	var v string
	var e int
	switch s[i] {
	case '"':
		v, e, err = parseSQLDoubleString(s, i+1)
	case '\'':
		v, e, err = parseSQLString(s, i+1)
	default:
		return "", pos, false, nil
	}
	if err != nil {
		return "", e, true, err
	}
	// The inner literal is fully consumed (quotes balanced), so the next ')'
	// closes CONVERT( — the "USING <charset>" tail contains no parentheses.
	for e < len(s) && s[e] != ')' {
		e++
	}
	if e >= len(s) {
		return "", e, true, fmt.Errorf("unterminated CONVERT(...) expression")
	}
	return v, e + 1, true, nil
}

// isIdentByte reports whether c can appear in a charset-introducer name.
func isIdentByte(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

// hasPrefixFold reports whether s begins with prefix, ASCII-case-insensitively.
func hasPrefixFold(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
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
			case 'Z':
				// MySQL escapes Ctrl-Z (0x1A) as \Z; without this it round-trips
				// to a literal 'Z', corrupting binary values (#495 follow-up).
				b.WriteByte(0x1a)
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
			case '0':
				// NUL — emitted inside _binary "…" for binary/BLOB columns.
				b.WriteByte(0)
			case 'Z':
				// Ctrl-Z (0x1A); see parseSQLString. Real mydumper binary output
				// contains \0 and \Z inside double-quoted _binary values.
				b.WriteByte(0x1a)
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
