package baseline

import (
	"bufio"
	"errors"
	"fmt"
	"io"
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

	// A growable reader (not a fixed-buffer bufio.Scanner) so a single physical
	// line larger than any preset cap — a LONGBLOB/JSON tuple, which mydumper
	// emits as one tuple per line — parses instead of aborting with
	// bufio.ErrTooLong. ReadString grows its buffer to the line length on
	// demand; there is no artificial per-line limit (#801).
	reader := bufio.NewReader(f)
	lineNum := 0
	inStatement := false // inside an INSERT whose ';' terminator hasn't been seen
	for {
		line, readErr := reader.ReadString('\n')
		if len(line) > 0 {
			lineNum++
			if err := readSQLLine(path, lineNum, line, &inStatement, fn); err != nil {
				return err
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return fmt.Errorf("%s: reading line %d: %w", path, lineNum+1, readErr)
		}
	}
	// A file that ends mid-statement (no ';') is truncated: fail loudly rather
	// than report a silently short row count.
	if inStatement {
		return fmt.Errorf("%s: unterminated INSERT statement (missing ';') — dump file may be truncated", path)
	}
	return nil
}

// readSQLLine processes one physical line of a mydumper SQL data file, feeding
// its tuples to fn and advancing the cross-line *inStatement flag. Blank and
// non-INSERT/REPLACE lines are no-ops. Extracted from ReadSQLFile's read loop so
// the loop can use a growable bufio.Reader (see #801) without the two skip
// branches tangling with EOF handling.
func readSQLLine(path string, lineNum int, line string, inStatement *bool, fn func(values []string, nulls []bool) error) error {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return nil
	}

	var fragment string
	if *inStatement {
		// Continuation line of a multi-row INSERT: ",(...)" tuples, the
		// last ending in ';'.
		fragment = trimmed
	} else {
		upper := strings.ToUpper(trimmed)
		// mysqldump --replace and mydumper --replace emit REPLACE INTO
		// instead of INSERT INTO; both carry row data the same way. Skipping
		// REPLACE lines silently dropped every such row.
		if !strings.HasPrefix(upper, "INSERT") && !strings.HasPrefix(upper, "REPLACE") {
			return nil
		}
		// Find VALUES keyword (after any column list).
		valIdx := findValuesKeyword(trimmed)
		if valIdx < 0 {
			// The line opens an INSERT/REPLACE but carries no VALUES clause.
			// In a complete mydumper/mysqldump data file every INSERT/REPLACE
			// statement has VALUES on this same line — so this is a truncated
			// statement (a partial trailing line, #468 shape 1) or an
			// unsupported layout (VALUES wrapped to a continuation line).
			// Silently skipping past it drops every row the statement carried
			// with a clean exit and a short count — the #461 silent data-loss
			// class at row granularity. Fail loud, matching the
			// unterminated-INSERT guard below.
			return fmt.Errorf("%s line %d: INSERT/REPLACE statement without a VALUES clause "+
				"— dump file may be truncated or in an unsupported layout: %q",
				path, lineNum, truncateForError(trimmed))
		}
		fragment = strings.TrimSpace(trimmed[valIdx+len("VALUES"):])
	}

	terminated, err := parseSQLTuples(fragment, fn)
	if err != nil {
		return fmt.Errorf("%s line %d: %w", path, lineNum, err)
	}
	*inStatement = !terminated
	return nil
}

// findValuesKeyword returns the byte offset of the statement-level VALUES
// keyword in an INSERT/REPLACE line, or -1 when there is none.
//
// A naive substring search for " VALUES" (the pre-#502 code) returns the FIRST
// occurrence, which can land INSIDE a backtick-quoted identifier with an
// embedded space (`Allowed Values`, `sensor values`): slicing there starts the
// tuple parser mid-identifier, which silently drops the first row of a
// multi-line INSERT or rejects a single-line file as "truncated". So scan at
// statement level: backtick-quoted identifiers are skipped whole (a doubled
// backtick is MySQL's escape for a literal one inside), and VALUES matches
// only as a standalone word. Everything before the real keyword is
// identifiers, keywords, and the (col, …) list — dumps quote every identifier,
// and VALUES is a reserved word, so an unquoted statement-level match is
// always the keyword. An unterminated backtick (a line truncated
// mid-identifier) runs to end-of-line and reports "no VALUES", which routes
// into the loud truncated-dump error at the call site.
func findValuesKeyword(s string) int {
	for i := 0; i < len(s); i++ {
		if s[i] == '`' {
			for i++; i < len(s); i++ {
				if s[i] == '`' {
					if i+1 < len(s) && s[i+1] == '`' {
						i++ // `` — escaped backtick, still inside the identifier
						continue
					}
					break // closing backtick; the outer i++ steps past it
				}
			}
			continue
		}
		if !hasPrefixFold(s[i:], "VALUES") {
			continue
		}
		// Word boundaries on both sides, so a suffix ("NVALUES") or prefix
		// ("VALUESX") of a longer unquoted token never matches.
		if i > 0 && isWordByte(s[i-1]) {
			continue
		}
		if i+len("VALUES") < len(s) && isWordByte(s[i+len("VALUES")]) {
			continue
		}
		return i
	}
	return -1
}

// isWordByte reports whether c can be part of an unquoted MySQL identifier
// (the word-boundary test for findValuesKeyword; '$' is legal in identifiers).
func isWordByte(c byte) bool {
	return isIdentByte(c) || c == '$'
}

// truncateForError caps a line excerpt used in an error message so a long
// (but truncated) INSERT doesn't flood the log with the whole statement.
func truncateForError(s string) string {
	const max = 80
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
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
		// Hex literal: 0x... Returned as the literal token; convertValue does the
		// type-aware decode to bytes for binary-family columns (#503 item 1).
		if end, ok := scanHexLiteral(s, pos); ok {
			return s[pos:end], false, end, nil
		}
		return parseDefaultValue(s, pos)

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
		// _binary 0x<hex> — an introducer before a --hex-blob literal. Return the
		// 0x… token undecoded so convertValue does the type-aware hex decode
		// (#503 item 1). Without this the whole "_binary 0x…" string was captured
		// as the column value.
		if end, ok := scanHexLiteral(s, i); ok {
			return s[i:end], end, true, nil
		}
		return "", pos, false, nil
	}
}

// scanHexLiteral reports whether s[pos:] begins with a 0x<hex-digits> literal and
// returns the offset just past it. A bare "0x" with no following hex digit is not
// a literal (ok=false). mydumper/mysqldump --hex-blob renders binary columns as
// exactly this form.
func scanHexLiteral(s string, pos int) (int, bool) {
	if pos+2 > len(s) || s[pos] != '0' || (s[pos+1] != 'x' && s[pos+1] != 'X') {
		return pos, false
	}
	end := pos + 2
	for end < len(s) && isHexDigit(s[end]) {
		end++
	}
	if end == pos+2 {
		return pos, false
	}
	return end, true
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
