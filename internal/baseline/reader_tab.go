package baseline

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// ReadTabFile reads a mydumper TSV (.dat) file and calls fn for each row.
// Values are raw strings; \N represents NULL (returned as "").
// The isNull return from fn indicates whether each position was NULL.
//
// TSV escaping: \t → tab, \n → newline, \\ → backslash, \N → NULL.
func ReadTabFile(path string, numCols int, fn func(values []string, nulls []bool) error) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open tab file %s: %w", path, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 1<<20) // 1 MB per line max
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		if line == "" {
			continue
		}
		values, nulls, err := parseTabRow(line, numCols)
		if err != nil {
			return fmt.Errorf("%s line %d: %w", path, lineNum, err)
		}
		if err := fn(values, nulls); err != nil {
			return err
		}
	}
	return scanner.Err()
}

// parseTabRow splits a TSV line into values, resolving mydumper escape sequences.
func parseTabRow(line string, numCols int) ([]string, []bool, error) {
	parts := strings.Split(line, "\t")
	values := make([]string, len(parts))
	nulls := make([]bool, len(parts))

	for i, part := range parts {
		if part == `\N` {
			nulls[i] = true
			values[i] = ""
			continue
		}
		values[i] = unescapeTab(part)
	}
	return values, nulls, nil
}

// unescapeTab resolves mydumper TSV escape sequences.
func unescapeTab(s string) string {
	if !strings.ContainsRune(s, '\\') {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	i := 0
	for i < len(s) {
		if s[i] == '\\' && i+1 < len(s) {
			switch s[i+1] {
			case 't':
				b.WriteByte('\t')
			case 'n':
				b.WriteByte('\n')
			case 'r':
				b.WriteByte('\r')
			case '\\':
				b.WriteByte('\\')
			case 'N':
				// \N mid-string — treat as literal \N (shouldn't happen in valid TSV)
				b.WriteByte('\\')
				b.WriteByte('N')
			default:
				b.WriteByte('\\')
				b.WriteByte(s[i+1])
			}
			i += 2
		} else {
			b.WriteByte(s[i])
			i++
		}
	}
	return b.String()
}
