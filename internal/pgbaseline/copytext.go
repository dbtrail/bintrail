package pgbaseline

import (
	"bytes"
	"fmt"
)

// This file hand-rolls the parser for PostgreSQL's COPY ... TO STDOUT
// (FORMAT text) output. encoding/csv is deliberately NOT used: COPY text is
// not CSV — fields are tab-separated with backslash escapes, and NULL is the
// two-byte sequence `\N`, distinguishable from an empty string.
//
// COPY TO (text) output grammar (PostgreSQL docs, "COPY", Text Format):
//   - one row per newline-terminated line;
//   - fields separated by literal tab bytes (a tab INSIDE a value is escaped
//     as `\t`, so a raw 0x09 is always a separator);
//   - COPY TO emits exactly these backslash escapes: \b \f \n \r \t \v \\
//     (it never emits octal/hex escapes — those are accepted on COPY FROM
//     input only);
//   - NULL is the field consisting of exactly `\N` (backslash + capital N).
//     A literal string "\N" is emitted as `\\N`, so the two never collide.
//
// Multibyte UTF-8 passes through untouched: every escape byte is ASCII, so a
// bytewise scan can never split a multibyte sequence.

// parseCopyTextLine splits one COPY text line (without its trailing newline)
// into unescaped field values plus a NULL mask. wantCols guards against a
// drifting column count (a schema change mid-COPY is impossible inside one
// statement, so a mismatch means the parser lost sync — fail loud).
func parseCopyTextLine(line []byte, wantCols int) (values []string, nulls []bool, err error) {
	// Raw tabs are always field separators (see grammar note above).
	fields := bytes.Split(line, []byte{'\t'})
	if len(fields) != wantCols {
		return nil, nil, fmt.Errorf("row has %d fields, want %d (line %q)", len(fields), wantCols, truncForErr(line))
	}
	values = make([]string, len(fields))
	nulls = make([]bool, len(fields))
	for i, f := range fields {
		// NULL is exactly `\N` BEFORE unescaping; a literal "\N" value arrives
		// as `\\N` (three bytes) and falls through to the unescaper.
		if len(f) == 2 && f[0] == '\\' && f[1] == 'N' {
			nulls[i] = true
			continue
		}
		v, err := unescapeCopyText(f)
		if err != nil {
			return nil, nil, fmt.Errorf("field %d: %w", i, err)
		}
		values[i] = v
	}
	return values, nulls, nil
}

// unescapeCopyText resolves the backslash escapes COPY TO (text) emits.
// An escape COPY TO never produces (or a trailing lone backslash) is treated
// as corruption and fails loud — this feeds a data-recovery baseline, so
// guessing at malformed input would publish silently wrong data.
func unescapeCopyText(f []byte) (string, error) {
	// Fast path: no backslash at all (the overwhelmingly common case).
	i := bytes.IndexByte(f, '\\')
	if i < 0 {
		return string(f), nil
	}
	out := make([]byte, 0, len(f))
	out = append(out, f[:i]...)
	for ; i < len(f); i++ {
		c := f[i]
		if c != '\\' {
			out = append(out, c)
			continue
		}
		i++
		if i >= len(f) {
			return "", fmt.Errorf("truncated escape: field ends with a lone backslash (%q)", truncForErr(f))
		}
		switch f[i] {
		case 'b':
			out = append(out, '\b')
		case 'f':
			out = append(out, '\f')
		case 'n':
			out = append(out, '\n')
		case 'r':
			out = append(out, '\r')
		case 't':
			out = append(out, '\t')
		case 'v':
			out = append(out, '\v')
		case '\\':
			out = append(out, '\\')
		default:
			return "", fmt.Errorf("unexpected escape %q in COPY text output (%q) — COPY TO emits only \\b \\f \\n \\r \\t \\v \\\\", string(f[i]), truncForErr(f))
		}
	}
	return string(out), nil
}

// copyRowFunc consumes one parsed COPY row.
type copyRowFunc func(values []string, nulls []bool) error

// copyTextSink is the io.Writer handed to pgconn.CopyTo: it re-assembles the
// CopyData chunk stream into lines, parses each line, and feeds rowFn
// synchronously. Returning an error from Write makes pgconn abort the COPY,
// so a downstream Parquet write failure stops the transfer immediately.
type copyTextSink struct {
	wantCols int
	rowFn    copyRowFunc
	buf      []byte // partial line carried across Write calls
	rows     int64
}

func newCopyTextSink(wantCols int, rowFn copyRowFunc) *copyTextSink {
	return &copyTextSink{wantCols: wantCols, rowFn: rowFn}
}

func (s *copyTextSink) Write(p []byte) (int, error) {
	s.buf = append(s.buf, p...)
	for {
		nl := bytes.IndexByte(s.buf, '\n')
		if nl < 0 {
			return len(p), nil
		}
		line := s.buf[:nl]
		if err := s.consumeLine(line); err != nil {
			return 0, err
		}
		s.buf = s.buf[nl+1:]
	}
}

// Flush verifies the stream ended cleanly. COPY TO terminates EVERY row with
// a newline, so leftover buffered bytes after a successful CopyTo are a
// protocol anomaly — treating the tail as a row would quietly accept a
// truncated or desynced stream, so it is an error instead (review blocker;
// the server-count check in processTable is the second line of defense).
func (s *copyTextSink) Flush() error {
	if len(s.buf) == 0 {
		return nil
	}
	tail := s.buf
	s.buf = nil
	return fmt.Errorf("COPY stream ended with an unterminated %d-byte line (%q) — protocol anomaly, refusing to accept a possibly truncated row", len(tail), truncForErr(tail))
}

func (s *copyTextSink) consumeLine(line []byte) error {
	// Defensive: the `\.` end-of-data marker belongs to file/embedded COPY
	// input, not the v3 wire protocol's COPY TO output — but skipping it is
	// harmless and guards against a future transport that includes it.
	if len(line) == 2 && line[0] == '\\' && line[1] == '.' {
		return nil
	}
	values, nulls, err := parseCopyTextLine(line, s.wantCols)
	if err != nil {
		return err
	}
	if err := s.rowFn(values, nulls); err != nil {
		return err
	}
	s.rows++
	return nil
}

// truncForErr caps raw COPY content quoted into error messages: the message
// must locate the problem, not dump row contents (which may be sensitive)
// into logs.
func truncForErr(b []byte) []byte {
	const max = 80
	if len(b) <= max {
		return b
	}
	out := make([]byte, max, max+3)
	copy(out, b[:max])
	return append(out, "..."...)
}
