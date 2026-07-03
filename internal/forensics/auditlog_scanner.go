package forensics

import (
	"bufio"
	"io"
	"log/slog"
)

// maxAuditLineBytes bounds a single audit record. A line longer than this is
// skipped (and counted) rather than aborting the scan: SERVER_AUDIT_QUERY_LOG_LIMIT
// can be raised so a single query record runs well past a megabyte, and one
// oversized record must not truncate the rest of the file — which is exactly
// what bufio.Scanner does when it returns ErrTooLong. The value matches the old
// bufio.Scanner max-token size, so normal input (every line under 1 MB) is
// parsed byte-for-byte as before.
const maxAuditLineBytes = 1024 * 1024

// auditLineScanner is a drop-in replacement for bufio.Scanner used by the
// audit-log parsers. Unlike bufio.Scanner — which aborts the whole file when a
// single line exceeds its buffer — an over-long line here is drained, skipped,
// and counted (see Skipped), and scanning continues with the next record. An
// oversized line is discarded in fixed-size fragments, never fully buffered, so
// memory stays bounded regardless of how large a single record is.
//
// The method set mirrors the bufio.Scanner methods the parsers use (Scan / Text
// / Err), plus Skipped, so each parser only swaps its constructor.
type auditLineScanner struct {
	r       *bufio.Reader
	line    string
	skipped int
	err     error
	done    bool
}

func newAuditLineScanner(r io.Reader) *auditLineScanner {
	return &auditLineScanner{r: bufio.NewReaderSize(r, 256*1024)}
}

// Scan advances to the next line within maxAuditLineBytes, returning false at
// end of input or on a read error (retrievable via Err). Over-long lines are
// drained, counted, and skipped — never returned to the caller. Like
// bufio.Scanner, the returned line has its trailing newline stripped, and a
// blank line is a valid (empty) result rather than an end condition.
func (s *auditLineScanner) Scan() bool {
	if s.done {
		return false
	}
	for {
		line, tooLong, err := s.readLine()
		if tooLong {
			s.skipped++
			if err != nil { // the oversized line ran to EOF (or a read error)
				s.finish(err)
				return false
			}
			continue // skip it; the next read starts at the following record
		}
		if len(line) > 0 {
			s.line = line
			if err != nil { // a final line with no trailing newline arrives with io.EOF
				s.finish(err)
			}
			return true
		}
		// No bytes and not over-long: either a blank line (err == nil) or the
		// end of input / a read error.
		if err != nil {
			s.finish(err)
			return false
		}
		s.line = ""
		return true
	}
}

// readLine reads one newline-terminated line, stripping the trailing newline.
// A line exceeding maxAuditLineBytes is drained (its bytes discarded in
// fragments) and reported as tooLong with an empty line. err is io.EOF at end
// of input, which may accompany a final line that has no trailing newline.
func (s *auditLineScanner) readLine() (line string, tooLong bool, err error) {
	var buf []byte
	for {
		frag, e := s.r.ReadSlice('\n')
		// contentLen is the line length excluding the single '\n' delimiter,
		// which ReadSlice includes only on the terminating fragment (e == nil).
		// Comparing content (not content+delimiter) means a line of exactly
		// maxAuditLineBytes is kept. That is one byte more permissive than the
		// old bufio.Scanner, which had to fit content+'\n' in its 1 MB buffer
		// and so aborted at maxAuditLineBytes content — a safe divergence: every
		// line the old scanner kept, this one keeps too; none is dropped.
		contentLen := len(buf) + len(frag)
		if e == nil && contentLen > 0 {
			contentLen--
		}
		if contentLen > maxAuditLineBytes {
			// Over the bound: stop accumulating and drain the remainder of this
			// line so the next Scan starts cleanly at the following record.
			return "", true, s.drainToNewline(e)
		}
		// frag aliases the reader's buffer and is only valid until the next
		// read, so copy it out.
		buf = append(buf, frag...)
		if e == bufio.ErrBufferFull {
			continue // partial line — more fragments follow
		}
		// e is nil (line complete) or io.EOF/other (last line, no newline).
		return trimTrailingNewline(buf), false, e
	}
}

// drainToNewline discards the rest of an over-long line without buffering it.
// e is the error from the ReadSlice that pushed the line over the bound; when
// it is ErrBufferFull the line continues and is drained fragment by fragment.
// Returns nil once the terminating newline is consumed, or io.EOF/other at end.
func (s *auditLineScanner) drainToNewline(e error) error {
	for e == bufio.ErrBufferFull {
		_, e = s.r.ReadSlice('\n')
	}
	return e
}

// finish latches the scanner closed and records a terminal read error. io.EOF
// is normal end-of-input, not an error.
func (s *auditLineScanner) finish(err error) {
	s.done = true
	if err != nil && err != io.EOF {
		s.err = err
	}
}

func (s *auditLineScanner) Text() string { return s.line }
func (s *auditLineScanner) Err() error   { return s.err }
func (s *auditLineScanner) Skipped() int { return s.skipped }

// foldOversized adds the scanner's oversized-line drops to a parser's running
// skip count and, when any occurred, logs them distinctly. They still count in
// skipped_lines (the total the caller reports), but an oversized drop is a whole
// audit record discarded for size — not a malformed line — and in a forensics
// context the dropped record may be the one that names the actor, so operators
// need it surfaced rather than folded away silently. Every scanner-backed parser
// calls this once, after its scan loop.
func foldOversized(scanner *auditLineScanner, skipped int) int {
	if n := scanner.Skipped(); n > 0 {
		slog.Warn("forensics: skipped audit record(s) exceeding the 1 MiB parser line limit; "+
			"attribution from those records is unavailable (raise the limit at the source or shorten the query text)",
			"oversized_records", n)
		return skipped + n
	}
	return skipped
}

// trimTrailingNewline strips a trailing "\n" then a trailing "\r", replicating
// bufio.ScanLines' dropCR so the line sequence is identical to bufio.Scanner's
// on normal input (including "\r\n" endings and a final CR-terminated line).
func trimTrailingNewline(b []byte) string {
	if n := len(b); n > 0 && b[n-1] == '\n' {
		b = b[:n-1]
	}
	if n := len(b); n > 0 && b[n-1] == '\r' {
		b = b[:n-1]
	}
	return string(b)
}
