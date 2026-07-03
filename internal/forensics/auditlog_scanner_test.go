package forensics

import (
	"bufio"
	"errors"
	"io"
	"strings"
	"testing"
)

// TestAuditLineScanner_MatchesBufioScanner is a differential test: on any input
// whose lines all fit within maxAuditLineBytes, auditLineScanner must yield the
// exact same line sequence as bufio.Scanner (the reader it replaces). This one
// test guards the newline / blank-line / CRLF / final-line-without-newline
// edges that a hand-rolled line reader gets wrong.
func TestAuditLineScanner_MatchesBufioScanner(t *testing.T) {
	inputs := map[string]string{
		"empty":                    "",
		"single line no newline":   "one line",
		"single line with newline": "one line\n",
		"two lines":                "a\nb\n",
		"two lines no trailing nl": "a\nb",
		"blank lines preserved":    "a\n\n\nb\n",
		"leading blank line":       "\nafter\n",
		"only newlines":            "\n\n\n",
		"crlf endings":             "a\r\nb\r\n",
		"crlf final no lf":         "a\r\nb\r",
		"trailing whitespace":      "a   \n   b\n",
		"csv-ish":                  `"2024-06-15","root","10.0.0.1","QUERY"` + "\n",
	}
	for name, in := range inputs {
		t.Run(name, func(t *testing.T) {
			want := bufioLines(in)
			got := auditScanLines(t, strings.NewReader(in))
			if len(got) != len(want) {
				t.Fatalf("line count = %d, want %d\n got=%q\nwant=%q", len(got), len(want), got, want)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Errorf("line %d = %q, want %q", i, got[i], want[i])
				}
			}
		})
	}
}

// TestAuditLineScanner_OversizedSkippedMidStream is the core #7 property: an
// over-long line is skipped and counted, and lines AFTER it are still returned —
// the exact case bufio.Scanner cannot handle (it aborts on ErrTooLong).
func TestAuditLineScanner_OversizedSkippedMidStream(t *testing.T) {
	huge := strings.Repeat("x", maxAuditLineBytes+1)
	in := "before\n" + huge + "\nafter\n"

	got := auditScanLines(t, strings.NewReader(in))
	want := []string{"before", "after"}
	if len(got) != len(want) || got[0] != "before" || got[1] != "after" {
		t.Fatalf("lines = %q, want %q (the record after the oversized one must survive)", got, want)
	}

	// And Skipped() reports the drop.
	sc := newAuditLineScanner(strings.NewReader(in))
	n := 0
	for sc.Scan() {
		n++
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
	if sc.Skipped() != 1 {
		t.Errorf("Skipped() = %d, want 1", sc.Skipped())
	}
}

// TestAuditLineScanner_MultipleConsecutiveOversized: several over-long lines in
// a row are each drained, skipped, and counted, and the valid lines bracketing
// them still come through — the scan never gets "stuck" on a run of big records.
func TestAuditLineScanner_MultipleConsecutiveOversized(t *testing.T) {
	huge := strings.Repeat("x", maxAuditLineBytes+1)
	in := "a\n" + huge + "\n" + huge + "\n" + huge + "\nb\n"

	sc := newAuditLineScanner(strings.NewReader(in))
	var got []string
	for sc.Scan() {
		got = append(got, sc.Text())
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("lines = %q, want [a b] (all three oversized records skipped)", got)
	}
	if sc.Skipped() != 3 {
		t.Errorf("Skipped() = %d, want 3", sc.Skipped())
	}
}

// TestAuditLineScanner_OversizedFinalLineNoNewline: an oversized last line with
// no trailing newline is skipped (counted) and ends the scan cleanly, not as an
// error.
func TestAuditLineScanner_OversizedFinalLineNoNewline(t *testing.T) {
	in := "kept\n" + strings.Repeat("y", maxAuditLineBytes+50) // no trailing \n

	sc := newAuditLineScanner(strings.NewReader(in))
	var got []string
	for sc.Scan() {
		got = append(got, sc.Text())
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
	if len(got) != 1 || got[0] != "kept" {
		t.Fatalf("lines = %q, want [kept]", got)
	}
	if sc.Skipped() != 1 {
		t.Errorf("Skipped() = %d, want 1", sc.Skipped())
	}
}

// TestAuditLineScanner_BoundaryKeptAndSpanningFills pins two size edges: a line
// of exactly maxAuditLineBytes content is KEPT (the old bufio.Scanner actually
// aborted here — it needed buffer room for the trailing newline too; this
// scanner is deliberately one byte more permissive and never drops a line the
// old one kept), and a valid line larger than the 256 KB internal read buffer
// (so it arrives in multiple ReadSlice fragments) is reassembled intact.
func TestAuditLineScanner_BoundaryKeptAndSpanningFills(t *testing.T) {
	t.Run("exactly at the byte bound is kept", func(t *testing.T) {
		line := strings.Repeat("a", maxAuditLineBytes)
		sc := newAuditLineScanner(strings.NewReader(line + "\n"))
		if !sc.Scan() {
			t.Fatalf("Scan() = false, want the exactly-bounded line kept (Err: %v)", sc.Err())
		}
		if len(sc.Text()) != maxAuditLineBytes {
			t.Errorf("line length = %d, want %d", len(sc.Text()), maxAuditLineBytes)
		}
		if sc.Skipped() != 0 {
			t.Errorf("Skipped() = %d, want 0 — a line at the bound must not be dropped", sc.Skipped())
		}
	})
	t.Run("line spanning several read-buffer fills is intact", func(t *testing.T) {
		line := strings.Repeat("z", 700*1024) // > 256 KB buffer, < 1 MB bound
		sc := newAuditLineScanner(strings.NewReader(line + "\n"))
		if !sc.Scan() {
			t.Fatalf("Scan() = false, want the multi-fragment line (Err: %v)", sc.Err())
		}
		if sc.Text() != line {
			t.Errorf("reassembled line length = %d, want %d (fragments not stitched correctly)", len(sc.Text()), len(line))
		}
	})
}

// TestAuditLineScanner_ReadErrorSurfaces: a genuine read error (not EOF) is
// reported via Err, never swallowed as a clean end of input.
func TestAuditLineScanner_ReadErrorSurfaces(t *testing.T) {
	boom := errors.New("disk exploded")
	sc := newAuditLineScanner(&errReader{data: "line one\n", err: boom})

	var got []string
	for sc.Scan() {
		got = append(got, sc.Text())
	}
	if len(got) != 1 || got[0] != "line one" {
		t.Fatalf("lines = %q, want [line one] before the error", got)
	}
	if !errors.Is(sc.Err(), boom) {
		t.Errorf("Err() = %v, want the underlying read error", sc.Err())
	}
}

// --- helpers ---------------------------------------------------------------

// auditScanLines drains an auditLineScanner into a slice, failing on error.
func auditScanLines(t *testing.T, r io.Reader) []string {
	t.Helper()
	sc := newAuditLineScanner(r)
	var out []string
	for sc.Scan() {
		out = append(out, sc.Text())
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("auditLineScanner Err() = %v, want nil", err)
	}
	return out
}

// bufioLines drains a bufio.Scanner (the reference implementation) into a slice.
func bufioLines(in string) []string {
	sc := bufio.NewScanner(strings.NewReader(in))
	sc.Buffer(make([]byte, 256*1024), 1024*1024)
	var out []string
	for sc.Scan() {
		out = append(out, sc.Text())
	}
	return out
}

// errReader yields data once, then the given error (simulating an I/O failure
// partway through a stream). The trailing newline in data ensures the first
// line completes before the error surfaces.
type errReader struct {
	data string
	err  error
	done bool
}

func (e *errReader) Read(p []byte) (int, error) {
	if e.done {
		return 0, e.err
	}
	n := copy(p, e.data)
	e.done = true
	return n, nil
}
