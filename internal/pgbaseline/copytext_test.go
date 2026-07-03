package pgbaseline

import (
	"reflect"
	"strings"
	"testing"
)

func TestParseCopyTextLine_EscapeTable(t *testing.T) {
	cases := []struct {
		name  string
		line  string
		want  []string
		nulls []bool
	}{
		{"plain", "1\thello", []string{"1", "hello"}, []bool{false, false}},
		{"tab inside value", `a\tb` + "\tx", []string{"a\tb", "x"}, []bool{false, false}},
		{"newline inside value", `line1\nline2` + "\tx", []string{"line1\nline2", "x"}, []bool{false, false}},
		{"carriage return", `a\rb` + "\tx", []string{"a\rb", "x"}, []bool{false, false}},
		{"backspace formfeed vtab", `a\bb` + "\t" + `c\fd\ve`, []string{"a\bb", "c\fd\ve"}, []bool{false, false}},
		{"backslash", `C:\\path\\to` + "\tx", []string{`C:\path\to`, "x"}, []bool{false, false}},
		{"NULL field", `\N` + "\tx", []string{"", "x"}, []bool{true, false}},
		{"empty string is NOT NULL", "\tx", []string{"", "x"}, []bool{false, false}},
		{"literal backslash-N is NOT NULL", `\\N` + "\tx", []string{`\N`, "x"}, []bool{false, false}},
		{"multibyte UTF-8", "café\t日本語🎉", []string{"café", "日本語🎉"}, []bool{false, false}},
		{"multibyte with escapes", `日本\t語` + "\t\\N", []string{"日本\t語", ""}, []bool{false, true}},
		{"all NULLs", `\N` + "\t" + `\N`, []string{"", ""}, []bool{true, true}},
		{"mixed escapes one field", `a\tb\nc\\d` + "\tx", []string{"a\tb\nc\\d", "x"}, []bool{false, false}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			values, nulls, err := parseCopyTextLine([]byte(tc.line), 2)
			if err != nil {
				t.Fatalf("parseCopyTextLine(%q): %v", tc.line, err)
			}
			if !reflect.DeepEqual(values, tc.want) {
				t.Errorf("values = %q, want %q", values, tc.want)
			}
			if !reflect.DeepEqual(nulls, tc.nulls) {
				t.Errorf("nulls = %v, want %v", nulls, tc.nulls)
			}
		})
	}
}

func TestParseCopyTextLine_Errors(t *testing.T) {
	cases := []struct {
		name    string
		line    string
		cols    int
		wantSub string
	}{
		{"too few fields", "a", 2, "1 fields, want 2"},
		{"too many fields", "a\tb\tc", 2, "3 fields, want 2"},
		{"unknown escape", `a\qb` + "\tx", 2, "unexpected escape"},
		{"trailing lone backslash", `abc\` + "\tx", 2, "lone backslash"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := parseCopyTextLine([]byte(tc.line), tc.cols)
			if err == nil {
				t.Fatalf("parseCopyTextLine(%q) succeeded, want error containing %q", tc.line, tc.wantSub)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q does not contain %q", err, tc.wantSub)
			}
		})
	}
}

// TestCopyTextSink_ChunkedWrites feeds the sink one byte at a time — the worst
// possible CopyData chunking — and checks rows re-assemble correctly across
// chunk boundaries (including a boundary in the middle of an escape sequence).
func TestCopyTextSink_ChunkedWrites(t *testing.T) {
	var got [][]string
	var gotNulls [][]bool
	sink := newCopyTextSink(2, func(values []string, nulls []bool) error {
		got = append(got, append([]string(nil), values...))
		gotNulls = append(gotNulls, append([]bool(nil), nulls...))
		return nil
	})
	data := "1\ta\\tb\n2\t\\N\n3\tcafé\n"
	for i := 0; i < len(data); i++ {
		if _, err := sink.Write([]byte{data[i]}); err != nil {
			t.Fatalf("Write byte %d: %v", i, err)
		}
	}
	if err := sink.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	want := [][]string{{"1", "a\tb"}, {"2", ""}, {"3", "café"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("rows = %q, want %q", got, want)
	}
	if !gotNulls[1][1] {
		t.Error("row 2 field 2 should be NULL")
	}
	if sink.rows != 3 {
		t.Errorf("sink.rows = %d, want 3", sink.rows)
	}
}

// TestCopyTextSink_FlushUnterminatedLine parses a final row lacking its
// trailing newline (defensive; COPY TO always terminates rows).
func TestCopyTextSink_FlushUnterminatedLine(t *testing.T) {
	var got [][]string
	sink := newCopyTextSink(1, func(values []string, nulls []bool) error {
		got = append(got, append([]string(nil), values...))
		return nil
	})
	if _, err := sink.Write([]byte("a\nb")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := sink.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	want := [][]string{{"a"}, {"b"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("rows = %q, want %q", got, want)
	}
	// Flush again is a no-op.
	if err := sink.Flush(); err != nil {
		t.Fatalf("second Flush: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("second Flush re-emitted rows: %d", len(got))
	}
}

// TestCopyTextSink_EndOfDataMarkerSkipped ignores a defensive `\.` line.
func TestCopyTextSink_EndOfDataMarkerSkipped(t *testing.T) {
	var rows int
	sink := newCopyTextSink(1, func([]string, []bool) error { rows++; return nil })
	if _, err := sink.Write([]byte("a\n\\.\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := sink.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if rows != 1 {
		t.Errorf("rows = %d, want 1 (the \\. marker must be skipped, not parsed)", rows)
	}
}

// TestCopyTextSink_RowFnErrorAborts propagates a downstream error out of Write
// so pgconn aborts the COPY.
func TestCopyTextSink_RowFnErrorAborts(t *testing.T) {
	sink := newCopyTextSink(1, func([]string, []bool) error {
		return errTest
	})
	if _, err := sink.Write([]byte("a\n")); err == nil {
		t.Fatal("Write succeeded, want the rowFn error propagated")
	}
}

var errTest = &testErr{}

type testErr struct{}

func (*testErr) Error() string { return "test error" }
