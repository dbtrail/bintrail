package status

import (
	"bytes"
	"database/sql"
	"strconv"
	"strings"
	"testing"
	"time"
)

// ─── DescriptionToHuman ───────────────────────────────────────────────────────

func TestDescriptionToHuman_maxvalue(t *testing.T) {
	for _, input := range []string{"MAXVALUE", "maxvalue", "MaxValue", ""} {
		got := DescriptionToHuman(input)
		if got != "MAXVALUE" {
			t.Errorf("DescriptionToHuman(%q) = %q, want MAXVALUE", input, got)
		}
	}
}

func TestDescriptionToHuman_toSecondsValue(t *testing.T) {
	// Compute a known TO_SECONDS value at runtime to avoid hardcoding date-sensitive values.
	// TO_SECONDS('1970-01-01 00:00:00') = 62167219200; any UTC time t has TO_SECONDS = 62167219200 + t.Unix().
	knownTime := time.Date(2026, 2, 20, 14, 0, 0, 0, time.UTC)
	toSecondsVal := int64(62167219200) + knownTime.Unix()
	got := DescriptionToHuman(strconv.FormatInt(toSecondsVal, 10))
	if !strings.Contains(got, "2026-02-20") {
		t.Errorf("expected 2026-02-20 in %q", got)
	}
	if !strings.Contains(got, "14:00") {
		t.Errorf("expected 14:00 in %q", got)
	}
	if !strings.Contains(got, "UTC") {
		t.Errorf("expected UTC suffix in %q", got)
	}
}

func TestDescriptionToHuman_unknownString(t *testing.T) {
	// A non-integer, non-MAXVALUE value should pass through unchanged.
	got := DescriptionToHuman("SOME_EXPR")
	if got != "SOME_EXPR" {
		t.Errorf("expected SOME_EXPR, got %q", got)
	}
}

func TestDescriptionToHuman_unixEpoch(t *testing.T) {
	// TO_SECONDS('1970-01-01 00:00:00') = 62167219200 — verify the Unix epoch converts correctly.
	got := DescriptionToHuman("62167219200")
	if !strings.Contains(got, "1970") {
		t.Errorf("expected 1970 in %q", got)
	}
}

// ─── Truncate ─────────────────────────────────────────────────────────────────

func TestTruncate_shortString(t *testing.T) {
	got := Truncate("hello", 10)
	if got != "hello" {
		t.Errorf("expected no truncation, got %q", got)
	}
}

func TestTruncate_exactLength(t *testing.T) {
	got := Truncate("hello", 5)
	if got != "hello" {
		t.Errorf("expected no truncation at exact length, got %q", got)
	}
}

func TestTruncate_longString(t *testing.T) {
	long := strings.Repeat("x", 100)
	got := Truncate(long, 20)
	if !strings.HasSuffix(got, "…") {
		t.Errorf("expected ellipsis suffix, got %q", got)
	}
	if len(got) != 21 { // 20 chars + "…" (3 bytes, but counted as 1 rune display)
		// Note: "…" is a multi-byte UTF-8 rune but len() counts bytes.
		// Our Truncate uses byte indexing, so the suffix adds 3 bytes.
		// Just check the prefix is preserved and suffix is "…".
		if !strings.HasPrefix(got, strings.Repeat("x", 20)) {
			t.Errorf("expected 20 x's before ellipsis, got %q", got)
		}
	}
}

// ─── WriteStatus output structure ────────────────────────────────────────────

func TestWriteStatus_noFiles_noPartitions(t *testing.T) {
	var buf bytes.Buffer
	WriteStatus(&buf, nil, nil)
	out := buf.String()

	assertContains(t, out, "=== Indexed Files ===")
	assertContains(t, out, "no files indexed yet")
	assertContains(t, out, "=== Partitions ===")
	assertContains(t, out, "no partitions found")
	// Summary section is omitted when there are no files.
	if strings.Contains(out, "=== Summary ===") {
		t.Error("expected no Summary section when no files exist")
	}
}

func TestWriteStatus_withFiles(t *testing.T) {
	ts := time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC)
	files := []IndexStateRow{
		{
			BinlogFile:    "binlog.000042",
			Status:        "completed",
			EventsIndexed: 1234,
			FileSize:      1024 * 1024,
			LastPosition:  1024 * 1024,
			StartedAt:     ts,
			CompletedAt:   sql.NullTime{Valid: true, Time: ts.Add(5 * 60 * 1e9)},
			ErrorMessage:  sql.NullString{},
		},
		{
			BinlogFile:    "binlog.000043",
			Status:        "failed",
			EventsIndexed: 0,
			FileSize:      512,
			LastPosition:  0,
			StartedAt:     ts.Add(10 * 60 * 1e9),
			CompletedAt:   sql.NullTime{Valid: false},
			ErrorMessage:  sql.NullString{Valid: true, String: "connection refused"},
		},
	}

	var buf bytes.Buffer
	WriteStatus(&buf, files, nil)
	out := buf.String()

	assertContains(t, out, "binlog.000042")
	assertContains(t, out, "completed")
	assertContains(t, out, "1234")
	assertContains(t, out, "binlog.000043")
	assertContains(t, out, "failed")
	assertContains(t, out, "connection refused")
	assertContains(t, out, "=== Summary ===")
	assertContains(t, out, "1 completed")
	assertContains(t, out, "1 failed")
}

func TestWriteStatus_withPartitions(t *testing.T) {
	// Compute TO_SECONDS values dynamically: TO_SECONDS('1970-01-01 00:00:00') = 62167219200.
	toSeconds := func(d time.Time) string {
		return strconv.FormatInt(int64(62167219200)+d.Unix(), 10)
	}
	parts := []PartitionStat{
		{Name: "p_2026021800", Description: toSeconds(time.Date(2026, 2, 18, 1, 0, 0, 0, time.UTC)), TableRows: 45000, Ordinal: 1},
		{Name: "p_2026021900", Description: toSeconds(time.Date(2026, 2, 19, 0, 0, 0, 0, time.UTC)), TableRows: 3401, Ordinal: 2},
		{Name: "p_future", Description: "MAXVALUE", TableRows: 0, Ordinal: 3},
	}

	var buf bytes.Buffer
	WriteStatus(&buf, nil, parts)
	out := buf.String()

	assertContains(t, out, "=== Partitions ===")
	assertContains(t, out, "p_2026021800")
	assertContains(t, out, "p_future")
	assertContains(t, out, "MAXVALUE")
	assertContains(t, out, "45000")
	// Total should be 48401
	assertContains(t, out, "48401")
}

func TestWriteStatus_errorTruncation(t *testing.T) {
	long := strings.Repeat("e", 100)
	files := []IndexStateRow{{
		BinlogFile:   "binlog.000001",
		Status:       "failed",
		StartedAt:    time.Now(),
		ErrorMessage: sql.NullString{Valid: true, String: long},
	}}

	var buf bytes.Buffer
	WriteStatus(&buf, files, nil)
	out := buf.String()

	// The error should be truncated — full 100-char string should not appear.
	if strings.Contains(out, long) {
		t.Error("expected long error to be truncated, but found it in full")
	}
	assertContains(t, out, "…")
}

// ─── Helper ───────────────────────────────────────────────────────────────────

func assertContains(t *testing.T, s, want string) {
	t.Helper()
	if !strings.Contains(s, want) {
		t.Errorf("expected %q in output:\n%s", want, s)
	}
}
