package status

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// ─── coverage / archives read failures (#1323) ──────────────────────────────────
//
// A FAILURE to load a section (err set, data nil) must render AS a failure in
// both formats — never as the section's silent absence, which a consumer reads
// as an affirmative fact ("no archives exist", "nothing to restore from").
// These are pure functions over StatusData, so they live in a plain unit test
// (the coverage_render_test.go stance); the CollectStatus wiring that SETS the
// fields is pinned by the integration test.

func TestStatusData_coverageReadError_text(t *testing.T) {
	var buf bytes.Buffer
	(&StatusData{CoverageErr: errors.New("query binlog_events coverage: Error 3024: max_execution_time exceeded")}).Write(&buf)
	out := buf.String()
	for _, want := range []string{"=== Restore Coverage ===", "NOT READ", "UNKNOWN, not empty", "max_execution_time"} {
		if !strings.Contains(out, want) {
			t.Errorf("a coverage read error must surface %q in the text report\n--- output ---\n%s", want, out)
		}
	}

	// No error, no coverage (never collected) → no tombstone.
	var empty bytes.Buffer
	(&StatusData{}).Write(&empty)
	if strings.Contains(empty.String(), "NOT READ") || strings.Contains(empty.String(), "=== Restore Coverage ===") {
		t.Errorf("no read error must mean no coverage tombstone:\n%s", empty.String())
	}
}

func TestStatusData_archivesReadError_text(t *testing.T) {
	var buf bytes.Buffer
	(&StatusData{ArchivesErr: errors.New("Error 1142: SELECT command denied on archive_state")}).Write(&buf)
	out := buf.String()
	for _, want := range []string{"=== Archives ===", "NOT READ", "read failure", "1142"} {
		if !strings.Contains(out, want) {
			t.Errorf("an archive_state read error must surface %q in the text report\n--- output ---\n%s", want, out)
		}
	}

	// No error, no archives (no archive tier) → no section, no tombstone.
	var empty bytes.Buffer
	(&StatusData{}).Write(&empty)
	if strings.Contains(empty.String(), "=== Archives ===") {
		t.Errorf("an index with no archive tier must not render an Archives block:\n%s", empty.String())
	}
}

func TestStatusData_sectionReadErrors_json(t *testing.T) {
	decode := func(t *testing.T, d *StatusData) (raw string, parsed struct {
		Archives      *json.RawMessage `json:"archives"`
		ArchivesError *struct {
			Error string `json:"error"`
		} `json:"archives_error"`
		Coverage      *json.RawMessage `json:"coverage"`
		CoverageError *struct {
			Error string `json:"error"`
		} `json:"coverage_error"`
	}) {
		t.Helper()
		var buf bytes.Buffer
		if err := d.WriteJSON(&buf); err != nil {
			t.Fatalf("WriteJSON: %v", err)
		}
		if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
			t.Fatalf("invalid JSON: %v\n%s", err, buf.String())
		}
		return buf.String(), parsed
	}

	t.Run("coverage read error", func(t *testing.T) {
		raw, got := decode(t, &StatusData{CoverageErr: errors.New("read timeout")})
		if got.Coverage != nil {
			t.Errorf("a read error must not fabricate a coverage object (its zero fields would read as a real empty index):\n%s", raw)
		}
		if got.CoverageError == nil {
			t.Fatalf("coverage_error missing — the read failure was silently omitted:\n%s", raw)
		}
		if !strings.Contains(got.CoverageError.Error, "read timeout") {
			t.Errorf("coverage_error.error must carry the underlying cause, got %q", got.CoverageError.Error)
		}
	})

	t.Run("archives read error", func(t *testing.T) {
		raw, got := decode(t, &StatusData{ArchivesErr: errors.New("Error 1054: Unknown column 'row_count'")})
		if got.Archives != nil {
			t.Errorf("a read error must not fabricate an archives object:\n%s", raw)
		}
		if got.ArchivesError == nil {
			t.Fatalf("archives_error missing — the read failure was silently omitted:\n%s", raw)
		}
		if !strings.Contains(got.ArchivesError.Error, "1054") {
			t.Errorf("archives_error.error must carry the underlying cause, got %q", got.ArchivesError.Error)
		}
	})

	t.Run("healthy report omits both keys", func(t *testing.T) {
		raw, got := decode(t, &StatusData{Archives: &ArchiveStats{TotalFiles: 1}, Coverage: &CoverageInfo{TotalEvents: 3}})
		if got.ArchivesError != nil || got.CoverageError != nil {
			t.Errorf("a healthy report must omit the section-error keys (a monitor's schema depends on absence):\n%s", raw)
		}
		if got.Archives == nil || got.Coverage == nil {
			t.Errorf("healthy data lost its sections:\n%s", raw)
		}
	})
}
