package status

import (
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// The rendering of an unreadable archive tier (#816) is a pure function over
// *CoverageInfo, so it belongs in a plain unit test — not behind
// //go:build integration, where `go test ./...` skips it silently on a machine
// with no MySQL.
//
// The integration test that shipped first put the rendering assertions there
// AND never inserted a binlog_events row, so EarliestEvent was invalid, the
// report took the "(none live)" branch, and the switch this change adds for
// the normal populated-index case was executed by nothing. Its
// "(includes archives)" assertion could not fail for the same reason.
func TestWriteStatus_archiveUnavailable(t *testing.T) {
	earliest := time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC)
	populated := func() *CoverageInfo {
		return &CoverageInfo{
			EarliestEvent: sql.NullTime{Time: earliest, Valid: true},
			LatestEvent:   sql.NullTime{Time: earliest.Add(48 * time.Hour), Valid: true},
			TotalEvents:   1200,
		}
	}

	t.Run("populated index, archives unreadable", func(t *testing.T) {
		c := populated()
		c.ArchiveUnavailable = true
		c.ArchiveError = "Error 1142: SELECT command denied"
		var buf strings.Builder
		(&StatusData{Coverage: c}).Write(&buf)
		out := buf.String()

		// THE branch: a live floor IS printed, and it must be labelled as
		// live-only rather than presented as the restore reach.
		if !strings.Contains(out, "2026-08-01 09:00:00 (LIVE INDEX ONLY") {
			t.Errorf("the earliest-event line does not mark itself live-only:\n%s", out)
		}
		if strings.Contains(out, "(includes archives)") {
			t.Errorf("claimed the window includes archives that were never read:\n%s", out)
		}
		if !strings.Contains(out, "NOT READ") || !strings.Contains(out, "LOWER BOUND") {
			t.Errorf("the report does not qualify the window:\n%s", out)
		}
		if !strings.Contains(out, "1142") {
			t.Errorf("the reason is not shown, so the operator cannot act:\n%s", out)
		}
	})

	t.Run("archives read and present", func(t *testing.T) {
		c := populated()
		c.ArchiveEarliestHour = sql.NullTime{Time: earliest.Add(-72 * time.Hour), Valid: true}
		c.ArchiveTotalRows = 900
		var buf strings.Builder
		(&StatusData{Coverage: c}).Write(&buf)
		out := buf.String()
		if !strings.Contains(out, "(includes archives)") {
			t.Errorf("a healthy archive tier lost its label:\n%s", out)
		}
		if strings.Contains(out, "NOT READ") {
			t.Errorf("a healthy archive tier was reported as unread:\n%s", out)
		}
	})

	t.Run("no archive tier at all stays quiet", func(t *testing.T) {
		var buf strings.Builder
		(&StatusData{Coverage: populated()}).Write(&buf)
		out := buf.String()
		if strings.Contains(out, "NOT READ") || strings.Contains(out, "LIVE INDEX ONLY") {
			t.Errorf("an index with no archives was given a scary banner:\n%s", out)
		}
	})

	t.Run("JSON carries the verdict and omits it when healthy", func(t *testing.T) {
		read := func(c *CoverageInfo) map[string]any {
			t.Helper()
			var buf strings.Builder
			if err := (&StatusData{Coverage: c}).WriteJSON(&buf); err != nil {
				t.Fatalf("WriteJSON: %v", err)
			}
			var parsed struct {
				Coverage map[string]any `json:"coverage"`
			}
			if err := json.Unmarshal([]byte(buf.String()), &parsed); err != nil {
				t.Fatalf("decode: %v\n%s", err, buf.String())
			}
			return parsed.Coverage
		}
		bad := populated()
		bad.ArchiveUnavailable = true
		bad.ArchiveError = "boom"
		if got := read(bad); got["archives_unavailable"] != true || got["archives_error"] != "boom" {
			t.Errorf("JSON lacks the unavailable verdict: %v", got)
		}
		// omitempty: a monitor's schema depends on the key being ABSENT when
		// the figures are complete, not present-and-false.
		if got := read(populated()); got["archives_unavailable"] != nil {
			t.Errorf("archives_unavailable present on a healthy report: %v", got)
		}
	})
}
