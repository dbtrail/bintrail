package status

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// ─── The DEGRADED verdict's explanation (#1296) ───────────────────────────────
//
// An operator on a live install read the old single sentence and reported three
// things, all fair: it implied they had misconfigured something, its remedy had
// no button anywhere, and "check the capture log" named neither a place nor a
// string to look for. Each test below pins one of those repairs, plus the
// honesty caveat the old text omitted entirely.

func explainJoined(skips map[string]CaptureSkipStat) string {
	return strings.Join(ExplainCaptureSkips(skips), "\n")
}

func TestExplainCaptureSkips_namesTheSkippedTables(t *testing.T) {
	out := explainJoined(map[string]CaptureSkipStat{
		CaptureSkipReasonTableNotInSnapshot: {
			Count:  3,
			LastAt: time.Date(2026, 8, 4, 19, 49, 33, 0, time.UTC),
			Tables: []string{"shop.plugin_log", "shop.plugin_meta"},
		},
	})
	for _, want := range []string{"shop.plugin_log", "shop.plugin_meta"} {
		if !strings.Contains(out, want) {
			t.Errorf("explanation does not name the skipped table %q:\n%s", want, out)
		}
	}
}

func TestExplainCaptureSkips_truncatedTableListSaysSo(t *testing.T) {
	out := explainJoined(map[string]CaptureSkipStat{
		CaptureSkipReasonTableNotInSnapshot: {
			Count: 99, Tables: []string{"a.b"}, TablesTruncated: true,
		},
	})
	if !strings.Contains(out, "and others") {
		t.Errorf("a capped table list must not read as the complete set:\n%s", out)
	}
}

// A ledger written before per-table attribution has NO table names. The
// explanation must then name none — inventing one, or rendering the empty list
// as "no tables", would be worse than saying nothing.
func TestExplainCaptureSkips_legacyLedgerNamesNoTable(t *testing.T) {
	out := explainJoined(map[string]CaptureSkipStat{
		CaptureSkipReasonTableNotInSnapshot: {Count: 3},
	})
	if !strings.Contains(out, "cannot name them") {
		t.Errorf("a ledger without table attribution must say so:\n%s", out)
	}
	if strings.Contains(out, "no tables") {
		t.Errorf("an empty table list must never render as 'no tables':\n%s", out)
	}
}

// The complaint that started this: the text read as operator error. A table
// appearing on the source is ordinary, and the explanation must say the stream's
// OWN auto-snapshot path is what did not run — not that the operator forgot
// something.
func TestExplainCaptureSkips_doesNotBlameTheOperator(t *testing.T) {
	out := explainJoined(map[string]CaptureSkipStat{
		CaptureSkipReasonTableNotInSnapshot: {Count: 3, Tables: []string{"shop.plugin_log"}},
	})
	if !strings.Contains(out, "ordinary") {
		t.Errorf("the ordinary cause must be named as ordinary:\n%s", out)
	}
	if !strings.Contains(out, "snapshot when it sees the CREATE/ALTER") {
		t.Errorf("the explanation must say the stream's own auto-snapshot path did not run:\n%s", out)
	}
}

// A fresh snapshot fixes capture GOING FORWARD only. Without this line an
// operator re-snapshots, sees green, and believes the hole closed.
func TestExplainCaptureSkips_statesWhatIsNotRecovered(t *testing.T) {
	out := explainJoined(map[string]CaptureSkipStat{
		CaptureSkipReasonTableNotInSnapshot: {Count: 3, Tables: []string{"shop.plugin_log"}},
	})
	if !strings.Contains(out, "recovers what was already skipped") {
		t.Errorf("the explanation must say the remedy is forward-only:\n%s", out)
	}
	if !strings.Contains(out, "binlogs covering that window") {
		t.Errorf("the only path back to the skipped events must be stated honestly:\n%s", out)
	}
}

// "Check the capture log" named neither a location nor a string. Both must be
// present.
func TestExplainCaptureSkips_namesTheLogAndTheLine(t *testing.T) {
	out := explainJoined(map[string]CaptureSkipStat{
		CaptureSkipReasonTableNotInSnapshot: {Count: 3},
	})
	if !strings.Contains(out, "docker compose logs bintrail") {
		t.Errorf("the log must be named concretely:\n%s", out)
	}
	if !strings.Contains(out, "table not in snapshot — skipping") {
		t.Errorf("the exact log line to look for must be named:\n%s", out)
	}
}

// The two causes of an absent table have OPPOSITE remedies. Sending a
// validation-excluded table to `bintrail snapshot` is the non-converging
// remediation #1199 fixed elsewhere: every future snapshot excludes it again.
func TestExplainCaptureSkips_excludedTableIsNotSentToResnapshot(t *testing.T) {
	out := explainJoined(map[string]CaptureSkipStat{
		CaptureSkipReasonTableExcludedFromSnapshot: {
			Count: 12, Tables: []string{"shop.audit_raw"}, LastDetail: "no explicit primary key",
		},
	})
	if !strings.Contains(out, "PRIMARY KEY") {
		t.Errorf("the excluded-table branch must ask for a primary key:\n%s", out)
	}
	if !strings.Contains(out, "Re-snapshotting is NOT the fix here") {
		t.Errorf("the excluded-table branch must refuse the re-snapshot remedy:\n%s", out)
	}
	if !strings.Contains(out, "no explicit primary key") {
		t.Errorf("the recorded exclusion reason must be surfaced:\n%s", out)
	}
	if strings.Contains(out, "Refresh schema snapshot") {
		t.Errorf("the excluded-table branch must not offer the console's re-snapshot action:\n%s", out)
	}
}

// The ordinary branch must NOT borrow the excluded branch's text, and vice
// versa — one shared remedy is exactly what made the message useless.
func TestExplainCaptureSkips_branchesDoNotShareRemedies(t *testing.T) {
	ordinary := explainJoined(map[string]CaptureSkipStat{
		CaptureSkipReasonTableNotInSnapshot: {Count: 1, Tables: []string{"a.b"}},
	})
	if !strings.Contains(ordinary, "Refresh schema snapshot") {
		t.Errorf("the ordinary branch must point at the console action:\n%s", ordinary)
	}
	if strings.Contains(ordinary, "Re-snapshotting is NOT the fix here") {
		t.Errorf("the ordinary branch must not carry the excluded branch's refusal:\n%s", ordinary)
	}
}

// The remedy names a SCHEMA SNAPSHOT; the console's neighbouring button creates
// a BASELINE. The old wording invited confusing the two.
func TestExplainCaptureSkips_distinguishesSnapshotFromBaseline(t *testing.T) {
	out := explainJoined(map[string]CaptureSkipStat{
		CaptureSkipReasonTableNotInSnapshot: {Count: 1, Tables: []string{"a.b"}},
	})
	if !strings.Contains(out, "not a baseline") {
		t.Errorf("the remedy must say a schema snapshot is not a baseline:\n%s", out)
	}
}

func TestExplainCaptureSkips_emptyLedgerExplainsNothing(t *testing.T) {
	if got := ExplainCaptureSkips(map[string]CaptureSkipStat{}); got != nil {
		t.Errorf("nothing skipped must explain nothing, got %v", got)
	}
	if got := ExplainCaptureSkips(map[string]CaptureSkipStat{"x": {Count: 0}}); got != nil {
		t.Errorf("a zero-count reason must explain nothing, got %v", got)
	}
}

// The text report and the console must render the SAME strings: the wire field
// is what makes that true, so its absence is a regression, not a cosmetic one.
func TestWriteStatus_degradedBlockCarriesTheExplanation(t *testing.T) {
	var buf bytes.Buffer
	ledger := `{"table_not_in_snapshot":{"count":3,"last_at":"2026-08-04T19:49:33Z","tables":["shop.plugin_log"]}}`
	WriteStatus(&buf, nil, nil, nil, nil, nil, captureStream(ledger))
	// Collapse the fixed-width wrapping before matching: the report wraps these
	// paragraphs to the column width, so a phrase assertion against the raw text
	// would break on an unrelated prose edit that shifts a line boundary.
	out := strings.Join(strings.Fields(buf.String()), " ")
	for _, want := range []string{
		"shop.plugin_log",
		"recovers what was already skipped",
		"docker compose logs bintrail",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("text status is missing %q:\n%s", want, out)
		}
	}
	// The old text sent everyone to the CLI with no other option; the console
	// action is now the first remedy named.
	if !strings.Contains(out, "bintrail snapshot --source-dsn") {
		t.Errorf("text status must still give the CLI form of the remedy:\n%s", out)
	}
}

func TestWriteStatusJSON_carriesExplanationAndTables(t *testing.T) {
	ledger := `{"table_not_in_snapshot":{"count":3,"last_at":"2026-08-04T19:49:33Z","tables":["shop.plugin_log"],"tables_truncated":true}}`
	out := decodeStatusJSON(t, captureStream(ledger))
	ch := out["stream"].(map[string]any)["capture_health"].(map[string]any)
	expl, ok := ch["explanation"].([]any)
	if !ok || len(expl) == 0 {
		t.Fatalf("capture_health carries no explanation: %v", ch)
	}
	joined, _ := json.Marshal(expl)
	if !strings.Contains(string(joined), "shop.plugin_log") {
		t.Errorf("the wire explanation does not name the table: %s", joined)
	}
	stat := ch["skipped"].(map[string]any)["table_not_in_snapshot"].(map[string]any)
	tables, _ := stat["tables"].([]any)
	if len(tables) != 1 || tables[0] != "shop.plugin_log" {
		t.Errorf("per-reason tables not carried on the wire: %v", stat)
	}
	if stat["tables_truncated"] != true {
		t.Errorf("tables_truncated not carried on the wire: %v", stat)
	}
}

// An "ok" verdict must carry no explanation — a green banner with remediation
// prose in it would be its own bug report.
func TestWriteStatusJSON_okVerdictHasNoExplanation(t *testing.T) {
	out := decodeStatusJSON(t, captureStream("{}"))
	ch := out["stream"].(map[string]any)["capture_health"].(map[string]any)
	if _, present := ch["explanation"]; present {
		t.Errorf("ok verdict must omit the explanation: %v", ch)
	}
}

func TestWrapAt_keepsLongWordsIntact(t *testing.T) {
	// A table name or a command broken across lines is uncopyable, which is
	// worse than an over-long line.
	got := wrapAt("run `bintrail-console-with-a-very-long-name --flag` now", 20)
	joined := strings.Join(got, "\n")
	if !strings.Contains(joined, "`bintrail-console-with-a-very-long-name") {
		t.Errorf("a long word must not be split: %q", got)
	}
	for _, line := range got {
		if strings.Contains(line, "  ") {
			t.Errorf("wrapping introduced double spaces: %q", line)
		}
	}
}
