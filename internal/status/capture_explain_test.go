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

// explainJoined renders with NO snapshot anchor (the zero time), which is the
// state of an index that holds no schema snapshot. The anchored states have
// their own tests below; these pin the per-reason cause/remedy prose, which the
// anchor does not touch.
func explainJoined(skips map[string]CaptureSkipStat) string {
	return strings.Join(ExplainCaptureSkips(skips, time.Time{}), "\n")
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
	if got := ExplainCaptureSkips(map[string]CaptureSkipStat{}, time.Time{}); got != nil {
		t.Errorf("nothing skipped must explain nothing, got %v", got)
	}
	if got := ExplainCaptureSkips(map[string]CaptureSkipStat{"x": {Count: 0}}, time.Time{}); got != nil {
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

// ─── The snapshot anchor (#1312) ─────────────────────────────────────────────
//
// The tally is monotonic, so it reads identically before and after a successful
// re-snapshot: an operator pressed the console's own "Refresh schema snapshot"
// button, reloaded, and got the same alarm. These pin the comparison that makes
// the tally answerable — and, just as hard, pin that it never claims "fixed".

func skipAt(ts string) CaptureSkipStat {
	t, err := time.Parse("2006-01-02 15:04:05", ts)
	if err != nil {
		panic(err)
	}
	return CaptureSkipStat{Count: 3, LastAt: t, Tables: []string{"shop.plugin_log"}}
}

func mustTime(t *testing.T, ts string) time.Time {
	t.Helper()
	parsed, err := time.Parse("2006-01-02 15:04:05", ts)
	if err != nil {
		t.Fatalf("bad fixture time %q: %v", ts, err)
	}
	return parsed
}

func TestSkipsPredateSnapshot(t *testing.T) {
	cases := []struct {
		name     string
		skips    map[string]CaptureSkipStat
		snapshot string
		want     bool
	}{
		{"skip before the snapshot is historic",
			map[string]CaptureSkipStat{CaptureSkipReasonTableNotInSnapshot: skipAt("2026-08-04 19:49:33")},
			"2026-08-11 12:00:00", true},
		{"skip after the snapshot is still active",
			map[string]CaptureSkipStat{CaptureSkipReasonTableNotInSnapshot: skipAt("2026-08-11 13:00:00")},
			"2026-08-11 12:00:00", false},
		// Equal timestamps are NOT historic: a skip stamped at the same second
		// the snapshot was taken could have come after it, and this verdict
		// decides whether an operator sees an alarm.
		{"a skip at the snapshot's own second is not historic",
			map[string]CaptureSkipStat{CaptureSkipReasonTableNotInSnapshot: skipAt("2026-08-11 12:00:00")},
			"2026-08-11 12:00:00", false},
		// One reason quiet does not make the ledger quiet.
		{"one active reason after the snapshot keeps the whole ledger active",
			map[string]CaptureSkipStat{
				CaptureSkipReasonTableNotInSnapshot:  skipAt("2026-08-04 19:49:33"),
				CaptureSkipReasonColumnCountMismatch: skipAt("2026-08-11 13:00:00"),
			},
			"2026-08-11 12:00:00", false},
		// A zero-count reason is not active and must not veto the verdict.
		{"a zero-count reason is ignored",
			map[string]CaptureSkipStat{
				CaptureSkipReasonTableNotInSnapshot:  skipAt("2026-08-04 19:49:33"),
				CaptureSkipReasonColumnCountMismatch: {Count: 0},
			},
			"2026-08-11 12:00:00", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := SkipsPredateSnapshot(c.skips, mustTime(t, c.snapshot)); got != c.want {
				t.Errorf("SkipsPredateSnapshot = %v, want %v", got, c.want)
			}
		})
	}
}

// No anchor and an undated skip both mean "cannot tell", and cannot-tell must
// never render as the quiet state — that is the direction that hides a live
// capture failure.
func TestSkipsPredateSnapshot_missingAnchorIsNeverHistoric(t *testing.T) {
	skips := map[string]CaptureSkipStat{CaptureSkipReasonTableNotInSnapshot: skipAt("2026-08-04 19:49:33")}
	if SkipsPredateSnapshot(skips, time.Time{}) {
		t.Error("no snapshot time must not read as historic")
	}
	undated := map[string]CaptureSkipStat{CaptureSkipReasonTableNotInSnapshot: {Count: 3}}
	if SkipsPredateSnapshot(undated, mustTime(t, "2026-08-11 12:00:00")) {
		t.Error("a skip with no last_at must not read as historic")
	}
	if SkipsPredateSnapshot(map[string]CaptureSkipStat{}, mustTime(t, "2026-08-11 12:00:00")) {
		t.Error("an empty ledger is not a historic ledger")
	}
}

func TestExplainCaptureSkips_historicSaysNothingSinceTheSnapshot(t *testing.T) {
	out := strings.Join(ExplainCaptureSkips(
		map[string]CaptureSkipStat{CaptureSkipReasonTableNotInSnapshot: skipAt("2026-08-04 19:49:33")},
		mustTime(t, "2026-08-11 12:00:00")), "\n")
	if !strings.Contains(out, "Nothing has been skipped since the current schema snapshot") {
		t.Errorf("the historic state must say the tally stopped moving:\n%s", out)
	}
	if !strings.Contains(out, "2026-08-11 12:00:00") {
		t.Errorf("the historic state must date the snapshot it compared against:\n%s", out)
	}
	// The whole reason this verdict is safe to show quietly.
	if !strings.Contains(out, "not proof the fix took hold") {
		t.Errorf("the historic state must not read as 'resolved':\n%s", out)
	}
	if !strings.Contains(out, "no writes skips nothing") {
		t.Errorf("an idle source makes 'nothing skipped since' vacuous; the text must say so:\n%s", out)
	}
	if !strings.Contains(out, "recovers what was already skipped") {
		t.Errorf("going quiet must not drop the permanent-loss statement:\n%s", out)
	}
}

func TestExplainCaptureSkips_activeSaysTheDropsAreCurrent(t *testing.T) {
	out := strings.Join(ExplainCaptureSkips(
		map[string]CaptureSkipStat{CaptureSkipReasonTableNotInSnapshot: skipAt("2026-08-11 13:00:00")},
		mustTime(t, "2026-08-11 12:00:00")), "\n")
	if !strings.Contains(out, "skipped AFTER the current schema snapshot") {
		t.Errorf("the active state must say the drops post-date the snapshot:\n%s", out)
	}
	if strings.Contains(out, "Nothing has been skipped since") {
		t.Errorf("the active state must not carry the historic wording:\n%s", out)
	}
}

// With no snapshot in the index there is nothing to compare against, so the
// pre-#1312 paragraph — including the manual ledger-clearing escape hatch — is
// still the honest thing to print.
func TestExplainCaptureSkips_noAnchorKeepsTheManualAcknowledgement(t *testing.T) {
	out := strings.Join(ExplainCaptureSkips(
		map[string]CaptureSkipStat{CaptureSkipReasonTableNotInSnapshot: skipAt("2026-08-04 19:49:33")},
		time.Time{}), "\n")
	if !strings.Contains(out, "does not clear on its own") {
		t.Errorf("without an anchor the caveat must stay:\n%s", out)
	}
	if strings.Contains(out, "current schema snapshot") {
		t.Errorf("without an anchor nothing may be claimed about a snapshot:\n%s", out)
	}
}
