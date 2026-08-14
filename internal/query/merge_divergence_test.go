package query

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func row(id uint64) ResultRow {
	return ResultRow{
		EventID:        id,
		BinlogFile:     "bin.000001",
		StartPos:       4,
		EndPos:         40,
		EventTimestamp: time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC),
		SchemaName:     "app",
		TableName:      "users",
		EventType:      1,
		PKValues:       "7",
		RowAfter:       map[string]any{"id": 7, "name": "alice"},
	}
}

// captureWarn swaps the default slog handler for the duration of fn.
func captureWarn(t *testing.T, fn func()) string {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)
	fn()
	return buf.String()
}

// #841: a partition that is archived but not yet dropped yields the same
// event_id from both sources. MergeResults kept the first appearance and the
// "MySQL first, MySQL wins" contract was a comment with nothing enforcing it,
// so a real divergence — an index row mutated after archiving, or two index
// generations writing under one bintrail_id — was discarded in silence.
func TestMergeResults_warnsOnDivergingDuplicate(t *testing.T) {
	live := row(1)
	archived := row(1)
	archived.RowAfter = map[string]any{"id": 7, "name": "MUTATED"}

	var out []ResultRow
	log := captureWarn(t, func() {
		out = MergeResults([]ResultRow{live, archived}, 0, "ASC")
	})

	if len(out) != 1 {
		t.Fatalf("dedup broken: got %d rows", len(out))
	}
	// First-seen still wins — this adds visibility, it does not change which
	// copy is returned. Changing that silently would be a worse bug than the
	// silence.
	if got, _ := out[0].RowAfter["name"].(string); got != "alice" {
		t.Errorf("the first-seen copy must still win, got %q", got)
	}
	if !strings.Contains(log, "disagree") {
		t.Errorf("a diverging duplicate was discarded silently:\n%s", log)
	}
	// The warning has to be actionable: the event alone does not tell an
	// operator where to look.
	for _, want := range []string{"event_id=1", "schema=app", "table=users"} {
		if !strings.Contains(log, want) {
			t.Errorf("warning lacks %q:\n%s", want, log)
		}
	}
}

// The invariant that keeps this from becoming noise: identical copies, and
// copies differing only in a column the archive predates, must stay silent.
func TestMergeResults_silentOnAgreeingDuplicates(t *testing.T) {
	id := uint32(99)
	txt := "UPDATE users SET name='alice'"
	commit := uint64(1754000000000000)

	live := row(2)
	live.ConnectionID = &id
	live.QueryText = &txt
	live.CommitTsUS = &commit

	// A pre-#699/#701/#18 archive loads those columns as NULL. Reporting
	// that as divergence would fire on every legacy archive in the fleet —
	// the cry-wolf failure that makes operators mute a warning for good.
	legacyArchive := row(2)

	identical := row(2)

	for _, tc := range []struct {
		name string
		rows []ResultRow
	}{
		{"identical copies", []ResultRow{live, live}},
		{"legacy archive missing later columns", []ResultRow{live, legacyArchive}},
		{"reverse order (archive first)", []ResultRow{legacyArchive, live}},
		{"no optional columns anywhere", []ResultRow{identical, identical}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := captureWarn(t, func() { MergeResults(tc.rows, 0, "ASC") })
			if strings.Contains(log, "disagree") {
				t.Errorf("agreeing copies produced a divergence warning:\n%s", log)
			}
		})
	}
}

// Both sides of the merge decode through UnmarshalRowImage, which sets
// dec.UseNumber(), so a number is json.Number on the index side AND on the
// archive side. An earlier version of this test asserted int64 vs float64 was
// not divergence — a pair production cannot produce, so it passed while
// proving nothing about the real path.
func TestMergeResults_decodedJSONTypesAgree(t *testing.T) {
	live := row(3)
	live.RowAfter = map[string]any{"id": json.Number("7"), "amount": json.Number("10.00"), "tags": []any{"a", "b"}}
	archived := row(3)
	archived.RowAfter = map[string]any{"id": json.Number("7"), "amount": json.Number("10.00"), "tags": []any{"a", "b"}}

	log := captureWarn(t, func() { MergeResults([]ResultRow{live, archived}, 0, "ASC") })
	if strings.Contains(log, "disagree") {
		t.Errorf("identically decoded row images were reported as divergence:\n%s", log)
	}
}

// A nested JSON array or object in a row image makes == on `any` PANIC
// ("comparing uncomparable type"). This is the reason the comparison is
// reflect.DeepEqual and not ==, and it is worth a test because a future
// "simplification" back to == would not fail any other one here.
func TestMergeResults_nestedJSONDoesNotPanic(t *testing.T) {
	live := row(4)
	live.RowAfter = map[string]any{"meta": map[string]any{"tags": []any{"x"}}}
	archived := row(4)
	archived.RowAfter = map[string]any{"meta": map[string]any{"tags": []any{"y"}}}

	log := captureWarn(t, func() { MergeResults([]ResultRow{live, archived}, 0, "ASC") })
	if !strings.Contains(log, "disagree") {
		t.Errorf("a nested-JSON divergence was not detected:\n%s", log)
	}
}

// %v-based comparison erased type: json.Number("7") and "7" both rendered as
// 7, true and "true" both as true. In MySQL JSON those are different values,
// and "re-marshalled by a different generation of writer" is one of the two
// causes this warning names — so the comparison was blind to a case it exists
// to catch.
func TestMergeResults_typeChangeIsDivergence(t *testing.T) {
	for _, tc := range []struct {
		name string
		a, b any
	}{
		{"number vs string", json.Number("7"), "7"},
		{"bool vs string", true, "true"},
		{"number vs bool", json.Number("1"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			live, archived := row(5), row(5)
			live.RowAfter = map[string]any{"v": tc.a}
			archived.RowAfter = map[string]any{"v": tc.b}
			log := captureWarn(t, func() { MergeResults([]ResultRow{live, archived}, 0, "ASC") })
			if !strings.Contains(log, "disagree") {
				t.Errorf("a type change was not detected as divergence:\n%s", log)
			}
		})
	}
}

// sameEvent has a dozen divergence branches and the suite exercised exactly
// one (RowAfter). Deleting `a.EndPos != b.EndPos` from the condition broke
// nothing. One mutation per field, per the project's own lesson that a helper
// must be mutated at every site rather than once.
func TestMergeResults_everyComparedFieldDetectsDivergence(t *testing.T) {
	id1, id2 := uint32(1), uint32(2)
	s1, s2 := "a", "b"
	c1, c2 := uint64(1), uint64(2)
	for _, tc := range []struct {
		name   string
		mutate func(*ResultRow)
	}{
		{"BinlogFile", func(r *ResultRow) { r.BinlogFile = "bin.000009" }},
		{"StartPos", func(r *ResultRow) { r.StartPos = 999 }},
		{"EndPos", func(r *ResultRow) { r.EndPos = 999 }},
		{"EventTimestamp", func(r *ResultRow) { r.EventTimestamp = r.EventTimestamp.Add(time.Hour) }},
		{"SchemaName", func(r *ResultRow) { r.SchemaName = "other" }},
		{"TableName", func(r *ResultRow) { r.TableName = "other" }},
		{"EventType", func(r *ResultRow) { r.EventType = 3 }},
		{"PKValues", func(r *ResultRow) { r.PKValues = "8" }},
		{"ChangedColumns", func(r *ResultRow) { r.ChangedColumns = []string{"name"} }},
		{"RowBefore", func(r *ResultRow) { r.RowBefore = map[string]any{"id": json.Number("9")} }},
		{"RowAfter", func(r *ResultRow) { r.RowAfter = map[string]any{"id": json.Number("9")} }},
		// The optional columns divergence only when BOTH sides are present.
		{"GTID both present", func(r *ResultRow) { r.GTID = &s2 }},
		{"QueryText both present", func(r *ResultRow) { r.QueryText = &s2 }},
		{"QueryHash both present", func(r *ResultRow) { r.QueryHash = &s2 }},
		{"ConnectionID both present", func(r *ResultRow) { r.ConnectionID = &id2 }},
		{"CommitTsUS both present", func(r *ResultRow) { r.CommitTsUS = &c2 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			live := row(6)
			live.GTID, live.QueryText, live.QueryHash = &s1, &s1, &s1
			live.ConnectionID, live.CommitTsUS = &id1, &c1
			archived := live
			tc.mutate(&archived)
			log := captureWarn(t, func() { MergeResults([]ResultRow{live, archived}, 0, "ASC") })
			if !strings.Contains(log, "disagree") {
				t.Errorf("a divergence in %s was not detected:\n%s", tc.name, log)
			}
		})
	}
}

// A systematically corrupt archive must not flood the log and bury the first
// report, which is the one an operator reads.
func TestMergeResults_divergenceLogIsBounded(t *testing.T) {
	var rows []ResultRow
	for i := uint64(100); i <= 139; i++ {
		a, b := row(i), row(i)
		b.RowAfter = map[string]any{"id": json.Number("7"), "name": "MUTATED"}
		rows = append(rows, a, b)
	}
	log := captureWarn(t, func() { MergeResults(rows, 0, "ASC") })
	if n := strings.Count(log, "disagree"); n > maxDivergenceReports {
		t.Errorf("logged %d individual divergences, cap is %d", n, maxDivergenceReports)
	}
	// ...but the total must still be reported, or the cap hides the scale.
	if !strings.Contains(log, "diverged_total=40") {
		t.Errorf("the suppressed tail was not summarised:\n%s", log)
	}
}

// #1325: the count is the merge layer's RESPONSE-LEVEL signal — the log line
// above dies in the daemon log, and the console/MCP surfaces build their
// warnings from this number. One count per diverging duplicate; agreeing
// duplicates and legacy-archive NULLs stay zero (the cry-wolf rule), and the
// wrapper must return byte-identical rows so no call site changes behavior.
func TestMergeResultsReport_countsDivergingDuplicates(t *testing.T) {
	agreeA, agreeB := row(10), row(10)
	divLiveA, divArchA := row(11), row(11)
	divArchA.RowAfter = map[string]any{"id": 7, "name": "MUTATED"}
	divLiveB, divArchB := row(12), row(12)
	divArchB.RowBefore = map[string]any{"id": 7, "name": "old"}

	in := []ResultRow{agreeA, agreeB, divLiveA, divArchA, divLiveB, divArchB}
	var merged []ResultRow
	var diverged int
	captureWarn(t, func() { merged, diverged = MergeResultsReport(in, 0, "ASC") })
	if diverged != 2 {
		t.Errorf("diverged = %d, want 2 (one per disagreeing duplicate, none for the agreeing pair)", diverged)
	}
	if len(merged) != 3 {
		t.Errorf("dedup broken: got %d rows, want 3", len(merged))
	}

	captureWarn(t, func() { _, diverged = MergeResultsReport([]ResultRow{agreeA, agreeB}, 0, "ASC") })
	if diverged != 0 {
		t.Errorf("agreeing duplicates reported diverged = %d, want 0", diverged)
	}

	// The wrapper is the compatibility contract: same rows, count discarded.
	var viaWrapper []ResultRow
	captureWarn(t, func() { viaWrapper = MergeResults(append([]ResultRow(nil), in...), 0, "ASC") })
	if len(viaWrapper) != len(merged) {
		t.Errorf("MergeResults returned %d rows, MergeResultsReport %d — the wrapper must be behavior-identical", len(viaWrapper), len(merged))
	}
}

// NOTE: captureWarn swaps the process-global slog.Default. Safe today because
// these tests run sequentially; it would race the moment anything in package
// query calls t.Parallel().

// MergeAndTrimReport must propagate the count on BOTH branches: the plain
// merge, and the limitPerPK path whose DESC re-sort runs a second (already
// deduplicated) merge that must not zero the finding out.
func TestMergeAndTrimReport_propagatesCount(t *testing.T) {
	mk := func() []ResultRow {
		live, arch := row(20), row(20)
		arch.RowAfter = map[string]any{"id": 7, "name": "MUTATED"}
		return []ResultRow{live, arch}
	}
	for _, tc := range []struct {
		name       string
		limitPerPK int
		order      string
	}{
		{"plain merge", 0, "ASC"},
		{"limitPerPK", 1, "ASC"},
		{"limitPerPK with DESC re-sort", 1, "DESC"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var diverged int
			captureWarn(t, func() { _, diverged = MergeAndTrimReport(mk(), 0, tc.limitPerPK, tc.order) })
			if diverged != 1 {
				t.Errorf("diverged = %d, want 1", diverged)
			}
		})
	}
}
