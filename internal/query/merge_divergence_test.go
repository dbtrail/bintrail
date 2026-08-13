package query

import (
	"bytes"
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

// Row images cross the archive as JSON, so the same number comes back as a
// different Go type on each side. A == on `any` would call every duplicate
// row a divergence.
func TestMergeResults_jsonRoundTripIsNotDivergence(t *testing.T) {
	live := row(3)
	live.RowAfter = map[string]any{"id": int64(7), "amount": 10.0}
	archived := row(3)
	archived.RowAfter = map[string]any{"id": float64(7), "amount": float64(10)}

	log := captureWarn(t, func() { MergeResults([]ResultRow{live, archived}, 0, "ASC") })
	if strings.Contains(log, "disagree") {
		t.Errorf("a JSON type round-trip was reported as divergence:\n%s", log)
	}
}

// A systematically corrupt archive must not flood the log and bury the first
// report, which is the one an operator reads.
func TestMergeResults_divergenceLogIsBounded(t *testing.T) {
	var rows []ResultRow
	for i := uint64(1); i <= 40; i++ {
		a, b := row(i), row(i)
		b.RowAfter = map[string]any{"id": 7, "name": "MUTATED"}
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
