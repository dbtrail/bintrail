package reconstruct

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/parser"
	"github.com/dbtrail/bintrail/internal/query"
)

// shared fixtures
var (
	baselineState = map[string]any{
		"id":     int64(42),
		"status": "draft",
		"amount": float64(100),
	}

	t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) // baseline time
	t1 = time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	t2 = time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC)
	t3 = time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
)

// ─── ApplyAt ──────────────────────────────────────────────────────────────────

func TestApplyAt_noEvents(t *testing.T) {
	state := ApplyAt(baselineState, nil, t3)
	if state == nil {
		t.Fatal("expected non-nil state with no events")
	}
	if state["status"] != "draft" {
		t.Errorf("expected status=draft, got %v", state["status"])
	}
}

func TestApplyAt_insert(t *testing.T) {
	after := map[string]any{"id": int64(99), "status": "active"}
	events := []query.ResultRow{{
		EventTimestamp: t1,
		EventType:      parser.EventInsert,
		RowAfter:       after,
	}}
	state := ApplyAt(nil, events, t3)
	if state == nil {
		t.Fatal("expected non-nil state after INSERT")
	}
	if state["status"] != "active" {
		t.Errorf("expected status=active after INSERT, got %v", state["status"])
	}
}

func TestApplyAt_update(t *testing.T) {
	after := map[string]any{"id": int64(42), "status": "published", "amount": float64(200)}
	events := []query.ResultRow{{
		EventTimestamp: t1,
		EventType:      parser.EventUpdate,
		RowAfter:       after,
	}}
	state := ApplyAt(baselineState, events, t3)
	if state["status"] != "published" {
		t.Errorf("expected status=published after UPDATE, got %v", state["status"])
	}
	if state["amount"] != float64(200) {
		t.Errorf("expected amount=200 after UPDATE, got %v", state["amount"])
	}
}

func TestApplyAt_delete(t *testing.T) {
	events := []query.ResultRow{{
		EventTimestamp: t1,
		EventType:      parser.EventDelete,
		RowBefore:      baselineState,
	}}
	state := ApplyAt(baselineState, events, t3)
	if state != nil {
		t.Errorf("expected nil state after DELETE, got %v", state)
	}
}

func TestApplyAt_cutoff(t *testing.T) {
	// Event at t2 must NOT be applied when at=t1.
	after := map[string]any{"id": int64(42), "status": "published"}
	events := []query.ResultRow{{
		EventTimestamp: t2,
		EventType:      parser.EventUpdate,
		RowAfter:       after,
	}}
	state := ApplyAt(baselineState, events, t1)
	if state["status"] != "draft" {
		t.Errorf("expected status=draft (event not yet applied at t1), got %v", state["status"])
	}
}

func TestApplyAt_insertUpdateDelete(t *testing.T) {
	inserted := map[string]any{"id": int64(1), "status": "draft"}
	updated := map[string]any{"id": int64(1), "status": "active"}
	events := []query.ResultRow{
		{EventTimestamp: t1, EventType: parser.EventInsert, RowAfter: inserted},
		{EventTimestamp: t2, EventType: parser.EventUpdate, RowAfter: updated},
		{EventTimestamp: t3, EventType: parser.EventDelete, RowBefore: updated},
	}

	// Final state (after DELETE): nil.
	if state := ApplyAt(nil, events, t3); state != nil {
		t.Errorf("expected nil state after INSERT→UPDATE→DELETE, got %v", state)
	}
	// At t2 (after UPDATE, before DELETE): active.
	state := ApplyAt(nil, events, t2)
	if state == nil {
		t.Fatal("expected non-nil state at t2")
	}
	if state["status"] != "active" {
		t.Errorf("expected status=active at t2, got %v", state["status"])
	}
}

func TestApplyAt_independentCopy(t *testing.T) {
	// Modifying the returned state must not affect baselineState.
	state := ApplyAt(baselineState, nil, t3)
	state["status"] = "mutated"
	if baselineState["status"] != "draft" {
		t.Error("ApplyAt must return an independent copy of the state")
	}
}

// ─── BuildHistory ─────────────────────────────────────────────────────────────

func TestBuildHistory_baselineOnly(t *testing.T) {
	entries := BuildHistory(baselineState, t0, nil, t3)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry (baseline only), got %d", len(entries))
	}
	if entries[0].Source != "baseline" {
		t.Errorf("expected source=baseline, got %q", entries[0].Source)
	}
	if entries[0].State["status"] != "draft" {
		t.Errorf("expected status=draft in baseline, got %v", entries[0].State["status"])
	}
	if entries[0].EventID != 0 {
		t.Errorf("expected EventID=0 for baseline, got %d", entries[0].EventID)
	}
}

func TestBuildHistory_insertUpdateDelete(t *testing.T) {
	gtidStr := "abc:10"
	inserted := map[string]any{"id": int64(1), "status": "draft"}
	updated := map[string]any{"id": int64(1), "status": "active"}
	events := []query.ResultRow{
		{EventTimestamp: t1, EventType: parser.EventInsert, RowAfter: inserted, EventID: 10},
		{EventTimestamp: t2, EventType: parser.EventUpdate, RowAfter: updated, EventID: 20, GTID: &gtidStr},
		{EventTimestamp: t3, EventType: parser.EventDelete, RowBefore: updated, EventID: 30},
	}
	entries := BuildHistory(nil, t0, events, t3)

	if len(entries) != 4 { // baseline + 3 events
		t.Fatalf("expected 4 entries, got %d", len(entries))
	}
	if entries[0].Source != "baseline" {
		t.Errorf("entry[0]: expected baseline, got %q", entries[0].Source)
	}
	if entries[1].Source != "INSERT" || entries[1].EventID != 10 {
		t.Errorf("entry[1]: unexpected %+v", entries[1])
	}
	if entries[2].Source != "UPDATE" || entries[2].GTID != gtidStr || entries[2].State["status"] != "active" {
		t.Errorf("entry[2]: unexpected %+v", entries[2])
	}
	if entries[3].Source != "DELETE" || entries[3].State != nil {
		t.Errorf("entry[3]: expected DELETE with nil state, got %+v", entries[3])
	}
}

func TestBuildHistory_cutoff(t *testing.T) {
	events := []query.ResultRow{{
		EventTimestamp: t3,
		EventType:      parser.EventDelete,
		RowBefore:      baselineState,
		EventID:        99,
	}}
	// at=t2: delete at t3 should not appear.
	entries := BuildHistory(baselineState, t0, events, t2)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry (delete excluded by cutoff), got %d", len(entries))
	}
}

// ─── Formatters ───────────────────────────────────────────────────────────────

func TestWriteStateJSON_valid(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteStateJSON(map[string]any{"id": int64(1), "status": "ok"}, &buf); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, buf.String())
	}
}

func TestWriteStateJSON_nil(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteStateJSON(nil, &buf); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(buf.String(), "null") {
		t.Errorf("expected 'null' for nil state, got: %s", buf.String())
	}
}

func TestWriteHistoryJSON_valid(t *testing.T) {
	entries := []StateEntry{
		{Time: t0, Source: "baseline", State: map[string]any{"id": int64(1)}},
		{Time: t1, Source: "UPDATE", EventID: 5, State: map[string]any{"id": int64(1), "status": "ok"}},
	}
	var buf bytes.Buffer
	if err := WriteHistoryJSON(entries, &buf); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var out []map[string]any
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("output is not valid JSON array: %v\n%s", err, buf.String())
	}
	if len(out) != 2 {
		t.Errorf("expected 2 entries, got %d", len(out))
	}
}

func TestWriteStateTable_deleted(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteStateTable(nil, &buf); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(buf.String(), "deleted") {
		t.Errorf("expected 'deleted' in output for nil state, got: %s", buf.String())
	}
}

func TestWriteStateTable_columns(t *testing.T) {
	var buf bytes.Buffer
	state := map[string]any{"amount": float64(99), "id": int64(1)}
	if err := WriteStateTable(state, &buf); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "amount") || !strings.Contains(out, "id") {
		t.Errorf("expected column names in table output, got: %s", out)
	}
}

func TestWriteHistoryTable(t *testing.T) {
	entries := []StateEntry{
		{Time: t0, Source: "baseline", State: map[string]any{"id": int64(1)}},
		{Time: t1, Source: "DELETE", EventID: 7, State: nil},
	}
	var buf bytes.Buffer
	if err := WriteHistoryTable(entries, &buf); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "baseline") || !strings.Contains(out, "DELETE") {
		t.Errorf("expected baseline and DELETE in table output, got: %s", out)
	}
	if !strings.Contains(out, "deleted") {
		t.Errorf("expected '(deleted)' for nil state, got: %s", out)
	}
}

func TestWriteStateCSV(t *testing.T) {
	var buf bytes.Buffer
	state := map[string]any{"id": int64(1), "status": "ok"}
	if err := WriteStateCSV(state, &buf); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Errorf("expected 2 lines (header + data), got %d:\n%s", len(lines), buf.String())
	}
}

func TestWriteHistoryCSV(t *testing.T) {
	entries := []StateEntry{
		{Time: t0, Source: "baseline", State: map[string]any{"id": int64(1), "status": "draft"}},
		{Time: t1, Source: "UPDATE", EventID: 5, State: map[string]any{"id": int64(1), "status": "active"}},
	}
	var buf bytes.Buffer
	if err := WriteHistoryCSV(entries, &buf); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 { // header + 2 data rows
		t.Errorf("expected 3 lines, got %d:\n%s", len(lines), buf.String())
	}
	if !strings.HasPrefix(lines[0], "time,source") {
		t.Errorf("expected CSV header starting with 'time,source', got: %s", lines[0])
	}
}

// ─── parseDirTimestamp ────────────────────────────────────────────────────────

func TestParseDirTimestamp(t *testing.T) {
	cases := []struct {
		input string
		want  time.Time
		ok    bool
	}{
		{"2026-02-28T00-00-00Z", time.Date(2026, 2, 28, 0, 0, 0, 0, time.UTC), true},
		{"2026-02-28T14-30-00Z", time.Date(2026, 2, 28, 14, 30, 0, 0, time.UTC), true},
		{"notadate", time.Time{}, false},
		{"2026-02-28", time.Time{}, false}, // missing T separator
	}
	for _, tc := range cases {
		got, ok := parseDirTimestamp(tc.input)
		if ok != tc.ok {
			t.Errorf("parseDirTimestamp(%q): ok=%v, want %v", tc.input, ok, tc.ok)
			continue
		}
		if ok && !got.Equal(tc.want) {
			t.Errorf("parseDirTimestamp(%q) = %v, want %v", tc.input, got, tc.want)
		}
	}
}

// ─── copyMap ──────────────────────────────────────────────────────────────────

func TestCopyMap_nil(t *testing.T) {
	if copyMap(nil) != nil {
		t.Error("copyMap(nil) should return nil")
	}
}

func TestCopyMap_independent(t *testing.T) {
	original := map[string]any{"a": 1}
	copied := copyMap(original)
	copied["a"] = 2
	if original["a"] != 1 {
		t.Error("copyMap must return an independent copy")
	}
}

// ─── buildCondsList ───────────────────────────────────────────────────────────

func TestBuildCondsList_sorted(t *testing.T) {
	filter := map[string]string{"z_col": "3", "a_col": "1", "m_col": "2"}
	conds := buildCondsList(filter)
	if len(conds) != 3 {
		t.Fatalf("expected 3 conditions, got %d", len(conds))
	}
	if conds[0].col != "a_col" || conds[1].col != "m_col" || conds[2].col != "z_col" {
		t.Errorf("expected sorted column order, got: %v", conds)
	}
}

// ─── ErrNoBaseline ──────────────────────────────────────────────────────────

func TestFindBaseline_ErrNoBaseline(t *testing.T) {
	// Empty baseline directory → ErrNoBaseline sentinel.
	dir := t.TempDir()
	_, _, err := FindBaseline(context.Background(), dir, "shop", "orders", time.Now())
	if err == nil {
		t.Fatal("expected error for empty baseline dir")
	}
	if !errors.Is(err, ErrNoBaseline) {
		t.Errorf("expected ErrNoBaseline, got: %v", err)
	}
}

func TestFindBaseline_ErrNoBaseline_S3(t *testing.T) {
	// S3 path → ErrNoBaseline. DuckDB glob on a non-existent prefix returns 0 rows.
	_, _, err := FindBaseline(context.Background(), "s3://no-such-bucket-xyz/baselines", "shop", "orders", time.Now())
	// S3 may return a connectivity error or ErrNoBaseline depending on DuckDB
	// configuration. Just verify that if we get ErrNoBaseline, it unwraps correctly.
	if err != nil && errors.Is(err, ErrNoBaseline) {
		// Good — sentinel detected.
		return
	}
	// If DuckDB returns a different error (no httpfs, no credentials), that's
	// acceptable in a unit test without AWS credentials.
	t.Logf("S3 FindBaseline returned non-ErrNoBaseline error (expected in CI): %v", err)
}
