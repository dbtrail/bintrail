package shim

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// ─── _flashback ──────────────────────────────────────────────────────────────

func TestParseFlashbackHappyPath(t *testing.T) {
	q, err := Parse(
		"SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00' WHERE id = 12345",
		"myapp",
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q.Type != TypeFlashback {
		t.Errorf("Type = %v, want TypeFlashback", q.Type)
	}
	if q.Schema != "myapp" || q.Table != "orders" || q.PKColumn != "id" || q.PKValue != "12345" {
		t.Errorf("unexpected: %+v", q)
	}
	want := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	if !q.AsOf.Equal(want) {
		t.Errorf("AsOf = %v, want %v", q.AsOf, want)
	}
}

// TestParseFlashbackFullTable pins the WHERE-less shape introduced
// for full-table reconstruction (issue #276). The PK fields are empty
// so the handler can dispatch on q.PKColumn == "" without parsing the
// SQL again.
func TestParseFlashbackFullTable(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		{"bare", "SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00'"},
		{"trailing_semicolon", "SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00';"},
		{"lower_case", "select * from _flashback.orders as of '2026-05-02 10:00:00'"},
		{"snapshot_variant", "SELECT * FROM _snapshot.orders AS OF '2026-05-02 10:00:00'"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q, err := Parse(tc.sql, "myapp")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if q.Table != "orders" {
				t.Errorf("Table = %q, want %q", q.Table, "orders")
			}
			if q.PKColumn != "" || q.PKValue != "" {
				t.Errorf("PKColumn/PKValue must be empty for full-table shape; got col=%q val=%q",
					q.PKColumn, q.PKValue)
			}
		})
	}
}

func TestParseFlashbackCaseInsensitive(t *testing.T) {
	q, err := Parse(
		"select * from _flashback.users as of '2026-01-01' where email = 'a@b.com'",
		"myapp",
	)
	if err != nil {
		t.Fatal(err)
	}
	if q.Type != TypeFlashback || q.PKValue != "a@b.com" {
		t.Errorf("unexpected: %+v", q)
	}
}

// ─── _snapshot ───────────────────────────────────────────────────────────────

func TestParseSnapshotHappyPath(t *testing.T) {
	q, err := Parse(
		"SELECT * FROM _snapshot.orders AS OF '2026-05-02 10:00:00' WHERE id = 1",
		"myapp",
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q.Type != TypeSnapshot {
		t.Errorf("Type = %v, want TypeSnapshot", q.Type)
	}
	if q.Table != "orders" {
		t.Errorf("Table = %q", q.Table)
	}
}

// ─── _diff ───────────────────────────────────────────────────────────────────

func TestParseDiffHappyPath(t *testing.T) {
	q, err := Parse(
		"SELECT * FROM _diff.orders BETWEEN '2026-05-01 00:00:00' AND '2026-05-02 00:00:00' WHERE id = 42",
		"myapp",
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q.Type != TypeDiff {
		t.Errorf("Type = %v, want TypeDiff", q.Type)
	}
	wantSince := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	wantUntil := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	if !q.Since.Equal(wantSince) || !q.Until.Equal(wantUntil) {
		t.Errorf("Since=%v, Until=%v, want %v..%v", q.Since, q.Until, wantSince, wantUntil)
	}
	if q.PKColumn != "id" || q.PKValue != "42" {
		t.Errorf("PK = %s=%q", q.PKColumn, q.PKValue)
	}
	if !q.AsOf.IsZero() {
		t.Errorf("AsOf should be zero for _diff, got %v", q.AsOf)
	}
}

func TestParseDiffRejectsReversedRange(t *testing.T) {
	_, err := Parse(
		"SELECT * FROM _diff.t BETWEEN '2026-05-02' AND '2026-05-01' WHERE id = 1",
		"myapp",
	)
	if err == nil {
		t.Fatal("expected error for reversed BETWEEN bounds")
	}
	if !strings.Contains(err.Error(), "out of order") {
		t.Errorf("error = %v, want 'out of order'", err)
	}
}

// ─── shared error paths ──────────────────────────────────────────────────────

func TestParseAcceptsTrailingSemicolon(t *testing.T) {
	_, err := Parse(
		"SELECT * FROM _flashback.t AS OF '2026-05-02 10:00:00' WHERE id = 1;",
		"myapp",
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestParseAcceptsRFC3339Timestamp(t *testing.T) {
	q, err := Parse(
		"SELECT * FROM _flashback.t AS OF '2026-05-02T10:00:00Z' WHERE id = 1",
		"myapp",
	)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	if !q.AsOf.Equal(want) {
		t.Errorf("AsOf = %v, want %v", q.AsOf, want)
	}
}

func TestParseDateOnly(t *testing.T) {
	q, err := Parse(
		"SELECT * FROM _flashback.t AS OF '2026-05-02' WHERE id = 1",
		"myapp",
	)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	if !q.AsOf.Equal(want) {
		t.Errorf("AsOf = %v, want %v", q.AsOf, want)
	}
}

func TestParseStringPK(t *testing.T) {
	q, err := Parse(
		"SELECT * FROM _flashback.users AS OF '2026-05-02' WHERE uuid = 'abc-123'",
		"myapp",
	)
	if err != nil {
		t.Fatal(err)
	}
	if q.PKValue != "abc-123" {
		t.Errorf("PKValue = %q, want abc-123", q.PKValue)
	}
}

func TestParseNegativePK(t *testing.T) {
	q, err := Parse(
		"SELECT * FROM _flashback.t AS OF '2026-05-02' WHERE id = -42",
		"myapp",
	)
	if err != nil {
		t.Fatal(err)
	}
	if q.PKValue != "-42" {
		t.Errorf("PKValue = %q, want -42", q.PKValue)
	}
}

func TestParseNotTimeTravelReturnsSentinel(t *testing.T) {
	cases := []string{
		"SELECT * FROM orders WHERE id = 1",
		"SELECT 1",
		"",
		"   ",
		"SHOW TABLES",
	}
	for _, sql := range cases {
		_, err := Parse(sql, "myapp")
		if !errors.Is(err, ErrNotTimeTravel) {
			t.Errorf("Parse(%q) error = %v, want ErrNotTimeTravel", sql, err)
		}
	}
}

func TestParseMalformedTimeTravelErrors(t *testing.T) {
	cases := []struct {
		sql     string
		wantSub string
	}{
		{
			"SELECT * FROM _flashback.orders WHERE id = 1",
			"malformed time-travel",
		},
		{
			"SELECT * FROM _diff.orders WHERE id = 1",
			"malformed time-travel",
		},
		{
			"SELECT * FROM _snapshot.orders AS OF 'not-a-time' WHERE id = 1",
			"invalid AS OF timestamp",
		},
		{
			"SELECT * FROM _diff.orders BETWEEN 'bad' AND '2026-05-02' WHERE id = 1",
			"invalid BETWEEN lower bound",
		},
		{
			"SELECT * FROM _diff.orders BETWEEN '2026-05-01' AND 'bad' WHERE id = 1",
			"invalid BETWEEN upper bound",
		},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			_, err := Parse(tc.sql, "myapp")
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %v, want containing %q", err, tc.wantSub)
			}
		})
	}
}

func TestParseRequiresSchema(t *testing.T) {
	_, err := Parse(
		"SELECT * FROM _flashback.t AS OF '2026-05-02' WHERE id = 1",
		"",
	)
	if err == nil {
		t.Fatal("expected error when defaultSchema is empty")
	}
	if !strings.Contains(err.Error(), "no schema selected") {
		t.Errorf("error = %v, want hint about USE", err)
	}
}

func TestQueryTypeString(t *testing.T) {
	cases := map[QueryType]string{
		TypeFlashback: "_flashback",
		TypeSnapshot:  "_snapshot",
		TypeDiff:      "_diff",
	}
	for tt, want := range cases {
		if got := tt.String(); got != want {
			t.Errorf("QueryType(%d).String() = %q, want %q", tt, got, want)
		}
	}
}
