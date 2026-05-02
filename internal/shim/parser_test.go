package shim

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestParseHappyPath(t *testing.T) {
	q, err := Parse(
		"SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00' WHERE id = 12345",
		"myapp",
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q.Schema != "myapp" {
		t.Errorf("Schema = %q, want %q", q.Schema, "myapp")
	}
	if q.Table != "orders" {
		t.Errorf("Table = %q, want %q", q.Table, "orders")
	}
	want := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	if !q.AsOf.Equal(want) {
		t.Errorf("AsOf = %v, want %v", q.AsOf, want)
	}
	if q.PKColumn != "id" {
		t.Errorf("PKColumn = %q, want %q", q.PKColumn, "id")
	}
	if q.PKValue != "12345" {
		t.Errorf("PKValue = %q, want %q", q.PKValue, "12345")
	}
}

func TestParseCaseInsensitive(t *testing.T) {
	q, err := Parse(
		"select * from _flashback.users as of '2026-01-01' where email = 'a@b.com'",
		"myapp",
	)
	if err != nil {
		t.Fatal(err)
	}
	if q.Table != "users" || q.PKColumn != "email" || q.PKValue != "a@b.com" {
		t.Errorf("unexpected parse result: %+v", q)
	}
}

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
		t.Errorf("PKValue = %q, want %q", q.PKValue, "abc-123")
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
		t.Errorf("PKValue = %q, want %q", q.PKValue, "-42")
	}
}

func TestParseNotFlashbackReturnsSentinel(t *testing.T) {
	cases := []string{
		"SELECT * FROM orders WHERE id = 1",
		"SELECT 1",
		"",
		"   ",
		"SHOW TABLES",
	}
	for _, sql := range cases {
		_, err := Parse(sql, "myapp")
		if !errors.Is(err, ErrNotFlashback) {
			t.Errorf("Parse(%q) error = %v, want ErrNotFlashback", sql, err)
		}
	}
}

func TestParseMalformedFlashbackErrors(t *testing.T) {
	cases := []struct {
		sql     string
		wantSub string
	}{
		{
			"SELECT * FROM _flashback.orders WHERE id = 1",
			"malformed _flashback query",
		},
		{
			"SELECT * FROM _flashback.orders AS OF '2026-05-02' AND id = 1",
			"malformed _flashback query",
		},
		{
			"SELECT * FROM _flashback.orders AS OF 'not-a-time' WHERE id = 1",
			"invalid AS OF timestamp",
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
