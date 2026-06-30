package main

import (
	"strings"
	"testing"
)

func TestQueryResultNotice(t *testing.T) {
	t.Run("ceiling supersedes the generic truncation notice", func(t *testing.T) {
		// After a cap, n == limit (== ceiling), so the generic arm would also
		// match; the ceiling message must win and must NOT say "increase the limit".
		got := queryResultNotice(true, 5_000_000, 1_000_000, 1_000_000, 1_000_000)
		if !strings.Contains(got, "exceeds the MCP query ceiling") {
			t.Errorf("want ceiling message, got %q", got)
		}
		if !strings.Contains(got, "bintrail query") {
			t.Errorf("want CLI escape-hatch hint, got %q", got)
		}
		if strings.Contains(got, "increase the limit") {
			t.Errorf("ceiling notice must not tell the agent to increase the limit: %q", got)
		}
	})

	t.Run("generic truncation notice when not capped and full", func(t *testing.T) {
		got := queryResultNotice(false, 100, 1_000_000, 100, 100)
		if !strings.Contains(got, "increase the limit") {
			t.Errorf("want generic truncation notice, got %q", got)
		}
		if strings.Contains(got, "ceiling") {
			t.Errorf("must not mention ceiling when no cap fired: %q", got)
		}
	})

	t.Run("no notice below the limit", func(t *testing.T) {
		if got := queryResultNotice(false, 100, 1_000_000, 42, 100); got != "" {
			t.Errorf("want empty notice for a partial result, got %q", got)
		}
	})
}

func TestApplyQueryCeiling(t *testing.T) {
	cases := []struct {
		name      string
		limit     int
		max       int
		wantLimit int
		wantCap   bool
	}{
		{"below ceiling unchanged", 500, 1000, 500, false},
		{"at ceiling unchanged", 1000, 1000, 1000, false},
		{"above ceiling capped", 5000, 1000, 1000, true},
		{"zero passes through (coercion runs earlier)", 0, 1000, 0, false},
		{"max<=0 disables capping", 9_999_999, 0, 9_999_999, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotLimit, gotCap := applyQueryCeiling(tc.limit, tc.max)
			if gotLimit != tc.wantLimit || gotCap != tc.wantCap {
				t.Fatalf("applyQueryCeiling(%d,%d) = (%d,%v), want (%d,%v)",
					tc.limit, tc.max, gotLimit, gotCap, tc.wantLimit, tc.wantCap)
			}
		})
	}
}

func TestMCPQueryMaxLimit(t *testing.T) {
	cases := []struct {
		name string
		env  string // "" = unset
		set  bool
		want int
	}{
		{"unset → default", "", false, defaultMCPQueryMaxLimit},
		{"valid override", "500000", true, 500_000},
		{"invalid → default", "abc", true, defaultMCPQueryMaxLimit},
		{"zero → default (not disengageable)", "0", true, defaultMCPQueryMaxLimit},
		{"negative → default", "-5", true, defaultMCPQueryMaxLimit},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.set {
				t.Setenv("BINTRAIL_MCP_QUERY_MAX_LIMIT", tc.env)
			} else {
				t.Setenv("BINTRAIL_MCP_QUERY_MAX_LIMIT", "")
			}
			if got := mcpQueryMaxLimit(); got != tc.want {
				t.Fatalf("mcpQueryMaxLimit() = %d, want %d", got, tc.want)
			}
		})
	}
}
