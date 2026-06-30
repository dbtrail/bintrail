package main

import "testing"

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
