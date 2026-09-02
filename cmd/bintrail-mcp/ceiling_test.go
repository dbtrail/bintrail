package main

import (
	"strings"
	"testing"
)

func TestQueryResultNotice(t *testing.T) {
	t.Run("ceiling supersedes the generic truncation notice", func(t *testing.T) {
		// After a cap, n == limit (== ceiling), so the generic arm would also
		// match; the ceiling message must win and must NOT say "increase the limit".
		got := queryResultNotice(true, 5_000_000, 1_000_000, 1_000_000, 1_000_000, 0, "")
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
		got := queryResultNotice(false, 100, 1_000_000, 100, 100, 0, "")
		if !strings.Contains(got, "increase the limit") {
			t.Errorf("want generic truncation notice, got %q", got)
		}
		if strings.Contains(got, "ceiling") {
			t.Errorf("must not mention ceiling when no cap fired: %q", got)
		}
	})

	// The truncation notice names which END the trim kept (#1439): a client
	// that asked for the last N must be able to tell whether the newest
	// events survived the cut. Both directions asserted, and asserted
	// disjoint, so swapping the arms cannot pass.
	t.Run("truncation names the kept end per direction", func(t *testing.T) {
		asc := queryResultNotice(false, 100, 1_000_000, 100, 100, 0, "ASC")
		if !strings.Contains(asc, "OLDEST") || strings.Contains(asc, "NEWEST") {
			t.Errorf("ASC truncation must say it kept the OLDEST events: %q", asc)
		}
		empty := queryResultNotice(false, 100, 1_000_000, 100, 100, 0, "")
		if !strings.Contains(empty, "OLDEST") {
			t.Errorf("the empty default is ASC and must say OLDEST: %q", empty)
		}
		desc := queryResultNotice(false, 100, 1_000_000, 100, 100, 0, "desc")
		if !strings.Contains(desc, "NEWEST") || strings.Contains(desc, "OLDEST") {
			t.Errorf("DESC truncation (case-insensitive) must say it kept the NEWEST events: %q", desc)
		}
	})

	// Under limit_per_pk the cut depends on the direction. ASC (or empty):
	// the outer limit cuts the newest end while the per-PK cap cuts old
	// events per row, so both ends — naming a kept end would lie. DESC: the
	// globally newest event is always bt_rn=1 in its own partition and row 0
	// of the page, so the newest end is provably INTACT and the notice must
	// keep saying so (claiming it was dropped is the same defect class).
	t.Run("limit_per_pk states the double cut under ASC", func(t *testing.T) {
		for _, order := range []string{"", "ASC"} {
			got := queryResultNotice(false, 100, 1_000_000, 100, 100, 5, order)
			if strings.Contains(got, "OLDEST") || strings.Contains(got, "NEWEST") {
				t.Errorf("order=%q: a per-PK-capped ASC truncation named a kept end: %q", order, got)
			}
			if !strings.Contains(got, "both ends") || !strings.Contains(got, "latest 5 events per row") {
				t.Errorf("order=%q: the per-PK arm does not state the double cut: %q", order, got)
			}
		}
	})
	t.Run("limit_per_pk under DESC keeps the NEWEST claim and names the interior drop", func(t *testing.T) {
		got := queryResultNotice(false, 100, 1_000_000, 100, 100, 5, "DESC")
		if !strings.Contains(got, "NEWEST") {
			t.Errorf("DESC + per-PK cap must still claim the newest end (it is provably intact): %q", got)
		}
		if strings.Contains(got, "both ends") {
			t.Errorf("DESC + per-PK cap claimed a cut at the newest end that never happens: %q", got)
		}
		if !strings.Contains(got, "latest 5 events per row") || !strings.Contains(got, "inside the window") {
			t.Errorf("DESC + per-PK cap does not name the interior drop: %q", got)
		}
	})

	t.Run("no notice below the limit", func(t *testing.T) {
		if got := queryResultNotice(false, 100, 1_000_000, 42, 100, 0, "DESC"); got != "" {
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
