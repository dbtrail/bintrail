package verify

import "testing"

// TestSortBaselinePairs pins the ordering the JSON report's `explain[]` array
// inherits. FindBaselinePair builds `pairs` by ranging a map, so without this
// sort two identical `verify --explain --format json` runs can emit the same
// drill-downs in different positions while `tables[]` (sorted in NewReport)
// stays put. Input below is deliberately reverse-ordered and mixes schemas so a
// no-op sort fails.
func TestSortBaselinePairs(t *testing.T) {
	pairs := []BaselinePair{
		{Schema: "shop", Table: "orders"},
		{Schema: "shop", Table: "customers"},
		{Schema: "analytics", Table: "sessions"},
		{Schema: "shop", Table: "audit"},
		{Schema: "analytics", Table: "events"},
	}
	sortBaselinePairs(pairs)

	want := []string{
		"analytics.events",
		"analytics.sessions",
		"shop.audit",
		"shop.customers",
		"shop.orders",
	}
	for i, w := range want {
		if got := pairs[i].Schema + "." + pairs[i].Table; got != w {
			t.Errorf("pairs[%d] = %q, want %q", i, got, w)
		}
	}
}

// TestSortBaselinePairsEmpty: nil and single-element inputs must not panic —
// FindBaselinePair returns a nil slice on the "fewer than two snapshots" path.
func TestSortBaselinePairsEmpty(t *testing.T) {
	sortBaselinePairs(nil)
	one := []BaselinePair{{Schema: "db", Table: "t"}}
	sortBaselinePairs(one)
	if len(one) != 1 || one[0].Table != "t" {
		t.Errorf("single-element sort mangled the slice: %+v", one)
	}
}
