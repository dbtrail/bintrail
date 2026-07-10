package metadata

import "testing"

// TestAllTables verifies the full-universe enumeration is complete and
// deterministically sorted by schema then table (map iteration order must
// never leak into report ordering).
func TestAllTables(t *testing.T) {
	r := NewResolverFromTables(1, map[string]*TableMeta{
		"beta.users":    {Schema: "beta", Table: "users"},
		"alpha.orders":  {Schema: "alpha", Table: "orders"},
		"alpha.invoice": {Schema: "alpha", Table: "invoice"},
	})
	got := r.AllTables()
	want := []struct{ schema, table string }{
		{"alpha", "invoice"}, {"alpha", "orders"}, {"beta", "users"},
	}
	if len(got) != len(want) {
		t.Fatalf("AllTables returned %d tables, want %d", len(got), len(want))
	}
	for i, w := range want {
		if got[i].Schema != w.schema || got[i].Table != w.table {
			t.Errorf("AllTables[%d] = %s.%s, want %s.%s", i, got[i].Schema, got[i].Table, w.schema, w.table)
		}
	}
	if n := len(NewResolverFromTables(1, nil).AllTables()); n != 0 {
		t.Errorf("empty resolver: got %d tables, want 0", n)
	}
}
