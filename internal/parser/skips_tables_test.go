package parser

import (
	"encoding/json"
	"testing"
)

// ─── Per-table skip attribution (#1296) ───────────────────────────────────────
//
// The capture-degraded verdict could say events were dropped but not WHICH
// table's — the first question an operator asks, and one only the daemon log
// could answer. These pin the ledger half of that fix, including the two
// properties a persisted, monotonic document needs: a bound, and a way to tell
// a capped list from a complete one.

func TestRecordSkipAttributed_recordsTheTable(t *testing.T) {
	c := NewSkipCounters(nil)
	c.RecordSkipAttributed(SkipTableNotInSnapshot, SkipAttribution{
		File: "binlog.000042", Pos: 512, Schema: "shop", Table: "plugin_log",
	})
	c.RecordSkipAttributed(SkipTableNotInSnapshot, SkipAttribution{
		File: "binlog.000042", Pos: 700, Schema: "shop", Table: "plugin_meta",
	})
	// The same table again must not appear twice.
	c.RecordSkipAttributed(SkipTableNotInSnapshot, SkipAttribution{
		File: "binlog.000042", Pos: 900, Schema: "shop", Table: "plugin_log",
	})

	st := decodeLedger(t, c)[SkipTableNotInSnapshot]
	if st.Count != 3 {
		t.Errorf("count = %d, want 3", st.Count)
	}
	want := []string{"shop.plugin_log", "shop.plugin_meta"}
	if len(st.Tables) != len(want) {
		t.Fatalf("tables = %v, want %v", st.Tables, want)
	}
	for i, w := range want {
		if st.Tables[i] != w {
			t.Errorf("tables[%d] = %q, want %q", i, st.Tables[i], w)
		}
	}
	if st.TablesTruncated {
		t.Error("two tables under the cap must not be flagged truncated")
	}
}

func TestRecordSkipAttributed_capsAndFlagsTheTableList(t *testing.T) {
	c := NewSkipCounters(nil)
	for i := range MaxLedgerTables + 5 {
		c.RecordSkipAttributed(SkipTableNotInSnapshot, SkipAttribution{
			Schema: "shop", Table: string(rune('a'+i)) + "_tbl",
		})
	}
	st := decodeLedger(t, c)[SkipTableNotInSnapshot]
	if len(st.Tables) != MaxLedgerTables {
		t.Errorf("tables kept %d entries, want the cap %d — this document is persisted and monotonic", len(st.Tables), MaxLedgerTables)
	}
	if !st.TablesTruncated {
		t.Error("a capped list must be flagged, or it reads as the complete set")
	}
}

// The exclusion reason is what tells an operator their table needs a primary
// key rather than another snapshot.
func TestRecordSkipAttributed_keepsTheDetail(t *testing.T) {
	c := NewSkipCounters(nil)
	c.RecordSkipAttributed(SkipTableExcludedFromSnapshot, SkipAttribution{
		Schema: "shop", Table: "audit_raw", Detail: "no explicit primary key",
	})
	st := decodeLedger(t, c)[SkipTableExcludedFromSnapshot]
	if st.LastDetail != "no explicit primary key" {
		t.Errorf("last_detail = %q, want the exclusion reason", st.LastDetail)
	}
}

// The two absent-table causes must stay under separate reasons: merged, half
// the operators get a remedy that can never converge.
func TestRecordSkip_excludedAndMissingAreDistinctReasons(t *testing.T) {
	c := NewSkipCounters(nil)
	c.RecordSkipAttributed(SkipTableNotInSnapshot, SkipAttribution{Schema: "a", Table: "b"})
	c.RecordSkipAttributed(SkipTableExcludedFromSnapshot, SkipAttribution{Schema: "c", Table: "d"})
	m := decodeLedger(t, c)
	if m[SkipTableNotInSnapshot].Count != 1 || m[SkipTableExcludedFromSnapshot].Count != 1 {
		t.Errorf("the two causes must be counted apart: %v", m)
	}
	if SkipTableNotInSnapshot == SkipTableExcludedFromSnapshot {
		t.Fatal("the reason keys must differ")
	}
}

// A ledger written before this change has no tables key; seeding it must not
// invent one.
func TestSeed_legacyLedgerHasNoTables(t *testing.T) {
	c := NewSkipCounters(nil)
	if err := c.Seed(`{"table_not_in_snapshot":{"count":3,"last_at":"2026-08-04T19:49:33Z"}}`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	st := decodeLedger(t, c)[SkipTableNotInSnapshot]
	if len(st.Tables) != 0 {
		t.Errorf("a legacy ledger must carry no table names, got %v", st.Tables)
	}
}

func decodeLedger(t *testing.T, c *SkipCounters) map[string]SkipStat {
	t.Helper()
	raw, err := c.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	m := map[string]SkipStat{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("unmarshal %s: %v", raw, err)
	}
	return m
}
