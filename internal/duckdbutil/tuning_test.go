package duckdbutil

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// TestTuningConstructors pins the two named budgets so renaming or retuning
// them is a deliberate, test-visible change. DefaultTuning must reproduce the
// values parquetquery.Fetch hardcoded before this struct existed; Ultrafast
// leaves threads/memory unset (DuckDB self-tunes) and sets S3Direct (httpfs).
func TestTuningConstructors(t *testing.T) {
	if got := DefaultTuning(); got.Threads != 2 || got.MemoryLimit != "4GB" || got.S3Direct {
		t.Fatalf("DefaultTuning() = %+v, want {Threads:2 MemoryLimit:4GB S3Direct:false}", got)
	}
	if got := Ultrafast(); got != (Tuning{S3Direct: true}) {
		t.Fatalf("Ultrafast() = %+v, want {S3Direct:true} (threads/memory unset)", got)
	}
}

// TestTuningApplyDefault: the conservative budget reaches DuckDB. threads is
// the clean deterministic signal; memory_limit is only checked for presence
// here because DuckDB renders it in base-2 units ('4GB' = 4×10^9 B → "3.7 GiB"),
// which is awkward to pin — the override test below proves the value wires
// through with a clean GiB unit.
func TestTuningApplyDefault(t *testing.T) {
	db := openDuckDB(t)
	DefaultTuning().Apply(context.Background(), db)

	if got := currentSetting(t, db, "threads"); got != "2" {
		t.Fatalf("threads after DefaultTuning = %q, want 2", got)
	}
	if mem := currentSetting(t, db, "memory_limit"); mem == "" {
		t.Fatal("memory_limit empty after DefaultTuning; want it set")
	}
	assertUsable(t, db)
}

// TestTuningApplyUltrafastIsNoOp: an empty Tuning must emit no SET at all, so
// DuckDB keeps its own native defaults. Proven by capturing the threads
// setting before and after Apply and asserting it is unchanged — this does not
// assume how many cores the test host has.
func TestTuningApplyUltrafastIsNoOp(t *testing.T) {
	db := openDuckDB(t)

	threadsBefore := currentSetting(t, db, "threads")
	memBefore := currentSetting(t, db, "memory_limit")
	Ultrafast().Apply(context.Background(), db)

	if after := currentSetting(t, db, "threads"); after != threadsBefore {
		t.Fatalf("Ultrafast().Apply changed threads %q → %q; want it left at DuckDB's default", threadsBefore, after)
	}
	if after := currentSetting(t, db, "memory_limit"); after != memBefore {
		t.Fatalf("Ultrafast().Apply changed memory_limit %q → %q; want it left at DuckDB's default", memBefore, after)
	}
	assertUsable(t, db)
}

// TestTuningApplyExplicitOverride: an operator-supplied Tuning (the --duckdb-*
// flags) takes effect verbatim. Uses a base-2 GiB value so DuckDB's rendered
// form is exact and unit-stable ("2.0 GiB"), unlike a base-10 GB suffix.
func TestTuningApplyExplicitOverride(t *testing.T) {
	db := openDuckDB(t)
	Tuning{Threads: 8, MemoryLimit: "2GiB"}.Apply(context.Background(), db)

	if got := currentSetting(t, db, "threads"); got != "8" {
		t.Fatalf("threads after override = %q, want 8", got)
	}
	if mem := currentSetting(t, db, "memory_limit"); mem != "2.0 GiB" {
		t.Fatalf("memory_limit after override = %q, want \"2.0 GiB\"", mem)
	}
	assertUsable(t, db)
}

func openDuckDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func currentSetting(t *testing.T, db *sql.DB, name string) string {
	t.Helper()
	var v string
	if err := db.QueryRow("SELECT current_setting('" + name + "')").Scan(&v); err != nil {
		t.Fatalf("read current_setting(%q): %v", name, err)
	}
	return v
}

func assertUsable(t *testing.T, db *sql.DB) {
	t.Helper()
	var one int
	if err := db.QueryRow("SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("session unusable after Apply: %v (got %d)", err, one)
	}
}
