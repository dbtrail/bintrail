package main

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/verify"
)

// TestTableFilter covers the nil-vs-empty semantics: no filter means "verify
// everything" (nil map, never restricts), a non-empty filter tracks what it
// has and hasn't matched via the mutable seen copy.
func TestTableFilter(t *testing.T) {
	if filter, seen := tableFilter(nil); filter != nil || seen != nil {
		t.Fatalf("no filter: got filter=%v seen=%v, want both nil", filter, seen)
	}
	filter, seen := tableFilter([]string{"wp.posts", "wp.users"})
	if len(filter) != 2 || !filter["wp.posts"] || !filter["wp.users"] {
		t.Fatalf("filter = %v, want both entries set", filter)
	}
	delete(seen, "wp.posts")
	if len(seen) != 1 || !seen["wp.users"] {
		t.Fatalf("seen after delete = %v, want only wp.users left", seen)
	}
}

// TestToWireResult covers the field mapping from the engine's TableResult to
// the console DTO, including the explainable flag being caller-controlled
// (not derived from Status alone — a live-source mismatch must never claim
// explainable, since the engine has no explain support for that mode).
func TestToWireResult(t *testing.T) {
	res := verify.TableResult{
		Schema: "wp", Table: "posts", Status: verify.StatusMismatch, Detail: "row count differs",
		SourceRows: 10, ReconstructRows: 9, Anchor: "mysql-bin.000123:456",
	}
	got := toWireResult(res, true)
	want := console.VerifyTableResult{
		Schema: "wp", Table: "posts", Status: "mismatch", Detail: "row count differs",
		SourceRows: 10, ReconstructRows: 9, Anchor: "mysql-bin.000123:456", Explainable: true,
	}
	if got != want {
		t.Errorf("toWireResult = %+v, want %+v", got, want)
	}
	if got := toWireResult(res, false); got.Explainable {
		t.Error("explainable must be exactly what the caller passed, not re-derived from Status")
	}
}

// TestIndexDBName covers the tolerant DSN-parse-failure handling (mirrors
// internal/cli/verify.go: a bad DSN never crashes DB-name resolution, it's
// simply left empty).
func TestIndexDBName(t *testing.T) {
	if got := indexDBName("idx:pw@tcp(127.0.0.1:3306)/binlog_index"); got != "binlog_index" {
		t.Errorf("indexDBName = %q, want binlog_index", got)
	}
	if got := indexDBName("not a dsn"); got != "" {
		t.Errorf("indexDBName(invalid) = %q, want empty", got)
	}
}
