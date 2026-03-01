package main

import (
	"testing"
)

// ─── cobra command wiring ─────────────────────────────────────────────────────

func TestSnapshotCmd_registered(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "snapshot" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'snapshot' command to be registered under rootCmd")
	}
}

func TestSnapshotCmd_requiredFlags(t *testing.T) {
	for _, name := range []string{"source-dsn", "index-dsn"} {
		flag := snapshotCmd.Flag(name)
		if flag == nil {
			t.Fatalf("flag --%s not registered", name)
		}
		if flag.Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
			t.Errorf("flag --%s is not marked required", name)
		}
	}
}

func TestSnapshotCmd_allFlagsRegistered(t *testing.T) {
	for _, name := range []string{"source-dsn", "index-dsn", "schemas"} {
		if snapshotCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on snapshotCmd", name)
		}
	}
}

func TestSnapshotCmd_emptyStringDefault(t *testing.T) {
	f := snapshotCmd.Flag("schemas")
	if f == nil {
		t.Fatal("flag --schemas not registered")
	}
	if f.DefValue != "" {
		t.Errorf("flag --schemas: expected empty default, got %q", f.DefValue)
	}
}

// ─── parseSchemaList ─────────────────────────────────────────────────────────

func TestParseSchemaList_empty(t *testing.T) {
	got := parseSchemaList("")
	if got != nil {
		t.Errorf("expected nil for empty string, got %v", got)
	}
}

func TestParseSchemaList_single(t *testing.T) {
	got := parseSchemaList("mydb")
	if len(got) != 1 || got[0] != "mydb" {
		t.Errorf("expected [mydb], got %v", got)
	}
}

func TestParseSchemaList_multiple(t *testing.T) {
	got := parseSchemaList("db1,db2,db3")
	if len(got) != 3 || got[0] != "db1" || got[1] != "db2" || got[2] != "db3" {
		t.Errorf("expected [db1 db2 db3], got %v", got)
	}
}

func TestParseSchemaList_trims(t *testing.T) {
	got := parseSchemaList(" db1 , db2 , db3 ")
	if len(got) != 3 || got[0] != "db1" || got[1] != "db2" || got[2] != "db3" {
		t.Errorf("expected trimmed [db1 db2 db3], got %v", got)
	}
}

func TestParseSchemaList_dropsEmpty(t *testing.T) {
	got := parseSchemaList("db1,,db2,")
	if len(got) != 2 || got[0] != "db1" || got[1] != "db2" {
		t.Errorf("expected [db1 db2] with empty entries dropped, got %v", got)
	}
}

func TestParseSchemaList_allEmpty(t *testing.T) {
	got := parseSchemaList(",,,")
	if len(got) != 0 {
		t.Errorf("expected empty result for all-empty entries, got %v", got)
	}
}

// TestParseSchemaList_whitespaceOnly verifies that a non-empty string containing
// only whitespace returns nil — the early "" guard doesn't fire, the loop runs,
// trims the single part to "", skips it, and returns nil.
func TestParseSchemaList_whitespaceOnly(t *testing.T) {
	if got := parseSchemaList("   "); got != nil {
		t.Errorf("expected nil for whitespace-only input, got %v", got)
	}
}
