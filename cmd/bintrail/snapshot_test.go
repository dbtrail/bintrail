package main

import (
	"testing"
)

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
