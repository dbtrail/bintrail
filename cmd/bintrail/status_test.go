package main

import (
	"strings"
	"testing"
)

// ─── cobra command wiring ─────────────────────────────────────────────────────

func TestStatusCmd_registered(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "status" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'status' command to be registered under rootCmd")
	}
}

func TestStatusCmd_indexDSN_required(t *testing.T) {
	flag := statusCmd.Flag("index-dsn")
	if flag == nil {
		t.Fatal("flag --index-dsn not registered on statusCmd")
	}
	if flag.Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
		t.Error("flag --index-dsn is not marked required on statusCmd")
	}
}

// ─── runStatus validation (no DB required) ────────────────────────────────────

// TestRunStatus_missingDBName verifies that a DSN without a database name is
// rejected before any connection attempt — the same guard used in rotate.go.
func TestRunStatus_missingDBName(t *testing.T) {
	saved := stIndexDSN
	t.Cleanup(func() { stIndexDSN = saved })

	stIndexDSN = "user:pass@tcp(localhost:3306)/" // valid syntax, no database name

	err := runStatus(statusCmd, nil)
	if err == nil {
		t.Fatal("expected error when DSN has no database name, got nil")
	}
	if !strings.Contains(err.Error(), "--index-dsn must include a database name") {
		t.Errorf("unexpected error: %v", err)
	}
}
