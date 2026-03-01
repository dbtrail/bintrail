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

func TestStatusCmd_indexDSN_emptyDefault(t *testing.T) {
	f := statusCmd.Flag("index-dsn")
	if f == nil {
		t.Fatal("flag --index-dsn not registered")
	}
	if f.DefValue != "" {
		t.Errorf("expected empty default for --index-dsn, got %q", f.DefValue)
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

// TestRunStatus_invalidDSN verifies that a syntactically invalid DSN is caught
// by mysql.ParseDSN and surfaces as an "invalid --index-dsn" error — before
// any connection is attempted.
func TestRunStatus_invalidDSN(t *testing.T) {
	saved := stIndexDSN
	t.Cleanup(func() { stIndexDSN = saved })

	stIndexDSN = "://this-is-not-a-valid-dsn"

	err := runStatus(statusCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid DSN, got nil")
	}
	if !strings.Contains(err.Error(), "invalid --index-dsn") {
		t.Errorf("expected 'invalid --index-dsn' in error, got: %v", err)
	}
}

// TestRunStatus_validDSNPassesGuards verifies that a DSN with a database name
// passes both pre-connection guards and fails only at config.Connect — proving
// neither the parse guard nor the DB-name guard fires.
func TestRunStatus_validDSNPassesGuards(t *testing.T) {
	saved := stIndexDSN
	t.Cleanup(func() { stIndexDSN = saved })

	stIndexDSN = "user:pass@tcp(localhost:3306)/binlog_index"

	err := runStatus(statusCmd, nil) // fails at config.Connect — that's fine
	if err != nil && strings.Contains(err.Error(), "invalid --index-dsn") {
		t.Errorf("parse guard should not fire for a valid DSN, got: %v", err)
	}
	if err != nil && strings.Contains(err.Error(), "--index-dsn must include a database name") {
		t.Errorf("DB-name guard should not fire when DB name is present, got: %v", err)
	}
}
