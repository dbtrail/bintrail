package main

import (
	"strings"
	"testing"
)

// ─── cobra command wiring ─────────────────────────────────────────────────────

func TestReconstructCmd_registered(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "reconstruct" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'reconstruct' command to be registered under rootCmd")
	}
}

func TestReconstructCmd_defaults(t *testing.T) {
	cases := []struct {
		flag string
		want string
	}{
		{"format", "json"},
		{"baseline-only", "false"},
		{"history", "false"},
	}
	for _, tc := range cases {
		f := reconstructCmd.Flag(tc.flag)
		if f == nil {
			t.Errorf("flag --%s not registered", tc.flag)
			continue
		}
		if f.DefValue != tc.want {
			t.Errorf("flag --%s: expected default %q, got %q", tc.flag, tc.want, f.DefValue)
		}
	}
}

func TestReconstructCmd_allFlagsRegistered(t *testing.T) {
	for _, name := range []string{
		"index-dsn", "schema", "table", "pk", "pk-columns",
		"at", "baseline-dir", "baseline-s3", "baseline-only",
		"history", "sql", "format",
	} {
		if reconstructCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on reconstructCmd", name)
		}
	}
}

// ─── runReconstruct validation (no DB required) ───────────────────────────────

type recFlagSnapshot struct {
	indexDSN, schema, table, pk, pkCols, at, baselineDir, baselineS3, sql, format string
	baselineOnly, history                                                           bool
}

func captureRecFlags() recFlagSnapshot {
	return recFlagSnapshot{
		indexDSN:    recIndexDSN,
		schema:      recSchema,
		table:       recTable,
		pk:          recPK,
		pkCols:      recPKColumns,
		at:          recAt,
		baselineDir: recBaselineDir,
		baselineS3:  recBaselineS3,
		baselineOnly: recBaselineOnly,
		history:     recHistory,
		sql:         recSQL,
		format:      recFormat,
	}
}

func applyRecFlags(s recFlagSnapshot) {
	recIndexDSN = s.indexDSN
	recSchema = s.schema
	recTable = s.table
	recPK = s.pk
	recPKColumns = s.pkCols
	recAt = s.at
	recBaselineDir = s.baselineDir
	recBaselineS3 = s.baselineS3
	recBaselineOnly = s.baselineOnly
	recHistory = s.history
	recSQL = s.sql
	recFormat = s.format
}

func TestRunReconstruct_sqlInvalidFormat(t *testing.T) {
	orig := captureRecFlags()
	t.Cleanup(func() { applyRecFlags(orig) })

	recSQL = "SELECT 1"
	recFormat = "badformat"

	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid --format in sql mode, got nil")
	}
	if !strings.Contains(err.Error(), "invalid --format") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunReconstruct_invalidFormat(t *testing.T) {
	orig := captureRecFlags()
	t.Cleanup(func() { applyRecFlags(orig) })

	recSQL = ""
	recFormat = "xml"

	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid --format, got nil")
	}
	if !strings.Contains(err.Error(), "invalid --format") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunReconstruct_schemaRequired(t *testing.T) {
	orig := captureRecFlags()
	t.Cleanup(func() { applyRecFlags(orig) })

	recSQL = ""
	recFormat = "json"
	recSchema = ""

	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error for missing --schema, got nil")
	}
	if !strings.Contains(err.Error(), "--schema") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunReconstruct_tableRequired(t *testing.T) {
	orig := captureRecFlags()
	t.Cleanup(func() { applyRecFlags(orig) })

	recSQL = ""
	recFormat = "json"
	recSchema = "mydb"
	recTable = ""

	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error for missing --table, got nil")
	}
	if !strings.Contains(err.Error(), "--table") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunReconstruct_pkRequired(t *testing.T) {
	orig := captureRecFlags()
	t.Cleanup(func() { applyRecFlags(orig) })

	recSQL = ""
	recFormat = "json"
	recSchema = "mydb"
	recTable = "orders"
	recPK = ""

	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error for missing --pk, got nil")
	}
	if !strings.Contains(err.Error(), "--pk") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunReconstruct_pkColumnsRequired(t *testing.T) {
	orig := captureRecFlags()
	t.Cleanup(func() { applyRecFlags(orig) })

	recSQL = ""
	recFormat = "json"
	recSchema = "mydb"
	recTable = "orders"
	recPK = "42"
	recPKColumns = ""

	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error for missing --pk-columns, got nil")
	}
	if !strings.Contains(err.Error(), "--pk-columns") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunReconstruct_baselineSourceRequired(t *testing.T) {
	orig := captureRecFlags()
	t.Cleanup(func() { applyRecFlags(orig) })

	recSQL = ""
	recFormat = "json"
	recSchema = "mydb"
	recTable = "orders"
	recPK = "42"
	recPKColumns = "id"
	recBaselineDir = ""
	recBaselineS3 = ""

	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error when neither --baseline-dir nor --baseline-s3 set, got nil")
	}
	if !strings.Contains(err.Error(), "--baseline-dir") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunReconstruct_indexDSNRequiredWithoutBaselineOnly(t *testing.T) {
	orig := captureRecFlags()
	t.Cleanup(func() { applyRecFlags(orig) })

	recSQL = ""
	recFormat = "json"
	recSchema = "mydb"
	recTable = "orders"
	recPK = "42"
	recPKColumns = "id"
	recBaselineDir = "/tmp/baselines"
	recBaselineS3 = ""
	recBaselineOnly = false
	recIndexDSN = ""

	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error for missing --index-dsn (without --baseline-only), got nil")
	}
	if !strings.Contains(err.Error(), "--index-dsn") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunReconstruct_historyAndBaselineOnlyMutuallyExclusive(t *testing.T) {
	orig := captureRecFlags()
	t.Cleanup(func() { applyRecFlags(orig) })

	recSQL = ""
	recFormat = "json"
	recSchema = "mydb"
	recTable = "orders"
	recPK = "42"
	recPKColumns = "id"
	recBaselineDir = "/tmp/baselines"
	recBaselineS3 = ""
	recBaselineOnly = true
	recHistory = true
	recIndexDSN = ""

	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error when --history and --baseline-only both set, got nil")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunReconstruct_pkColumnsMismatch(t *testing.T) {
	orig := captureRecFlags()
	t.Cleanup(func() { applyRecFlags(orig) })

	recSQL = ""
	recFormat = "json"
	recSchema = "mydb"
	recTable = "orders"
	recPK = "42|99"     // 2 values
	recPKColumns = "id" // 1 column
	recBaselineDir = "/tmp/baselines"
	recBaselineS3 = ""
	recBaselineOnly = false
	recHistory = false
	recIndexDSN = "root:pass@tcp(localhost:3306)/idx"

	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error for pk/pk-columns count mismatch, got nil")
	}
	if !strings.Contains(err.Error(), "must match") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunReconstruct_invalidAt(t *testing.T) {
	orig := captureRecFlags()
	t.Cleanup(func() { applyRecFlags(orig) })

	recSQL = ""
	recFormat = "json"
	recSchema = "mydb"
	recTable = "orders"
	recPK = "42"
	recPKColumns = "id"
	recBaselineDir = "/tmp/baselines"
	recBaselineS3 = ""
	recBaselineOnly = false
	recHistory = false
	recIndexDSN = "root:pass@tcp(localhost:3306)/idx"
	recAt = "not-a-date"

	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid --at, got nil")
	}
	if !strings.Contains(err.Error(), "--at") {
		t.Errorf("unexpected error: %v", err)
	}
}
