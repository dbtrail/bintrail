package main

import (
	"strings"
	"testing"
)

// ─── cobra command wiring ─────────────────────────────────────────────────────

func TestRecoverCmd_registered(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "recover" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'recover' command to be registered under rootCmd")
	}
}

func TestRecoverCmd_indexDSN_required(t *testing.T) {
	flag := recoverCmd.Flag("index-dsn")
	if flag == nil {
		t.Fatal("flag --index-dsn not registered")
	}
	if flag.Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
		t.Error("flag --index-dsn is not marked required")
	}
}

func TestRecoverCmd_defaults(t *testing.T) {
	cases := []struct {
		flag string
		want string
	}{
		{"limit", "1000"},
		{"dry-run", "false"},
	}
	for _, tc := range cases {
		f := recoverCmd.Flag(tc.flag)
		if f == nil {
			t.Errorf("flag --%s not registered", tc.flag)
			continue
		}
		if f.DefValue != tc.want {
			t.Errorf("flag --%s: expected default %q, got %q", tc.flag, tc.want, f.DefValue)
		}
	}
}

func TestRecoverCmd_emptyStringDefaults(t *testing.T) {
	for _, name := range []string{
		"schema", "table", "pk", "event-type", "gtid", "since", "until", "output",
	} {
		f := recoverCmd.Flag(name)
		if f == nil {
			t.Errorf("flag --%s not registered", name)
			continue
		}
		if f.DefValue != "" {
			t.Errorf("flag --%s: expected empty default, got %q", name, f.DefValue)
		}
	}
}

func TestRecoverCmd_allFlagsRegistered(t *testing.T) {
	for _, name := range []string{
		"index-dsn", "schema", "table", "pk", "event-type",
		"gtid", "since", "until", "flag", "output", "dry-run", "limit",
	} {
		if recoverCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on recoverCmd", name)
		}
	}
}

// ─── runRecover validation (no DB required) ───────────────────────────────────

func TestRunRecover_noOutputOrDryRun(t *testing.T) {
	savedOut, savedDry := rOutput, rDryRun
	t.Cleanup(func() { rOutput = savedOut; rDryRun = savedDry })

	rOutput = ""
	rDryRun = false

	err := runRecover(recoverCmd, nil)
	if err == nil {
		t.Fatal("expected error when neither --output nor --dry-run is set, got nil")
	}
	if !strings.Contains(err.Error(), "--output or --dry-run") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRunRecover_pkRequiresSchemaTable(t *testing.T) {
	savedPK, savedS, savedT, savedDry := rPK, rSchema, rTable, rDryRun
	t.Cleanup(func() { rPK = savedPK; rSchema = savedS; rTable = savedT; rDryRun = savedDry })

	rDryRun = true // pass the first check
	rPK = "42"
	rSchema = ""
	rTable = ""

	err := runRecover(recoverCmd, nil)
	if err == nil {
		t.Fatal("expected error when --pk used without --schema/--table, got nil")
	}
	if !strings.Contains(err.Error(), "--pk requires") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRunRecover_invalidEventType(t *testing.T) {
	savedET, savedPK, savedDry := rEventType, rPK, rDryRun
	t.Cleanup(func() { rEventType = savedET; rPK = savedPK; rDryRun = savedDry })

	rDryRun = true
	rPK = ""
	rEventType = "MERGE"

	err := runRecover(recoverCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid --event-type, got nil")
	}
	if !strings.Contains(err.Error(), "MERGE") {
		t.Errorf("expected 'MERGE' in error, got: %v", err)
	}
}

func TestRunRecover_invalidSince(t *testing.T) {
	savedSince, savedET, savedPK, savedDry := rSince, rEventType, rPK, rDryRun
	t.Cleanup(func() {
		rSince = savedSince
		rEventType = savedET
		rPK = savedPK
		rDryRun = savedDry
	})

	rDryRun = true
	rPK = ""
	rEventType = ""
	rSince = "bad-date"

	err := runRecover(recoverCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid --since, got nil")
	}
	if !strings.Contains(err.Error(), "--since") {
		t.Errorf("expected '--since' in error, got: %v", err)
	}
}

func TestRunRecover_invalidUntil(t *testing.T) {
	savedUntil, savedSince, savedET, savedPK, savedDry :=
		rUntil, rSince, rEventType, rPK, rDryRun
	t.Cleanup(func() {
		rUntil = savedUntil
		rSince = savedSince
		rEventType = savedET
		rPK = savedPK
		rDryRun = savedDry
	})

	rDryRun = true
	rPK = ""
	rEventType = ""
	rSince = ""
	rUntil = "bad-date"

	err := runRecover(recoverCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid --until, got nil")
	}
	if !strings.Contains(err.Error(), "--until") {
		t.Errorf("expected '--until' in error, got: %v", err)
	}
}

// ─── --pk partial flag combinations ──────────────────────────────────────────

// TestRunRecover_pkWithSchemaOnly verifies that --pk + --schema (no --table)
// is rejected — the OR guard fires when either schema or table is missing.
func TestRunRecover_pkWithSchemaOnly(t *testing.T) {
	savedPK, savedS, savedT, savedDry := rPK, rSchema, rTable, rDryRun
	t.Cleanup(func() { rPK = savedPK; rSchema = savedS; rTable = savedT; rDryRun = savedDry })

	rDryRun = true // pass the output/dry-run guard
	rPK = "42"
	rSchema = "mydb"
	rTable = ""

	err := runRecover(recoverCmd, nil)
	if err == nil {
		t.Fatal("expected error when --pk used with --schema but no --table, got nil")
	}
	if !strings.Contains(err.Error(), "--pk requires") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestRunRecover_pkWithTableOnly verifies that --pk + --table (no --schema)
// is also rejected — the symmetric case of pkWithSchemaOnly.
func TestRunRecover_pkWithTableOnly(t *testing.T) {
	savedPK, savedS, savedT, savedDry := rPK, rSchema, rTable, rDryRun
	t.Cleanup(func() { rPK = savedPK; rSchema = savedS; rTable = savedT; rDryRun = savedDry })

	rDryRun = true
	rPK = "42"
	rSchema = ""
	rTable = "orders"

	err := runRecover(recoverCmd, nil)
	if err == nil {
		t.Fatal("expected error when --pk used with --table but no --schema, got nil")
	}
	if !strings.Contains(err.Error(), "--pk requires") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// ─── positive-path guard tests ────────────────────────────────────────────────

// TestRunRecover_outputSetPassesOutputCheck verifies that the output/dry-run
// guard does NOT fire when --output is non-empty (even without --dry-run).
func TestRunRecover_outputSetPassesOutputCheck(t *testing.T) {
	savedOut, savedDry := rOutput, rDryRun
	t.Cleanup(func() { rOutput = savedOut; rDryRun = savedDry })

	rOutput = "/tmp/recovery.sql"
	rDryRun = false

	err := runRecover(recoverCmd, nil) // fails later at config.Connect — that's fine
	if err != nil && strings.Contains(err.Error(), "--output or --dry-run") {
		t.Errorf("output guard should not fire when --output is set, got: %v", err)
	}
}

// TestRunRecover_dryRunPassesOutputCheck verifies that the output/dry-run
// guard does NOT fire when --dry-run is true (even without --output).
func TestRunRecover_dryRunPassesOutputCheck(t *testing.T) {
	savedOut, savedDry := rOutput, rDryRun
	t.Cleanup(func() { rOutput = savedOut; rDryRun = savedDry })

	rOutput = ""
	rDryRun = true

	err := runRecover(recoverCmd, nil) // fails later at config.Connect — that's fine
	if err != nil && strings.Contains(err.Error(), "--output or --dry-run") {
		t.Errorf("output guard should not fire when --dry-run is set, got: %v", err)
	}
}

// TestRunRecover_pkWithBothSchemaAndTablePassesGuard verifies the PK guard
// does NOT fire when --pk, --schema, and --table are all provided.
func TestRunRecover_pkWithBothSchemaAndTablePassesGuard(t *testing.T) {
	savedPK, savedS, savedT, savedDry := rPK, rSchema, rTable, rDryRun
	t.Cleanup(func() { rPK = savedPK; rSchema = savedS; rTable = savedT; rDryRun = savedDry })

	rDryRun = true
	rPK = "42"
	rSchema = "mydb"
	rTable = "orders"

	err := runRecover(recoverCmd, nil) // fails later at config.Connect — that's fine
	if err != nil && strings.Contains(err.Error(), "--pk requires") {
		t.Errorf("PK guard should not fire when both --schema and --table are set, got: %v", err)
	}
}

// TestRunRecover_validEventTypes verifies that INSERT, UPDATE, and DELETE are
// all accepted by ParseEventType and do not trigger the event-type error.
func TestRunRecover_validEventTypes(t *testing.T) {
	for _, et := range []string{"INSERT", "UPDATE", "DELETE"} {
		t.Run(et, func(t *testing.T) {
			savedET, savedDry, savedPK := rEventType, rDryRun, rPK
			t.Cleanup(func() { rEventType = savedET; rDryRun = savedDry; rPK = savedPK })

			rDryRun = true
			rPK = ""
			rEventType = et

			err := runRecover(recoverCmd, nil) // fails later at config.Connect
			if err != nil && strings.Contains(err.Error(), et) &&
				strings.Contains(strings.ToLower(err.Error()), "invalid") {
				t.Errorf("event type %q should be valid, got: %v", et, err)
			}
		})
	}
}
