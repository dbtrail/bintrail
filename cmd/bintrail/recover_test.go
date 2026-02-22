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

func TestRecoverCmd_allFlagsRegistered(t *testing.T) {
	for _, name := range []string{
		"index-dsn", "schema", "table", "pk", "event-type",
		"gtid", "since", "until", "output", "dry-run", "limit",
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
		rSince = savedSince; rEventType = savedET; rPK = savedPK; rDryRun = savedDry
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
		rUntil = savedUntil; rSince = savedSince; rEventType = savedET
		rPK = savedPK; rDryRun = savedDry
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
