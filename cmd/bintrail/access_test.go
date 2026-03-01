package main

import (
	"strings"
	"testing"
)

// ─── Cobra command wiring ─────────────────────────────────────────────────────

func TestAccessCmd_registered(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "access" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'access' command to be registered under rootCmd")
	}
}

func TestAccessCmd_subcommandsRegistered(t *testing.T) {
	subNames := make(map[string]bool)
	for _, sub := range accessCmd.Commands() {
		subNames[sub.Use] = true
	}
	for _, want := range []string{"add", "remove", "list"} {
		if !subNames[want] {
			t.Errorf("expected subcommand %q registered under accessCmd", want)
		}
	}
}

func TestAccessCmd_indexDSNRequired(t *testing.T) {
	f := accessCmd.PersistentFlags().Lookup("index-dsn")
	if f == nil {
		t.Fatal("persistent flag --index-dsn not registered on accessCmd")
	}
	if f.Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
		t.Error("--index-dsn should be marked required on accessCmd")
	}
}

// ─── access add ──────────────────────────────────────────────────────────────

func TestAccessAddCmd_flagsRegistered(t *testing.T) {
	for _, name := range []string{"profile", "flag", "permission"} {
		if accessAddCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on accessAddCmd", name)
		}
	}
}

func TestAccessAddCmd_requiredFlags(t *testing.T) {
	for _, name := range []string{"profile", "flag", "permission"} {
		f := accessAddCmd.Flag(name)
		if f == nil {
			t.Fatalf("flag --%s not registered on accessAddCmd", name)
		}
		if f.Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
			t.Errorf("--%s should be required on accessAddCmd", name)
		}
	}
}

// ─── access remove ────────────────────────────────────────────────────────────

func TestAccessRemoveCmd_flagsRegistered(t *testing.T) {
	for _, name := range []string{"profile", "flag"} {
		if accessRemoveCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on accessRemoveCmd", name)
		}
	}
}

func TestAccessRemoveCmd_requiredFlags(t *testing.T) {
	for _, name := range []string{"profile", "flag"} {
		f := accessRemoveCmd.Flag(name)
		if f == nil {
			t.Fatalf("flag --%s not registered on accessRemoveCmd", name)
		}
		if f.Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
			t.Errorf("--%s should be required on accessRemoveCmd", name)
		}
	}
}

// ─── access list ─────────────────────────────────────────────────────────────

func TestAccessListCmd_profileOptional(t *testing.T) {
	f := accessListCmd.Flag("profile")
	if f == nil {
		t.Fatal("flag --profile not registered on accessListCmd")
	}
	if f.Annotations["cobra_annotation_bash_completion_one_required_flag"] != nil {
		t.Error("--profile should not be required on accessListCmd")
	}
}

// ─── Validation ──────────────────────────────────────────────────────────────

func TestRunAccessAdd_invalidPermission(t *testing.T) {
	old := aclPermission
	t.Cleanup(func() { aclPermission = old })
	aclPermission = "readwrite"

	err := runAccessAdd(accessAddCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid permission value")
	}
	if !strings.Contains(err.Error(), "--permission must be") {
		t.Errorf("unexpected error message: %v", err)
	}
}
