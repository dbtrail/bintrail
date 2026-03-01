package main

import (
	"testing"
)

// ─── Cobra command wiring ─────────────────────────────────────────────────────

func TestProfileCmd_registered(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "profile" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'profile' command to be registered under rootCmd")
	}
}

func TestProfileCmd_subcommandsRegistered(t *testing.T) {
	subNames := make(map[string]bool)
	for _, sub := range profileCmd.Commands() {
		subNames[sub.Use] = true
	}
	for _, want := range []string{"add <name>", "remove <name>", "list"} {
		if !subNames[want] {
			t.Errorf("expected subcommand %q registered under profileCmd", want)
		}
	}
}

func TestProfileCmd_indexDSNRequired(t *testing.T) {
	f := profileCmd.PersistentFlags().Lookup("index-dsn")
	if f == nil {
		t.Fatal("persistent flag --index-dsn not registered on profileCmd")
	}
	if f.Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
		t.Error("--index-dsn should be marked required on profileCmd")
	}
}

// ─── profile add ─────────────────────────────────────────────────────────────

func TestProfileAddCmd_descriptionOptional(t *testing.T) {
	f := profileAddCmd.Flag("description")
	if f == nil {
		t.Fatal("flag --description not registered on profileAddCmd")
	}
	if f.Annotations["cobra_annotation_bash_completion_one_required_flag"] != nil {
		t.Error("--description should not be required on profileAddCmd")
	}
	if f.DefValue != "" {
		t.Errorf("--description default should be empty, got %q", f.DefValue)
	}
}
