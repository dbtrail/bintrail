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
