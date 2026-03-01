package main

import (
	"testing"
)

// ─── Cobra command wiring ─────────────────────────────────────────────────────

func TestFlagCmd_registered(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "flag" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'flag' command to be registered under rootCmd")
	}
}

func TestFlagCmd_subcommandsRegistered(t *testing.T) {
	subNames := make(map[string]bool)
	for _, sub := range flagCmd.Commands() {
		subNames[sub.Use] = true
	}
	for _, want := range []string{"add <flag-name>", "remove <flag-name>", "list"} {
		if !subNames[want] {
			t.Errorf("expected subcommand %q registered under flagCmd", want)
		}
	}
}

func TestFlagCmd_indexDSNRequired(t *testing.T) {
	f := flagCmd.PersistentFlags().Lookup("index-dsn")
	if f == nil {
		t.Fatal("persistent flag --index-dsn not registered on flagCmd")
	}
	if f.Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
		t.Error("--index-dsn should be marked required on flagCmd")
	}
}

// ─── flag add ────────────────────────────────────────────────────────────────

func TestFlagAddCmd_flagsRegistered(t *testing.T) {
	for _, name := range []string{"schema", "table", "column"} {
		if flagAddCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on flagAddCmd", name)
		}
	}
}

func TestFlagAddCmd_schemaRequired(t *testing.T) {
	f := flagAddCmd.Flag("schema")
	if f == nil {
		t.Fatal("flag --schema not registered")
	}
	if f.Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
		t.Error("--schema should be marked required on flagAddCmd")
	}
}

func TestFlagAddCmd_tableRequired(t *testing.T) {
	f := flagAddCmd.Flag("table")
	if f == nil {
		t.Fatal("flag --table not registered")
	}
	if f.Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
		t.Error("--table should be marked required on flagAddCmd")
	}
}

func TestFlagAddCmd_columnOptional(t *testing.T) {
	f := flagAddCmd.Flag("column")
	if f == nil {
		t.Fatal("flag --column not registered on flagAddCmd")
	}
	if f.Annotations["cobra_annotation_bash_completion_one_required_flag"] != nil {
		t.Error("--column should not be marked required on flagAddCmd")
	}
	if f.DefValue != "" {
		t.Errorf("--column default should be empty, got %q", f.DefValue)
	}
}

// ─── flag remove ─────────────────────────────────────────────────────────────

func TestFlagRemoveCmd_flagsRegistered(t *testing.T) {
	for _, name := range []string{"schema", "table", "column"} {
		if flagRemoveCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on flagRemoveCmd", name)
		}
	}
}

func TestFlagRemoveCmd_schemaRequired(t *testing.T) {
	f := flagRemoveCmd.Flag("schema")
	if f == nil {
		t.Fatal("flag --schema not registered on flagRemoveCmd")
	}
	if f.Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
		t.Error("--schema should be marked required on flagRemoveCmd")
	}
}

func TestFlagRemoveCmd_tableRequired(t *testing.T) {
	f := flagRemoveCmd.Flag("table")
	if f == nil {
		t.Fatal("flag --table not registered on flagRemoveCmd")
	}
	if f.Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
		t.Error("--table should be marked required on flagRemoveCmd")
	}
}

// ─── flag list ───────────────────────────────────────────────────────────────

func TestFlagListCmd_flagsRegistered(t *testing.T) {
	for _, name := range []string{"schema", "table"} {
		if flagListCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on flagListCmd", name)
		}
	}
}

func TestFlagListCmd_filtersOptional(t *testing.T) {
	for _, name := range []string{"schema", "table"} {
		f := flagListCmd.Flag(name)
		if f == nil {
			t.Fatalf("flag --%s not registered on flagListCmd", name)
		}
		if f.Annotations["cobra_annotation_bash_completion_one_required_flag"] != nil {
			t.Errorf("--%s should not be required on flagListCmd", name)
		}
	}
}
