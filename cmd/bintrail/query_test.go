package main

import (
	"strings"
	"testing"
)

// ─── cobra command wiring ─────────────────────────────────────────────────────

func TestQueryCmd_registered(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "query" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'query' command to be registered under rootCmd")
	}
}

func TestQueryCmd_indexDSN_required(t *testing.T) {
	flag := queryCmd.Flag("index-dsn")
	if flag == nil {
		t.Fatal("flag --index-dsn not registered")
	}
	if flag.Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
		t.Error("flag --index-dsn is not marked required")
	}
}

func TestQueryCmd_defaults(t *testing.T) {
	cases := []struct {
		flag string
		want string
	}{
		{"format", "table"},
		{"limit", "100"},
	}
	for _, tc := range cases {
		f := queryCmd.Flag(tc.flag)
		if f == nil {
			t.Errorf("flag --%s not registered", tc.flag)
			continue
		}
		if f.DefValue != tc.want {
			t.Errorf("flag --%s: expected default %q, got %q", tc.flag, tc.want, f.DefValue)
		}
	}
}

func TestQueryCmd_allFlagsRegistered(t *testing.T) {
	for _, name := range []string{
		"index-dsn", "schema", "table", "pk", "event-type",
		"gtid", "since", "until", "changed-column", "format", "limit",
	} {
		if queryCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on queryCmd", name)
		}
	}
}

// ─── runQuery validation (no DB required) ─────────────────────────────────────

func TestRunQuery_pkRequiresSchemaTable(t *testing.T) {
	saved, savedS, savedT := qPK, qSchema, qTable
	t.Cleanup(func() { qPK = saved; qSchema = savedS; qTable = savedT })

	qPK = "42"
	qSchema = ""
	qTable = ""

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error when --pk used without --schema/--table, got nil")
	}
	if !strings.Contains(err.Error(), "--pk requires") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRunQuery_changedColRequiresSchemaTable(t *testing.T) {
	saved, savedS, savedT := qChangedCol, qSchema, qTable
	t.Cleanup(func() { qChangedCol = saved; qSchema = savedS; qTable = savedT })

	qChangedCol = "status"
	qSchema = ""
	qTable = ""

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error when --changed-column used without --schema/--table, got nil")
	}
	if !strings.Contains(err.Error(), "--changed-column requires") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRunQuery_invalidFormat(t *testing.T) {
	savedFmt, savedPK, savedCol := qFormat, qPK, qChangedCol
	t.Cleanup(func() { qFormat = savedFmt; qPK = savedPK; qChangedCol = savedCol })

	qPK = ""
	qChangedCol = ""
	qFormat = "xml"

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid --format, got nil")
	}
	if !strings.Contains(err.Error(), "invalid --format") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRunQuery_invalidEventType(t *testing.T) {
	savedET, savedPK, savedCol, savedFmt := qEventType, qPK, qChangedCol, qFormat
	t.Cleanup(func() {
		qEventType = savedET; qPK = savedPK; qChangedCol = savedCol; qFormat = savedFmt
	})

	qPK = ""
	qChangedCol = ""
	qFormat = "table"
	qEventType = "UPSERT"

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid --event-type, got nil")
	}
	if !strings.Contains(err.Error(), "UPSERT") {
		t.Errorf("expected 'UPSERT' in error, got: %v", err)
	}
}

func TestRunQuery_invalidSince(t *testing.T) {
	savedSince, savedPK, savedCol, savedFmt, savedET := qSince, qPK, qChangedCol, qFormat, qEventType
	t.Cleanup(func() {
		qSince = savedSince; qPK = savedPK; qChangedCol = savedCol
		qFormat = savedFmt; qEventType = savedET
	})

	qPK = ""
	qChangedCol = ""
	qFormat = "table"
	qEventType = ""
	qSince = "not-a-date"

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid --since, got nil")
	}
	if !strings.Contains(err.Error(), "--since") {
		t.Errorf("expected '--since' in error, got: %v", err)
	}
}

func TestRunQuery_invalidUntil(t *testing.T) {
	savedUntil, savedPK, savedCol, savedFmt, savedET, savedSince :=
		qUntil, qPK, qChangedCol, qFormat, qEventType, qSince
	t.Cleanup(func() {
		qUntil = savedUntil; qPK = savedPK; qChangedCol = savedCol
		qFormat = savedFmt; qEventType = savedET; qSince = savedSince
	})

	qPK = ""
	qChangedCol = ""
	qFormat = "table"
	qEventType = ""
	qSince = ""
	qUntil = "not-a-date"

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid --until, got nil")
	}
	if !strings.Contains(err.Error(), "--until") {
		t.Errorf("expected '--until' in error, got: %v", err)
	}
}
