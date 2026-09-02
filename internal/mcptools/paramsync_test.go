package mcptools

import (
	"reflect"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// ─── CLI ↔ MCP filter parity (#962) ─────────────────────────────────────────
//
// QueryArgs/RecoverArgs are parallel copies of the CLI query/recover filter
// surfaces, and before #962 they had drifted silently: MCP recover exposed
// changed_column (which CLI recover deliberately refuses — a changed-column
// filter names row VERSIONS, and reversing a filtered subset of a row's
// history can produce a state that never existed) and lacked --pks and
// --limit-per-pk. Nothing failed, because the two surfaces were only prose-
// synchronized.
//
// These tests make the drift structural: every CLI flag must either map to an
// MCP param (kebab-case → snake_case) or be recorded below with the reason it
// is CLI-only, and every MCP param must map back to a CLI flag. Adding a
// filter to one surface without the other — or without an explicit, documented
// exception — fails the build.

// recoverCLIOnly are `bintrail recover` flags that deliberately have no MCP
// recover param. Every entry must still exist as a CLI flag (a stale entry
// fails the test) and must NOT have an MCP counterpart.
var recoverCLIOnly = map[string]string{
	"output":                 "CLI file plumbing; the MCP tool always returns the script text in the result",
	"dry-run":                "CLI stdout plumbing; the MCP tool is always dry-run (it returns the script, never applies it)",
	"format":                 "CLI text/json envelope rendering; the MCP result is always the script text",
	"max-script-bytes":       "script-budget override for operator-run large recoveries; the MCP surface keeps the serving process's budget (see the ScriptBudgetError rewrite in MakeRecoverTool)",
	"allow-gaps":             "proceed-despite-gaps escape hatch; MCP recover REFUSES on archive trouble and offers no_archive instead (#1285)",
	"suppress-triggers":      "apply-side codegen (PG session_replication_role) for scripts an operator will run",
	"restore-auto-increment": "apply-side codegen (MySQL AUTO_INCREMENT checklist) for scripts an operator will run",
	"ultrafast":              "DuckDB resource tuning; the long-lived MCP daemons keep the safe default (#510/#511)",
	"duckdb-threads":         "DuckDB resource tuning; the long-lived MCP daemons keep the safe default (#510/#511)",
	"duckdb-memory-limit":    "DuckDB resource tuning; the long-lived MCP daemons keep the safe default (#510/#511)",
}

// queryCLIOnly is the same ledger for `bintrail query` vs the MCP query tool.
var queryCLIOnly = map[string]string{
	"archive-dir":         "explicit archive source override; the MCP surface uses env config + archive_state auto-discovery",
	"archive-s3":          "explicit archive source override; the MCP surface uses env config + archive_state auto-discovery",
	"bintrail-id":         "companion to the explicit archive source flags above",
	"include-snapshot":    "baseline Parquet scan mode; on MCP that capability is the reconstruct tool",
	"baseline":            "companion to include-snapshot",
	"ultrafast":           "DuckDB resource tuning; the long-lived MCP daemons keep the safe default (#510/#511)",
	"duckdb-threads":      "DuckDB resource tuning; the long-lived MCP daemons keep the safe default (#510/#511)",
	"duckdb-memory-limit": "DuckDB resource tuning; the long-lived MCP daemons keep the safe default (#510/#511)",
}

// cliFlagNames returns the local flag names registered on the named CLI
// read-plane command, via the same registration path the real binaries use
// (cli.AddReadCommands), so the test pins the production flag set rather than
// a copy of it.
func cliFlagNames(t *testing.T, name string) map[string]bool {
	t.Helper()
	root := &cobra.Command{Use: "bintrail"}
	cli.AddReadCommands(root)
	cmd, _, err := root.Find([]string{name})
	if err != nil || cmd == nil || cmd.Name() != name {
		t.Fatalf("CLI command %q not found via cli.AddReadCommands: %v", name, err)
	}
	names := map[string]bool{}
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		if f.Name == "help" {
			return
		}
		names[f.Name] = true
	})
	if len(names) == 0 {
		t.Fatalf("CLI command %q registered no flags — the enumeration is broken", name)
	}
	return names
}

// mcpParamNames reflects the json parameter names out of a tool args struct —
// the exact names the inferred input schema advertises to MCP clients.
func mcpParamNames(t *testing.T, args any) map[string]bool {
	t.Helper()
	rt := reflect.TypeOf(args)
	names := map[string]bool{}
	for i := range rt.NumField() {
		tag := rt.Field(i).Tag.Get("json")
		name, _, _ := strings.Cut(tag, ",")
		if name == "" || name == "-" {
			t.Fatalf("%s.%s has no json name; every tool param must name its wire form", rt.Name(), rt.Field(i).Name)
		}
		names[name] = true
	}
	return names
}

func assertParamParity(t *testing.T, cliCmd string, args any, cliOnly map[string]string) {
	t.Helper()
	flags := cliFlagNames(t, cliCmd)
	params := mcpParamNames(t, args)

	// The exception ledger must stay live: a stale entry means the CLI flag is
	// gone (delete the entry) or an MCP param appeared anyway (the reason no
	// longer holds — resolve it explicitly, don't let both coexist).
	for name := range cliOnly {
		if !flags[name] {
			t.Errorf("cliOnly exception %q is not a `bintrail %s` flag anymore; delete the stale exception", name, cliCmd)
		}
		if params[strings.ReplaceAll(name, "-", "_")] {
			t.Errorf("cliOnly exception %q also exists as an MCP param; it is not CLI-only — remove one side or the exception", name)
		}
	}

	for name := range flags {
		if _, excepted := cliOnly[name]; excepted {
			continue
		}
		if want := strings.ReplaceAll(name, "-", "_"); !params[want] {
			t.Errorf("`bintrail %s` flag --%s has no %q param on the MCP %s tool: add the param, or record it in the cliOnly ledger with the reason it must not exist on MCP",
				cliCmd, name, want, cliCmd)
		}
	}
	for name := range params {
		if kebab := strings.ReplaceAll(name, "_", "-"); !flags[kebab] {
			t.Errorf("MCP %s tool param %q has no --%s flag on `bintrail %s`: the surfaces drifted — if the CLI omits the flag DELIBERATELY (changed-column on recover is the precedent), the MCP tool must omit the param too",
				cliCmd, name, kebab, cliCmd)
		}
	}
}

func TestRecoverToolParams_matchCLIRecoverFlags(t *testing.T) {
	assertParamParity(t, "recover", RecoverArgs{}, recoverCLIOnly)
}

func TestQueryToolParams_matchCLIQueryFlags(t *testing.T) {
	assertParamParity(t, "query", QueryArgs{}, queryCLIOnly)
}

// TestRecoverSurfaces_neverChangedColumn names the design decision the parity
// tests enforce implicitly: NEITHER recover surface filters by changed column.
// A changed-column filter selects row versions; reversing a filtered subset of
// a row's history is unsafe (the reverse-image WHERE clauses assume the
// intervening, un-reverted versions). If either side ever grows it, this fails
// and forces the decision to be made explicitly on both.
func TestRecoverSurfaces_neverChangedColumn(t *testing.T) {
	if cliFlagNames(t, "recover")["changed-column"] {
		t.Error("`bintrail recover` grew --changed-column; decide the reversal-safety question explicitly before mirroring it to MCP")
	}
	if mcpParamNames(t, RecoverArgs{})["changed_column"] {
		t.Error("RecoverArgs exposes changed_column; CLI recover deliberately refuses it (#962)")
	}
}
