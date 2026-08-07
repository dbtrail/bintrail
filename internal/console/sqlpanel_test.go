package console

// The #1177 escape suite. These tests drive the REAL production functions
// (runSandboxedSQL / openSandboxedSession / the handler through the mux), not a
// hand-built lookalike session: the security posture is exactly what these
// calls configure, so a test session assembled by hand would prove nothing.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/views"
)

// writeSQLPanelArchive writes one archived partition in the exact Hive layout
// rotation produces, through the real archive column set and Parquet writer
// (the same fixture shape internal/views' execute test uses).
func writeSQLPanelArchive(t *testing.T, root, id string) {
	t.Helper()
	path := filepath.Join(root, "bintrail_id="+id, "event_date=2026-05-01", "event_hour=03", "events.parquet")
	w, err := baseline.NewWriter(path, archive.BinlogEventColumns, baseline.WriterConfig{
		Compression: "none", RowGroupSize: 100,
	})
	if err != nil {
		t.Fatalf("archive writer: %v", err)
	}
	values := []string{
		"1", "binlog.000001", "100", "200", "2026-05-01 03:00:00", "",
		"42", "shop", "orders", "2", "1",
		`["status"]`, `{"id":1,"status":"new"}`, `{"id":1,"status":"paid"}`,
		"1", "", "", "1777000000000000",
	}
	nulls := make([]bool, len(archive.BinlogEventColumns))
	nulls[5] = true  // gtid
	nulls[15] = true // query_text
	nulls[16] = true // query_hash
	if len(values) != len(archive.BinlogEventColumns) {
		t.Fatalf("fixture has %d values for %d columns — update the fixture with the column set",
			len(values), len(archive.BinlogEventColumns))
	}
	if err := w.WriteRow(values, nulls); err != nil {
		t.Fatalf("write archive row: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close archive writer: %v", err)
	}
}

// writeSQLPanelBaseline writes a real two-row baseline snapshot readable by
// DuckDB and returns (baselineRoot, tableParquetPath).
func writeSQLPanelBaseline(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	schemaFile := filepath.Join(t.TempDir(), "shop.orders-schema.sql")
	ddl := "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `status` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	if err := os.WriteFile(schemaFile, []byte(ddl), 0o644); err != nil {
		t.Fatalf("write schema file: %v", err)
	}
	cols, err := baseline.ParseSchema(schemaFile)
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}
	path := filepath.Join(root, "2026-04-30T03-00-00Z", "shop", "orders.parquet")
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("baseline writer: %v", err)
	}
	for _, r := range [][]string{{"1", "new"}, {"2", "paid"}} {
		if err := w.WriteRow(r, []bool{false, false}); err != nil {
			t.Fatalf("write baseline row: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close baseline writer: %v", err)
	}
	return root, path
}

// panelInput assembles the views.Input runSandboxedSQL receives, exactly as
// buildViewsInput would resolve it for these roots.
func panelInput(archiveSources []string, baselineRoot, baselinePath string) views.Input {
	in := views.Input{
		GeneratedAt:    time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Version:        "test",
		ArchiveSources: archiveSources,
	}
	if baselineRoot != "" {
		in.BaselineSource = baselineRoot
		in.BaselineSnapshot = time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC)
		in.Baselines = []views.BaselineTable{{Schema: "shop", Table: "orders", Path: baselinePath}}
	}
	return in
}

// newSQLPanelServer builds a Server whose boot bundle points at a REAL baseline
// snapshot (DuckDB-readable, unlike the empty-file fixtures the listing tests
// use) with the panel opted in.
func newSQLPanelServer(t *testing.T, baselineRoot string, enabled bool) *Server {
	t.Helper()
	s := &Server{token: "t", version: "v0.50.0", cm: newConnManager(nil, false), sqlPanel: enabled}
	s.cm.boot = &bundle{baselineSrc: baselineRoot, baselineConfigured: baselineRoot != ""}
	s.mux = s.buildHandler()
	return s
}

// TestSQLPanel_readsArchiveAndBaseline is the positive control: the sandboxed
// session serves the same events/state views the downloadable artifact defines.
// Without it, every "denied" below could equally mean "the session is broken".
func TestSQLPanel_readsArchiveAndBaseline(t *testing.T) {
	archiveRoot := t.TempDir()
	const id = "11111111-2222-3333-4444-555555555555"
	writeSQLPanelArchive(t, archiveRoot, id)
	baselineRoot, baselinePath := writeSQLPanelBaseline(t)
	in := panelInput([]string{filepath.Join(archiveRoot, "bintrail_id="+id)}, baselineRoot, baselinePath)

	res, err := runSandboxedSQL(context.Background(), in, `SELECT event_type, schema_name FROM events`)
	if err != nil {
		t.Fatalf("query events view: %v", err)
	}
	if res.RowCount != 1 || res.Truncated {
		t.Fatalf("events: rows=%d truncated=%v, want 1/false", res.RowCount, res.Truncated)
	}
	if res.Rows[0][0] != "UPDATE" || res.Rows[0][1] != "shop" {
		t.Errorf("events row = %v, want [UPDATE shop]", res.Rows[0])
	}

	res, err = runSandboxedSQL(context.Background(), in, `SELECT count(*) FROM state_shop_orders WHERE status = 'paid'`)
	if err != nil {
		t.Fatalf("query state view: %v", err)
	}
	if fmt.Sprint(res.Rows[0][0]) != "1" {
		t.Errorf("state view count = %v, want 1", res.Rows[0][0])
	}
}

// TestSQLPanel_gateBlocksRawFileAccess is the FIRST of the two forensics-leak
// defenses (the gate). The sandbox necessarily grants read access to the archive
// roots so the views work, so the gate must stop a query from reading the raw
// files under them — either through a reader function or the replacement-scan
// form — which would otherwise expose the paid forensics columns the events view
// withholds. The critical case is the direct read of a real archive file's
// connection_id.
func TestSQLPanel_gateBlocksRawFileAccess(t *testing.T) {
	archiveRoot := t.TempDir()
	const id = "11111111-2222-3333-4444-555555555555"
	writeSQLPanelArchive(t, archiveRoot, id)
	source := filepath.Join(archiveRoot, "bintrail_id="+id)
	archiveFile := filepath.Join(source, "event_date=2026-05-01", "event_hour=03", "events.parquet")
	in := panelInput([]string{source}, "", "")

	esc := func(s string) string { return strings.ReplaceAll(s, "'", "''") }
	for _, tc := range []struct{ name, stmt string }{
		// The forensics bypass the events-view filter alone would miss: read the
		// raw archive Parquet, which carries all 18 columns.
		{"read_parquet forensics column", fmt.Sprintf("SELECT connection_id FROM read_parquet('%s')", archiveFile)},
		{"replacement scan forensics column", fmt.Sprintf("SELECT connection_id FROM '%s'", archiveFile)},
		{"replacement scan glob", fmt.Sprintf("SELECT * FROM '%s/*/*/*.parquet'", source)},
		{"parquet_scan", fmt.Sprintf("SELECT * FROM parquet_scan('%s')", archiveFile)},
		{"parquet_metadata", fmt.Sprintf("SELECT * FROM parquet_metadata('%s')", archiveFile)},
		{"glob the archive root", fmt.Sprintf("SELECT * FROM glob('%s/*')", source)},
		{"read_text an archive file", fmt.Sprintf("SELECT * FROM read_text('%s')", archiveFile)},
		{"read_csv", fmt.Sprintf("SELECT * FROM read_csv('%s')", archiveFile)},
		{"read_json_objects_auto", fmt.Sprintf("SELECT * FROM read_json_objects_auto('%s')", archiveFile)},
		// Casing / nesting must not evade the AST walk.
		{"nested reader in CTE", fmt.Sprintf("WITH q AS (SELECT * FROM READ_PARQUET('%s')) SELECT * FROM q", archiveFile)},
		// The dynamic-SQL re-entry functions: DuckDB re-parses their STRING
		// argument at bind time, after this gate — a denylist of reader names
		// never sees the read. The allowlist refuses the outer function itself.
		// These are the exact leaks the #1177 review measured (connection_id=42).
		{"query() dynamic SQL", fmt.Sprintf("SELECT * FROM query('SELECT connection_id FROM read_parquet(''%s'')')", esc(archiveFile))},
		{"query_table()", fmt.Sprintf("SELECT connection_id FROM query_table('%s')", archiveFile)},
		{"json_execute_serialized_sql", fmt.Sprintf("SELECT * FROM json_execute_serialized_sql(json_serialize_sql('SELECT connection_id FROM read_parquet(''%s'')'))", esc(archiveFile))},
		// A reader nested in a set operation or a join must be caught too — the
		// walk recurses generically, it does not special-case the from-clause, so
		// these pin that the recursion (not a from-clause-only optimization) is
		// what holds.
		{"reader in UNION", fmt.Sprintf("SELECT schema_name FROM events UNION SELECT connection_id FROM read_parquet('%s')", archiveFile)},
		{"reader in JOIN", fmt.Sprintf("SELECT e.schema_name FROM events e JOIN read_parquet('%s') r ON true", archiveFile)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := runSandboxedSQL(context.Background(), in, tc.stmt)
			var ue *sqlUserError
			if !errors.As(err, &ue) {
				t.Fatalf("raw file access was not refused by the gate (rows leaked: %+v): %v", res, err)
			}
			if !strings.Contains(ue.msg, "not available") {
				t.Fatalf("expected the denied-read message, got: %v", ue.msg)
			}
		})
	}
}

// TestSQLPanel_onlyAllowlistedTableFunctionsReachable is the durable guard the
// allowlist earns: it enumerates EVERY table function the sandboxed DuckDB
// exposes and asserts the gate refuses each one that is not on the allowlist.
// A future DuckDB (or a loaded extension) that adds a new file reader or
// SQL-re-entry function is therefore denied by default — and if someone widens
// allowedTableFunctions to something dangerous, this still passes only because
// the walk denies everything else. It runs the REAL gate (sqlPanelGate), so it
// also proves the AST shape assumption holds for every function at once.
func TestSQLPanel_onlyAllowlistedTableFunctionsReachable(t *testing.T) {
	db, cleanup, err := openSandboxedSession(context.Background(),
		func() views.Input { r, p := writeSQLPanelBaseline(t); return panelInput(nil, r, p) }())
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	// duckdb_functions() is itself a table function — read it on a plain
	// session, before the sandbox would (correctly) refuse it.
	plain, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	rows, err := plain.Query(`SELECT DISTINCT function_name FROM duckdb_functions() WHERE function_type = 'table'`)
	if err != nil {
		t.Fatalf("enumerate table functions: %v", err)
	}
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		names = append(names, n)
	}
	rows.Close()
	if len(names) < 20 {
		t.Fatalf("only %d table functions enumerated — the query is wrong", len(names))
	}

	denied := map[string]bool{}
	for _, name := range names {
		if allowedTableFunctions[strings.ToLower(name)] {
			continue
		}
		// A bare zero-arg call parses (argument errors are a bind-time concern,
		// after json_serialize_sql), so the gate can classify every one.
		stmt := fmt.Sprintf(`SELECT * FROM "%s"()`, name)
		err := sqlPanelGate(context.Background(), db, stmt)
		var ue *sqlUserError
		if err == nil {
			t.Errorf("gate ALLOWED non-allowlisted table function %q — potential file/SQL read", name)
			continue
		}
		if !errors.As(err, &ue) {
			// json_serialize_sql couldn't parse the bare call for this function
			// (some need arguments to parse) — not a policy pass, skip it.
			continue
		}
		if !strings.Contains(ue.msg, "not available") {
			t.Errorf("%q denied with an unexpected message: %v", name, ue.msg)
		}
		denied[strings.ToLower(name)] = true
	}

	// Guard against a vacuous pass (if bare calls mostly failed to parse, the
	// loop above would assert almost nothing): the dangerous functions the
	// gate exists to stop MUST be in the enumerated-and-denied set.
	t.Logf("enumerated %d table functions, %d denied by the gate", len(names), len(denied))
	for _, must := range []string{"read_parquet", "query", "query_table", "json_execute_serialized_sql", "duckdb_secrets", "glob"} {
		if !denied[must] {
			t.Errorf("%q was not among the enumerated-and-denied functions — the guard is not exercising the readers it claims to", must)
		}
	}
}

// TestSQLPanel_allowlistPinnedToExactlyTwo makes the "fails closed" guarantee
// durable against a contributor WIDENING the allowlist: the enumeration guard
// above consults the map under test, so it can never catch an ADDITION to it.
// Anything new here (a convenience reader, a dynamic-SQL function) is a live
// in-root forensics-leak primitive — the archive Parquet is inside an allowed
// root — so the set is pinned to exactly its two justified generators.
func TestSQLPanel_allowlistPinnedToExactlyTwo(t *testing.T) {
	want := map[string]bool{"range": true, "generate_series": true}
	if len(allowedTableFunctions) != len(want) {
		t.Fatalf("allowedTableFunctions has %d entries, want exactly %d (%v) — a new from-clause table function is now reachable in the sandbox",
			len(allowedTableFunctions), len(want), want)
	}
	for name := range allowedTableFunctions {
		if !want[name] {
			t.Errorf("allowedTableFunctions has an UNEXPECTED entry %q — justify it with an escape test and add it to want here, or remove it", name)
		}
	}
	for name := range want {
		if !allowedTableFunctions[name] {
			t.Errorf("allowedTableFunctions is missing the expected generator %q", name)
		}
	}
}

// TestSQLPanel_sandboxDeniesOutOfRootReads is the SECOND defense (the filesystem
// sandbox), tested DIRECTLY on a production-built session so it holds even if the
// gate above were bypassed: a read outside the allowed roots — a sibling temp
// dir, a file in the shared os.TempDir(), an S3 path, or an arbitrary HTTPS URL
// (the SSRF the issue names) — dies on DuckDB's own config/permission check. No
// bucket or credentials involved, so it reproduces anywhere. The panel's spill
// directory is private on purpose: DuckDB implicitly allows file access under
// temp_directory, so pointing it at the shared os.TempDir() would widen the
// sandbox to everything in /tmp.
func TestSQLPanel_sandboxDeniesOutOfRootReads(t *testing.T) {
	baselineRoot, baselinePath := writeSQLPanelBaseline(t)
	db, cleanup, err := openSandboxedSession(context.Background(), panelInput(nil, baselineRoot, baselinePath))
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	secret := t.TempDir()
	if err := os.WriteFile(filepath.Join(secret, "s.csv"), []byte("x\n99\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	sharedTmp := filepath.Join(os.TempDir(), fmt.Sprintf("bintrail-sqlpanel-escape-%d.csv", os.Getpid()))
	if err := os.WriteFile(sharedTmp, []byte("x\n7\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(sharedTmp)

	// A sibling directory that STRING-PREFIXES the allowed root: the allowed
	// entry is "<root>/", so real DuckDB must not admit "<root>evil/". This
	// proves the trailing-separator boundary in the ENGINE, not just in the
	// allowed_directories literal (TestSQLPanelAllowedListBoundary is the string
	// half). "<root>" is a lake, "<root>evil" is the lakeevil it must not admit.
	siblingPrefix := baselineRoot + "evil"
	if err := os.MkdirAll(siblingPrefix, 0o755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(siblingPrefix)
	if err := os.WriteFile(filepath.Join(siblingPrefix, "s.csv"), []byte("x\n5\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	const denied = "disabled by configuration"
	for _, tc := range []struct{ name, stmt string }{
		{"read sibling temp dir", fmt.Sprintf("SELECT * FROM read_csv('%s/s.csv')", secret)},
		{"sibling prefix of an allowed root (lake vs lakeevil)", fmt.Sprintf("SELECT * FROM read_csv('%s/s.csv')", siblingPrefix)},
		{"read shared os.TempDir file", fmt.Sprintf("SELECT * FROM read_csv('%s')", sharedTmp)},
		{"glob outside the roots", fmt.Sprintf("SELECT * FROM glob('%s/*')", secret)},
		{"replacement scan out of root", fmt.Sprintf("SELECT * FROM '%s/s.csv'", secret)},
		{"s3 read (no root allows it)", "SELECT * FROM read_parquet('s3://any-bucket/x.parquet')"},
		{"arbitrary https URL (SSRF)", "SELECT * FROM read_text('https://example.com/')"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Execute directly on the session — NOT through runSandboxedSQL —
			// so the gate is bypassed and it is the sandbox under test.
			_, err := db.ExecContext(context.Background(), tc.stmt)
			if err == nil {
				t.Fatal("the sandbox allowed a read outside the allowed roots")
			}
			if !strings.Contains(err.Error(), denied) {
				t.Fatalf("expected the permission refusal (%q), got: %v", denied, err)
			}
		})
	}
}

// TestSQLPanelAllowedListBoundary pins the trailing-separator boundary in the
// allowed_directories literal: an allowed ".../lake" must not string-prefix
// match ".../lakeevil". This is the whole defense against a same-bucket sibling
// prefix, and it is pure string logic — no DuckDB needed to prove it.
func TestSQLPanelAllowedListBoundary(t *testing.T) {
	got := sqlPanelAllowedList(views.Input{
		ArchiveSources: []string{"s3://bucket/lake", "/local/archive/"},
		BaselineSource: "s3://bucket/baselines",
	})
	for _, want := range []string{"'s3://bucket/lake/'", "'/local/archive/'", "'s3://bucket/baselines/'"} {
		if !strings.Contains(got, want) {
			t.Errorf("allowed list %s is missing a directory-boundary entry %s", got, want)
		}
	}
	if strings.Contains(got, "'s3://bucket/lake',") || strings.Contains(got, "'s3://bucket/lake']") {
		t.Errorf("an entry lacks its trailing separator — 'lake' would admit 'lakeevil': %s", got)
	}
}

// TestSQLPanel_withholdsForensicsColumns is the open-core boundary (#1177): the
// panel executes server-side, so its `events` view must NOT expose the paid
// forensics columns the console's eventDTO omits (connection_id, query_text,
// query_hash). Reading them must fail as unknown columns; the free columns must
// still be there. Serving these would give away a paid EE surface from the
// Apache-licensed core.
func TestSQLPanel_withholdsForensicsColumns(t *testing.T) {
	archiveRoot := t.TempDir()
	const id = "11111111-2222-3333-4444-555555555555"
	writeSQLPanelArchive(t, archiveRoot, id)
	in := panelInput([]string{filepath.Join(archiveRoot, "bintrail_id="+id)}, "", "")

	for _, col := range []string{"connection_id", "query_text", "query_hash"} {
		_, err := runSandboxedSQL(context.Background(), in, "SELECT "+col+" FROM events")
		var ue *sqlUserError
		if !errors.As(err, &ue) {
			t.Errorf("SELECT %s FROM events did not refuse (forensics column leaked): %v", col, err)
		}
	}
	// A free column is unaffected.
	if _, err := runSandboxedSQL(context.Background(), in, "SELECT schema_name FROM events"); err != nil {
		t.Errorf("a non-forensics column was wrongly withheld: %v", err)
	}
	// SELECT * must not carry them either.
	res, err := runSandboxedSQL(context.Background(), in, "SELECT * FROM events")
	if err != nil {
		t.Fatalf("SELECT *: %v", err)
	}
	for _, c := range res.Columns {
		switch strings.ToLower(c) {
		case "connection_id", "query_text", "query_hash":
			t.Errorf("SELECT * exposed the forensics column %q", c)
		}
	}
}

// TestSQLPanel_gateRefusesSecretIntrospection: the secrets manager exposes an S3
// secret's access-key id even with the value redacted, so the gate refuses the
// table functions that read it — matched on the parsed AST, so casing, comments
// and nesting do not evade it.
func TestSQLPanel_gateRefusesSecretIntrospection(t *testing.T) {
	baselineRoot, baselinePath := writeSQLPanelBaseline(t)
	in := panelInput(nil, baselineRoot, baselinePath)
	for _, stmt := range []string{
		"SELECT * FROM duckdb_secrets()",
		"select SECRET_STRING from DuckDB_Secrets()",
		"WITH q AS (SELECT * FROM duckdb_secrets()) SELECT * FROM q",
		"SELECT (SELECT name FROM duckdb_secrets() LIMIT 1)",
		"SELECT * FROM which_secret('s3://x/y', 's3')",
		"SELECT * FROM duckdb_secrets() /* comment */",
	} {
		_, err := runSandboxedSQL(context.Background(), in, stmt)
		var ue *sqlUserError
		if !errors.As(err, &ue) || !strings.Contains(ue.msg, "not available") {
			t.Errorf("%q: expected the denied-function refusal, got: %v", stmt, err)
		}
	}
	// An allowlisted generator, by contrast, runs — the gate is a FROM-clause
	// allowlist, not a blanket ban on all table-shaped syntax.
	if _, err := runSandboxedSQL(context.Background(), in, "SELECT count(*) FROM range(10)"); err != nil {
		t.Errorf("range() should be allowed: %v", err)
	}
}

// TestSQLPanel_gateRefusesNonSelect: the read-only layer. DuckDB's
// allowed_directories carve-out permits writes INSIDE the roots (COPY TO,
// ATTACH of a writable database), so every non-SELECT statement must die on the
// gate — classified by DuckDB's own parser, before execution.
func TestSQLPanel_gateRefusesNonSelect(t *testing.T) {
	baselineRoot, baselinePath := writeSQLPanelBaseline(t)
	in := panelInput(nil, baselineRoot, baselinePath)

	for _, tc := range []struct {
		name, stmt string
	}{
		{"COPY TO inside the allowed root", fmt.Sprintf("COPY (SELECT 1) TO '%s/evil.parquet'", baselineRoot)},
		{"ATTACH inside the allowed root", fmt.Sprintf("ATTACH '%s/evil.db' AS x", baselineRoot)},
		{"CREATE TABLE", "CREATE TABLE t AS SELECT 1"},
		// CREATE SECRET survives lock_configuration (secrets are not settings);
		// the gate refuses it here. (enable_external_access=false is also a
		// barrier — it denies the persistent secret store — so the gate is a
		// primary, not the only, guard.) Pinned on purpose.
		{"CREATE SECRET", "CREATE OR REPLACE SECRET evil (TYPE s3, KEY_ID 'k', SECRET 's')"},
		{"SET", "SET enable_external_access = true"},
		{"PRAGMA", "PRAGMA memory_limit='99GB'"},
		{"INSTALL", "INSTALL httpfs"},
		{"multi-statement", "SELECT 1; COPY (SELECT 1) TO '/x.csv'"},
		{"two SELECTs", "SELECT 1; SELECT 2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runSandboxedSQL(context.Background(), in, tc.stmt)
			var ue *sqlUserError
			if !errors.As(err, &ue) {
				t.Fatalf("expected the statement gate to refuse, got: %v", err)
			}
		})
	}

	// A syntax error surfaces the parser's own message — usable feedback, not
	// a generic refusal.
	_, err := runSandboxedSQL(context.Background(), in, "SELEC 1")
	var ue *sqlUserError
	if !errors.As(err, &ue) || !strings.Contains(strings.ToLower(ue.msg), "error") {
		t.Fatalf("syntax error should surface the parser message, got: %v", err)
	}
}

// TestSQLPanel_lockedConfigurationHolds exercises the lock layer DIRECTLY on a
// session built by the production setup: even if the SELECT-only gate ever
// regressed, no statement may widen the sandbox after it is locked.
func TestSQLPanel_lockedConfigurationHolds(t *testing.T) {
	baselineRoot, baselinePath := writeSQLPanelBaseline(t)
	db, cleanup, err := openSandboxedSession(context.Background(), panelInput(nil, baselineRoot, baselinePath))
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	for _, stmt := range []string{
		"SET enable_external_access = true",
		"SET allowed_directories = ['/']",
		"SET lock_configuration = false",
		"SET memory_limit = '99GB'",
		"SET threads = 64",
		"SET temp_directory = '/'",
	} {
		if _, err := db.Exec(stmt); err == nil {
			t.Errorf("%s: succeeded after lock_configuration", stmt)
		} else if !strings.Contains(err.Error(), "locked") {
			t.Errorf("%s: expected the configuration-locked refusal, got: %v", stmt, err)
		}
	}
}

// TestSQLPanel_capsRowsAndBytes: interactive surface, bounded response.
func TestSQLPanel_capsRowsAndBytes(t *testing.T) {
	baselineRoot, baselinePath := writeSQLPanelBaseline(t)
	in := panelInput(nil, baselineRoot, baselinePath)

	res, err := runSandboxedSQL(context.Background(), in, "SELECT * FROM range(2000)")
	if err != nil {
		t.Fatal(err)
	}
	if res.RowCount != sqlPanelMaxRows || !res.Truncated {
		t.Errorf("rows=%d truncated=%v, want %d/true", res.RowCount, res.Truncated, sqlPanelMaxRows)
	}

	// Wide rows must trip the byte budget long before the row cap.
	res, err = runSandboxedSQL(context.Background(), in, "SELECT repeat('x', 5000000) FROM range(5)")
	if err != nil {
		t.Fatal(err)
	}
	if !res.Truncated || res.RowCount >= 5 {
		t.Errorf("rows=%d truncated=%v, want the byte budget to truncate below 5 rows", res.RowCount, res.Truncated)
	}
}

// TestSQLPanel_timeoutInterrupts: the hard wall-clock budget must INTERRUPT the
// running query, not just abandon it — a bounded response with an unbounded
// query still chewing daemon CPU would be no bound at all.
func TestSQLPanel_timeoutInterrupts(t *testing.T) {
	baselineRoot, baselinePath := writeSQLPanelBaseline(t)
	in := panelInput(nil, baselineRoot, baselinePath)

	old := sqlPanelTimeout
	sqlPanelTimeout = 300 * time.Millisecond
	defer func() { sqlPanelTimeout = old }()

	start := time.Now()
	_, err := runSandboxedSQL(context.Background(), in, "SELECT count(*) FROM range(200000000000)")
	elapsed := time.Since(start)
	var ue *sqlUserError
	if !errors.As(err, &ue) || !strings.Contains(ue.msg, "exceeded") {
		t.Fatalf("expected the timeout refusal, got: %v", err)
	}
	if elapsed > 10*time.Second {
		t.Fatalf("timeout took %s to interrupt — the query was not actually canceled", elapsed)
	}
}

// TestSQLPanel_cancelInterrupts is the human Cancel button: aborting the fetch
// kills the request context, and the query must stop promptly. No cancel
// endpoint exists — this IS the cancellation path.
func TestSQLPanel_cancelInterrupts(t *testing.T) {
	baselineRoot, baselinePath := writeSQLPanelBaseline(t)
	in := panelInput(nil, baselineRoot, baselinePath)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err := runSandboxedSQL(ctx, in, "SELECT count(*) FROM range(200000000000)")
	elapsed := time.Since(start)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
	if elapsed > 10*time.Second {
		t.Fatalf("cancellation took %s to interrupt — the query was not actually stopped", elapsed)
	}
}

// TestSQLPanelHandler drives POST /api/sql through the full mux (auth
// middleware, route registration, authz classification included), so a broken
// wire — not just a broken engine — fails here.
func TestSQLPanelHandler(t *testing.T) {
	t.Run("disabled by default", func(t *testing.T) {
		baselineRoot, _ := writeSQLPanelBaseline(t)
		srv := newSQLPanelServer(t, baselineRoot, false)
		rec, body := doServersReq(t, srv, "POST", "/api/sql", `{"sql":"SELECT 1"}`)
		if rec.Code != http.StatusForbidden || !strings.Contains(string(body), "BINTRAIL_CONSOLE_SQL_PANEL") {
			t.Fatalf("code=%d body=%s, want 403 naming the opt-in", rec.Code, body)
		}
	})
	t.Run("success over the baseline", func(t *testing.T) {
		baselineRoot, _ := writeSQLPanelBaseline(t)
		srv := newSQLPanelServer(t, baselineRoot, true)
		rec, body := doServersReq(t, srv, "POST", "/api/sql",
			`{"sql":"SELECT id, status FROM state_shop_orders ORDER BY id"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("code=%d body=%s", rec.Code, body)
		}
		s := string(body)
		for _, want := range []string{`"columns":["id","status"]`, `"row_count":2`, `"paid"`} {
			if !strings.Contains(s, want) {
				t.Errorf("response missing %s: %s", want, s)
			}
		}
	})
	t.Run("statement error is a 422 with the engine message", func(t *testing.T) {
		baselineRoot, _ := writeSQLPanelBaseline(t)
		srv := newSQLPanelServer(t, baselineRoot, true)
		rec, body := doServersReq(t, srv, "POST", "/api/sql", `{"sql":"SELECT nope FROM state_shop_orders"}`)
		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("code=%d body=%s, want 422", rec.Code, body)
		}
	})
	t.Run("busy returns 429", func(t *testing.T) {
		baselineRoot, _ := writeSQLPanelBaseline(t)
		srv := newSQLPanelServer(t, baselineRoot, true)
		srv.sqlPanelBusy.Store(true)
		defer srv.sqlPanelBusy.Store(false)
		rec, body := doServersReq(t, srv, "POST", "/api/sql", `{"sql":"SELECT 1"}`)
		if rec.Code != http.StatusTooManyRequests {
			t.Fatalf("code=%d body=%s, want 429", rec.Code, body)
		}
	})
	t.Run("nothing to query", func(t *testing.T) {
		srv := newSQLPanelServer(t, "", true)
		rec, body := doServersReq(t, srv, "POST", "/api/sql", `{"sql":"SELECT 1"}`)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("code=%d body=%s, want 404", rec.Code, body)
		}
	})
	t.Run("no-archive server refused", func(t *testing.T) {
		baselineRoot, _ := writeSQLPanelBaseline(t)
		srv := newSQLPanelServer(t, baselineRoot, true)
		srv.cm.boot.noArchive = true
		rec, body := doServersReq(t, srv, "POST", "/api/sql", `{"sql":"SELECT 1"}`)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("code=%d body=%s, want 404", rec.Code, body)
		}
	})
	t.Run("empty and malformed bodies", func(t *testing.T) {
		baselineRoot, _ := writeSQLPanelBaseline(t)
		srv := newSQLPanelServer(t, baselineRoot, true)
		for _, body := range []string{`{"sql":"  "}`, `{bad`} {
			rec, got := doServersReq(t, srv, "POST", "/api/sql", body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("body %q: code=%d resp=%s, want 400", body, rec.Code, got)
			}
		}
	})
	t.Run("oversized statement refused", func(t *testing.T) {
		baselineRoot, _ := writeSQLPanelBaseline(t)
		srv := newSQLPanelServer(t, baselineRoot, true)
		big := `{"sql":"SELECT '` + strings.Repeat("x", sqlPanelMaxStatementBytes) + `'"}`
		rec, body := doServersReq(t, srv, "POST", "/api/sql", big)
		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("code=%d body=%s, want 413", rec.Code, body)
		}
	})
}

// TestSQLPanelHandler_profileSessionRefused: the RBAC gate, enforced at the
// endpoint (not the UI), before any bundle is resolved — same shape as
// recover-cascade's.
func TestSQLPanelHandler_profileSessionRefused(t *testing.T) {
	srv, err := New(Config{
		Listen: "127.0.0.1:8090", Token: "static-tok", SQLPanel: true,
		AuthPath: filepath.Join(t.TempDir(), "auth.yaml"),
	})
	if err != nil {
		t.Fatal(err)
	}
	tok, _, err := srv.sessions.IssueWithPolicy("sam@example.com",
		&ext.AccessPolicy{Permissions: ext.AllPermissions(), Profile: "sensitive"})
	if err != nil {
		t.Fatal(err)
	}
	w := postJSON(t, srv, "/api/sql", tok, `{"sql":"SELECT 1"}`)
	if w.Code != http.StatusForbidden {
		t.Fatalf("profiled POST /api/sql = %d body=%s, want 403", w.Code, w.Body.String())
	}
}

// TestSQLPanelAvailable mirrors the handler's gates, which is the point: the
// capability decides whether the UI shows the tab, and a tab that only errors
// is a lie. It also pins that Config.SQLPanel actually reaches the server —
// delete that wire and the "enabled" cases fail.
func TestSQLPanelAvailable(t *testing.T) {
	baselineRoot, _ := writeSQLPanelBaseline(t)

	for _, tc := range []struct {
		name    string
		enabled bool
		b       *bundle
		want    bool
	}{
		{"enabled with a baseline", true, &bundle{baselineSrc: baselineRoot}, true},
		{"not opted in", false, &bundle{baselineSrc: baselineRoot}, false},
		{"archives disabled", true, &bundle{baselineSrc: baselineRoot, noArchive: true}, false},
		{"nothing configured", true, &bundle{}, false},
		{"no bundle", true, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Server{cm: newConnManager(nil, false), sqlPanel: tc.enabled}
			r := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/sql", nil)
			if got := s.sqlPanelAvailable(r, tc.b); got != tc.want {
				t.Fatalf("sqlPanelAvailable = %v, want %v", got, tc.want)
			}
		})
	}

	// The Config wire: New must carry SQLPanel onto the server.
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", SQLPanel: true})
	if err != nil {
		t.Fatal(err)
	}
	if !srv.sqlPanel {
		t.Error("Config.SQLPanel did not reach Server.sqlPanel")
	}
}

// TestSQLPanelCapabilityAdvertised reads the real /api/capabilities payload:
// the tab's gate in the SPA is this boolean, so it must track the opt-in.
func TestSQLPanelCapabilityAdvertised(t *testing.T) {
	baselineRoot, _ := writeSQLPanelBaseline(t)
	for _, tc := range []struct {
		enabled bool
		want    string
	}{
		{true, `"sql":true`},
		{false, `"sql":false`},
	} {
		srv := newSQLPanelServer(t, baselineRoot, tc.enabled)
		rec, body := doServersReq(t, srv, "GET", "/api/capabilities", "")
		if rec.Code != http.StatusOK || !strings.Contains(string(body), tc.want) {
			t.Fatalf("enabled=%v: code=%d body=%s, want %s", tc.enabled, rec.Code, body, tc.want)
		}
	}
}
