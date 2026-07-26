package recovery

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

// ─── Apply-side codegen switches (#1003) ──────────────────────────────────────

// autoIncSection identifies the opt-in AUTO_INCREMENT block. It must be more
// specific than the bare word AUTO_INCREMENT: the always-on header note already
// says "AUTO_INCREMENT/serial counters are NOT restored by this script", so a
// substring check on the word alone would match every script.
const autoIncSection = "AUTO_INCREMENT restore (--restore-auto-increment)"

// applyCodegenRow is a minimal reversible event for the preamble/epilogue tests
// below (a DELETE reverses to an INSERT, which needs only row_before).
func applyCodegenRow(schema, table string) query.ResultRow {
	return query.ResultRow{
		EventID: 1, SchemaName: schema, TableName: table,
		EventType: parser.EventDelete, EventTimestamp: time.Unix(0, 0).UTC(),
		RowBefore: map[string]any{"id": "1"},
	}
}

func mustGenerate(t *testing.T, g *Generator, rows []query.ResultRow) string {
	t.Helper()
	var buf bytes.Buffer
	if _, err := g.GenerateSQLFromRows(rows, &buf); err != nil {
		t.Fatalf("GenerateSQLFromRows: %v", err)
	}
	return buf.String()
}

func newSuppressed(d Dialect) *Generator {
	g := NewForDialect(nil, nil, d)
	g.SetSuppressTriggers(true)
	return g
}

// TestSuppressTriggers_PGOnlyAndOptIn pins the #1003 PostgreSQL trigger
// suppression: the SET LOCAL appears only on the PG dialect AND only when the
// operator opted in (it requires superuser / GRANT SET ON PARAMETER and disables
// FK constraint triggers, so it must never be the silent default), and it lands
// INSIDE the transaction — a SET LOCAL outside one would warn and do nothing.
func TestSuppressTriggers_PGOnlyAndOptIn(t *testing.T) {
	const srr = "SET LOCAL session_replication_role = replica;"
	rows := []query.ResultRow{applyCodegenRow("public", "t")}

	pgOn := mustGenerate(t, newSuppressed(PostgresDialect), rows)
	if !strings.Contains(pgOn, srr) {
		t.Errorf("PG script with --suppress-triggers must contain %q, got:\n%s", srr, pgOn)
	}
	begin := strings.Index(pgOn, "\nBEGIN;")
	set := strings.Index(pgOn, srr)
	commit := strings.Index(pgOn, "\nCOMMIT;")
	if begin < 0 || set < begin || commit < set {
		t.Errorf("SET LOCAL session_replication_role must sit between BEGIN and COMMIT, got:\n%s", pgOn)
	}

	pgOff := mustGenerate(t, NewForDialect(nil, nil, PostgresDialect), rows)
	if strings.Contains(pgOff, "session_replication_role") {
		t.Errorf("PG script must NOT suppress triggers by default, got:\n%s", pgOff)
	}

	// MySQL never emits it — with or without the flag. MySQL has no session
	// toggle to suppress triggers; implying otherwise in the script would be a
	// false promise.
	for _, g := range []*Generator{New(nil, nil), newSuppressed(MySQLDialect)} {
		out := mustGenerate(t, g, []query.ResultRow{applyCodegenRow("db", "t")})
		if strings.Contains(out, "session_replication_role") {
			t.Errorf("MySQL script must NEVER emit session_replication_role, got:\n%s", out)
		}
	}
}

// TestRestoreAutoIncrement_MySQLOnlyAfterCommit pins the #1003 AUTO_INCREMENT
// checklist: absent by default, present (commented out, one entry per written
// table, positioned AFTER COMMIT because ALTER TABLE implicitly commits) when
// opted in, and never emitted on the PostgreSQL path.
func TestRestoreAutoIncrement_MySQLOnlyAfterCommit(t *testing.T) {
	rows := []query.ResultRow{applyCodegenRow("db", "orders"), applyCodegenRow("db", "items")}

	off := mustGenerate(t, New(nil, nil), rows)
	if strings.Contains(off, autoIncSection) || strings.Contains(off, "ALTER TABLE") {
		t.Errorf("MySQL script must NOT emit the AUTO_INCREMENT checklist by default, got:\n%s", off)
	}

	g := New(nil, nil)
	g.SetRestoreAutoIncrement(true)
	on := mustGenerate(t, g, rows)

	for _, want := range []string{
		"--   ALTER TABLE `db`.`items` AUTO_INCREMENT = <N>;",
		"--   ALTER TABLE `db`.`orders` AUTO_INCREMENT = <N>;",
		"--   SELECT IFNULL(MAX(`<auto_increment_column>`), 0) + 1 FROM `db`.`orders`;",
	} {
		if !strings.Contains(on, want) {
			t.Errorf("opted-in MySQL script must contain %q, got:\n%s", want, on)
		}
	}
	// Every emitted ALTER must be commented out: an uncommented DDL here would
	// run with an N nobody chose.
	for _, line := range strings.Split(on, "\n") {
		if strings.Contains(line, "ALTER TABLE") && !strings.HasPrefix(strings.TrimSpace(line), "--") {
			t.Errorf("AUTO_INCREMENT statement must be commented out, got line: %q", line)
		}
	}
	commit := strings.Index(on, "\nCOMMIT;")
	alter := strings.Index(on, "ALTER TABLE")
	if commit < 0 || alter < commit {
		t.Errorf("the AUTO_INCREMENT block must follow COMMIT (ALTER TABLE implicitly commits), got:\n%s", on)
	}

	pg := NewForDialect(nil, nil, PostgresDialect)
	pg.SetRestoreAutoIncrement(true)
	out := mustGenerate(t, pg, []query.ResultRow{applyCodegenRow("public", "t")})
	if strings.Contains(out, autoIncSection) {
		t.Errorf("PG script must NEVER emit the MySQL AUTO_INCREMENT checklist, got:\n%s", out)
	}
}

// ─── Comment-injection defense (#1120) ────────────────────────────────────────

// TestCommentInjection_PKNewlineCannotEscapeTheHeaderComment pins the vector
// that ships today: a newline inside a VARCHAR primary key is ordinary data, and
// the per-event header comment interpolated it raw. Because a "--" comment ends
// at the first newline, everything after it became executable SQL INSIDE the
// script's BEGIN/COMMIT.
//
// PKValues is the right probe for a whole-script assertion: it reaches the
// header comment ONLY — the reversal's WHERE/VALUES clauses are rebuilt from the
// row image — so the executable statements stay byte-identical to an ordinary
// script and every remaining line must be a comment or one of them. That is what
// makes this catch an unsanitized site anywhere in the body, not just the one
// line the fix touched.
func TestCommentInjection_PKNewlineCannotEscapeTheHeaderComment(t *testing.T) {
	row := applyCodegenRow("db", "orders")
	row.PKValues = "1\nDROP TABLE users;"

	out := mustGenerate(t, New(nil, nil), []query.ResultRow{row})

	// Every statement an ordinary MySQL reversal script is allowed to contain.
	allowed := map[string]bool{
		"BEGIN;":                    true,
		"SET time_zone = '+00:00';": true,
		"SET sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION';": true,
		"COMMIT;": true,
		"INSERT INTO `db`.`orders` (`id`) VALUES ('1');": true,
	}
	for _, line := range strings.Split(out, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") || allowed[trimmed] {
			continue
		}
		t.Errorf("line escaped its comment and would execute: %q\nfull script:\n%s", line, out)
	}
}

// TestCommentInjection_TableNewlineCannotEscapeTheAutoIncrementBlock covers the
// sharpest case (#1110's opt-in block): there the "--" prefix is the ENTIRE
// safety mechanism, and the block sits after COMMIT, so anything that breaks out
// runs standalone rather than inside the transaction. MySQL permits any BMP
// character except U+0000 in a backtick-quoted identifier, newline included.
//
// The assertion is over the whole post-COMMIT REGION — which is comment-only by
// design — so a site in that block left unsanitized fails here even though the
// fix never touched its Fprintf.
func TestCommentInjection_TableNewlineCannotEscapeTheAutoIncrementBlock(t *testing.T) {
	g := New(nil, nil)
	g.SetRestoreAutoIncrement(true)
	out := mustGenerate(t, g, []query.ResultRow{applyCodegenRow("db", "or\nders")})

	_, after, found := strings.Cut(out, "\nCOMMIT;\n")
	if !found {
		t.Fatalf("script has no COMMIT to anchor the AUTO_INCREMENT block:\n%s", out)
	}
	if !strings.Contains(after, autoIncSection) {
		t.Fatalf("opted-in script must emit the AUTO_INCREMENT block after COMMIT, got:\n%s", after)
	}
	for _, line := range strings.Split(after, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		t.Errorf("post-COMMIT line is not commented and would execute: %q\nblock:\n%s", line, after)
	}
}

// TestRestoreAutoIncrement_NoRowsEmitsNothing: with no matched events there is
// no table whose counter could need restoring, and the early return must stay a
// bare "no events" line rather than growing a checklist for an empty reversal.
func TestRestoreAutoIncrement_NoRowsEmitsNothing(t *testing.T) {
	g := New(nil, nil)
	g.SetRestoreAutoIncrement(true)
	if out := mustGenerate(t, g, nil); strings.Contains(out, autoIncSection) {
		t.Errorf("empty reversal must not emit the AUTO_INCREMENT checklist, got:\n%s", out)
	}
}
