package cascaderecover_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/cascaderecover"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
)

// emit runs EmitSQL into a buffer with a DB-free generator (recovery.New(nil, …)
// never touches the DB for an empty row set). Returns the full byte output, the
// statement count, and any error. With rows==nil, GenerateSQLFromRows emits a
// single deterministic "-- No events matched…" line (no time.Now() timestamp),
// so the surrounding preamble + wrapper bytes can be pinned exactly.
func emit(t *testing.T, hdr cascaderecover.Header, rows []query.ResultRow, setNull []cascade.SetNullRestore, resolver *metadata.Resolver) (string, int, error) {
	t.Helper()
	var buf bytes.Buffer
	n, err := cascaderecover.EmitSQL(&buf, recovery.New(nil, resolver), rows, setNull, resolver, hdr)
	return buf.String(), n, err
}

// TestEmitSQL_scriptBudgetRefusesCleanly verifies the #654 budget guard in the
// cascade path: when the rows exceed the script-size budget, EmitSQL refuses
// BEFORE writing its preamble, so it leaves NO dangling `SET FOREIGN_KEY_CHECKS=0`
// on the writer (the footgun a refusal after the preamble would create).
func TestEmitSQL_scriptBudgetRefusesCleanly(t *testing.T) {
	hdr := cascaderecover.Header{Schema: "shop", Table: "orders", Parents: 1, Children: 0}
	rows := []query.ResultRow{{
		EventType:  event.EventDelete,
		SchemaName: "shop", TableName: "orders",
		PKValues:  "1",
		RowBefore: map[string]any{"id": float64(1), "blob": strings.Repeat("x", 1<<20)}, // 1 MiB
	}}

	gen := recovery.New(nil, nil)
	gen.SetMaxScriptBytes(1024) // tiny budget → the 1 MiB row trips it

	var buf bytes.Buffer
	n, err := cascaderecover.EmitSQL(&buf, gen, rows, nil, nil, hdr)

	var be *recovery.ScriptBudgetError
	if !errors.As(err, &be) {
		t.Fatalf("want *ScriptBudgetError, got %v", err)
	}
	if n != 0 {
		t.Errorf("want 0 statements on refusal, got %d", n)
	}
	if buf.Len() != 0 {
		t.Fatalf("refusal must write nothing (no dangling FK-disable), wrote %d bytes: %q", buf.Len(), buf.String())
	}
}

// TestEmitSQL_unresolvedToastMarkerRefusesCleanly verifies the #592 guard on
// the cascade path, hoisted for the same reason as the budget guard above: a
// row carrying the residual unchanged-TOAST marker must refuse BEFORE the
// preamble is written, leaving NO dangling `SET FOREIGN_KEY_CHECKS=0` on the
// writer — GenerateSQLFromRows' own refusal would come after the preamble.
func TestEmitSQL_unresolvedToastMarkerRefusesCleanly(t *testing.T) {
	hdr := cascaderecover.Header{Schema: "shop", Table: "orders", Parents: 1, Children: 0}
	rows := []query.ResultRow{{
		EventType:  event.EventDelete,
		SchemaName: "shop", TableName: "orders",
		PKValues:  "1",
		RowBefore: map[string]any{"id": "1", "body": map[string]any{event.UnchangedToastKey: true}},
	}}

	var buf bytes.Buffer
	n, err := cascaderecover.EmitSQL(&buf, recovery.New(nil, nil), rows, nil, nil, hdr)
	if err == nil {
		t.Fatalf("expected a loud error, got n=%d output:\n%s", n, buf.String())
	}
	for _, want := range []string{"unresolved unchanged-TOAST marker", "capture invariant violated", "shop.orders", "body"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q:\n%s", want, err)
		}
	}
	if n != 0 {
		t.Errorf("want 0 statements on refusal, got %d", n)
	}
	if buf.Len() != 0 {
		t.Fatalf("refusal must write nothing (no dangling FK-disable), wrote %d bytes: %q", buf.Len(), buf.String())
	}
}

// TestEmitSQL_generationFailureRefusesCleanly locks the #835 invariant: when any
// parent/victim row cannot be turned into a reversal statement (here a
// synthesized victim shape — DELETE with EventID 0 and a nil before image), the
// generator's #784 refusal must surface as a loud error AND leave ZERO bytes on
// the writer. Before the pre-preamble buffering, the refusal fired after the
// preamble was written, stranding a dangling `SET FOREIGN_KEY_CHECKS=0` (with no
// re-enable) in the output — in the CLI text path that partial script was
// flushed to --output despite the non-zero exit.
func TestEmitSQL_generationFailureRefusesCleanly(t *testing.T) {
	hdr := cascaderecover.Header{Schema: "shop", Table: "orders", Parents: 1, Children: 1}
	rows := []query.ResultRow{
		{ // renderable parent DELETE
			EventType:  event.EventDelete,
			SchemaName: "shop", TableName: "orders",
			PKValues:  "1",
			RowBefore: map[string]any{"id": float64(1)},
		},
		{ // un-renderable synthesized victim: nil before image, EventID 0
			EventType:  event.EventDelete,
			SchemaName: "shop", TableName: "order_items",
			PKValues: "10",
		},
	}

	var buf bytes.Buffer
	n, err := cascaderecover.EmitSQL(&buf, recovery.New(nil, nil), rows, nil, nil, hdr)
	if err == nil {
		t.Fatalf("expected a loud error, got n=%d output:\n%s", n, buf.String())
	}
	if !strings.Contains(err.Error(), "could not be reversed") {
		t.Errorf("error should carry the #784 partial-generation refusal, got: %v", err)
	}
	// The failing row is a cascade-synthesized victim (EventID 0, per the row comment
	// above) — the refusal must name it by schema.table+PK so multiple failing victims
	// are distinguishable, not by the untraceable, always-identical "event 0".
	if !strings.Contains(err.Error(), "shop.order_items pk=10") {
		t.Errorf("error should name the failing victim by schema.table+PK, got: %v", err)
	}
	if strings.Contains(err.Error(), "event 0:") {
		t.Errorf("error should not use the untraceable 'event 0' form for a cascade-synthesized victim, got: %v", err)
	}
	if n != 0 {
		t.Errorf("want 0 statements on refusal, got %d", n)
	}
	if buf.Len() != 0 {
		t.Fatalf("refusal must write nothing (no dangling FK-disable), wrote %d bytes: %q", buf.Len(), buf.String())
	}
}

// TestEmitSQL_rowsRenderBetweenPreambleAndFooter guards the pre-preamble
// buffering reorder (#835) on the success path: with a real renderable row the
// composition order must stay preamble → generator output → closing
// FK-checks re-enable, and the statement count must equal the row count.
func TestEmitSQL_rowsRenderBetweenPreambleAndFooter(t *testing.T) {
	hdr := cascaderecover.Header{Schema: "shop", Table: "orders", Parents: 1, Children: 0}
	rows := []query.ResultRow{{
		EventType:  event.EventDelete,
		SchemaName: "shop", TableName: "orders",
		PKValues:  "1",
		RowBefore: map[string]any{"id": float64(1)},
	}}

	got, n, err := emit(t, hdr, rows, nil, nil)
	if err != nil {
		t.Fatalf("EmitSQL: %v", err)
	}
	if n != 1 {
		t.Errorf("statement count = %d, want 1", n)
	}
	fkOff := strings.Index(got, "SET FOREIGN_KEY_CHECKS=0;")
	insert := strings.Index(got, "INSERT INTO `shop`.`orders`")
	fkOn := strings.Index(got, "SET FOREIGN_KEY_CHECKS=1;")
	if fkOff < 0 || insert < 0 || fkOn < 0 {
		t.Fatalf("missing section (fkOff=%d insert=%d fkOn=%d):\n%s", fkOff, insert, fkOn, got)
	}
	if !(fkOff < insert && insert < fkOn) {
		t.Errorf("section order broken (fkOff=%d insert=%d fkOn=%d):\n%s", fkOff, insert, fkOn, got)
	}
}

// TestEmitSQL_goldenPhase1 pins the byte-exact Phase-1 (no baseline) script: the
// full preamble (including the literal em-dash in the Phase-1 line), the
// FK-checks wrapper, and the empty-result body.
func TestEmitSQL_goldenPhase1(t *testing.T) {
	hdr := cascaderecover.Header{
		Schema: "shop", Table: "orders",
		Parents: 1, Children: 2,
		BaselineActive: false,
	}
	const want = `-- bintrail recover-cascade: reverse ON DELETE CASCADE / SET NULL side effects on shop.orders
-- Re-inserts 1 deleted parent row(s) and 2 cascade-deleted child row(s); restores 0 SET NULL'd FK(s)
-- that InnoDB removed/nulled below the binlog (MySQL Bug #32506). NEVER auto-applied.
--
-- Phase-1 (binlog-window) recovery: a child untouched within --lookback and not
-- in a baseline is NOT reconstructed — pass --baseline-dir/--baseline-s3 to enable
-- Phase-2 fallback. "Complete" means everything DETECTABLE was recovered.
--
-- If you have already re-created a deleted parent, delete its INSERT below:
-- SET FOREIGN_KEY_CHECKS=0 does NOT suppress PRIMARY KEY violations.

SET FOREIGN_KEY_CHECKS=0;

-- No events matched the specified criteria.

SET FOREIGN_KEY_CHECKS=1;
`
	got, n, err := emit(t, hdr, nil, nil, nil)
	if err != nil {
		t.Fatalf("EmitSQL: %v", err)
	}
	if got != want {
		t.Errorf("byte-identity drift.\n--- got ---\n%q\n--- want ---\n%q", got, want)
	}
	if n != 0 {
		t.Errorf("statement count = %d, want 0 (no rows, no SET NULL)", n)
	}
}

// TestEmitSQL_goldenPhase2Active pins the Phase-2 ("baseline fallback ACTIVE")
// preamble branch.
func TestEmitSQL_goldenPhase2Active(t *testing.T) {
	hdr := cascaderecover.Header{
		Schema: "shop", Table: "orders",
		Parents: 3, Children: 5,
		BaselineActive: true,
	}
	const want = `-- bintrail recover-cascade: reverse ON DELETE CASCADE / SET NULL side effects on shop.orders
-- Re-inserts 3 deleted parent row(s) and 5 cascade-deleted child row(s); restores 0 SET NULL'd FK(s)
-- that InnoDB removed/nulled below the binlog (MySQL Bug #32506). NEVER auto-applied.
--
-- Phase-2 baseline fallback ACTIVE: children present in a covered baseline are
-- reconstructed even if untouched within the window. Tables NOT covered by a
-- baseline are flagged above. "Complete" means everything DETECTABLE was recovered.
--
-- If you have already re-created a deleted parent, delete its INSERT below:
-- SET FOREIGN_KEY_CHECKS=0 does NOT suppress PRIMARY KEY violations.

SET FOREIGN_KEY_CHECKS=0;

-- No events matched the specified criteria.

SET FOREIGN_KEY_CHECKS=1;
`
	got, _, err := emit(t, hdr, nil, nil, nil)
	if err != nil {
		t.Fatalf("EmitSQL: %v", err)
	}
	if got != want {
		t.Errorf("byte-identity drift.\n--- got ---\n%q\n--- want ---\n%q", got, want)
	}
}

// TestEmitSQL_goldenCaveats pins the INCOMPLETE-RECOVERY caveats block (one line
// per caveat, three-space indent, em-dash header).
func TestEmitSQL_goldenCaveats(t *testing.T) {
	hdr := cascaderecover.Header{
		Schema: "shop", Table: "orders",
		Parents: 1, Children: 0,
		BaselineActive: false,
		Caveats: []string{
			"archived partitions exist; coverage unknown",
			"per-parent overflow at --limit",
		},
	}
	const want = `-- bintrail recover-cascade: reverse ON DELETE CASCADE / SET NULL side effects on shop.orders
-- Re-inserts 1 deleted parent row(s) and 0 cascade-deleted child row(s); restores 0 SET NULL'd FK(s)
-- that InnoDB removed/nulled below the binlog (MySQL Bug #32506). NEVER auto-applied.
--
-- Phase-1 (binlog-window) recovery: a child untouched within --lookback and not
-- in a baseline is NOT reconstructed — pass --baseline-dir/--baseline-s3 to enable
-- Phase-2 fallback. "Complete" means everything DETECTABLE was recovered.
--
-- If you have already re-created a deleted parent, delete its INSERT below:
-- SET FOREIGN_KEY_CHECKS=0 does NOT suppress PRIMARY KEY violations.
--
-- !!! INCOMPLETE RECOVERY — the result is provably partial:
--   - archived partitions exist; coverage unknown
--   - per-parent overflow at --limit

SET FOREIGN_KEY_CHECKS=0;

-- No events matched the specified criteria.

SET FOREIGN_KEY_CHECKS=1;
`
	got, _, err := emit(t, hdr, nil, nil, nil)
	if err != nil {
		t.Fatalf("EmitSQL: %v", err)
	}
	if got != want {
		t.Errorf("byte-identity drift.\n--- got ---\n%q\n--- want ---\n%q", got, want)
	}
}

// orderResolver builds a DB-free resolver for shop.orders with PK `id` and a
// nullable FK `customer_id`, used to exercise the SET NULL restoration path.
func orderResolver() *metadata.Resolver {
	tm := &metadata.TableMeta{
		Schema: "shop", Table: "orders",
		Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
			{Name: "customer_id", OrdinalPosition: 2, DataType: "int"},
		},
		PKColumns: []string{"id"},
	}
	return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{"shop.orders": tm})
}

// TestEmitSQL_goldenSetNull pins the SET NULL section: its idempotent-restore
// header, the guarded UPDATE (… AND fk IS NULL, produced by FormatSetNullRestore
// — the extraction must keep delegating to it), the per-statement semicolon, and
// the incremented statement count.
func TestEmitSQL_goldenSetNull(t *testing.T) {
	hdr := cascaderecover.Header{
		Schema: "shop", Table: "orders",
		Parents: 1, Children: 0,
		BaselineActive: false,
	}
	setNull := []cascade.SetNullRestore{{
		Schema: "shop", Table: "orders", Column: "customer_id",
		Value:    42,
		PKValues: "7",
		Row:      map[string]any{"id": 7, "customer_id": nil},
	}}
	// The UPDATE is built by recovery.FormatSetNullRestore; we pin its exact text
	// here (a double-quoted literal — a backtick raw string cannot contain the
	// backtick identifier quoting).
	const update = "UPDATE `shop`.`orders` SET `customer_id` = 42 WHERE `id` = 7 AND `customer_id` IS NULL"
	want := `-- bintrail recover-cascade: reverse ON DELETE CASCADE / SET NULL side effects on shop.orders
-- Re-inserts 1 deleted parent row(s) and 0 cascade-deleted child row(s); restores 1 SET NULL'd FK(s)
-- that InnoDB removed/nulled below the binlog (MySQL Bug #32506). NEVER auto-applied.
--
-- Phase-1 (binlog-window) recovery: a child untouched within --lookback and not
-- in a baseline is NOT reconstructed — pass --baseline-dir/--baseline-s3 to enable
-- Phase-2 fallback. "Complete" means everything DETECTABLE was recovered.
--
-- If you have already re-created a deleted parent, delete its INSERT below:
-- SET FOREIGN_KEY_CHECKS=0 does NOT suppress PRIMARY KEY violations.

SET FOREIGN_KEY_CHECKS=0;

-- No events matched the specified criteria.

-- SET NULL FK restorations (idempotent: only rows whose FK is still NULL):
` + update + `;

SET FOREIGN_KEY_CHECKS=1;
`
	got, n, err := emit(t, hdr, nil, setNull, orderResolver())
	if err != nil {
		t.Fatalf("EmitSQL: %v", err)
	}
	if got != want {
		t.Errorf("byte-identity drift.\n--- got ---\n%q\n--- want ---\n%q", got, want)
	}
	if !strings.Contains(got, "AND `customer_id` IS NULL") {
		t.Errorf("idempotency guard `... AND fk IS NULL` missing from SET NULL restore")
	}
	if n != 1 {
		t.Errorf("statement count = %d, want 1 (0 rows + 1 SET NULL)", n)
	}
}

// TestEmitSQL_setNullNilResolverIsAllOrNothing locks the #571-review invariant:
// when SET NULL rows exist but the pre-validation cannot build their statements
// (here, a nil resolver), EmitSQL must abort BEFORE writing a single byte — never
// a half-written script missing its closing SET FOREIGN_KEY_CHECKS=1.
func TestEmitSQL_setNullNilResolverIsAllOrNothing(t *testing.T) {
	hdr := cascaderecover.Header{Schema: "shop", Table: "orders"}
	setNull := []cascade.SetNullRestore{{
		Schema: "shop", Table: "orders", Column: "customer_id",
		Value: 42, PKValues: "7", Row: map[string]any{"id": 7},
	}}
	var buf bytes.Buffer
	n, err := cascaderecover.EmitSQL(&buf, recovery.New(nil, nil), nil, setNull, nil, hdr)
	if err == nil {
		t.Fatal("expected an error when SET NULL rows exist with a nil resolver")
	}
	if buf.Len() != 0 {
		t.Errorf("all-or-nothing violated: %d bytes written before abort:\n%q", buf.Len(), buf.String())
	}
	if n != 0 {
		t.Errorf("statement count = %d, want 0 on abort", n)
	}
}

// TestEmitSQL_setNullUnresolvableTableIsAllOrNothing covers the second abort
// point: the resolver exists but lacks the table, so Resolve fails. Still
// all-or-nothing — zero bytes written.
func TestEmitSQL_setNullUnresolvableTableIsAllOrNothing(t *testing.T) {
	hdr := cascaderecover.Header{Schema: "shop", Table: "orders"}
	setNull := []cascade.SetNullRestore{{
		Schema: "shop", Table: "unknown", Column: "customer_id",
		Value: 42, PKValues: "7", Row: map[string]any{"id": 7},
	}}
	var buf bytes.Buffer
	n, err := cascaderecover.EmitSQL(&buf, recovery.New(nil, orderResolver()), nil, setNull, orderResolver(), hdr)
	if err == nil {
		t.Fatal("expected an error when the SET NULL table is absent from the resolver")
	}
	if buf.Len() != 0 {
		t.Errorf("all-or-nothing violated: %d bytes written before abort:\n%q", buf.Len(), buf.String())
	}
	if n != 0 {
		t.Errorf("statement count = %d, want 0 on abort", n)
	}
}

// TestEmitSQL_setNullAbsentPKColumnIsAllOrNothing covers the THIRD abort point
// the package doc advertises: FormatSetNullRestore fails because the child row is
// missing a PK column. Still all-or-nothing — the resolver knows the table, but
// the row can't satisfy the WHERE, so EmitSQL aborts with zero bytes written.
func TestEmitSQL_setNullAbsentPKColumnIsAllOrNothing(t *testing.T) {
	hdr := cascaderecover.Header{Schema: "shop", Table: "orders"}
	setNull := []cascade.SetNullRestore{{
		Schema: "shop", Table: "orders", Column: "customer_id",
		Value: 42, PKValues: "7",
		Row: map[string]any{"customer_id": nil}, // PK column "id" absent
	}}
	var buf bytes.Buffer
	n, err := cascaderecover.EmitSQL(&buf, recovery.New(nil, orderResolver()), nil, setNull, orderResolver(), hdr)
	if err == nil {
		t.Fatal("expected an error when the SET NULL row is missing a PK column")
	}
	if buf.Len() != 0 {
		t.Errorf("all-or-nothing violated: %d bytes written before abort:\n%q", buf.Len(), buf.String())
	}
	if n != 0 {
		t.Errorf("statement count = %d, want 0 on abort", n)
	}
}

// TestEmitSQL_goldenSetNullMultiRow locks the derived-count refactor for N>1: with
// two SET NULL rows the preamble must read "restores 2", the section header must
// be emitted EXACTLY ONCE (not per row), one guarded UPDATE per row appears in
// order, and the statement count accumulates to 2. This is the case that proves
// the len(setNullRows)-derived count cannot desync from the statements emitted.
func TestEmitSQL_goldenSetNullMultiRow(t *testing.T) {
	hdr := cascaderecover.Header{
		Schema: "shop", Table: "orders",
		Parents: 1, Children: 0,
		BaselineActive: false,
	}
	setNull := []cascade.SetNullRestore{
		{Schema: "shop", Table: "orders", Column: "customer_id", Value: 42, PKValues: "7", Row: map[string]any{"id": 7, "customer_id": nil}},
		{Schema: "shop", Table: "orders", Column: "customer_id", Value: 99, PKValues: "8", Row: map[string]any{"id": 8, "customer_id": nil}},
	}
	const update1 = "UPDATE `shop`.`orders` SET `customer_id` = 42 WHERE `id` = 7 AND `customer_id` IS NULL"
	const update2 = "UPDATE `shop`.`orders` SET `customer_id` = 99 WHERE `id` = 8 AND `customer_id` IS NULL"
	want := `-- bintrail recover-cascade: reverse ON DELETE CASCADE / SET NULL side effects on shop.orders
-- Re-inserts 1 deleted parent row(s) and 0 cascade-deleted child row(s); restores 2 SET NULL'd FK(s)
-- that InnoDB removed/nulled below the binlog (MySQL Bug #32506). NEVER auto-applied.
--
-- Phase-1 (binlog-window) recovery: a child untouched within --lookback and not
-- in a baseline is NOT reconstructed — pass --baseline-dir/--baseline-s3 to enable
-- Phase-2 fallback. "Complete" means everything DETECTABLE was recovered.
--
-- If you have already re-created a deleted parent, delete its INSERT below:
-- SET FOREIGN_KEY_CHECKS=0 does NOT suppress PRIMARY KEY violations.

SET FOREIGN_KEY_CHECKS=0;

-- No events matched the specified criteria.

-- SET NULL FK restorations (idempotent: only rows whose FK is still NULL):
` + update1 + `;
` + update2 + `;

SET FOREIGN_KEY_CHECKS=1;
`
	got, n, err := emit(t, hdr, nil, setNull, orderResolver())
	if err != nil {
		t.Fatalf("EmitSQL: %v", err)
	}
	if got != want {
		t.Errorf("byte-identity drift.\n--- got ---\n%q\n--- want ---\n%q", got, want)
	}
	// The section header is emitted once regardless of row count.
	if c := strings.Count(got, "-- SET NULL FK restorations"); c != 1 {
		t.Errorf("SET NULL section header count = %d, want 1", c)
	}
	if n != 2 {
		t.Errorf("statement count = %d, want 2 (0 rows + 2 SET NULL)", n)
	}
}
