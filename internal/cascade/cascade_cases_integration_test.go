//go:build integration

package cascade_test

import (
	"context"
	"database/sql"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestLoadCascadeFKsFromIndex pins the productionized, source-less loader: after
// a snapshot, the FK graph WITH rules is readable from the index's
// fk_constraints, scoped by schema, with both CASCADE and non-CASCADE edges.
func TestLoadCascadeFKsFromIndex(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE parent (id INT PRIMARY KEY) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE child_c (
		id INT PRIMARY KEY, pid INT,
		CONSTRAINT fk_c FOREIGN KEY (pid) REFERENCES parent(id) ON DELETE CASCADE
	) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE child_r (
		id INT PRIMARY KEY, pid INT,
		CONSTRAINT fk_r FOREIGN KEY (pid) REFERENCES parent(id) ON DELETE RESTRICT
	) ENGINE=InnoDB`)
	// A composite (multi-column) FK must round-trip as multiple rows sharing one
	// constraint_name, so the synthesis composite-FK guard can detect it.
	testutil.MustExec(t, sourceDB, `CREATE TABLE pcomp (a INT, b INT, PRIMARY KEY (a, b)) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE child_comp (
		id INT PRIMARY KEY, fa INT, fb INT,
		CONSTRAINT fk_comp FOREIGN KEY (fa, fb) REFERENCES pcomp(a, b) ON DELETE CASCADE
	) ENGINE=InnoDB`)

	if _, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	fks, err := cascade.LoadCascadeFKs(ctx, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("LoadCascadeFKs: %v", err)
	}
	byTable := map[string]cascade.CascadeFK{}
	for _, fk := range fks {
		byTable[fk.Table] = fk
	}
	if c, ok := byTable["child_c"]; !ok || c.DeleteRule != "CASCADE" ||
		c.ReferencedTable != "parent" || c.ReferencedColumn != "id" || c.Column != "pid" {
		t.Errorf("child_c edge missing/wrong: %+v (ok=%v)", c, ok)
	}
	if r, ok := byTable["child_r"]; !ok || r.DeleteRule != "RESTRICT" {
		t.Errorf("child_r edge missing/wrong: %+v (ok=%v)", r, ok)
	}

	// The composite FK must come back as 2 rows sharing constraint_name (and
	// referenced columns a,b) — what the synthesis composite-FK guard keys on.
	var comp []cascade.CascadeFK
	for _, fk := range fks {
		if fk.Table == "child_comp" {
			comp = append(comp, fk)
		}
	}
	if len(comp) != 2 {
		t.Fatalf("composite FK must load as 2 column rows, got %d: %+v", len(comp), comp)
	}
	if comp[0].ConstraintName != "fk_comp" || comp[1].ConstraintName != "fk_comp" {
		t.Errorf("composite FK rows must share constraint_name fk_comp, got %q/%q",
			comp[0].ConstraintName, comp[1].ConstraintName)
	}

	// Scope: an unrelated schema yields nothing.
	none, err := cascade.LoadCascadeFKs(ctx, indexDB, []string{"no_such_schema"})
	if err != nil {
		t.Fatalf("LoadCascadeFKs(none): %v", err)
	}
	if len(none) != 0 {
		t.Errorf("want 0 edges for an unrelated schema, got %d", len(none))
	}
}

// TestSynthesizeVictims_ruleGate pins the deliberate non-bug: only ON DELETE
// CASCADE / SET NULL edges are synthesized. A pure RESTRICT edge and an
// ON-UPDATE-CASCADE-only edge (the dbtrail conflation) must yield nothing — and,
// being neither rule, never even hit the index, so no events are needed.
func TestSynthesizeVictims_ruleGate(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	eng := query.New(indexDB)

	parentDel := query.ResultRow{
		SchemaName: "app", TableName: "parent", EventType: event.EventDelete,
		PKValues: "1", RowBefore: map[string]any{"id": float64(1)},
		EventTimestamp: time.Now(),
	}
	fks := []cascade.CascadeFK{
		{Schema: "app", Table: "child_r", ConstraintName: "fk_r", Column: "pid",
			ReferencedSchema: "app", ReferencedTable: "parent", ReferencedColumn: "id",
			DeleteRule: "RESTRICT", UpdateRule: "RESTRICT"},
		{Schema: "app", Table: "child_u", ConstraintName: "fk_u", Column: "pid",
			ReferencedSchema: "app", ReferencedTable: "parent", ReferencedColumn: "id",
			DeleteRule: "RESTRICT", UpdateRule: "CASCADE"}, // ON UPDATE CASCADE only
	}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, fks,
		[]query.ResultRow{parentDel}, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.Victims) != 0 {
		t.Errorf("non-CASCADE/SET-NULL edges must yield no victims, got %d: %+v", len(res.Victims), res.Victims)
	}
	if len(res.SetNullRows) != 0 {
		t.Errorf("non-CASCADE/SET-NULL edges must yield no SET NULL restores, got %d: %+v", len(res.SetNullRows), res.SetNullRows)
	}
}

// TestSynthesizeVictims_childFKColumnSkew pins the child-side DDL-skew guard
// (#832): when a cascade OLDER than a child FK-column rename ("pid" -> "parent_id")
// is recovered, the candidate scan uses the LATEST snapshot's column name against
// events whose row-images still carry the old name, so ColumnEq matches 0 rows —
// indistinguishable from "no children existed". Synthesis must NOT report a clean
// Complete; it must flag skew (mirroring the parent-side "noref" caveat).
func TestSynthesizeVictims_childFKColumnSkew(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)

	T := time.Now().UTC()
	h := T.Add(-30 * time.Minute).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	ts := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	// Child events predate the rename: their row-images carry the OLD name "pid".
	// child 10 referenced parent 1 (via the column that is now called parent_id).
	testutil.InsertEvent(t, db, "b.000001", 10, 20, ts, nil, dbName, "child", 1, "10", nil, nil, []byte(`{"id":10,"pid":1}`))

	// The FK graph is the LATEST snapshot: the column is already renamed parent_id.
	fks := []cascade.CascadeFK{{
		Schema: dbName, Table: "child", ConstraintName: "fk", Column: "parent_id",
		ReferencedSchema: dbName, ReferencedTable: "parent", ReferencedColumn: "id",
		DeleteRule: "CASCADE",
	}}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, fks, parentDelete(dbName, T), cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.Victims) != 0 {
		t.Fatalf("renamed FK column must match 0 candidates, got %d: %v", len(res.Victims), victimList(res.Victims))
	}
	if res.Complete() || !strings.Contains(strings.Join(res.Incomplete, " "), "absent from every sampled") {
		t.Errorf("child-side DDL-skew must flag incompleteness, not a clean Complete; Incomplete=%v", res.Incomplete)
	}
}

// TestSynthesizeVictims_zeroChildrenNotFlaggedAsSkew is the control for the skew
// guard: a parent with genuinely no matching children — but whose child table
// carries the FK column under its snapshot name (referencing OTHER parents) — must
// stay Complete. The skew probe must not turn a legitimate zero-victim result into
// a false caveat.
func TestSynthesizeVictims_zeroChildrenNotFlaggedAsSkew(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)

	T := time.Now().UTC()
	h := T.Add(-30 * time.Minute).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	ts := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	// child 10 references parent 2 (NOT the deleted parent 1); the FK column
	// parent_id is present under its snapshot name.
	testutil.InsertEvent(t, db, "b.000001", 10, 20, ts, nil, dbName, "child", 1, "10", nil, nil, []byte(`{"id":10,"parent_id":2}`))

	fks := []cascade.CascadeFK{{
		Schema: dbName, Table: "child", ConstraintName: "fk", Column: "parent_id",
		ReferencedSchema: dbName, ReferencedTable: "parent", ReferencedColumn: "id",
		DeleteRule: "CASCADE",
	}}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, fks, parentDelete(dbName, T), cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.Victims) != 0 {
		t.Fatalf("parent 1 has no children, got %d victims: %v", len(res.Victims), victimList(res.Victims))
	}
	if !res.Complete() {
		t.Errorf("a genuine no-children result (FK column present) must stay Complete, got Incomplete=%v", res.Incomplete)
	}
}

// TestSynthesizeVictims_setNull pins ON DELETE SET NULL: a child that referenced
// the deleted parent (and survives) becomes a SetNullRestore (not a victim), and
// it is NOT recursed (no row was deleted). A child re-pointed away is excluded.
func TestSynthesizeVictims_setNull(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)

	T := time.Now().UTC()
	h := T.Add(-30 * time.Minute).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	ts := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	// child 10 references parent 1 (SET NULL victim); child 13 re-pointed to 2.
	testutil.InsertEvent(t, db, "b.000001", 10, 20, ts, nil, dbName, "child", 1, "10", nil, nil, []byte(`{"id":10,"pid":1}`))
	testutil.InsertEvent(t, db, "b.000001", 20, 30, ts, nil, dbName, "child", 1, "13", nil, nil, []byte(`{"id":13,"pid":1}`))
	testutil.InsertEvent(t, db, "b.000001", 30, 40, ts, nil, dbName, "child", 2 /*UPDATE*/, "13", nil, []byte(`{"id":13,"pid":1}`), []byte(`{"id":13,"pid":2}`))

	fks := []cascade.CascadeFK{{
		Schema: dbName, Table: "child", ConstraintName: "fk", Column: "pid",
		ReferencedSchema: dbName, ReferencedTable: "parent", ReferencedColumn: "id",
		DeleteRule: "SET NULL",
	}}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, fks, parentDelete(dbName, T), cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.Victims) != 0 {
		t.Errorf("SET NULL must produce no DELETE victims (rows survive), got %v", res.Victims)
	}
	if len(res.SetNullRows) != 1 || res.SetNullRows[0].PKValues != "10" || res.SetNullRows[0].Column != "pid" {
		t.Fatalf("want one SET NULL restore for child:10 column pid, got %+v", res.SetNullRows)
	}
	if got := cascadeValToStr(res.SetNullRows[0].Value); got != "1" {
		t.Errorf("restore value should be the parent key 1, got %q", got)
	}
	for _, sr := range res.SetNullRows {
		if sr.PKValues == "13" {
			t.Errorf("re-pointed child 13 must NOT be a SET NULL restore")
		}
	}
}

func cascadeValToStr(v any) string { return fmt.Sprintf("%v", v) }

// TestSynthesizeVictims_compositeFKSkipped pins the composite-FK guard: a
// multi-column CASCADE FK cannot be reconstructed by the single-column victim
// match, so it must be SKIPPED with a warning, never silently mis-synthesized.
func TestSynthesizeVictims_compositeFKSkipped(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	eng := query.New(indexDB)

	parentDel := query.ResultRow{
		SchemaName: "app", TableName: "parent", EventType: event.EventDelete,
		PKValues: "1|9", RowBefore: map[string]any{"k1": float64(1), "k2": float64(9)},
		EventTimestamp: time.Now(),
	}
	// One composite CASCADE FK = two rows sharing the constraint name.
	fks := []cascade.CascadeFK{
		{Schema: "app", Table: "child", ConstraintName: "fk_comp", Column: "pk1",
			ReferencedSchema: "app", ReferencedTable: "parent", ReferencedColumn: "k1", DeleteRule: "CASCADE"},
		{Schema: "app", Table: "child", ConstraintName: "fk_comp", Column: "pk2",
			ReferencedSchema: "app", ReferencedTable: "parent", ReferencedColumn: "k2", DeleteRule: "CASCADE"},
	}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, fks,
		[]query.ResultRow{parentDel}, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.Victims) != 0 {
		t.Errorf("composite FK must be skipped, not mis-synthesized; got %d victims", len(res.Victims))
	}
	if res.Complete() || !strings.Contains(strings.Join(res.Incomplete, " "), "composite FK") {
		t.Errorf("composite FK skip must flag incompleteness; Incomplete: %v", res.Incomplete)
	}
}

// TestSynthesizeVictims_selfRefAndCompositePK exercises two robustness cases in
// one real cascade: a SELF-REFERENCING tree (node.parent_id → node.id ON DELETE
// CASCADE) and a COMPOSITE-PK child (leaf PK (node_id, seq)). Deleting the tree
// root cascades through the whole subtree (none of it binlogged); synthesis must
// recurse over the self-ref without a false cycle and restore both tables
// byte-exact, including the pipe-delimited composite PKs.
func TestSynthesizeVictims_selfRefAndCompositePK(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE node (
		id        INT PRIMARY KEY,
		parent_id INT NULL,
		label     VARCHAR(32),
		CONSTRAINT fk_self FOREIGN KEY (parent_id) REFERENCES node(id) ON DELETE CASCADE
	) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE leaf (
		node_id INT,
		seq     INT,
		payload VARCHAR(32),
		PRIMARY KEY (node_id, seq),
		CONSTRAINT fk_leaf FOREIGN KEY (node_id) REFERENCES node(id) ON DELETE CASCADE
	) ENGINE=InnoDB`)

	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	resolver, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	currentBinlog, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition: %v", err)
	}

	// Tree: 1(root) → {2,3}; 2 → {4}. Leaves hang off nodes 2 and 4.
	testutil.MustExec(t, sourceDB, "INSERT INTO node VALUES (1,NULL,'root'),(2,1,'a'),(3,1,'b'),(4,2,'c')")
	testutil.MustExec(t, sourceDB, "INSERT INTO leaf VALUES (2,1,'l21'),(2,2,'l22'),(4,1,'l41')")

	// Update the GRANDCHILD (node 4) and its composite-PK leaf in a strictly
	// LATER statement than their parent's last event. This is the discriminating
	// timing: the per-level window must end at the ROOT delete time T, not at
	// each victim's own timestamp — otherwise node 4's (and leaf 4|1's) post-T1
	// update falls outside the deeper window and a stale state is recovered.
	time.Sleep(1100 * time.Millisecond) // binlog ts is second-granular
	testutil.MustExec(t, sourceDB, "UPDATE node SET label='c2' WHERE id=4")
	testutil.MustExec(t, sourceDB, "UPDATE leaf SET payload='l41b' WHERE node_id=4 AND seq=1")

	time.Sleep(1100 * time.Millisecond) // parent delete strictly after every child event

	wantNode := checksum(t, sourceDB, "node")
	wantLeaf := checksum(t, sourceDB, "leaf")

	// Delete the root; InnoDB cascades to nodes 2,3,4 and all leaves INTERNALLY
	// (only the node id=1 delete is binlogged).
	testutil.MustExec(t, sourceDB, "DELETE FROM node WHERE id=1")
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")

	tmpDir := t.TempDir()
	cp := exec.Command("docker", "cp",
		fmt.Sprintf("bintrail-test-mysql:/var/lib/mysql/%s", currentBinlog),
		filepath.Join(tmpDir, currentBinlog))
	if out, cperr := cp.CombinedOutput(); cperr != nil {
		t.Fatalf("docker cp %s: %v\n%s", currentBinlog, cperr, out)
	}

	p := parser.New(tmpDir, resolver, parser.Filters{Schemas: map[string]bool{sourceName: true}}, nil)
	idx := indexer.New(indexDB, 100)
	events := make(chan parser.Event, 256)
	parseErr := make(chan error, 1)
	go func() {
		defer close(events)
		parseErr <- p.ParseFile(ctx, currentBinlog, events)
	}()
	if _, rerr := idx.Run(ctx, events); rerr != nil {
		t.Fatalf("indexer.Run: %v", rerr)
	}
	if perr := <-parseErr; perr != nil {
		t.Fatalf("ParseFile: %v", perr)
	}

	eng := query.New(indexDB)
	del := event.EventDelete
	nodeDeletes := mustFetch(t, eng, query.Options{Schema: sourceName, Table: "node", EventType: &del})
	if len(nodeDeletes) != 1 || nodeDeletes[0].PKValues != "1" {
		t.Fatalf("want only the root node delete indexed, got %d: %v", len(nodeDeletes), pkList(nodeDeletes))
	}

	fks, err := cascade.LoadCascadeFKs(ctx, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("LoadCascadeFKs: %v", err)
	}
	res, err := cascade.SynthesizeVictims(ctx, eng, fks, nodeDeletes, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if !res.Complete() {
		t.Errorf("expected a complete reconstruction, got Incomplete=%v", res.Incomplete)
	}
	victims := res.Victims
	// Expect nodes 2,3,4 (self-ref recursion) + leaves 2|1, 2|2, 4|1 (composite PK).
	got := map[string]bool{}
	for _, v := range victims {
		got[v.TableName+":"+v.PKValues] = true
	}
	for _, want := range []string{"node:2", "node:3", "node:4", "leaf:2|1", "leaf:2|2", "leaf:4|1"} {
		if !got[want] {
			t.Errorf("missing synthesized victim %s; got %v", want, victimList(victims))
		}
	}
	if len(victims) != 6 {
		t.Errorf("want 6 victims, got %d: %v", len(victims), victimList(victims))
	}

	// node 4 was UPDATEd after its parent's last event; with the root-T window it
	// must be recovered at its LATEST label ('c2'), not the stale insert ('c').
	// The byte-exact checksum below also enforces this; assert it explicitly too.
	for _, v := range victims {
		if v.TableName == "node" && v.PKValues == "4" && v.RowBefore["label"] != "c2" {
			t.Errorf("node 4 recovered at stale label %v, want c2", v.RowBefore["label"])
		}
	}

	// Depth cap (read-only re-run on the same index): MaxDepth=1 must reconstruct
	// only the root's direct children and flag the deeper subtree as incomplete.
	resD, err := cascade.SynthesizeVictims(ctx, eng, fks, nodeDeletes, cascade.Options{MaxDepth: 1})
	if err != nil {
		t.Fatalf("SynthesizeVictims(MaxDepth=1): %v", err)
	}
	if resD.Complete() || !strings.Contains(strings.Join(resD.Incomplete, " "), "MaxDepth") {
		t.Errorf("MaxDepth=1 must flag depth incompleteness; Incomplete=%v", resD.Incomplete)
	}
	for _, v := range resD.Victims {
		if v.TableName == "node" && v.PKValues == "4" {
			t.Errorf("MaxDepth=1 must not reach grandchild node:4; got %v", victimList(resD.Victims))
		}
	}

	// Candidate cap: CandidateLimit=1 truncates node 1's two children and flags it.
	resC, err := cascade.SynthesizeVictims(ctx, eng, fks, nodeDeletes, cascade.Options{CandidateLimit: 1})
	if err != nil {
		t.Fatalf("SynthesizeVictims(CandidateLimit=1): %v", err)
	}
	if resC.Complete() || !strings.Contains(strings.Join(resC.Incomplete, " "), "more than 1") {
		t.Errorf("CandidateLimit=1 must flag truncation; Incomplete=%v", resC.Incomplete)
	}

	// Recover (root from binlog + synthesized subtree) and apply FK-checks-off.
	gen := recoveryNew(t, indexDB, resolver)
	rows := append(append([]query.ResultRow{}, nodeDeletes...), victims...)
	sqlText := generateSQL(t, gen, rows)
	applyFKOff(t, sourceName, sqlText)

	if got := checksum(t, sourceDB, "node"); got != wantNode {
		t.Errorf("node checksum mismatch: want %d, got %d", wantNode, got)
	}
	if got := checksum(t, sourceDB, "leaf"); got != wantLeaf {
		t.Errorf("leaf checksum mismatch: want %d, got %d", wantLeaf, got)
	}
}

// --- small shared helpers for the recovery+apply tail ---

func recoveryNew(t *testing.T, indexDB *sql.DB, resolver *metadata.Resolver) *recovery.Generator {
	t.Helper()
	return recovery.New(indexDB, resolver)
}

func generateSQL(t *testing.T, gen *recovery.Generator, rows []query.ResultRow) string {
	t.Helper()
	var buf strings.Builder
	if _, err := gen.GenerateSQLFromRows(rows, &buf); err != nil {
		t.Fatalf("GenerateSQLFromRows: %v", err)
	}
	return buf.String()
}

func applyFKOff(t *testing.T, dbName, sqlText string) {
	t.Helper()
	conn, err := sql.Open("mysql", testutil.BaseDSN()+"/"+dbName+"?multiStatements=true")
	if err != nil {
		t.Fatalf("open apply conn: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Exec("SET FOREIGN_KEY_CHECKS=0;\n" + sqlText + "\nSET FOREIGN_KEY_CHECKS=1;"); err != nil {
		t.Fatalf("apply recovery SQL: %v", err)
	}
}

// captureAndIndex flushes to a clean binlog, runs dml (which performs the
// inserts/updates/deletes to capture), seals + copies the file, and parses it
// into the index. One captured window per call.
func captureAndIndex(t *testing.T, sourceDB, indexDB *sql.DB, resolver *metadata.Resolver, sourceName string, dml func()) {
	t.Helper()
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	currentBinlog, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition: %v", err)
	}
	dml()
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")

	tmpDir := t.TempDir()
	cp := exec.Command("docker", "cp",
		fmt.Sprintf("bintrail-test-mysql:/var/lib/mysql/%s", currentBinlog),
		filepath.Join(tmpDir, currentBinlog))
	if out, e := cp.CombinedOutput(); e != nil {
		t.Fatalf("docker cp %s: %v\n%s", currentBinlog, e, out)
	}
	p := parser.New(tmpDir, resolver, parser.Filters{Schemas: map[string]bool{sourceName: true}}, nil)
	idx := indexer.New(indexDB, 100)
	events := make(chan parser.Event, 256)
	perr := make(chan error, 1)
	go func() { defer close(events); perr <- p.ParseFile(context.Background(), currentBinlog, events) }()
	if _, e := idx.Run(context.Background(), events); e != nil {
		t.Fatalf("indexer.Run: %v", e)
	}
	if e := <-perr; e != nil {
		t.Fatalf("ParseFile: %v", e)
	}
}

// TestSynthesizeVictims_multiPathDedup pins the multi-path dedup: a child
// reachable from the deleted parent via TWO CASCADE FKs must be emitted ONCE,
// or recovery would double-INSERT its PK and fail on apply.
func TestSynthesizeVictims_multiPathDedup(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE p (id INT PRIMARY KEY) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE c (
		id INT PRIMARY KEY, ref1 INT, ref2 INT,
		CONSTRAINT fk1 FOREIGN KEY (ref1) REFERENCES p(id) ON DELETE CASCADE,
		CONSTRAINT fk2 FOREIGN KEY (ref2) REFERENCES p(id) ON DELETE CASCADE
	) ENGINE=InnoDB`)

	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	resolver, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	captureAndIndex(t, sourceDB, indexDB, resolver, sourceName, func() {
		testutil.MustExec(t, sourceDB, "INSERT INTO p VALUES (1)")
		testutil.MustExec(t, sourceDB, "INSERT INTO c VALUES (100,1,1)") // references p=1 via BOTH FKs
		time.Sleep(1100 * time.Millisecond)
		testutil.MustExec(t, sourceDB, "DELETE FROM p WHERE id=1") // cascades c once
	})

	eng := query.New(indexDB)
	del := event.EventDelete
	parentDeletes := mustFetch(t, eng, query.Options{Schema: sourceName, Table: "p", EventType: &del})
	fks, err := cascade.LoadCascadeFKs(ctx, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("LoadCascadeFKs: %v", err)
	}

	res, err := cascade.SynthesizeVictims(ctx, eng, fks, parentDeletes, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.Victims) != 1 {
		t.Fatalf("multi-path child must be emitted exactly once, got %d: %v", len(res.Victims), victimList(res.Victims))
	}
	if res.Victims[0].TableName != "c" || res.Victims[0].PKValues != "100" {
		t.Errorf("want victim c:100, got %+v", res.Victims[0])
	}
}

// TestSynthesizeVictims_multiRootIndependentT pins the breadth dimension of the
// root-T fix: two independent cascades deleted at T_A ≪ T_B, each must use its
// OWN root T. cb is updated BETWEEN T_A and T_B; only per-root T_B recovers its
// latest 'b1' — a collapse to a single global T (first/min/now) recovers stale 'b0'.
func TestSynthesizeVictims_multiRootIndependentT(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE pa (id INT PRIMARY KEY) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE pb (id INT PRIMARY KEY) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE ca (
		id INT PRIMARY KEY, pid INT, val VARCHAR(16),
		CONSTRAINT fka FOREIGN KEY (pid) REFERENCES pa(id) ON DELETE CASCADE
	) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE cb (
		id INT PRIMARY KEY, pid INT, val VARCHAR(16),
		CONSTRAINT fkb FOREIGN KEY (pid) REFERENCES pb(id) ON DELETE CASCADE
	) ENGINE=InnoDB`)

	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	resolver, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	captureAndIndex(t, sourceDB, indexDB, resolver, sourceName, func() {
		testutil.MustExec(t, sourceDB, "INSERT INTO pa VALUES (1)")
		testutil.MustExec(t, sourceDB, "INSERT INTO pb VALUES (2)")
		testutil.MustExec(t, sourceDB, "INSERT INTO ca VALUES (10,1,'a0')")
		testutil.MustExec(t, sourceDB, "INSERT INTO cb VALUES (20,2,'b0')")
		testutil.MustExec(t, sourceDB, "DELETE FROM pa WHERE id=1") // T_A: cascades ca
		time.Sleep(1100 * time.Millisecond)
		testutil.MustExec(t, sourceDB, "UPDATE cb SET val='b1' WHERE id=20") // between T_A and T_B
		time.Sleep(1100 * time.Millisecond)
		testutil.MustExec(t, sourceDB, "DELETE FROM pb WHERE id=2") // T_B: cascades cb
	})

	eng := query.New(indexDB)
	del := event.EventDelete
	paDel := mustFetch(t, eng, query.Options{Schema: sourceName, Table: "pa", EventType: &del})
	pbDel := mustFetch(t, eng, query.Options{Schema: sourceName, Table: "pb", EventType: &del})
	parentDeletes := append(append([]query.ResultRow{}, paDel...), pbDel...)
	fks, err := cascade.LoadCascadeFKs(ctx, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("LoadCascadeFKs: %v", err)
	}

	res, err := cascade.SynthesizeVictims(ctx, eng, fks, parentDeletes, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	byPK := map[string]query.ResultRow{}
	for _, v := range res.Victims {
		byPK[v.TableName+":"+v.PKValues] = v
	}
	ca, ok := byPK["ca:10"]
	if !ok {
		t.Fatalf("ca:10 missing; got %v", victimList(res.Victims))
	}
	if ca.RowBefore["val"] != "a0" {
		t.Errorf("ca:10 val = %v, want a0", ca.RowBefore["val"])
	}
	cb, ok := byPK["cb:20"]
	if !ok {
		t.Fatalf("cb:20 missing; got %v", victimList(res.Victims))
	}
	if cb.RowBefore["val"] != "b1" {
		t.Errorf("cb:20 recovered at %v, want b1 (per-root T regression — using a collapsed global T?)", cb.RowBefore["val"])
	}
}

// TestSynthesizeVictims_setNullTwoColumnsSameRow is the regression for the #571
// review CRITICAL: a SetNullRestore is per-COLUMN, but the dedup key was per-ROW,
// so a child carrying TWO single-column SET NULL FKs to the same deleted parent
// (e.g. mgr + mentor → parent.id) had its second column silently dropped. Both
// columns must be restored, and the result must stay Complete.
func TestSynthesizeVictims_setNullTwoColumnsSameRow(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	eng := query.New(db)

	T := time.Now().UTC()
	h := T.Add(-30 * time.Minute).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	ts := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	// child 10 points at parent 1 via BOTH mgr and mentor; the cascade nulls both.
	testutil.InsertEvent(t, db, "b.000001", 10, 20, ts, nil, dbName, "child", 1, "10", nil, nil, []byte(`{"id":10,"mgr":1,"mentor":1}`))

	fks := []cascade.CascadeFK{
		{Schema: dbName, Table: "child", ConstraintName: "fk_mgr", Column: "mgr",
			ReferencedSchema: dbName, ReferencedTable: "parent", ReferencedColumn: "id", DeleteRule: "SET NULL"},
		{Schema: dbName, Table: "child", ConstraintName: "fk_mentor", Column: "mentor",
			ReferencedSchema: dbName, ReferencedTable: "parent", ReferencedColumn: "id", DeleteRule: "SET NULL"},
	}
	res, err := cascade.SynthesizeVictims(context.Background(), eng, fks, parentDelete(dbName, T), cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(res.Victims) != 0 {
		t.Errorf("SET NULL produces no DELETE victims, got %v", res.Victims)
	}
	cols := map[string]bool{}
	for _, sr := range res.SetNullRows {
		if sr.PKValues != "10" {
			t.Errorf("unexpected restore for pk=%s", sr.PKValues)
		}
		cols[sr.Column] = true
	}
	if len(res.SetNullRows) != 2 || !cols["mgr"] || !cols["mentor"] {
		t.Fatalf("both mgr and mentor of child:10 must be restored (per-column key), got %+v", res.SetNullRows)
	}
	if !res.Complete() {
		t.Errorf("two-column SET NULL restore should be complete, got %v", res.Incomplete)
	}
}

// TestSynthesizeVictims_setNullGuardProtectsRepointedRow proves the `IS NULL`
// guard is load-bearing, not decorative. A child re-pointed to a NEW parent after
// the (unlogged) SET NULL still surfaces a STALE restore candidate — its pre-null
// INSERT image is the only event matching the fk=parent scan, so a SetNullRestore
// to the OLD parent IS emitted. Only the runtime `AND fk IS NULL` predicate stops
// it from clobbering the live re-pointed value. This applies the generated UPDATE
// against the real row and asserts the row is unchanged.
func TestSynthesizeVictims_setNullGuardProtectsRepointedRow(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE parent (id INT PRIMARY KEY) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE child (
		id  INT PRIMARY KEY,
		mgr INT NULL,
		CONSTRAINT fk_mgr FOREIGN KEY (mgr) REFERENCES parent(id) ON DELETE SET NULL
	) ENGINE=InnoDB`)

	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	resolver, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	testutil.MustExec(t, sourceDB, "INSERT INTO parent VALUES (1),(2)")

	// Capture the whole child history: its INSERT (mgr=1, the stale candidate),
	// the parent delete (InnoDB nulls child.mgr, UNLOGGED), then the operator's
	// re-point to parent 2 (logged UPDATE before=NULL/after=2).
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	currentBinlog, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition: %v", err)
	}
	testutil.MustExec(t, sourceDB, "INSERT INTO child VALUES (10,1)")
	testutil.MustExec(t, sourceDB, "DELETE FROM parent WHERE id=1")
	testutil.MustExec(t, sourceDB, "UPDATE child SET mgr=2 WHERE id=10")
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")

	tmpDir := t.TempDir()
	cp := exec.Command("docker", "cp",
		fmt.Sprintf("bintrail-test-mysql:/var/lib/mysql/%s", currentBinlog),
		filepath.Join(tmpDir, currentBinlog))
	if out, cperr := cp.CombinedOutput(); cperr != nil {
		t.Fatalf("docker cp %s: %v\n%s", currentBinlog, cperr, out)
	}
	p := parser.New(tmpDir, resolver, parser.Filters{Schemas: map[string]bool{sourceName: true}}, nil)
	idx := indexer.New(indexDB, 100)
	events := make(chan parser.Event, 256)
	parseErr := make(chan error, 1)
	go func() { defer close(events); parseErr <- p.ParseFile(ctx, currentBinlog, events) }()
	if _, rerr := idx.Run(ctx, events); rerr != nil {
		t.Fatalf("indexer.Run: %v", rerr)
	}
	if perr := <-parseErr; perr != nil {
		t.Fatalf("ParseFile: %v", perr)
	}

	// Live truth after the re-point: child 10 now points at parent 2.
	wantChild := checksum(t, sourceDB, "child")

	eng := query.New(indexDB)
	del := event.EventDelete
	parentDeletes := mustFetch(t, eng, query.Options{Schema: sourceName, Table: "parent", EventType: &del})
	if len(parentDeletes) != 1 || parentDeletes[0].PKValues != "1" {
		t.Fatalf("want only the parent 1 delete indexed, got %v", pkList(parentDeletes))
	}
	fks, err := cascade.LoadCascadeFKs(ctx, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("LoadCascadeFKs: %v", err)
	}
	res, err := cascade.SynthesizeVictims(ctx, eng, fks, parentDeletes, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	// The synthesis can't distinguish the re-pointed child from a still-nulled
	// one, so it DOES emit a stale restore to mgr=1 — the guard is the defense.
	if len(res.SetNullRows) != 1 || res.SetNullRows[0].PKValues != "10" {
		t.Fatalf("expected one (stale) SET NULL restore for child:10, got %+v", res.SetNullRows)
	}

	tm, err := resolver.Resolve(sourceName, "child")
	if err != nil {
		t.Fatalf("resolve child: %v", err)
	}
	sr := res.SetNullRows[0]
	stmt, err := recovery.FormatSetNullRestore(sr.Schema, sr.Table, sr.Column, sr.Value, tm.PKColumnMetas(), sr.Row)
	if err != nil {
		t.Fatalf("FormatSetNullRestore: %v", err)
	}
	if !strings.Contains(stmt, "IS NULL") {
		t.Fatalf("restore must carry the IS NULL guard: %q", stmt)
	}
	applyFKOff(t, sourceName, stmt+";")

	// The guard must have made the UPDATE a no-op: child 10 still points at 2.
	if got := checksum(t, sourceDB, "child"); got != wantChild {
		t.Errorf("guard failed: re-pointed child was clobbered (checksum want %d, got %d)", wantChild, got)
	}
	var mgr sql.NullInt64
	if err := sourceDB.QueryRow("SELECT mgr FROM child WHERE id=10").Scan(&mgr); err != nil {
		t.Fatalf("read child: %v", err)
	}
	if !mgr.Valid || mgr.Int64 != 2 {
		t.Errorf("child 10 mgr should remain 2 (re-pointed), got %v", mgr)
	}
}
