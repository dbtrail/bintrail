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
// CASCADE edges are delete-synthesized. A pure RESTRICT edge and an
// ON-UPDATE-CASCADE-only edge (the dbtrail conflation) must yield no victims —
// and, being non-CASCADE, never even hit the index, so no events are needed.
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
	victims, _, err := cascade.SynthesizeVictims(context.Background(), eng, fks,
		[]query.ResultRow{parentDel}, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(victims) != 0 {
		t.Errorf("non-ON-DELETE-CASCADE edges must yield no victims, got %d: %+v", len(victims), victims)
	}
}

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
	victims, warnings, err := cascade.SynthesizeVictims(context.Background(), eng, fks,
		[]query.ResultRow{parentDel}, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	if len(victims) != 0 {
		t.Errorf("composite FK must be skipped, not mis-synthesized; got %d victims", len(victims))
	}
	if !strings.Contains(strings.Join(warnings, " "), "composite FK") {
		t.Errorf("composite FK skip must warn; warnings: %v", warnings)
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
	victims, warnings, err := cascade.SynthesizeVictims(ctx, eng, fks, nodeDeletes, cascade.Options{})
	if err != nil {
		t.Fatalf("SynthesizeVictims: %v", err)
	}
	for _, w := range warnings {
		t.Logf("warning: %s", w)
	}
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
