//go:build integration

package shim

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// pgRelation builds the minimal PGRelationSchema WritePGSnapshot needs:
// an `id` PK plus one payload column, mirroring the shape the pgcapture
// decoder emits from a RelationMessage.
func pgRelation(schema, table string, cols ...string) *metadata.PGRelationSchema {
	rel := &metadata.PGRelationSchema{
		Schema: schema,
		Table:  table,
		Columns: []metadata.PGRelationColumn{
			{Name: "id", Ordinal: 1, IsPK: true, TypeOID: 23, TypeMod: -1},
		},
	}
	for i, c := range cols {
		rel.Columns = append(rel.Columns, metadata.PGRelationColumn{
			Name: c, Ordinal: i + 2, TypeOID: 25, TypeMod: -1,
		})
	}
	return rel
}

// TestShowTablesFromVirtual_PGPerTableSnapshots pins issue #603: a
// PostgreSQL source persists ONE table per snapshot_id (WritePGSnapshot
// allocates MAX+1 on every pgoutput RelationMessage), so the shim's
// latest-snapshot resolver saw only the last table that had DML.
// `SHOW TABLES FROM _flashback` against a PG index with three tables
// returned a single row. The fix loads the newest snapshot PER TABLE
// (metadata.NewLatestPerTableResolver), so all three list.
//
// No live PostgreSQL is needed: WritePGSnapshot writes into the MySQL
// INDEX database, which is all the shim reads.
func TestShowTablesFromVirtual_PGPerTableSnapshots(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Three relations arrive on the stream, each becoming its own
	// snapshot_id (1, 2, 3). "items" is the last one that saw DML.
	for _, tbl := range []string{"users", "orders", "items"} {
		if _, err := metadata.WritePGSnapshot(context.Background(), db, pgRelation("public", tbl, "payload")); err != nil {
			t.Fatalf("WritePGSnapshot(%s): %v", tbl, err)
		}
	}

	h := NewHandler(db, slog.New(slog.NewTextHandler(io.Discard, nil)))
	h.UseDB("public")

	res, err := h.HandleQuery("SHOW TABLES FROM _flashback")
	if err != nil {
		t.Fatalf("SHOW TABLES FROM _flashback: %v", err)
	}
	if res == nil || res.Resultset == nil {
		t.Fatal("expected resultset, got nil")
	}

	var got []string
	for _, cells := range rowCells(t, res.Resultset) {
		got = append(got, cells[0])
	}
	want := []string{"items", "orders", "users"} // Tables() sorts by name
	if !slices.Equal(got, want) {
		t.Errorf("SHOW TABLES FROM _flashback = %v, want %v (issue #603: only the latest per-table snapshot's table listed)", got, want)
	}
}

// TestColumnOrderFor_PGNonLatestTable pins the columnOrderFor seam of
// #603: with per-table PG snapshots, any table that is not the
// latest-snapshotted one resolved to nil column order (alphabetical
// fallback on the wire, and — pre-#600 — silently dropped columns).
// With the per-table-newest resolver, every snapshotted table resolves
// its own DDL order.
func TestColumnOrderFor_PGNonLatestTable(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// users is snapshot 1; orders (snapshot 2) is the latest.
	if _, err := metadata.WritePGSnapshot(context.Background(), db, pgRelation("public", "users", "zz_email", "aa_name")); err != nil {
		t.Fatalf("WritePGSnapshot(users): %v", err)
	}
	if _, err := metadata.WritePGSnapshot(context.Background(), db, pgRelation("public", "orders", "total")); err != nil {
		t.Fatalf("WritePGSnapshot(orders): %v", err)
	}

	h := NewHandler(db, slog.New(slog.NewTextHandler(io.Discard, nil)))

	// DDL (ordinal) order, not alphabetical — proves the resolver found
	// the table's own snapshot rather than degrading to the nil fallback.
	want := []string{"id", "zz_email", "aa_name"}
	if got := h.columnOrderFor("public", "users"); !slices.Equal(got, want) {
		t.Errorf("columnOrderFor(public.users) = %v, want %v (issue #603: non-latest PG table lost its column order)", got, want)
	}
	if got := h.columnOrderFor("public", "orders"); !slices.Equal(got, []string{"id", "total"}) {
		t.Errorf("columnOrderFor(public.orders) = %v, want [id total]", got)
	}
}
