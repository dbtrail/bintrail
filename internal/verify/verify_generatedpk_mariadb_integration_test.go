//go:build integration

package verify

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestVerifyTable_mariadbSystemVersioning_generatedPKGate pins the premise the
// whole #1266 gate rests on against a REAL MariaDB source: creating a
// system-versioned table with explicit period columns extends its PRIMARY KEY
// with row_end, and the snapshot pipeline records that member as generated
// (metadata derives is_generated from GENERATION_EXPRESSION, which MariaDB
// fills with "ROW END") — so reconstruct.GeneratedPKColumn fires on
// production-shaped metadata, not only on the hand-built resolvers of the
// unit tests. If MariaDB ever changes the PK extension or the
// information_schema signal, this test fails instead of the prose in
// GeneratedPKColumn's doc comment silently going stale.
func TestVerifyTable_mariadbSystemVersioning_generatedPKGate(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, fmt.Sprintf(
		"CREATE TABLE `%s`.`sv_orders` ("+
			"`id` INT PRIMARY KEY, `val` VARCHAR(20), "+
			"`row_start` TIMESTAMP(6) GENERATED ALWAYS AS ROW START, "+
			"`row_end` TIMESTAMP(6) GENERATED ALWAYS AS ROW END, "+
			"PERIOD FOR SYSTEM_TIME(`row_start`,`row_end`)"+
			") WITH SYSTEM VERSIONING", sourceName))

	if _, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	resolver, err := metadata.NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	tm, err := resolver.Resolve(sourceName, "sv_orders")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	// The premise, on real metadata: MariaDB extended the PK with row_end and
	// the snapshot marked it generated.
	c, ok := reconstruct.GeneratedPKColumn(tm.PKColumnMetas())
	if !ok {
		t.Fatalf("GeneratedPKColumn = false; PK metas = %+v — MariaDB no longer extends the PK with a "+
			"generated period column, or the snapshot lost the is_generated signal", tm.PKColumnMetas())
	}
	if c.Name != "row_end" {
		t.Fatalf("generated PK member = %q, want row_end", c.Name)
	}

	// End-to-end verdict through the real path: inconclusive with the
	// versioning-aware reason, before any source fingerprinting happens.
	res, err := VerifyTable(context.Background(), Config{Resolver: resolver, SourceDB: sourceDB, IndexDB: indexDB},
		sourceName, "sv_orders")
	if err != nil {
		t.Fatalf("VerifyTable: %v", err)
	}
	if res.Status != StatusInconclusive {
		t.Fatalf("status = %q, want inconclusive", res.Status)
	}
	if !strings.Contains(res.Detail, "generated column") {
		t.Fatalf("want the generated-PK reason, got: %q", res.Detail)
	}
}
