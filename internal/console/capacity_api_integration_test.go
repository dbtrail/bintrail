//go:build integration

package console

import (
	"context"
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationCapacityProbeSeesRegistryDSN pins the seam every unit test
// in capacity_api_test.go sets by hand: a REGISTRY server's bundle, built by
// Resolve's lazy open and published through newBundleDerived, must carry the
// entry's DSN, or /api/capacity hands the doctor's probe "" and its locality
// gate (mysql.ParseDSN("") normalizes to 127.0.0.1:3306) opens for a remote
// index whose @@hostname happens to match this host: a WRONG free-space
// number reported as measured.
func TestIntegrationCapacityProbeSeesRegistryDSN(t *testing.T) {
	srv, _ := seedConsoleData(t)
	t.Cleanup(srv.cm.CloseAll)

	db2, dbName2 := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db2)
	dsn := testutil.IntegrationDSN(dbName2)
	entry, err := srv.cm.reg.Add(ServerEntry{Name: "reg", DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}

	b, err := srv.cm.Resolve(context.Background(), entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	if b.dsn != dsn {
		t.Fatalf("published registry bundle dsn = %q, want the entry's DSN", b.dsn)
	}

	// And through the handler, addressed by the registry id.
	seen := stubCapacityProbe(srv, capacityFixture(0, 1<<30, true), nil)
	rec, body := doReqOn(t, srv, entry.ID, "GET", "/api/capacity", "")
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, body)
	}
	if seen.dsn != dsn || seen.dbName != dbName2 {
		t.Fatalf("probe asked for dsn=%q db=%q, want the registry entry's %q / %q", seen.dsn, seen.dbName, dsn, dbName2)
	}
}
