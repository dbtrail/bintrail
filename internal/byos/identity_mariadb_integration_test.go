//go:build integration

package byos

import (
	"context"
	"testing"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/serverid"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestResolveServerIdentity_MariaDB_Integration exercises the full identity path
// against a REAL MariaDB source. MariaDB has no @@server_uuid, so the anchor is
// synthesized from the address; ResolveServer must then register a stable,
// non-empty bintrail_id, and a repeat call must return the same id. This is the
// end-to-end proof that two MariaDB servers (distinct addresses → distinct
// anchors) auto-separate into distinct archive prefixes.
func TestResolveServerIdentity_MariaDB_Integration(t *testing.T) {
	sourceDB, srcName := testutil.CreateTestMariaDB(t) // skips/fails if no MariaDB
	indexDB, _ := testutil.CreateTestDB(t)             // MySQL index (13306)
	testutil.InitIndexTables(t, indexDB)

	dsn := testutil.MariaDBBaseDSN() + "/" + srcName
	ctx := context.Background()

	id1, err := ResolveServerIdentity(ctx, sourceDB, indexDB, dsn)
	if err != nil {
		t.Fatalf("ResolveServerIdentity (MariaDB): %v", err)
	}
	if id1 == "" {
		t.Fatal("expected a synthesized bintrail_id for MariaDB, got empty")
	}

	// Idempotent: the same source resolves to the same bintrail_id (rule 1
	// no-op against the row just registered).
	id2, err := ResolveServerIdentity(ctx, sourceDB, indexDB, dsn)
	if err != nil {
		t.Fatalf("second ResolveServerIdentity: %v", err)
	}
	if id1 != id2 {
		t.Errorf("bintrail_id not stable across calls: first %q, second %q", id1, id2)
	}

	// The registered anchor is the address-derived synthetic UUID — confirming
	// the synthesis path (not @@server_uuid) was taken.
	host, port, _, _, err := config.ParseSourceDSN(dsn)
	if err != nil {
		t.Fatalf("ParseSourceDSN: %v", err)
	}
	want := serverid.SyntheticServerUUID(host, port)
	var got string
	if err := indexDB.QueryRow(
		"SELECT server_uuid FROM bintrail_servers WHERE bintrail_id = ?", id1).Scan(&got); err != nil {
		t.Fatalf("read bintrail_servers: %v", err)
	}
	if got != want {
		t.Errorf("registered server_uuid = %q, want synthesized %q", got, want)
	}
}
