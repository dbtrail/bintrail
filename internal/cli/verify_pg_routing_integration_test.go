//go:build integration

package cli

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
	"github.com/dbtrail/dbtrail/internal/verify"
)

// setVerifyGlobals points the verify command's package globals at the test
// index and restores every touched value on cleanup — the same
// save/restore-discipline verify_format_test.go's setVerifyFormat applies,
// extended to the fields this file needs. The seam is reset to nil (its core-
// binary default) so no other test inherits a stub provider.
func setVerifyGlobals(t *testing.T, indexDSN, sourceDSN, baselineDir, check, tables string) {
	t.Helper()
	prevIdx, prevSrc, prevBase, prevCheck, prevTables := vfyIndexDSN, vfySourceDSN, vfyBaselineDir, vfyCheck, vfyTables
	vfyIndexDSN, vfySourceDSN, vfyBaselineDir, vfyCheck, vfyTables = indexDSN, sourceDSN, baselineDir, check, tables
	t.Cleanup(func() {
		vfyIndexDSN, vfySourceDSN, vfyBaselineDir, vfyCheck, vfyTables = prevIdx, prevSrc, prevBase, prevCheck, prevTables
		SetPGLiveVerifyConnect(nil)
	})
}

// seedPGIndex marks the test index as a PostgreSQL capture (stream_state
// flavor) and publishes the given relations as schema snapshots — one
// snapshot_id per relation, exactly the shape WritePGSnapshot gives a real PG
// index (the shape that makes the MAX(snapshot_id) enumeration name a single
// table).
func seedPGIndex(t *testing.T, db *sql.DB, tables ...string) {
	t.Helper()
	for _, tbl := range tables {
		if _, err := metadata.WritePGSnapshot(context.Background(), db, &metadata.PGRelationSchema{
			Schema: "public", Table: tbl,
			Columns: []metadata.PGRelationColumn{{Name: "id", Ordinal: 1, IsPK: true}},
		}); err != nil {
			t.Fatalf("WritePGSnapshot(%s): %v", tbl, err)
		}
	}
	if _, err := db.Exec(`
		INSERT INTO stream_state (id, mode, binlog_file, binlog_position, gtid_set, flavor, last_checkpoint, server_id)
		VALUES (1, 'gtid', '0/0', 0, '0/0', 'postgres', UTC_TIMESTAMP(), 1)`); err != nil {
		t.Fatalf("seed stream_state: %v", err)
	}
}

// TestIntegrationVerifyLivePGRouting covers the two runtime behaviors of the
// PG live-source seam that no unit test can reach (they need a real index to
// read the flavor from):
//
//   - seam EMPTY (the core bintrail binary): the run refuses with a message
//     naming bintrail-pg — never a raw MySQL-driver error against the
//     postgres:// DSN;
//   - seam FILLED: the flavor routing actually reaches the provider (the
//     stub's sentinel error comes back through the connect wrapper), proving
//     runVerify dispatched to runVerifyLivePG and not the MySQL path.
func TestIntegrationVerifyLivePGRouting(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	seedPGIndex(t, db, "orders")
	setVerifyGlobals(t, testutil.IntegrationDSN(dbName), "postgres://u:p@127.0.0.1:9/x", t.TempDir(), checkContent, "")

	newCmd := func() *cobra.Command {
		cmd := &cobra.Command{}
		// A bare command's Context() is nil until Execute sets one; the verify
		// paths hand it to database/sql, which panics on a nil Context (and
		// the deferred db.Close then deadlocks against the panicking query —
		// a silent 10-minute hang, not a failure).
		cmd.SetContext(context.Background())
		cmd.SetOut(&bytes.Buffer{})
		return cmd
	}

	t.Run("core binary refuses with a pointer to bintrail-pg", func(t *testing.T) {
		SetPGLiveVerifyConnect(nil)
		err := runVerify(newCmd(), nil)
		if err == nil || !strings.Contains(err.Error(), "bintrail-pg verify") {
			t.Fatalf("err = %v, want the nil-seam refusal naming bintrail-pg verify", err)
		}
	})

	t.Run("filled seam receives the routed connect", func(t *testing.T) {
		sentinel := errors.New("stub-connect-refused")
		SetPGLiveVerifyConnect(func(ctx context.Context, dsn string) (verify.PGSourceChecksum, func() error, error) {
			if dsn != "postgres://u:p@127.0.0.1:9/x" {
				t.Errorf("seam received dsn %q, want the --source-dsn value", dsn)
			}
			return nil, nil, sentinel
		})
		err := runVerify(newCmd(), nil)
		if err == nil || !errors.Is(err, sentinel) {
			t.Fatalf("err = %v, want the stub's sentinel via the connect wrapper", err)
		}
		if !strings.Contains(err.Error(), "connect to source database") {
			t.Errorf("err = %v, want the connect wrapper's prefix", err)
		}
	})
}

// TestIntegrationVerifyRecoverInputsPGEnumeration is the mutation guard for
// verifyTargetTablesForFlavor's PG branch: on a PG index (one relation per
// snapshot_id) the MySQL-default MAX(snapshot_id) enumeration names ONE
// table, so a default `verify --check recover` silently walked a single
// relation and reported green. With two published relations, both must appear
// in the report.
func TestIntegrationVerifyRecoverInputsPGEnumeration(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	seedPGIndex(t, db, "orders", "items")
	setVerifyGlobals(t, testutil.IntegrationDSN(dbName), "", "", checkRecover, "")

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background()) // see newCmd in the routing test
	var out bytes.Buffer
	cmd.SetOut(&out)
	// The exit status may legitimately be non-nil (two empty chains report
	// per-table verdicts the report may count as unproven) — the assertion is
	// the ENUMERATION: both relations must be walked and reported.
	_ = runVerify(cmd, nil)
	for _, tbl := range []string{"public.orders", "public.items"} {
		if !strings.Contains(out.String(), tbl) {
			t.Errorf("report is missing %s — PG enumeration fell back to the single-relation MAX(snapshot_id) query:\n%s", tbl, out.String())
		}
	}
}
