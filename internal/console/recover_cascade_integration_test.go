//go:build integration

package console

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// seedCascadeConsole builds a Server over a fresh index holding a parent DELETE
// plus two child INSERTs that referenced it, and the fk_constraints row marking
// child.pid -> parent.id ON DELETE CASCADE. The cascade child deletes are NOT
// indexed — that is the blind spot the endpoint reconstructs. Extra Config is
// applied via the mutator (e.g. RBAC rules).
func seedCascadeConsole(t *testing.T, mutate func(*Config)) (*Server, string) {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	childTs := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	parentTs := h.Add(20 * time.Minute).Format("2006-01-02 15:04:05")

	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, childTs, nil,
		dbName, "child", 1 /*INSERT*/, "10", nil, nil, []byte(`{"id":10,"pid":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, childTs, nil,
		dbName, "child", 1 /*INSERT*/, "11", nil, nil, []byte(`{"id":11,"pid":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, parentTs, nil,
		dbName, "parent", 3 /*DELETE*/, "1", nil, []byte(`{"id":1}`), nil)

	testutil.MustExec(t, db, `INSERT INTO fk_constraints
		(snapshot_id, constraint_name, schema_name, table_name, column_name, ordinal_position,
		 referenced_schema_name, referenced_table_name, referenced_column_name, delete_rule, update_rule)
		VALUES (1, 'fk_child', ?, 'child', 'pid', 1, ?, 'parent', 'id', 'CASCADE', 'RESTRICT')`,
		dbName, dbName)

	// A schema snapshot so the bundle's resolver is non-nil (PK columns known) —
	// required for the Phase-2 gate to engage and harmless to Phase-1 INSERT output.
	snapTs := h.Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "parent", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "pid", 2, "", "int", "YES")

	cfg := Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, NoArchive: true}
	if mutate != nil {
		mutate(&cfg)
	}
	srv, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return srv, dbName
}

// TestIntegrationRecoverCascade_endToEnd drives POST /api/recover-cascade over a
// seeded cascade and checks the emitted SQL re-inserts the parent and its two
// cascade-deleted children inside the FK-checks wrapper, reports complete, and
// never leaks connection_id (text-only response, no event rows).
func TestIntegrationRecoverCascade_endToEnd(t *testing.T) {
	srv, dbName := seedCascadeConsole(t, nil)

	rec, body := doReq(t, srv, "POST", "/api/recover-cascade", `{"schema":"`+dbName+`","table":"parent"}`)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, body)
	}
	var resp recoverCascadeResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, body)
	}

	for _, want := range []string{
		"SET FOREIGN_KEY_CHECKS=0;",
		"SET FOREIGN_KEY_CHECKS=1;",
		"Phase-1",
		"`" + dbName + "`.`parent`",
		"`" + dbName + "`.`child`",
	} {
		if !strings.Contains(resp.SQL, want) {
			t.Errorf("SQL missing %q\n---\n%s", want, resp.SQL)
		}
	}
	if c := strings.Count(resp.SQL, "`"+dbName+"`.`child`"); c != 2 {
		t.Errorf("want 2 child INSERTs, got %d\n---\n%s", c, resp.SQL)
	}
	if resp.VictimCount != 2 {
		t.Errorf("victim_count = %d, want 2", resp.VictimCount)
	}
	if !resp.Complete {
		t.Errorf("a clean cascade with no archives should be complete; incomplete=%v", resp.Incomplete)
	}
	// connection_id is the paid-forensics boundary: it must never appear in the
	// cascade response (the response carries SQL + counts only, no event rows).
	if strings.Contains(string(body), "connection_id") {
		t.Errorf("connection_id leaked into the cascade response: %s", body)
	}

	// Capability is reported true (free tier, no profile).
	_, capsBody := doReq(t, srv, "GET", "/api/capabilities", "")
	var caps capabilitiesResponse
	if err := json.Unmarshal(capsBody, &caps); err != nil {
		t.Fatalf("decode caps: %v", err)
	}
	if !caps.RecoverCascade {
		t.Errorf("capability recover_cascade should be true, got: %s", capsBody)
	}
}

// TestIntegrationRecoverCascade_incompleteWithArchives: when archived partitions
// exist and no parent matches the live index, the result is flagged incomplete
// (the deleted parent may itself be archived — the dangerous "nothing found" case).
func TestIntegrationRecoverCascade_incompleteWithArchives(t *testing.T) {
	srv, dbName := seedCascadeConsole(t, nil)
	// Register an archived partition (S3-resolvable: a key carrying bintrail_id=)
	// so the unconditional archive probe reports that archives physically exist.
	testutil.MustExec(t, srv.cm.boot.db, `INSERT INTO archive_state
		(bintrail_id, partition_name, local_path, s3_bucket, s3_key)
		VALUES ('bt', 'p_x', '', 'bucket', 'pfx/bintrail_id=bt/p_x.parquet')`)

	// A PK that matches no live parent → with archives present, that is a hard caveat.
	rec, body := doReq(t, srv, "POST", "/api/recover-cascade",
		`{"schema":"`+dbName+`","table":"parent","pk":"999"}`)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, body)
	}
	var resp recoverCascadeResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Complete {
		t.Errorf("result should be INCOMPLETE when archives exist and no parent matched")
	}
	if len(resp.Incomplete) == 0 {
		t.Errorf("incomplete[] should carry the archived-partition caveat")
	}
}

// TestIntegrationRecoverCascade_phase2HeaderWhenBaselineConfigured covers the
// console's Phase-2 GATING decision (the synthesis itself is the CLI's verbatim-
// ported, CLI-tested provider). With a baseline dir AND a resolver, the handler
// constructs the provider and the emitted SQL flips to the Phase-2 header — even
// though the empty baseline dir yields no extra rows (FindBaseline → no baseline
// → Phase-1). The capability advertises the sub-flag in lockstep.
func TestIntegrationRecoverCascade_phase2HeaderWhenBaselineConfigured(t *testing.T) {
	srv, dbName := seedCascadeConsole(t, func(c *Config) {
		c.NoArchive = false        // required for baselineConfigured
		c.BaselineDir = t.TempDir() // empty dir → no baseline rows, provider still wired
	})

	rec, body := doReq(t, srv, "POST", "/api/recover-cascade", `{"schema":"`+dbName+`","table":"parent"}`)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, body)
	}
	var resp recoverCascadeResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !strings.Contains(resp.SQL, "Phase-2 baseline fallback ACTIVE") {
		t.Errorf("a baseline-configured bundle should emit the Phase-2 header\n---\n%s", resp.SQL)
	}

	// Capability sub-flag is true (baseline + resolver both present) — and equals
	// the handler's gate, closing the over-advertise seam.
	_, capsBody := doReq(t, srv, "GET", "/api/capabilities", "")
	var caps capabilitiesResponse
	if err := json.Unmarshal(capsBody, &caps); err != nil {
		t.Fatalf("decode caps: %v", err)
	}
	if !caps.RecoverCascadeBaseline {
		t.Errorf("recover_cascade_baseline should be true with baseline + resolver, got: %s", capsBody)
	}
}

// TestIntegrationRecoverCascade_refusedUnderProfile: an active redact/deny
// profile both flips the capability to false and makes the endpoint 403 (cascade
// victim synthesis cannot honor redaction — the leak guard).
func TestIntegrationRecoverCascade_refusedUnderProfile(t *testing.T) {
	// The guard fires on any redact/deny rule (it checks presence, not scope), so
	// the schema literal here need not match the generated test DB name.
	srv, dbName := seedCascadeConsole(t, func(c *Config) {
		c.NoArchive = false
		c.RedactColumns = []query.SchemaTableColumn{{Schema: "app", Table: "child", Column: "pid"}}
	})

	rec, body := doReq(t, srv, "POST", "/api/recover-cascade", `{"schema":"`+dbName+`","table":"parent"}`)
	if rec.Code != 403 {
		t.Fatalf("status = %d, want 403 (body=%s)", rec.Code, body)
	}

	_, capsBody := doReq(t, srv, "GET", "/api/capabilities", "")
	var caps capabilitiesResponse
	if err := json.Unmarshal(capsBody, &caps); err != nil {
		t.Fatalf("decode caps: %v", err)
	}
	if caps.RecoverCascade {
		t.Errorf("capability recover_cascade must be false under an RBAC profile, got: %s", capsBody)
	}
}
