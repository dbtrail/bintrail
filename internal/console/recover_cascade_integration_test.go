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
		c.NoArchive = false         // required for baselineConfigured
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

// TestIntegrationRecover_autoCascade_endToEnd is the merge's core guarantee: a
// plain POST /api/recover on a foreign-key PARENT whose DELETE cascaded auto-
// detects the cascade and emits ONE combined script — the parent re-INSERT AND
// its two invisible children, inside the FK-checks wrapper — with cascade_detected
// + victim_count set. No separate tab, no extra request. The parent must appear
// exactly once (base rows carry it; synthesized victims are children-only).
func TestIntegrationRecover_autoCascade_endToEnd(t *testing.T) {
	srv, dbName := seedCascadeConsole(t, nil)

	rec, body := doReq(t, srv, "POST", "/api/recover", `{"schema":"`+dbName+`","table":"parent"}`)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, body)
	}
	var resp recoverResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, body)
	}

	if !resp.CascadeDetected {
		t.Errorf("cascade_detected should be true for a parent DELETE recover\n---\n%s", resp.SQL)
	}
	if resp.VictimCount != 2 {
		t.Errorf("victim_count = %d, want 2", resp.VictimCount)
	}
	for _, want := range []string{
		"SET FOREIGN_KEY_CHECKS=0;",
		"SET FOREIGN_KEY_CHECKS=1;",
		"recover (cascade-aware)",                   // the Combined preamble, not "recover-cascade"
		"Re-creates 2 cascade-deleted child row(s)", // Combined header counts the children
		"`" + dbName + "`.`parent`",
		"`" + dbName + "`.`child`",
	} {
		if !strings.Contains(resp.SQL, want) {
			t.Errorf("SQL missing %q\n---\n%s", want, resp.SQL)
		}
	}
	// Parent re-inserted exactly once (base rows), children exactly twice (victims).
	if c := strings.Count(resp.SQL, "`"+dbName+"`.`parent`"); c != 1 {
		t.Errorf("want the parent re-inserted once, got %d\n---\n%s", c, resp.SQL)
	}
	if c := strings.Count(resp.SQL, "`"+dbName+"`.`child`"); c != 2 {
		t.Errorf("want 2 child INSERTs, got %d\n---\n%s", c, resp.SQL)
	}
	// connection_id is the paid-forensics boundary: the merged response carries SQL
	// + counts only (no event rows), exactly like the legacy cascade endpoint.
	if strings.Contains(string(body), "connection_id") {
		t.Errorf("connection_id leaked into the recover response: %s", body)
	}
}

// TestIntegrationRecover_autoCascade_skippedWhenNotParent locks the gate: a
// recover on the CHILD table (INSERTs only, not a cascade parent) takes the plain
// path — no cascade synthesis, no FK-checks wrapper — even though the table
// participates in the same FK. cascade_detected stays false.
func TestIntegrationRecover_autoCascade_skippedWhenNotParent(t *testing.T) {
	srv, dbName := seedCascadeConsole(t, nil)

	rec, body := doReq(t, srv, "POST", "/api/recover", `{"schema":"`+dbName+`","table":"child"}`)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, body)
	}
	var resp recoverResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, body)
	}
	if resp.CascadeDetected {
		t.Errorf("cascade_detected must be false for a non-parent (child) recover")
	}
	if strings.Contains(resp.SQL, "SET FOREIGN_KEY_CHECKS=0;") {
		t.Errorf("plain recover should not emit the cascade FK-checks wrapper\n---\n%s", resp.SQL)
	}
}

// TestIntegrationRecover_autoCascade_rbacWarns: under an RBAC profile cascade
// synthesis is disabled (it cannot honor redaction), but a parent-DELETE recover
// must NOT silently emit a parent-only script as if whole — it warns. The recover
// itself still succeeds (unlike the cascade endpoint, which 403s).
func TestIntegrationRecover_autoCascade_rbacWarns(t *testing.T) {
	srv, dbName := seedCascadeConsole(t, func(c *Config) {
		// A redact rule on an unrelated table is enough to activate the profile (the
		// guard checks presence, not scope), and leaves the parent recover working.
		c.RedactColumns = []query.SchemaTableColumn{{Schema: "app", Table: "child", Column: "pid"}}
	})

	rec, body := doReq(t, srv, "POST", "/api/recover", `{"schema":"`+dbName+`","table":"parent"}`)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, body)
	}
	var resp recoverResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, body)
	}
	if resp.CascadeDetected {
		t.Errorf("cascade synthesis must stay disabled under an RBAC profile")
	}
	var warned bool
	for _, w := range resp.Warnings {
		if strings.Contains(w, "RBAC redaction profile is active") {
			warned = true
		}
	}
	if !warned {
		t.Errorf("a parent-DELETE recover under RBAC must warn that cascade children are NOT included; warnings=%v", resp.Warnings)
	}
}

// TestIntegrationRecover_autoCascade_mixedEvents pins the combined path's
// data-integrity contract: a parent with BOTH a non-DELETE event (UPDATE) and a
// DELETE in the window. The combined script must reverse the UPDATE AND re-create
// the parent from the DELETE AND include the synthesized children — because
// cascadeRecover emits over the MERGED base rows (every event type), while the
// synthesis uses a DELETE-only parent fetch purely to discover children. A
// refactor that emitted over the synthesis's DELETE-only parents would silently
// drop the UPDATE reversal — a data-loss-shaped regression this test catches.
func TestIntegrationRecover_autoCascade_mixedEvents(t *testing.T) {
	srv, dbName := seedCascadeConsole(t, nil)
	// A parent UPDATE between the children (h+10m) and the seeded parent DELETE
	// (h+20m), on the same pk=1.
	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	updTs := h.Add(15 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, srv.cm.boot.db, "binlog.000001", 250, 260, updTs, nil,
		dbName, "parent", 2 /*UPDATE*/, "1", []byte(`["status"]`),
		[]byte(`{"id":1,"status":"old"}`), []byte(`{"id":1,"status":"new"}`))

	rec, body := doReq(t, srv, "POST", "/api/recover", `{"schema":"`+dbName+`","table":"parent"}`)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, body)
	}
	var resp recoverResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, body)
	}
	if !resp.CascadeDetected || resp.VictimCount != 2 {
		t.Errorf("want cascade_detected + 2 victims, got detected=%v victims=%d", resp.CascadeDetected, resp.VictimCount)
	}
	// The non-DELETE base reversal must survive alongside the parent re-INSERT and
	// the 2 child INSERTs.
	if !strings.Contains(resp.SQL, "UPDATE `"+dbName+"`.`parent`") {
		t.Errorf("the parent UPDATE reversal was dropped from the combined script (regression: emitted over DELETE-only parents?)\n---\n%s", resp.SQL)
	}
	if !strings.Contains(resp.SQL, "INSERT INTO `"+dbName+"`.`parent`") {
		t.Errorf("the parent re-INSERT (from the DELETE) is missing\n---\n%s", resp.SQL)
	}
	if c := strings.Count(resp.SQL, "`"+dbName+"`.`child`"); c != 2 {
		t.Errorf("want 2 child INSERTs, got %d\n---\n%s", c, resp.SQL)
	}
}

// TestIntegrationRecover_autoCascade_setNull covers the SET NULL arm of the
// combined path: a parent whose child FK is ON DELETE SET NULL. Recover on the
// parent must fold an idempotent guarded UPDATE (… AND fk IS NULL) into the same
// FK-checks-wrapped script and report set_null_count, with zero victims (a
// SET NULL child survives — it is restored, not re-inserted). Self-seeded (not
// seedCascadeConsole, whose CASCADE counts other tests assert).
func TestIntegrationRecover_autoCascade_setNull(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	childTs := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	parentTs := h.Add(20 * time.Minute).Format("2006-01-02 15:04:05")

	// One child row pointing at parent.id=1 via a SET NULL FK column `pid`, plus
	// the parent DELETE (the cascade SET-NULL update InnoDB ran is NOT indexed).
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, childTs, nil,
		dbName, "snchild", 1 /*INSERT*/, "20", nil, nil, []byte(`{"id":20,"pid":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, parentTs, nil,
		dbName, "parent", 3 /*DELETE*/, "1", nil, []byte(`{"id":1}`), nil)

	testutil.MustExec(t, db, `INSERT INTO fk_constraints
		(snapshot_id, constraint_name, schema_name, table_name, column_name, ordinal_position,
		 referenced_schema_name, referenced_table_name, referenced_column_name, delete_rule, update_rule)
		VALUES (1, 'fk_snchild', ?, 'snchild', 'pid', 1, ?, 'parent', 'id', 'SET NULL', 'RESTRICT')`,
		dbName, dbName)

	snapTs := h.Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "parent", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "snchild", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "snchild", "pid", 2, "", "int", "YES")

	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, NoArchive: true})
	if err != nil {
		t.Fatal(err)
	}

	rec, body := doReq(t, srv, "POST", "/api/recover", `{"schema":"`+dbName+`","table":"parent"}`)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, body)
	}
	var resp recoverResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, body)
	}
	if !resp.CascadeDetected {
		t.Errorf("cascade_detected should be true for a SET NULL parent recover")
	}
	if resp.SetNullCount != 1 {
		t.Errorf("set_null_count = %d, want 1", resp.SetNullCount)
	}
	if resp.VictimCount != 0 {
		t.Errorf("victim_count = %d, want 0 (a SET NULL child survives — restored, not re-inserted)", resp.VictimCount)
	}
	for _, want := range []string{
		"SET FOREIGN_KEY_CHECKS=0;",
		"SET FOREIGN_KEY_CHECKS=1;",
		"`" + dbName + "`.`parent`",  // the parent re-INSERT
		"`" + dbName + "`.`snchild`", // the SET NULL restore UPDATE
		"IS NULL",                    // the idempotent guard
	} {
		if !strings.Contains(resp.SQL, want) {
			t.Errorf("SQL missing %q\n---\n%s", want, resp.SQL)
		}
	}
}

// TestIntegrationRecover_autoCascade_skippedForPostgres locks the dialect gate:
// cascade auto-detection is a MySQL/MariaDB binlog blind-spot fix. A PostgreSQL-
// flavored index captures cascade deletes as real events (no blind spot), so a
// parent recover takes the plain path — no synthesis, no misleading "0 victims".
func TestIntegrationRecover_autoCascade_skippedForPostgres(t *testing.T) {
	srv, dbName := seedCascadeConsole(t, nil)
	// Stamp the index as PostgreSQL-sourced (DialectForIndex reads stream_state.flavor).
	testutil.MustExec(t, srv.cm.boot.db,
		`INSERT INTO stream_state (id, mode, flavor, last_checkpoint, server_id)
		 VALUES (1, 'gtid', 'postgres', UTC_TIMESTAMP(), 1)
		 ON DUPLICATE KEY UPDATE flavor='postgres'`)

	rec, body := doReq(t, srv, "POST", "/api/recover", `{"schema":"`+dbName+`","table":"parent"}`)
	if rec.Code != 200 {
		t.Fatalf("status = %d, body = %s", rec.Code, body)
	}
	var resp recoverResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, body)
	}
	if resp.CascadeDetected {
		t.Errorf("cascade auto-detection must be skipped for a PG-flavored index")
	}
	if strings.Contains(resp.SQL, "SET FOREIGN_KEY_CHECKS=0;") {
		t.Errorf("PG recover should not emit the MySQL cascade FK-checks wrapper\n---\n%s", resp.SQL)
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
