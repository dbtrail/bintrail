//go:build integration

// Integration coverage for #1131: a fresh stream (no checkpoint, no
// --start-file/--start-gtid) against a gtid_mode=ON source must auto-discover
// the executed GTID set, checkpoint in GTID mode, and resume from that
// checkpoint on restart.
//
// The shared integration MySQL runs gtid_mode=OFF, so this test steps it up
// online (OFF → OFF_PERMISSIVE → ON_PERMISSIVE → ON, MySQL 8.0's documented
// no-restart transition) and steps it back down afterward so the rest of the
// suite is unaffected. Tests within this package run sequentially, so no other
// test in the package can observe the ON window; a cross-PACKAGE integration
// run executed in parallel against the same container could, which is why the
// window is kept as short as the two streaming runs allow.
package streamrun

import (
	"database/sql"
	"strconv"
	"strings"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// stepGTIDModeOn steps the shared MySQL server's gtid_mode from OFF to ON
// online and registers a cleanup that steps it back down and restores
// enforce_gtid_consistency. It skips the test when the server refuses the
// stepping (missing SUPER/SYSTEM_VARIABLES_ADMIN, or an unexpected starting
// state) — refusal must never fail the suite, only reduce its coverage.
func stepGTIDModeOn(t *testing.T, db *sql.DB) {
	t.Helper()

	var mode string
	if err := db.QueryRow("SELECT @@GLOBAL.gtid_mode").Scan(&mode); err != nil {
		t.Skipf("cannot read gtid_mode: %v", err)
	}
	if strings.EqualFold(mode, "ON") {
		return // already ON (e.g. a dedicated GTID container); nothing to restore
	}
	if !strings.EqualFold(mode, "OFF") {
		t.Skipf("gtid_mode=%s: not a state this test steps from", mode)
	}
	var enforce string
	if err := db.QueryRow("SELECT @@GLOBAL.enforce_gtid_consistency").Scan(&enforce); err != nil {
		t.Skipf("cannot read enforce_gtid_consistency: %v", err)
	}

	if _, err := db.Exec("SET GLOBAL enforce_gtid_consistency = ON"); err != nil {
		t.Skipf("SET GLOBAL refused (%v); cannot step gtid_mode online", err)
	}
	// Registered before the upward steps so a partial climb (skip mid-loop)
	// is also walked back. Each downward step is a no-op when already at or
	// below that state; errors are deliberately ignored (best-effort restore),
	// but a failed restore is loudly logged — a container left at gtid_mode=ON
	// would make position-mode assertions elsewhere flake mysteriously.
	t.Cleanup(func() {
		for _, m := range []string{"ON_PERMISSIVE", "OFF_PERMISSIVE", "OFF"} {
			db.Exec("SET GLOBAL gtid_mode = " + m)
		}
		db.Exec("SET GLOBAL enforce_gtid_consistency = " + enforce)
		var restored string
		if err := db.QueryRow("SELECT @@GLOBAL.gtid_mode").Scan(&restored); err == nil &&
			!strings.EqualFold(restored, "OFF") {
			t.Logf("WARNING: could not step gtid_mode back down (still %s); later position-mode tests on this container may misbehave", restored)
		}
	})
	for _, m := range []string{"OFF_PERMISSIVE", "ON_PERMISSIVE", "ON"} {
		if _, err := db.Exec("SET GLOBAL gtid_mode = " + m); err != nil {
			t.Skipf("SET GLOBAL gtid_mode = %s refused (%v); cannot step online", m, err)
		}
	}
}

// TestIntegrationFirstRunAutoDiscoversGTIDMode is the headline #1131 path
// against a real server: real CurrentGTIDExecuted discovery, a real GTID-mode
// checkpoint round-tripped through stream_state, and a restart that resumes
// from it exactly-once. The stubbed unit tests in streamrun_test.go pin the
// resolver's branch logic; this pins that One() wires the real discovery
// callbacks and that the resulting checkpoint actually drives a resume.
func TestIntegrationFirstRunAutoDiscoversGTIDMode(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	sourceDB, sourceName := testutil.CreateTestDB(t)
	sourceDSN := testutil.IntegrationDSN(sourceName)
	const serverIDBase = 99860

	var logBin string
	if err := sourceDB.QueryRow("SELECT @@log_bin").Scan(&logBin); err != nil || logBin != "1" {
		t.Skip("skipping: binary logging not enabled on test MySQL")
	}

	stepGTIDModeOn(t, sourceDB)

	// Created AFTER the step to ON so at least this DDL transaction owns a
	// GTID — @@GLOBAL.gtid_executed is then provably non-empty and discovery
	// cannot fall into the empty-set position fallback.
	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id     INT PRIMARY KEY,
		amount INT NOT NULL
	)`)

	// Sanity-check the production helper against the real server before
	// spending two streaming runs on it.
	set, err := config.CurrentGTIDExecuted(sourceDB)
	if err != nil {
		t.Fatalf("CurrentGTIDExecuted against gtid_mode=ON source: %v", err)
	}
	if set == "" {
		t.Fatal("CurrentGTIDExecuted returned empty on a gtid_mode=ON source that has executed transactions")
	}
	if strings.ContainsAny(set, " \n\t") {
		t.Fatalf("CurrentGTIDExecuted returned a set with whitespace: %q", set)
	}

	baseCfg := func(serverID uint32) Config {
		return Config{
			IndexDSN:   testutil.IntegrationDSN(indexName),
			SourceDSN:  sourceDSN,
			Flavor:     gomysql.MySQLFlavor,
			ServerID:   serverID,
			BatchSize:  1,
			Schemas:    sourceName,
			Checkpoint: 1,
			GapTimeout: 30,
			Format:     "text",
			SSLMode:    "preferred",
			Deps:       testStreamDeps(),
		}
	}
	insert := func(lo, hi int) func() {
		return func() {
			for i := lo; i <= hi; i++ {
				testutil.MustExec(t, sourceDB, "INSERT INTO orders (id, amount) VALUES (?, ?)", i, i*10)
			}
		}
	}
	indexedThrough := func(hi int) func() bool {
		return func() bool {
			var n int
			if err := indexDB.QueryRow(`SELECT COUNT(*) FROM binlog_events
				WHERE schema_name = ? AND table_name = 'orders' AND pk_values = ?`,
				sourceName, strconv.Itoa(hi)).Scan(&n); err != nil {
				t.Fatalf("poll indexed pk %d: %v", hi, err)
			}
			return n > 0
		}
	}

	// ── run 1: first run, no flags — must select GTID mode by discovery ───
	if err := runOneUntil(t, baseCfg(serverIDBase), true, insert(1, 5), indexedThrough(5)); err != nil {
		t.Fatalf("run 1 (first-run GTID discovery): %v", err)
	}
	saved, err := loadStreamState(indexDB)
	if err != nil {
		t.Fatalf("loadStreamState after run 1: %v", err)
	}
	if saved == nil {
		t.Fatal("run 1 saved no checkpoint")
	}
	if saved.mode != "gtid" {
		t.Fatalf("run 1 checkpoint mode = %q, want \"gtid\" — first-run auto-discovery did not select GTID mode on a gtid_mode=ON source", saved.mode)
	}
	if strings.TrimSpace(saved.gtidSet) == "" {
		t.Fatal("run 1 checkpointed in GTID mode with an EMPTY gtid_set — live-source verify would still be inconclusive")
	}
	assertExactlyOnce(t, indexedPKs(t, indexDB, sourceName, "orders"), pkRange(1, 5))

	// ── run 2: restart — must RESUME from the GTID checkpoint ─────────────
	// waitAttached=false: on a resume the start point comes from the saved
	// checkpoint, so writes issued immediately are replayed once attached.
	if err := runOneUntil(t, baseCfg(serverIDBase+1), false, insert(6, 8), indexedThrough(8)); err != nil {
		t.Fatalf("run 2 (resume from GTID checkpoint): %v", err)
	}
	resumed, err := loadStreamState(indexDB)
	if err != nil {
		t.Fatalf("loadStreamState after run 2: %v", err)
	}
	if resumed.mode != "gtid" {
		t.Fatalf("run 2 checkpoint mode = %q, want \"gtid\" (resume must not fall back to position mode)", resumed.mode)
	}
	// Exactly-once across the restart: nothing lost, nothing re-delivered.
	assertExactlyOnce(t, indexedPKs(t, indexDB, sourceName, "orders"), pkRange(1, 8))
}
