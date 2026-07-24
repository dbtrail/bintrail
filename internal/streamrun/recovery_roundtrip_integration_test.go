//go:build integration

package streamrun

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestRecoverRoundTrip_ApplyAndAssert is the #942 acceptance test: it closes the
// loop every other recover test leaves open. Instead of asserting on the emitted
// SQL *string*, it captures a hostile-typed table's pre-mutation state, mutates it
// through the REAL stream/index pipeline (StreamParser -> indexer -> streamLoop),
// generates the reversal script with `recover`, APPLIES that script to the live
// source, and asserts SELECT * byte-equals the pre-mutation capture.
//
// The fixture is scary-first (BLOB/TEXT/JSON/BIGINT UNSIGNED>2^63/BIT(64)/ENUM/
// SET/TIMESTAMP/DECIMAL(30,10)/latin1/backslash+quote+semicolon strings + an
// all-NULL untouched row) and exercises all three reversal shapes at once:
//   - reverse-UPDATE : id=1 mutated, restored to its before-image
//   - reverse-DELETE : id=2 deleted, restored via a synthesized INSERT
//   - reverse-INSERT : id=4 inserted, removed via a synthesized DELETE
//   - pass-through    : id=3 never touched, must survive unchanged
//
// Future corruption fixes add a column to `hostile` instead of a string
// assertion. Mirrors the PG harness (internal/pgstreamrun TestOne_PGType
// RoundTripMatrix); the systemic hole this fills was MySQL-only (#653, #662,
// #666/#668, #756, #786, #788 all shipped past a green string-only suite).
func TestRecoverRoundTrip_ApplyAndAssert(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	sourceDB, sourceName := testutil.CreateTestDB(t)
	// Pin the session (and pool it to a single connection so the pin sticks) to
	// UTC: the reversal script pins its apply session to UTC, so the before/after
	// SELECT captures must read TIMESTAMP under the same offset to compare equal.
	sourceDB.SetMaxOpenConns(1)
	testutil.MustExec(t, sourceDB, "SET time_zone = '+00:00'")

	testutil.MustExec(t, sourceDB, `CREATE TABLE hostile (
		id       INT PRIMARY KEY,
		c_blob   BLOB,
		c_text   TEXT,
		c_json   JSON,
		c_ubig   BIGINT UNSIGNED,
		c_bit    BIT(64),
		c_enum   ENUM('alpha','beta','gamma'),
		c_set    SET('x','y','z'),
		c_ts     TIMESTAMP NULL,
		c_dec    DECIMAL(30,10),
		c_latin  VARCHAR(64) CHARACTER SET latin1,
		c_tricky VARCHAR(255)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)

	// Skip if binary logging is not enabled (mirrors the sibling live-repl test).
	var logBin string
	if err := sourceDB.QueryRow("SELECT @@log_bin").Scan(&logBin); err != nil || logBin != "1" {
		t.Skip("skipping: binary logging not enabled on test MySQL")
	}

	// ── S0: pre-mutation baseline (rows 1,2,3). Inserted BEFORE we capture the
	// binlog position, so the stream never sees them — recover reverses only the
	// mutation window, and S0 is the ground truth to restore back to.
	s0Inserts := []string{
		`INSERT INTO hostile VALUES (1, X'00FF0042494E4C4F47', 'line1
line2 with \\ backslash', '{"z":1,"a":[2,3],"m":"x"}', 18446744073709551615, X'FFFFFFFFFFFFFFFF', 'beta', 'x,z', '2021-03-14 09:26:53', 12345678901234.5678901234, CONVERT('café-Ñ' USING latin1), 'he said "O''Brien\\", val; DROP TABLE x;--')`,
		`INSERT INTO hostile VALUES (2, X'DEADBEEF', 'row two text', '{"b":2}', 9223372036854775808, X'0000000000000001', 'alpha', 'y', '1999-12-31 23:59:58', -0.0000000001, CONVERT('niño' USING latin1), 'plain;text')`,
		`INSERT INTO hostile VALUES (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)`,
	}
	for _, stmt := range s0Inserts {
		testutil.MustExec(t, sourceDB, stmt)
	}

	// Canonical capture: HEX() binary columns so the comparison is exact and
	// printable; NULLs render distinctly. Both captures read the same connection.
	const canonicalSelect = `SELECT id, HEX(c_blob), c_text, c_json, c_ubig, HEX(c_bit),
		c_enum, c_set, c_ts, c_dec, HEX(c_latin), c_tricky
		FROM hostile WHERE id IN (1,2,3) ORDER BY id`
	capture := func() []string {
		t.Helper()
		rows, err := sourceDB.Query(canonicalSelect)
		if err != nil {
			t.Fatalf("capture query: %v", err)
		}
		defer rows.Close()
		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("capture columns: %v", err)
		}
		var out []string
		for rows.Next() {
			cells := make([]sql.RawBytes, len(cols))
			ptrs := make([]any, len(cols))
			for i := range cells {
				ptrs[i] = &cells[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatalf("capture scan: %v", err)
			}
			parts := make([]string, len(cols))
			for i, c := range cells {
				if c == nil {
					parts[i] = cols[i] + "=<NULL>"
				} else {
					parts[i] = cols[i] + "=" + string(c) // copy: RawBytes is reused
				}
			}
			out = append(out, strings.Join(parts, " | "))
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("capture rows: %v", err)
		}
		return out
	}
	s0 := capture()
	if len(s0) != 3 {
		t.Fatalf("expected 3 baseline rows, got %d", len(s0))
	}

	// Capture the binlog position AFTER S0 so the stream starts clean, then
	// snapshot the schema for the resolver.
	binlogFile, binlogPos, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Skipf("skipping: cannot read binlog position: %v", err)
	}
	if _, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	// Syncer connection details from the source DSN.
	mc, err := drivermysql.ParseDSN(testutil.IntegrationDSN(sourceName))
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	hostStr, portStr, err := net.SplitHostPort(mc.Addr)
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	portN, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		t.Fatalf("ParseUint(port): %v", err)
	}

	const serverID = 99942
	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID: serverID,
		Flavor:   "mysql",
		Host:     hostStr,
		Port:     uint16(portN),
		User:     mc.User,
		Password: mc.Passwd,
		// Match production (streamrun.go, #757): render TIMESTAMP columns in UTC.
		// Without this go-mysql formats them in the host's local tz, so a TIMESTAMP
		// round-trip would shift by the host's UTC offset — a test-only artifact,
		// not a product bug (production always pins UTC here).
		TimestampStringLocation: time.UTC,
	})
	defer syncer.Close()
	streamer, syncErr := syncer.StartSync(gomysql.Position{Name: binlogFile, Pos: binlogPos})
	if syncErr != nil {
		t.Skipf("skipping: StartSync failed (replication may not be granted): %v", syncErr)
	}

	resolver, err := metadata.NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	filters := parser.Filters{Schemas: map[string]bool{sourceName: true}}
	sp := parser.NewStreamParser(resolver, filters, nil)
	// batch size 1 so each event flushes immediately and the count-poll below is
	// deterministic (streamLoop otherwise only flushes on batch-full or cancel).
	idx := indexer.New(indexDB, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	events := make(chan parser.Event, 100)
	parseErrCh := make(chan error, 1)
	go func() {
		defer close(events)
		parseErrCh <- sp.Run(ctx, streamer, events)
	}()
	state := &streamState{mode: "position", serverID: serverID}
	loopErrCh := make(chan error, 1)
	go func() {
		loopErrCh <- streamLoop(ctx, events, idx, indexDB, time.Minute, state, observe.ForSource("test-942"), nil)
	}()

	// ── S1: mutate the live source. One row per reversal shape.
	mutations := []string{
		`UPDATE hostile SET c_blob=X'CAFEBABE', c_text='mutated text', c_json='{"changed":true}',
			c_ubig=1, c_bit=X'00000000000000FF', c_enum='gamma', c_set='x,y,z',
			c_ts='2000-01-01 00:00:00', c_dec=999.9990000000, c_latin=CONVERT('Zürich' USING latin1),
			c_tricky='new; value' WHERE id=1`,
		`DELETE FROM hostile WHERE id=2`,
		`INSERT INTO hostile VALUES (4, X'01', 'inserted row', '{"n":4}', 4, X'0000000000000004',
			'beta', 'z', '2010-05-05 05:05:05', 4.4000000000, CONVERT('four' USING latin1), 'four;4')`,
	}
	for _, stmt := range mutations {
		testutil.MustExec(t, sourceDB, stmt)
	}

	// Wait until all three mutation events are indexed, then stop the stream.
	waitIndexedCount(t, indexDB, sourceName, 3, 20*time.Second)
	cancel()
	if err := <-loopErrCh; err != nil {
		t.Fatalf("streamLoop: %v", err)
	}
	if parseErr := <-parseErrCh; parseErr != nil &&
		!errors.Is(parseErr, context.Canceled) &&
		!errors.Is(parseErr, context.DeadlineExceeded) {
		t.Fatalf("StreamParser error: %v", parseErr)
	}

	// ── Generate the reversal script with `recover` (default MySQL dialect).
	// A fresh context: ctx was just cancelled to stop the stream.
	var buf strings.Builder
	n, err := recovery.New(indexDB, resolver).
		GenerateSQL(context.Background(), query.Options{Schema: sourceName, Table: "hostile", Limit: 100}, &buf)
	if err != nil {
		t.Fatalf("GenerateSQL: %v", err)
	}
	if n != 3 {
		t.Fatalf("recover reversed %d events, want 3 (one per mutation)\nSQL:\n%s", n, buf.String())
	}

	// ── Apply the reversal script to the live source. multiStatements is required
	// so the BEGIN;/SET/.../COMMIT; script runs in one round trip.
	applyDB, err := sql.Open("mysql", testutil.IntegrationDSN(sourceName)+"&multiStatements=true")
	if err != nil {
		t.Fatalf("open apply conn: %v", err)
	}
	defer applyDB.Close()
	if _, err := applyDB.Exec(buf.String()); err != nil {
		t.Fatalf("reversal script failed to apply against MySQL: %v\nSQL:\n%s", err, buf.String())
	}

	// ── Assert: SELECT * now byte-equals the pre-mutation capture, and the
	// reverse-INSERT removed id=4.
	restored := capture()
	if len(restored) != len(s0) {
		t.Fatalf("row count after restore: got %d, want %d\n  S0:       %v\n  restored: %v",
			len(restored), len(s0), s0, restored)
	}
	for i := range s0 {
		if restored[i] != s0[i] {
			t.Errorf("round-trip mismatch at row %d:\n  want (S0):       %s\n  got  (restored): %s",
				i+1, s0[i], restored[i])
		}
	}
	var extra int
	if err := sourceDB.QueryRow("SELECT COUNT(*) FROM hostile WHERE id = 4").Scan(&extra); err != nil {
		t.Fatalf("count id=4: %v", err)
	}
	if extra != 0 {
		t.Errorf("reverse-INSERT did not remove id=4 (found %d rows)", extra)
	}
}

// waitIndexedCount polls binlog_events until at least want rows exist for the
// given source schema, or fails after timeout.
func waitIndexedCount(t *testing.T, db *sql.DB, schema string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		var n int
		if err := db.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE schema_name = ?", schema).Scan(&n); err != nil {
			t.Fatalf("count binlog_events: %v", err)
		}
		if n >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d indexed events in schema %q (have %d)", want, schema, n)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
