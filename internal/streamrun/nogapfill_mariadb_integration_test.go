//go:build integration

package streamrun

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// newestSurvivingBinlog returns the last row of SHOW BINARY LOGS (newest
// binlog). Column-tolerant scan like earliestBinlogFile: only Log_name is
// needed, and the column count varies across versions.
func newestSurvivingBinlog(t *testing.T, db *sql.DB) string {
	t.Helper()
	rows, err := db.Query("SHOW BINARY LOGS")
	if err != nil {
		t.Fatalf("SHOW BINARY LOGS: %v", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("SHOW BINARY LOGS columns: %v", err)
	}
	var newest string
	for rows.Next() {
		var name string
		dest := make([]any, len(cols))
		dest[0] = &name
		for i := 1; i < len(dest); i++ {
			dest[i] = new(sql.RawBytes)
		}
		if err := rows.Scan(dest...); err != nil {
			t.Fatalf("scan SHOW BINARY LOGS: %v", err)
		}
		newest = name
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate SHOW BINARY LOGS: %v", err)
	}
	if newest == "" {
		t.Fatal("SHOW BINARY LOGS returned no rows")
	}
	return newest
}

// TestOne_MariaDBGTID_noGapFillRefusesUnfillableGap is the regression test for
// the --no-gap-fill refusal under an unfillable MariaDB GTID gap (#517
// residual, review-deferred from #515). It drives the SAME cmd-layer One()
// entry point the CLI uses — a pure unit test cannot reach this refusal: the
// decision sits inline in One() after live-server-only steps (connectHelper,
// binlog-format validation, resolver bootstrap), and the MariaDB gap detector
// it must flow through queries a real source. The staging:
//
//  1. write transactions, rotate, then PURGE BINARY LOGS so the purge floor
//     (BINLOG_GTID_POS over the oldest surviving binlog — MariaDB has no
//     @@gtid_purged) is non-empty and covers those transactions;
//  2. plant a GTID-mode checkpoint strictly BEHIND that floor in stream_state;
//  3. run One() with NoGapFill — detectMariaDBGTIDGap must classify the gap
//     unfillable and One() must refuse to start instead of auto-advancing.
func TestOne_MariaDBGTID_noGapFillRefusesUnfillableGap(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	sourceDB, sourceName := testutil.CreateTestMariaDB(t)

	var logBin string
	if err := sourceDB.QueryRow("SELECT @@log_bin").Scan(&logBin); err != nil || logBin != "1" {
		testutil.SkipOrFailMariaDB(t, "binary logging not enabled on test MariaDB")
	}

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id     INT PRIMARY KEY AUTO_INCREMENT,
		amount DECIMAL(10,2) NOT NULL
	)`)
	for i := range 3 {
		testutil.MustExec(t, sourceDB,
			"INSERT INTO orders (amount) VALUES (?)", float64(i+1)*10.0)
	}

	// Rotate and purge until the oldest SURVIVING binlog carries a non-empty
	// starting GTID state (the purge floor). Two MariaDB wrinkles make this a
	// loop rather than one FLUSH+PURGE: the binlog created at server startup
	// has an EMPTY Gtid_list (nothing executed yet, so purging up to it still
	// yields no floor), and PURGE silently KEEPS any file still referenced by
	// the crash-safe binlog checkpoint. Writing a transaction after each
	// rotation advances the checkpoint into the new file so the next PURGE can
	// drop the older ones; one or two rounds normally converge, the loop
	// absorbs checkpoint lag. (PURGE cannot go through a prepared statement;
	// the filename comes from SHOW BINARY LOGS, not user input.)
	var floor string
	for attempt := 0; attempt < 5 && floor == ""; attempt++ {
		testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
		testutil.MustExec(t, sourceDB, "INSERT INTO orders (amount) VALUES (999.99)")
		newest := newestSurvivingBinlog(t, sourceDB)
		testutil.MustExec(t, sourceDB, fmt.Sprintf("PURGE BINARY LOGS TO '%s'", newest))

		earliest, err := earliestBinlogFile(context.Background(), sourceDB)
		if err != nil {
			t.Fatalf("earliestBinlogFile: %v", err)
		}
		var floorNS sql.NullString
		if err := sourceDB.QueryRow("SELECT BINLOG_GTID_POS(?, 4)", earliest).Scan(&floorNS); err != nil {
			t.Fatalf("BINLOG_GTID_POS(%q, 4): %v", earliest, err)
		}
		floor = strings.TrimSpace(floorNS.String)
		if floor == "" {
			time.Sleep(200 * time.Millisecond)
		}
	}
	if floor == "" {
		testutil.SkipOrFailMariaDB(t,
			"purge floor still empty after repeated FLUSH+PURGE — cannot stage an unfillable gap")
	}

	// Plant a checkpoint strictly behind the floor: the floor's first triple's
	// domain and server, at sequence 1. The floor's max sequence covers at
	// least the CREATE TABLE + 3 INSERTs above, so seq 1 can never cover it —
	// exactly the "required GTIDs have been purged" shape.
	first := strings.TrimSpace(strings.SplitN(floor, ",", 2)[0])
	parts := strings.Split(first, "-")
	if len(parts) != 3 {
		t.Fatalf("unexpected purge floor triple %q (full floor %q)", first, floor)
	}
	if seq, perr := strconv.ParseUint(parts[2], 10, 64); perr != nil || seq < 2 {
		t.Fatalf("purge floor %q max seq %q must be >= 2 for a seq-1 checkpoint to be behind it", floor, parts[2])
	}
	stale := parts[0] + "-" + parts[1] + "-1"

	testutil.MustExec(t, indexDB, `INSERT INTO stream_state
		(id, mode, gtid_set, flavor, last_checkpoint, server_id)
		VALUES (1, 'gtid', ?, 'mariadb', UTC_TIMESTAMP(), 1)`, stale)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	err := One(ctx, Config{
		IndexDSN:   testutil.IntegrationDSN(indexName),
		SourceDSN:  testutil.MariaDBBaseDSN() + "/" + sourceName + "?parseTime=true",
		Flavor:     gomysql.MariaDBFlavor,
		ServerID:   99993,
		BatchSize:  100,
		Schemas:    sourceName,
		Checkpoint: 5,
		GapTimeout: 30,
		Format:     "text",
		SSLMode:    "preferred",
		NoGapFill:  true,
		Deps:       testStreamDeps(),
	})
	if err == nil {
		t.Fatal("One() with --no-gap-fill must refuse to start over an unfillable MariaDB GTID gap, got nil")
	}

	// Pin the refusal MESSAGE shape: it must name the flag the operator set
	// and carry the MariaDB detector's diagnosis, so the refusal is
	// actionable, not a bare "gap detected".
	msg := err.Error()
	for _, want := range []string{
		"binlog gap detected and --no-gap-fill is set", // the refusal, naming the flag
		"CANNOT be filled", // the MariaDB detector's verdict...
		"purge floor",      // ...its mechanism (GTIDs purged past the floor)...
		"permanently lost", // ...and its consequence
		stale,              // the operator's own checkpoint, for orientation
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal message missing %q:\n%s", want, msg)
		}
	}

	// A refusal must be a pure refusal: no auto-advance, no gap_lost stamp.
	// The planted checkpoint survives byte-identical so the operator decides
	// what happens to the lost window, not bintrail.
	var gtidSet string
	var gapLostAt sql.NullTime
	if qerr := indexDB.QueryRow(
		"SELECT gtid_set, gap_lost_at FROM stream_state WHERE id = 1").Scan(&gtidSet, &gapLostAt); qerr != nil {
		t.Fatalf("reload stream_state: %v", qerr)
	}
	if gtidSet != stale {
		t.Errorf("stream_state.gtid_set = %q after the refusal, want the untouched %q", gtidSet, stale)
	}
	if gapLostAt.Valid {
		t.Errorf("gap_lost_at was stamped (%v) by a --no-gap-fill refusal; must remain NULL", gapLostAt.Time)
	}
}
