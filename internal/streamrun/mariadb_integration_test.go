//go:build integration

package streamrun

import (
	"context"
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
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestStreamLoop_liveReplication_mariadb is the MariaDB-source end-to-end guard
// (alpha). It pairs a MySQL INDEX (CreateTestDB, 13306) with a MariaDB SOURCE
// (CreateTestMariaDB, 13307) and streams real row events through the full
// engine. Beyond proving rows are captured, it is the discriminator for two
// MariaDB-specific fixes that no unit test exercises:
//
//   - config.CurrentBinlogPosition must succeed against MariaDB (SHOW MASTER
//     STATUS returns 4 columns there, not 5 — the column-tolerant scan).
//   - the BinlogSyncer Flavor="mariadb" handshake + the MariadbGTIDEvent parser
//     case must populate domain-server-seq GTIDs on indexed rows.
//
// It runs in POSITION mode (the recommended MariaDB resume mode for the alpha)
// and asserts via the indexed-event counter, not raw event counts — MariaDB
// interleaves ANNOTATE_ROWS / GTID_LIST / BINLOG_CHECKPOINT events the indexer
// never counts.
func TestStreamLoop_liveReplication_mariadb(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	sourceDB, sourceName := testutil.CreateTestMariaDB(t)

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id     INT PRIMARY KEY AUTO_INCREMENT,
		amount DECIMAL(10,2) NOT NULL
	)`)

	var logBin string
	if err := sourceDB.QueryRow("SELECT @@log_bin").Scan(&logBin); err != nil || logBin != "1" {
		t.Skip("skipping: binary logging not enabled on test MariaDB")
	}

	// THE position-discovery discriminator: this call, not memory, proves the
	// SHOW BINARY LOG STATUS / SHOW MASTER STATUS fallback chain works on the
	// pinned MariaDB. A 4-vs-5 column scan mismatch would fail here.
	binlogFile, binlogPos, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition against MariaDB failed (position discovery regression): %v", err)
	}

	if _, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	mc, err := drivermysql.ParseDSN(testutil.MariaDBBaseDSN() + "/" + sourceName + "?parseTime=true")
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

	for i := range 5 {
		testutil.MustExec(t, sourceDB,
			"INSERT INTO orders (amount) VALUES (?)", float64(i+1)*10.0)
	}

	// The MariaDB flavor drives the replication handshake (mariadb_slave_capability
	// + the MariaDB GTID dump command).
	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID: 99997,
		Flavor:   gomysql.MariaDBFlavor,
		Host:     hostStr,
		Port:     uint16(portN),
		User:     mc.User,
		Password: mc.Passwd,
	})
	defer syncer.Close()

	streamer, syncErr := syncer.StartSync(gomysql.Position{Name: binlogFile, Pos: binlogPos})
	if syncErr != nil {
		testutil.SkipOrFailMariaDB(t, "StartSync against MariaDB failed (replication may not be granted): %v", syncErr)
	}

	resolver, err := metadata.NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	filters := parser.Filters{Schemas: map[string]bool{sourceName: true}}

	sp := parser.NewStreamParser(resolver, filters, nil)
	idx := indexer.New(indexDB, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	events := make(chan parser.Event, 100)
	parseErrCh := make(chan error, 1)
	go func() {
		defer close(events)
		parseErrCh <- sp.Run(ctx, streamer, events)
	}()

	state := &streamState{mode: "position", flavor: gomysql.MariaDBFlavor, serverID: 99997}
	if err := streamLoop(ctx, events, idx, indexDB, time.Minute, state, observe.ForSource("test-mariadb"), nil); err != nil {
		t.Fatalf("streamLoop: %v", err)
	}

	parseErr := <-parseErrCh
	if parseErr != nil &&
		!errors.Is(parseErr, context.DeadlineExceeded) &&
		!errors.Is(parseErr, context.Canceled) {
		t.Fatalf("StreamParser error: %v", parseErr)
	}

	if state.eventsIndexed < 5 {
		t.Errorf("expected at least 5 events indexed from MariaDB, got %d", state.eventsIndexed)
	}

	// The checkpoint must record the MariaDB flavor so a later resume re-parses
	// any saved gtid_set with the MariaDB parser.
	loaded, err := loadStreamState(indexDB)
	if err != nil {
		t.Fatalf("loadStreamState: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected checkpoint to be saved after streaming")
	}
	if loaded.flavor != gomysql.MariaDBFlavor {
		t.Errorf("checkpoint flavor = %q, want mariadb", loaded.flavor)
	}

	// At least one indexed row must carry a MariaDB domain-server-seq GTID,
	// proving the MariadbGTIDEvent parser case fired end-to-end.
	var gtid string
	if err := indexDB.QueryRow(
		`SELECT gtid FROM binlog_events WHERE gtid IS NOT NULL AND gtid <> '' ORDER BY event_id LIMIT 1`,
	).Scan(&gtid); err != nil {
		t.Fatalf("query indexed gtid: %v", err)
	}
	// MariaDB GTID is "domain-server-seq" (digits and dashes), never a MySQL UUID.
	if strings.Count(gtid, "-") != 2 || strings.Contains(gtid, ":") {
		t.Errorf("indexed gtid %q is not MariaDB domain-server-seq form", gtid)
	}
}

// TestStreamLoop_gtidAdvancesOnCommit_mariadb is the MariaDB sibling of
// TestStreamLoop_gtidAdvancesOnCommit_live. It runs streamLoop in GTID mode with
// a *MariadbGTIDSet accumulator so MariadbGTIDSet.Update() — the durable-checkpoint
// path that position mode never exercises — is covered in CI. It proves the GTID
// checkpoint advances (and is re-parseable) for a MariaDB source.
func TestStreamLoop_gtidAdvancesOnCommit_mariadb(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	sourceDB, sourceName := testutil.CreateTestMariaDB(t)

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id INT PRIMARY KEY AUTO_INCREMENT, amount DECIMAL(10,2) NOT NULL)`)
	if _, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	binlogFile, binlogPos, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition: %v", err)
	}

	// Seed the accumulator from the source's current GTID position so Update()
	// appends onto a realistic non-empty *MariadbGTIDSet.
	var seedGTID string
	if err := sourceDB.QueryRow("SELECT @@gtid_binlog_pos").Scan(&seedGTID); err != nil {
		t.Fatalf("read @@gtid_binlog_pos: %v", err)
	}
	accGTID, err := parseGTIDSetForFlavor(gomysql.MariaDBFlavor, seedGTID)
	if err != nil {
		t.Fatalf("parse seed GTID %q: %v", seedGTID, err)
	}

	mc, err := drivermysql.ParseDSN(testutil.MariaDBBaseDSN() + "/" + sourceName + "?parseTime=true")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	hostStr, portStr, _ := net.SplitHostPort(mc.Addr)
	portN, _ := strconv.ParseUint(portStr, 10, 16)

	testutil.MustExec(t, sourceDB, "INSERT INTO orders (amount) VALUES (10.0)")
	testutil.MustExec(t, sourceDB, "INSERT INTO orders (amount) VALUES (20.0)")

	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID: 99996, Flavor: gomysql.MariaDBFlavor,
		Host: hostStr, Port: uint16(portN), User: mc.User, Password: mc.Passwd,
	})
	defer syncer.Close()

	streamer, syncErr := syncer.StartSync(gomysql.Position{Name: binlogFile, Pos: binlogPos})
	if syncErr != nil {
		testutil.SkipOrFailMariaDB(t, "StartSync against MariaDB failed: %v", syncErr)
	}

	resolver, err := metadata.NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	sp := parser.NewStreamParser(resolver, parser.Filters{Schemas: map[string]bool{sourceName: true}}, nil)
	idx := indexer.New(indexDB, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	events := make(chan parser.Event, 100)
	parseErrCh := make(chan error, 1)
	go func() {
		defer close(events)
		parseErrCh <- sp.Run(ctx, streamer, events)
	}()

	state := &streamState{mode: "gtid", flavor: gomysql.MariaDBFlavor, serverID: 99996, accGTID: accGTID}
	if err := streamLoop(ctx, events, idx, indexDB, time.Minute, state, observe.ForSource("test-mariadb-gtid"), nil); err != nil {
		t.Fatalf("streamLoop: %v", err)
	}

	parseErr := <-parseErrCh
	if parseErr != nil &&
		!errors.Is(parseErr, context.DeadlineExceeded) &&
		!errors.Is(parseErr, context.Canceled) {
		t.Fatalf("StreamParser error: %v", parseErr)
	}

	// advanceGTID must have grown the durable set via MariadbGTIDSet.Update().
	if state.gtidSet == "" {
		t.Fatal("expected accumulated GTID set after committing transactions, got empty")
	}
	if strings.Contains(state.gtidSet, ":") {
		t.Errorf("accumulated set %q is not MariaDB domain-server-seq form", state.gtidSet)
	}
	// The persisted set must re-parse with the MariaDB parser (resume contract).
	if _, err := parseGTIDSetForFlavor(gomysql.MariaDBFlavor, state.gtidSet); err != nil {
		t.Errorf("accumulated MariaDB set %q does not re-parse: %v", state.gtidSet, err)
	}
}
