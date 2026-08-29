//go:build integration

package cliapp

import (
	"errors"
	"net"
	"strconv"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/telemetry"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestStartBYOSSyncerWrapsServerErrors drives the agent's StartSync exit with
// a real server: a user holding REPLICATION CLIENT (so the position query
// succeeds) but not REPLICATION SLAVE is refused at COM_REGISTER_SLAVE, which
// go-mysql reads synchronously inside StartSync — never through GetEvent, so
// this is the one STARTUP exit StreamParser.Run's wrapping cannot cover. The
// error must leave startBYOSSyncer as a *parser.ReplicationError so usage
// telemetry classifies it (db_permission) instead of unknown (#1503).
//
// MySQL reports the missing privilege as 1045 (access denied), the same
// number a failed handshake would produce, so the refusal is attributed to
// the grant by a positive control: granting REPLICATION SLAVE to the same
// user makes the same call succeed.
func TestStartBYOSSyncerWrapsServerErrors(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	rootDB, err := config.Connect(testutil.BaseDSN() + "/")
	if err != nil {
		t.Fatalf("connect as root: %v", err)
	}
	// Registered BEFORE the DROP USER cleanup: cleanups run LIFO (and after
	// any defer), so a deferred Close would already have closed the
	// connection the DROP needs and the user would be left behind.
	t.Cleanup(func() { rootDB.Close() })
	baseCfg, err := drivermysql.ParseDSN(testutil.BaseDSN() + "/")
	if err != nil {
		t.Fatalf("parse base DSN: %v", err)
	}
	host, portStr, err := net.SplitHostPort(baseCfg.Addr)
	if err != nil {
		t.Fatalf("split %q: %v", baseCfg.Addr, err)
	}
	port, _ := strconv.Atoi(portStr)

	const user, pass = "bt_noslave", "noslave-pw"
	testutil.MustExec(t, rootDB, "DROP USER IF EXISTS '"+user+"'@'%'")
	testutil.MustExec(t, rootDB, "CREATE USER '"+user+"'@'%' IDENTIFIED BY '"+pass+"'")
	testutil.MustExec(t, rootDB, "GRANT REPLICATION CLIENT, SELECT ON *.* TO '"+user+"'@'%'")
	t.Cleanup(func() { rootDB.Exec("DROP USER IF EXISTS '" + user + "'@'%'") })

	sourceDB, err := config.Connect(user + ":" + pass + "@tcp(" + baseCfg.Addr + ")/")
	if err != nil {
		t.Fatalf("connect as %s: %v", user, err)
	}
	defer sourceDB.Close()

	newSyncer := func(serverID uint32) *replication.BinlogSyncer {
		return replication.NewBinlogSyncer(byosSyncerConfig(serverID, "mysql", host, uint16(port), user, pass))
	}
	assertRefused := func(t *testing.T, err error) {
		t.Helper()
		if err == nil {
			t.Fatal("StartSync without REPLICATION SLAVE must fail, got a streamer")
		}
		var re *parser.ReplicationError
		if !errors.As(err, &re) {
			t.Fatalf("startBYOSSyncer returned %T (%v), want a chain carrying *parser.ReplicationError", err, err)
		}
		if re.MySQLErrorNumber() != 1045 {
			t.Errorf("server error = %d, want 1045 (access denied)", re.MySQLErrorNumber())
		}
		if got := telemetry.ClassifyError(err); got != telemetry.ClassDBPermission {
			t.Errorf("ClassifyError = %q, want %q", got, telemetry.ClassDBPermission)
		}
	}

	t.Run("position", func(t *testing.T) {
		syncer := newSyncer(4_000_000_123)
		defer syncer.Close()
		_, err := startBYOSSyncer(sourceDB, syncer, "mysql", "")
		assertRefused(t, err)
	})
	t.Run("gtid", func(t *testing.T) {
		// The GTID branch reaches the same server-side check; the set is
		// never consulted because the refusal comes first.
		syncer := newSyncer(4_000_000_124)
		defer syncer.Close()
		_, err := startBYOSSyncer(sourceDB, syncer, "mysql", "00000000-0000-0000-0000-000000000001:1")
		assertRefused(t, err)
	})

	// Positive control: the grant is what was refused, not the handshake.
	testutil.MustExec(t, rootDB, "GRANT REPLICATION SLAVE ON *.* TO '"+user+"'@'%'")
	syncer := newSyncer(4_000_000_125)
	defer syncer.Close()
	streamer, err := startBYOSSyncer(sourceDB, syncer, "mysql", "")
	if err != nil {
		t.Fatalf("with REPLICATION SLAVE granted the same call must succeed, got %v", err)
	}
	if streamer == nil {
		t.Fatal("nil streamer on success")
	}
}
