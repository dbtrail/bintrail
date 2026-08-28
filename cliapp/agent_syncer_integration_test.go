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
// this is the one exit StreamParser.Run's wrapping cannot cover. The error
// must leave startBYOSSyncer as a *parser.ReplicationError so usage telemetry
// classifies it (db_permission) instead of unknown (#1503).
func TestStartBYOSSyncerWrapsServerErrors(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	rootDB, err := config.Connect(testutil.BaseDSN() + "/")
	if err != nil {
		t.Fatalf("connect as root: %v", err)
	}
	defer rootDB.Close()
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

	syncer := replication.NewBinlogSyncer(byosSyncerConfig(4_000_000_123, "mysql", host, uint16(port), user, pass))
	defer syncer.Close()

	streamer, err := startBYOSSyncer(sourceDB, syncer, "mysql", "")
	if err == nil {
		t.Fatal("StartSync without REPLICATION SLAVE must fail, got a streamer")
	}
	_ = streamer
	var re *parser.ReplicationError
	if !errors.As(err, &re) {
		t.Fatalf("startBYOSSyncer returned %T (%v), want a chain carrying *parser.ReplicationError", err, err)
	}
	if got := telemetry.ClassifyError(err); got != telemetry.ClassDBPermission {
		t.Errorf("ClassifyError = %q (server error %d), want %q", got, re.MySQLErrorNumber(), telemetry.ClassDBPermission)
	}
}
