//go:build integration

package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/dbtrail/bintrail/internal/config"
	"github.com/dbtrail/bintrail/internal/testutil"
)

// TestCachingSha2ColdAuthSeam codifies the MySQL 8.4 auth-seam verification
// (#421): on 8.4 mysql_native_password is disabled by default, so every
// connection bintrail makes — the index connection (go-sql-driver, via
// config.Connect) AND the source replication handshake (go-mysql, via
// BinlogSyncer) — authenticates with caching_sha2_password over a plaintext
// network, which on a cold cache requires the server's public key (full auth).
//
// Why a FRESH user per path: caching_sha2's cache is per-account, and a
// never-authenticated account has NO fast-auth fallback — its first connect
// must complete full auth via public-key retrieval or fail. So a successful
// plaintext connect by a fresh user (DROP+CREATE = empty server-side cache)
// necessarily exercised the cold full-auth / public-key path; there is no
// other way for it to succeed. The rest of the integration suite mostly runs
// over already-primed (fast-auth) connections, so it does not deterministically
// guard the cold path the way this test does. Each path uses its own fresh user
// so neither primes the other.
//
// It is meaningful on both MySQL 8.0 (caching_sha2 is the default plugin since
// 8.0.4) and 8.4 (where it is also the only enabled one). It is deliberately
// version-independent — it explicitly creates a caching_sha2 user, so it guards
// the cold-auth code path on both cells; the matrix's whole-suite run on 8.4 is
// what additionally proves the 8.4 native_password-OFF default doesn't break us.
func TestCachingSha2ColdAuthSeam(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	rootDB, err := config.Connect(testutil.BaseDSN() + "/")
	if err != nil {
		t.Fatalf("connect as root: %v", err)
	}
	defer rootDB.Close()

	// The server address (host:port) the suite is pointed at; the fresh users
	// connect to the same server with different credentials.
	baseCfg, err := drivermysql.ParseDSN(testutil.BaseDSN() + "/")
	if err != nil {
		t.Fatalf("parse base DSN: %v", err)
	}

	// makeColdUser DROP+CREATEs a caching_sha2 user (cold server-side cache)
	// with replication grants, and returns a plaintext DSN for it.
	makeColdUser := func(t *testing.T, name string) string {
		t.Helper()
		const pass = "Sha2-Cold-Pw-123!"
		testutil.MustExec(t, rootDB, "DROP USER IF EXISTS '"+name+"'@'%'")
		testutil.MustExec(t, rootDB, "CREATE USER '"+name+"'@'%' IDENTIFIED WITH caching_sha2_password BY '"+pass+"'")
		testutil.MustExec(t, rootDB, "GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO '"+name+"'@'%'")
		t.Cleanup(func() { rootDB.Exec("DROP USER IF EXISTS '" + name + "'@'%'") })

		var plugin string
		if err := rootDB.QueryRow(
			"SELECT plugin FROM mysql.user WHERE user = ? AND host = '%'", name).Scan(&plugin); err != nil {
			t.Fatalf("read auth plugin for %s: %v", name, err)
		}
		if plugin != "caching_sha2_password" {
			t.Fatalf("user %s has plugin %q, want caching_sha2_password", name, plugin)
		}
		return fmt.Sprintf("%s:%s@tcp(%s)/", name, pass, baseCfg.Addr)
	}

	// ── Index path: go-sql-driver via config.Connect, cold full-auth ─────────
	t.Run("index connection (go-sql-driver)", func(t *testing.T) {
		dsn := makeColdUser(t, "bt_authseam_idx")
		db, err := config.Connect(dsn)
		if err != nil {
			t.Fatalf("config.Connect with cold caching_sha2 over plaintext failed "+
				"(public-key retrieval for full auth is broken?): %v", err)
		}
		defer db.Close()
		var one int
		if err := db.QueryRow("SELECT 1").Scan(&one); err != nil || one != 1 {
			t.Fatalf("SELECT 1 after auth: got %d, err %v", one, err)
		}
	})

	// ── Replication path: go-mysql BinlogSyncer, cold full-auth ──────────────
	t.Run("replication handshake (go-mysql)", func(t *testing.T) {
		var logBin string
		if err := rootDB.QueryRow("SELECT @@log_bin").Scan(&logBin); err != nil || logBin != "1" {
			t.Skip("skipping: binary logging not enabled on test MySQL")
		}
		file, pos, err := config.CurrentBinlogPosition(rootDB)
		if err != nil {
			t.Skipf("skipping: cannot read binlog position: %v", err)
		}

		dsn := makeColdUser(t, "bt_authseam_repl")
		host, port, user, password, err := config.ParseSourceDSN(dsn)
		if err != nil {
			t.Fatalf("config.ParseSourceDSN: %v", err)
		}

		// Omit TLSConfig (nil → plaintext) to force the caching_sha2 full-auth
		// path. NOTE stream.go defaults to --ssl-mode=preferred (a NON-nil
		// TLSConfig) and reaches plaintext only via its TLS fallback
		// (stream.go ~1234) or --ssl-mode=disabled; this mirrors that plaintext
		// path, where go-mysql must complete full auth via public-key retrieval.
		syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
			ServerID: 99123,
			Flavor:   "mysql",
			Host:     host,
			Port:     port,
			User:     user,
			Password: password,
		})
		defer syncer.Close()

		streamer, err := syncer.StartSync(gomysql.Position{Name: file, Pos: pos})
		if err != nil {
			t.Fatalf("BinlogSyncer.StartSync with cold caching_sha2 over plaintext failed "+
				"(go-mysql public-key retrieval for full auth is broken?): %v", err)
		}

		// The handshake already succeeded; reading one event confirms a live
		// stream. A quiet source times out, which is fine.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := streamer.GetEvent(ctx); err != nil && err != context.DeadlineExceeded {
			t.Fatalf("GetEvent after handshake: %v", err)
		}
	})
}
