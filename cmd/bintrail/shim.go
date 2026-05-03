package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/go-mysql-org/go-mysql/server"
	"github.com/spf13/cobra"

	"github.com/dbtrail/bintrail/internal/config"
	"github.com/dbtrail/bintrail/internal/shim"
)

// shimCmd serves the BYOS time-travel SQL endpoint as an in-process
// MySQL-protocol server. Customers run this alongside the bintrail
// agent on the same host; ProxySQL routes _flashback / _diff /
// _snapshot virtual-schema queries to its --listen address.
//
// Recognised statement shapes (handled by internal/shim/parser.go):
//   - SELECT * FROM _flashback.<table> AS OF '<ts>' WHERE <col> = <value>
//   - SELECT * FROM _snapshot.<table>  AS OF '<ts>' WHERE <col> = <value>
//   - SELECT * FROM _diff.<table>      BETWEEN '<t1>' AND '<t2>' WHERE <col> = <value>
//
// Time-range queries are answered against the live bintrail MySQL
// index plus any S3 archives auto-discovered via archive_state — the
// same merge pipeline `bintrail query` and `bintrail recover` use.
//
// Authentication: TenantAuth validates that the connecting username
// appears in shim.yaml; password validation is delegated to ProxySQL
// (which authenticates the application connection against
// mysql_pass_sha1 and only then forwards to the shim hostgroup). The
// default --listen of 127.0.0.1:3308 keeps the shim unreachable from
// the network — only ProxySQL on the same host can connect.
var shimCmd = &cobra.Command{
	Use:   "shim",
	Short: "Run the BYOS time-travel SQL MySQL-protocol server",
	Long: `Run an in-process MySQL-protocol server that answers BYOS
time-travel SQL queries against the three virtual schemas
_flashback / _snapshot / _diff. Intended to sit behind ProxySQL on the
same host — see docs/byos-time-travel-sql.md for the end-to-end setup.

The shim auto-discovers S3 archives via archive_state, so queries that
reach back beyond the live MySQL index's retention window resolve
transparently from Parquet. Use --no-archive to disable that and stay
index-only.

Authentication validates only that the connecting username appears in
shim.yaml. ProxySQL holds the password gate. The default --listen of
127.0.0.1:3308 keeps the shim unreachable from the network.`,
	RunE: runShim,
}

var (
	shListen     string
	shIndexDSN   string
	shShimConfig string
	shNoArchive  bool
)

func init() {
	shimCmd.Flags().StringVar(&shListen, "listen", "127.0.0.1:3308", "Listen address for the MySQL protocol port (default: localhost-only — keep ProxySQL as the auth gate)")
	shimCmd.Flags().StringVar(&shIndexDSN, "index-dsn", "", "DSN of the bintrail MySQL index")
	shimCmd.Flags().StringVar(&shShimConfig, "shim-config", "shim.yaml", "Path to shim.yaml (the file produced by 'bintrail init-shim')")
	shimCmd.Flags().BoolVar(&shNoArchive, "no-archive", false, "Skip archive auto-discovery; query only the live MySQL index")
	_ = shimCmd.MarkFlagRequired("index-dsn")
	bindCommandEnv(shimCmd)
	rootCmd.AddCommand(shimCmd)
}

func runShim(cmd *cobra.Command, args []string) error {
	users, err := shim.LoadTenantUsers(shShimConfig)
	if err != nil {
		return err
	}
	auth, err := shim.NewTenantAuth(users)
	if err != nil {
		return err
	}

	db, err := config.Connect(shIndexDSN)
	if err != nil {
		return fmt.Errorf("connect to index: %w", err)
	}
	defer db.Close()

	// Eager Ping so a misconfigured DSN fails at startup rather than at
	// the first _flashback query the customer actually runs. config.Connect
	// is lazy: it sets parseTime + a TCP timeout but does not exchange a
	// packet until the first query.
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := db.PingContext(pingCtx); err != nil {
		pingCancel()
		return fmt.Errorf("ping index DB: %w", err)
	}
	pingCancel()

	listener, err := net.Listen("tcp", shListen)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", shListen, err)
	}
	defer listener.Close()

	if !isLoopbackAddr(listener.Addr()) {
		slog.Warn(
			"shim is bound to a non-loopback address; password validation is delegated to ProxySQL — "+
				"ensure no other client has direct network access to this port",
			"addr", listener.Addr().String(),
		)
	}

	slog.Info("shim listening", "addr", listener.Addr().String(), "tenants", len(users))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// SIGINT / SIGTERM → cancel ctx → close listener → accept loop returns.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		slog.Info("shim shutting down")
		cancel()
		listener.Close()
	}()

	cfg := shim.Config{AllowGaps: true, NoArchive: shNoArchive}
	serveLoop(ctx, listener, db, auth, cfg)
	return nil
}

// serveLoop accepts MySQL protocol connections one at a time. Each
// connection runs in its own goroutine with its own Handler instance
// (Handler holds per-connection state: the currently-selected
// database).
func serveLoop(ctx context.Context, listener net.Listener, db *sql.DB, auth shim.TenantAuth, cfg shim.Config) {
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return // graceful shutdown
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			slog.Error("accept failed", "err", err)
			// Brief backoff so a persistent accept error doesn't
			// burn CPU.
			time.Sleep(100 * time.Millisecond)
			continue
		}
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			handleConn(c, db, auth, cfg)
		}(conn)
	}
}

// isLoopbackAddr reports whether addr resolves to a loopback address.
// Used at startup so the operator gets a loud warning if the shim is
// reachable from the network — the auth model assumes ProxySQL on the
// same host is the only legitimate caller.
func isLoopbackAddr(addr net.Addr) bool {
	tcp, ok := addr.(*net.TCPAddr)
	if !ok {
		return false
	}
	if tcp.IP == nil || tcp.IP.IsUnspecified() {
		return false
	}
	return tcp.IP.IsLoopback()
}

// handleConn wraps one accepted TCP connection in go-mysql/server's
// Conn (which performs the MySQL handshake + auth) and dispatches
// every COM_QUERY through our Handler.
func handleConn(c net.Conn, db *sql.DB, auth shim.TenantAuth, cfg shim.Config) {
	defer c.Close()

	handler := shim.NewHandlerWithConfig(db, cfg, slog.Default())
	srv := server.NewDefaultServer()
	mysqlConn, err := server.NewCustomizedConn(c, srv, auth, handler)
	if err != nil {
		slog.Error("mysql handshake failed", "err", err, "remote", c.RemoteAddr())
		return
	}
	for {
		if err := mysqlConn.HandleCommand(); err != nil {
			if !errors.Is(err, net.ErrClosed) {
				slog.Debug("connection ended", "err", err, "remote", c.RemoteAddr())
			}
			return
		}
	}
}
