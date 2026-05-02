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
// MVP scope:
//   - Only `_flashback.<table> AS OF '<ts>' WHERE <col> = <value>` is
//     answered. _diff and _snapshot are out of scope for this PR.
//   - Auth accepts any credentials. A future revision will validate
//     against shim.yaml's mysql_user / mysql_pass_sha1.
//   - Reads from the bintrail MySQL index only. Buffer + S3 archive
//     merging is wired-in by `bintrail query` already and will be
//     hooked into the shim in a follow-up.
var shimCmd = &cobra.Command{
	Use:   "shim",
	Short: "Run the BYOS time-travel SQL MySQL-protocol server (MVP)",
	Long: `Run an in-process MySQL-protocol server that answers
` + "`SELECT * FROM _flashback.<table> AS OF '<ts>' WHERE <col> = <value>`" + ` queries
by querying the bintrail MySQL index. Intended to sit behind ProxySQL —
see docs/byos-time-travel-sql.md.

This is an MVP. Authentication accepts any credentials and the only
query shape supported is _flashback. Use --listen to change the port
(default :3308).`,
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

	listener, err := net.Listen("tcp", shListen)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", shListen, err)
	}
	defer listener.Close()

	slog.Info("shim listening", "addr", shListen, "tenants", len(users))

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
