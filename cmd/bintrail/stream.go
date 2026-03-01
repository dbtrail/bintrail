package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"

	"github.com/bintrail/bintrail/internal/config"
	"github.com/bintrail/bintrail/internal/indexer"
	"github.com/bintrail/bintrail/internal/observe"
	"github.com/bintrail/bintrail/internal/parser"
)

var streamCmd = &cobra.Command{
	Use:   "stream",
	Short: "Index events from a live MySQL replication stream",
	Long: `Connects to a MySQL server as a replica over the replication protocol and
indexes binlog row events in real-time into binlog_events.

Unlike 'bintrail index', this command does not require access to binlog files
on disk and works with managed MySQL (RDS, Aurora, Cloud SQL).

Start position must be specified on the first run via --start-file or
--start-gtid. On subsequent runs the saved checkpoint is resumed automatically.

Graceful shutdown: send SIGINT or SIGTERM to flush the current batch and write
a checkpoint before exiting.`,
	RunE: runStream,
}

var (
	strmIndexDSN    string
	strmSourceDSN   string
	strmServerID    uint32
	strmStartFile   string
	strmStartPos    uint32
	strmStartGTID   string
	strmBatchSize   int
	strmSchemas     string
	strmTables      string
	strmCheckpoint  int
	strmMetricsAddr string
	strmSSLMode     string
	strmSSLCA       string
	strmSSLCert     string
	strmSSLKey      string
)

func init() {
	streamCmd.Flags().StringVar(&strmIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	streamCmd.Flags().StringVar(&strmSourceDSN, "source-dsn", "", "DSN for the source MySQL server (required)")
	streamCmd.Flags().Uint32Var(&strmServerID, "server-id", 0, "Unique replica server ID (required, must differ from all other servers)")
	streamCmd.Flags().StringVar(&strmStartFile, "start-file", "", "Initial binlog file (mutually exclusive with --start-gtid)")
	streamCmd.Flags().Uint32Var(&strmStartPos, "start-pos", 4, "Initial position within start file")
	streamCmd.Flags().StringVar(&strmStartGTID, "start-gtid", "", "Initial GTID set (mutually exclusive with --start-file)")
	streamCmd.Flags().IntVar(&strmBatchSize, "batch-size", 1000, "Events per batch INSERT")
	streamCmd.Flags().StringVar(&strmSchemas, "schemas", "", "Only index events from these schemas (comma-separated)")
	streamCmd.Flags().StringVar(&strmTables, "tables", "", "Only index these tables (comma-separated, e.g. mydb.orders)")
	streamCmd.Flags().IntVar(&strmCheckpoint, "checkpoint", 10, "Checkpoint interval in seconds")
	streamCmd.Flags().StringVar(&strmMetricsAddr, "metrics-addr", "", "Address to expose Prometheus metrics (e.g. :9090); empty = disabled")
	streamCmd.Flags().StringVar(&strmSSLMode, "ssl-mode", "preferred", "TLS mode: disabled, preferred, required, verify-ca, verify-identity")
	streamCmd.Flags().StringVar(&strmSSLCA, "ssl-ca", "", "Path to CA certificate file for TLS verification (omit to use system CAs)")
	streamCmd.Flags().StringVar(&strmSSLCert, "ssl-cert", "", "Path to client certificate file for mutual TLS")
	streamCmd.Flags().StringVar(&strmSSLKey, "ssl-key", "", "Path to client private key file for mutual TLS")
	_ = streamCmd.MarkFlagRequired("index-dsn")
	_ = streamCmd.MarkFlagRequired("source-dsn")
	_ = streamCmd.MarkFlagRequired("server-id")

	rootCmd.AddCommand(streamCmd)
}

// ─── streamState ─────────────────────────────────────────────────────────────

// streamState holds the current replication position and counters used for
// checkpointing. It is persisted to stream_state after each checkpoint interval.
type streamState struct {
	mode          string // "position" or "gtid"
	binlogFile    string
	binlogPos     uint64
	gtidSet       string // serialized GTID set (GTID mode only)
	eventsIndexed int64
	lastEventTime sql.NullTime
	serverID      uint32

	// accGTID is the in-memory accumulated GTID set (GTID mode only).
	// It is serialized to gtidSet on checkpoint.
	accGTID *gomysql.MysqlGTIDSet
}

// loadStreamState loads the saved stream_state row, returning nil if no row exists.
func loadStreamState(db *sql.DB) (*streamState, error) {
	var s streamState
	var gtidSet sql.NullString
	err := db.QueryRow(`
		SELECT mode, binlog_file, binlog_position, gtid_set,
		       events_indexed, last_event_time, server_id
		FROM stream_state WHERE id = 1`).Scan(
		&s.mode, &s.binlogFile, &s.binlogPos, &gtidSet,
		&s.eventsIndexed, &s.lastEventTime, &s.serverID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query stream_state: %w", err)
	}
	if gtidSet.Valid {
		s.gtidSet = gtidSet.String
	}
	return &s, nil
}

// saveCheckpoint persists the current stream state to the stream_state table.
func saveCheckpoint(db *sql.DB, state *streamState) error {
	var gtidSet any
	if state.gtidSet != "" {
		gtidSet = state.gtidSet
	}
	var lastEventTime any
	if state.lastEventTime.Valid {
		lastEventTime = state.lastEventTime.Time
	}
	_, err := db.Exec(`
		INSERT INTO stream_state
		    (id, mode, binlog_file, binlog_position, gtid_set,
		     events_indexed, last_event_time, last_checkpoint, server_id)
		VALUES (1, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(), ?)
		ON DUPLICATE KEY UPDATE
		    binlog_file     = VALUES(binlog_file),
		    binlog_position = VALUES(binlog_position),
		    gtid_set        = VALUES(gtid_set),
		    events_indexed  = VALUES(events_indexed),
		    last_event_time = VALUES(last_event_time),
		    last_checkpoint = UTC_TIMESTAMP(),
		    server_id       = VALUES(server_id)`,
		state.mode, state.binlogFile, state.binlogPos, gtidSet,
		state.eventsIndexed, lastEventTime, state.serverID)
	return err
}

// ─── TLS configuration ────────────────────────────────────────────────────────

// buildTLSConfig returns a *tls.Config for the given ssl-mode, or nil for
// "disabled". serverName is the target host (used only for verify-identity).
func buildTLSConfig(mode, ca, cert, key, serverName string) (*tls.Config, error) {
	if mode == "disabled" {
		return nil, nil
	}
	switch mode {
	case "preferred", "required", "verify-ca", "verify-identity":
	default:
		return nil, fmt.Errorf("invalid --ssl-mode %q: must be one of disabled, preferred, required, verify-ca, verify-identity", mode)
	}
	if (cert == "") != (key == "") {
		return nil, fmt.Errorf("--ssl-cert and --ssl-key must both be specified together")
	}

	cfg := &tls.Config{}

	// Load CA pool (optional — system CAs used when empty).
	var caPool *x509.CertPool
	if ca != "" {
		pem, err := os.ReadFile(ca)
		if err != nil {
			return nil, fmt.Errorf("read --ssl-ca %q: %w", ca, err)
		}
		caPool = x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("--ssl-ca %q: no valid certificates found", ca)
		}
		cfg.RootCAs = caPool
	}

	// Load client certificate for mutual TLS.
	if cert != "" {
		kp, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, fmt.Errorf("load --ssl-cert/--ssl-key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{kp}
	}

	switch mode {
	case "preferred", "required":
		// Encrypt the connection but skip server certificate verification.
		cfg.InsecureSkipVerify = true //nolint:gosec // intentional for these modes
	case "verify-ca":
		// Verify the certificate chain against the CA pool but not the hostname.
		cfg.InsecureSkipVerify = true //nolint:gosec // hostname check done via VerifyConnection
		cfg.VerifyConnection = func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("server presented no certificate")
			}
			opts := x509.VerifyOptions{
				Roots:         caPool, // nil → system CAs
				Intermediates: x509.NewCertPool(),
			}
			for _, c := range cs.PeerCertificates[1:] {
				opts.Intermediates.AddCert(c)
			}
			_, err := cs.PeerCertificates[0].Verify(opts)
			return err
		}
	case "verify-identity":
		// Full TLS verification: certificate chain + hostname.
		cfg.ServerName = serverName
	}

	return cfg, nil
}

// ─── Start position resolution ───────────────────────────────────────────────

// parseSourceDSN extracts the host, port, user, and password from a
// go-sql-driver DSN for use in BinlogSyncerConfig.
func parseSourceDSN(dsn string) (host string, port uint16, user, password string, err error) {
	cfg, parseErr := drivermysql.ParseDSN(dsn)
	if parseErr != nil {
		return "", 0, "", "", fmt.Errorf("invalid --source-dsn: %w", parseErr)
	}
	if strings.EqualFold(cfg.Net, "unix") {
		return "", 0, "", "", fmt.Errorf("--source-dsn uses a unix socket; binlog replication requires a TCP address")
	}
	h, p, splitErr := net.SplitHostPort(cfg.Addr)
	if splitErr != nil {
		return "", 0, "", "", fmt.Errorf("invalid address in --source-dsn %q: %w", cfg.Addr, splitErr)
	}
	portN, convErr := strconv.ParseUint(p, 10, 16)
	if convErr != nil {
		return "", 0, "", "", fmt.Errorf("invalid port in --source-dsn: %w", convErr)
	}
	return h, uint16(portN), cfg.User, cfg.Passwd, nil
}

// resolveStart determines the start position for replication. It returns the
// mode ("position" or "gtid"), file, GTID string, pos, and an optional
// pre-parsed MysqlGTIDSet (non-nil only in GTID mode).
func resolveStart(
	startFile, startGTID string, startPos uint32,
	saved *streamState,
) (mode, file, gtidStr string, pos uint32, accGTID *gomysql.MysqlGTIDSet, err error) {
	if startFile != "" && startGTID != "" {
		return "", "", "", 0, nil, fmt.Errorf("--start-file and --start-gtid are mutually exclusive")
	}

	if startFile != "" || startGTID != "" {
		if saved != nil {
			slog.Warn("--start-* flag provided; overriding saved stream state")
		}
		if startGTID != "" {
			gs, parseErr := gomysql.ParseMysqlGTIDSet(startGTID)
			if parseErr != nil {
				return "", "", "", 0, nil, fmt.Errorf("invalid --start-gtid: %w", parseErr)
			}
			return "gtid", "", startGTID, 0, gs.(*gomysql.MysqlGTIDSet), nil
		}
		return "position", startFile, "", startPos, nil, nil
	}

	if saved != nil {
		if saved.mode == "gtid" {
			slog.Info("resuming from GTID set", "gtid_set", saved.gtidSet)
			gs, parseErr := gomysql.ParseMysqlGTIDSet(saved.gtidSet)
			if parseErr != nil {
				return "", "", "", 0, nil, fmt.Errorf("invalid saved gtid_set %q: %w", saved.gtidSet, parseErr)
			}
			return "gtid", "", saved.gtidSet, 0, gs.(*gomysql.MysqlGTIDSet), nil
		}
		slog.Info("resuming from position", "file", saved.binlogFile, "pos", saved.binlogPos)
		return "position", saved.binlogFile, "", uint32(saved.binlogPos), nil, nil
	}

	return "", "", "", 0, nil, fmt.Errorf(
		"no start position specified and no saved stream state found; " +
			"provide --start-file or --start-gtid to begin streaming")
}

// ─── Stream loop ─────────────────────────────────────────────────────────────

// streamLoop consumes parser events, flushes batches to MySQL, and writes
// checkpoints to stream_state at the given interval.
func streamLoop(
	ctx context.Context,
	events <-chan parser.Event,
	idx *indexer.Indexer,
	db *sql.DB,
	checkpointInterval time.Duration,
	state *streamState,
) error {
	batch := make([]parser.Event, 0, idx.BatchSize())
	ticker := time.NewTicker(checkpointInterval)
	defer ticker.Stop()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		observe.StreamBatchSize.Observe(float64(len(batch)))
		n, err := idx.InsertBatch(batch)
		state.eventsIndexed += n
		observe.StreamEventsIndexed.Add(float64(n))
		observe.StreamBatchFlushes.Inc()
		batch = batch[:0]
		if err != nil {
			observe.StreamErrors.WithLabelValues("batch_flush").Inc()
		}
		return err
	}

	checkpoint := func() {
		if err := flush(); err != nil {
			slog.Warn("batch flush failed", "error", err)
			return
		}
		if err := saveCheckpoint(db, state); err != nil {
			slog.Warn("saveCheckpoint failed", "error", err)
			observe.StreamErrors.WithLabelValues("checkpoint").Inc()
		} else {
			observe.StreamCheckpointSaves.Inc()
			slog.Info("checkpoint saved",
				"file", state.binlogFile,
				"pos", state.binlogPos,
				"events_indexed", state.eventsIndexed)
		}
	}

	for {
		select {
		case <-ctx.Done():
			checkpoint()
			return nil

		case <-ticker.C:
			checkpoint()

		case ev, ok := <-events:
			if !ok {
				checkpoint()
				return nil
			}
			// Update position tracking from each event.
			if ev.BinlogFile != "" {
				state.binlogFile = ev.BinlogFile
			}
			state.binlogPos = ev.EndPos
			if ev.GTID != "" && state.accGTID != nil {
				if err := state.accGTID.Update(ev.GTID); err != nil {
					slog.Warn("failed to update GTID set", "gtid", ev.GTID, "error", err)
					observe.StreamErrors.WithLabelValues("gtid_update").Inc()
				} else {
					state.gtidSet = state.accGTID.String()
				}
			}
			if !ev.Timestamp.IsZero() {
				state.lastEventTime = sql.NullTime{Time: ev.Timestamp, Valid: true}
			}

			observe.StreamEventsReceived.Inc()
			if !ev.Timestamp.IsZero() {
				ts := float64(ev.Timestamp.Unix())
				observe.StreamLastEventTimestamp.Set(ts)
				observe.StreamReplicationLag.Set(float64(time.Now().Unix()) - ts)
			}
			slog.Debug("event received",
				"schema", ev.Schema,
				"table", ev.Table,
				"type", ev.EventType,
				"gtid", ev.GTID)

			batch = append(batch, ev)
			if len(batch) >= idx.BatchSize() {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

// ─── runStream ───────────────────────────────────────────────────────────────

func runStream(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	// ── 1. Connect to index database ─────────────────────────────────────────
	indexDB, err := config.Connect(strmIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer indexDB.Close()

	// ── 2. Connect to source database: validate binlog_row_image ─────────────
	sourceDB, err := config.Connect(strmSourceDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to source MySQL: %w", err)
	}
	defer sourceDB.Close()

	if err := validateBinlogFormat(sourceDB); err != nil {
		return err
	}
	fmt.Println("Source: binlog_format=ROW ✓")

	if err := validateBinlogRowImage(sourceDB); err != nil {
		return err
	}
	fmt.Println("Source: binlog_row_image=FULL ✓")

	if err := validateNoFKCascades(sourceDB, parseSchemaList(strmSchemas)); err != nil {
		return err
	}
	fmt.Println("Source: no FK cascades ✓")

	// ── 3. Schema snapshot + resolver ────────────────────────────────────────
	resolver, err := ensureResolver(indexDB, sourceDB, parseSchemaList(strmSchemas))
	if err != nil {
		return err
	}
	fmt.Printf("Snapshot: id=%d, tables=%d\n", resolver.SnapshotID(), resolver.TableCount())

	// ── 4. Filters ────────────────────────────────────────────────────────────
	filters := buildIndexFilters(strmSchemas, strmTables)

	// ── 5. Determine start position ───────────────────────────────────────────
	saved, err := loadStreamState(indexDB)
	if err != nil {
		return fmt.Errorf("failed to load stream state: %w", err)
	}

	mode, startFile, startGTIDStr, startPos, accGTID, err := resolveStart(
		strmStartFile, strmStartGTID, strmStartPos, saved)
	if err != nil {
		return err
	}

	state := &streamState{
		mode:     mode,
		serverID: strmServerID,
		accGTID:  accGTID,
	}
	if saved != nil {
		state.eventsIndexed = saved.eventsIndexed
	}
	if startGTIDStr != "" {
		state.gtidSet = startGTIDStr
	}

	// ── 6. Parse source DSN for BinlogSyncer ─────────────────────────────────
	host, port, user, password, err := parseSourceDSN(strmSourceDSN)
	if err != nil {
		return err
	}

	// ── 6b. Build TLS config ──────────────────────────────────────────────────
	tlsCfg, err := buildTLSConfig(strmSSLMode, strmSSLCA, strmSSLCert, strmSSLKey, host)
	if err != nil {
		return err
	}

	// ── 7. Create BinlogSyncer ────────────────────────────────────────────────
	syncerCfg := replication.BinlogSyncerConfig{
		ServerID:             strmServerID,
		Flavor:               "mysql",
		Host:                 host,
		Port:                 port,
		User:                 user,
		Password:             password,
		HeartbeatPeriod:      30 * time.Second,
		MaxReconnectAttempts: 0, // infinite retry
		TLSConfig:            tlsCfg,
	}

	// Use a closure defer so the active syncer is always closed on exit,
	// even if we replace it during the preferred-mode TLS fallback below.
	// The nil guard prevents a panic if an early-return is added before
	// syncer is assigned.
	var syncer *replication.BinlogSyncer
	defer func() {
		if syncer != nil {
			syncer.Close()
		}
	}()
	syncer = replication.NewBinlogSyncer(syncerCfg)

	// startStreamer starts sync from the resolved position/GTID set.
	startStreamer := func() (*replication.BinlogStreamer, error) {
		switch mode {
		case "position":
			s, startErr := syncer.StartSync(gomysql.Position{Name: startFile, Pos: startPos})
			if startErr != nil {
				return nil, fmt.Errorf("StartSync(%s, %d): %w", startFile, startPos, startErr)
			}
			return s, nil
		case "gtid":
			gset, parseErr := gomysql.ParseGTIDSet("mysql", startGTIDStr)
			if parseErr != nil {
				return nil, fmt.Errorf("parse start GTID set: %w", parseErr)
			}
			s, startErr := syncer.StartSyncGTID(gset)
			if startErr != nil {
				return nil, fmt.Errorf("StartSyncGTID: %w", startErr)
			}
			return s, nil
		default:
			return nil, fmt.Errorf("unexpected mode %q", mode)
		}
	}

	// ── 8. Start sync ─────────────────────────────────────────────────────────
	streamer, startErr := startStreamer()
	if startErr != nil && strmSSLMode == "preferred" {
		// preferred: TLS attempt failed — retry without TLS.
		slog.Warn("initial connection failed; retrying without TLS (--ssl-mode preferred)", "error", startErr)
		syncer.Close()
		syncerCfg.TLSConfig = nil
		syncer = replication.NewBinlogSyncer(syncerCfg)
		streamer, startErr = startStreamer()
		if startErr != nil {
			return startErr
		}
	} else if startErr != nil {
		return startErr
	}

	switch mode {
	case "position":
		fmt.Printf("Streaming from %s position %d\n", startFile, startPos)
	case "gtid":
		fmt.Printf("Streaming from GTID set: %s\n", startGTIDStr)
	}

	// ── 9. Signal handler ─────────────────────────────────────────────────────
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case sig := <-sigCh:
			slog.Info("received signal — shutting down gracefully", "signal", sig.String())
			cancel()
		case <-ctx.Done():
		}
	}()

	// ── 9b. Optional Prometheus metrics HTTP server ───────────────────────────
	if strmMetricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		metricsServer := &http.Server{Addr: strmMetricsAddr, Handler: mux}
		go func() {
			slog.Info("metrics server starting", "addr", strmMetricsAddr)
			if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("metrics server error", "error", err)
			}
		}()
		defer func() {
			shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutCancel()
			_ = metricsServer.Shutdown(shutCtx)
		}()
	}

	// ── 10. Launch StreamParser in a goroutine ────────────────────────────────
	sp := parser.NewStreamParser(resolver, filters, nil)
	idx := indexer.New(indexDB, strmBatchSize)

	events := make(chan parser.Event, 1000)
	parseErrCh := make(chan error, 1)

	go func() {
		defer close(events)
		parseErrCh <- sp.Run(ctx, streamer, events)
	}()

	// ── 11. Run stream loop with checkpointing ────────────────────────────────
	fmt.Printf("Streaming started (server-id=%d, checkpoint=%ds)\n", strmServerID, strmCheckpoint)
	loopErr := streamLoop(ctx, events, idx, indexDB,
		time.Duration(strmCheckpoint)*time.Second, state)

	parseErr := <-parseErrCh

	// ── 12. Summary ───────────────────────────────────────────────────────────
	fmt.Printf("\nEvents indexed: %d\n", state.eventsIndexed)
	fmt.Printf("Last position:  %s:%d\n", state.binlogFile, state.binlogPos)

	if loopErr != nil {
		return loopErr
	}
	if parseErr != nil && !errors.Is(parseErr, context.Canceled) {
		return parseErr
	}
	return nil
}
