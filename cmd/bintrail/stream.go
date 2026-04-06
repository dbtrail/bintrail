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

	"github.com/dbtrail/bintrail/internal/cliutil"
	"github.com/dbtrail/bintrail/internal/config"
	"github.com/dbtrail/bintrail/internal/indexer"
	"github.com/dbtrail/bintrail/internal/metadata"
	"github.com/dbtrail/bintrail/internal/observe"
	"github.com/dbtrail/bintrail/internal/parser"
	"github.com/dbtrail/bintrail/internal/serverid"
)

var streamCmd = &cobra.Command{
	Use:   "stream",
	Short: "Index events from a live MySQL replication stream",
	Long: `Connects to a MySQL server as a replica over the replication protocol and
indexes binlog row events in real-time into binlog_events.

Unlike 'bintrail index', this command does not require access to binlog files
on disk and works with managed MySQL (RDS, Aurora, Cloud SQL).

Start position must be specified on the first run via --start-file or
--start-gtid. On subsequent runs the saved checkpoint is resumed automatically,
even if --start-file/--start-gtid are still present on the command line. This
makes re-running the same command idempotent.

Use --reset to clear the saved checkpoint and force a new start position:

  bintrail stream --reset --start-file mysql-bin.000500 ...

Without --reset, the checkpoint always wins (idempotent behavior is preserved).

Gap detection: on restart, bintrail checks whether the source still has the
binlogs needed to resume from the checkpoint. If binlogs have been purged, it
auto-advances to the earliest available position and logs a warning. Use
--no-gap-fill to refuse to start when an unfillable gap is detected.

Important: configure binlog retention to at least 2 days
(binlog_expire_logs_seconds >= 172800) to give bintrail time to fill gaps.

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
	strmFormat      string
	strmReset       bool
	strmNoGapFill   bool
	strmGapTimeout  int
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
	streamCmd.Flags().StringVar(&strmFormat, "format", "text", "Output format: text or json")
	streamCmd.Flags().BoolVar(&strmReset, "reset", false, "Clear saved checkpoint before starting (forces use of --start-file/--start-gtid)")
	streamCmd.Flags().BoolVar(&strmNoGapFill, "no-gap-fill", false, "Refuse to start if an unfillable binlog gap is detected (instead of auto-advancing past purged data)")
	streamCmd.Flags().IntVar(&strmGapTimeout, "gap-timeout", 30, "Timeout in seconds for gap-detection queries (SHOW BINARY LOGS, @@gtid_purged, @@gtid_executed); raise on RDS instances with many binlog files")
	_ = streamCmd.MarkFlagRequired("index-dsn")
	_ = streamCmd.MarkFlagRequired("source-dsn")
	_ = streamCmd.MarkFlagRequired("server-id")
	bindCommandEnv(streamCmd)

	rootCmd.AddCommand(streamCmd)
}

// ─── streamState ───────────────────────────────────────────────────────────────────

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
	bintrailID    string // resolved server identity (empty = unknown, stored as NULL)

	// accGTID is the in-memory accumulated GTID set (GTID mode only).
	// It is serialized to gtidSet on checkpoint.
	accGTID *gomysql.MysqlGTIDSet
}

// loadStreamState loads the saved stream_state row, returning nil if no row exists.
func loadStreamState(db *sql.DB) (*streamState, error) {
	var s streamState
	var gtidSet, bintrailID sql.NullString
	err := db.QueryRow(`
		SELECT mode, binlog_file, binlog_position, gtid_set,
		       events_indexed, last_event_time, server_id, bintrail_id
		FROM stream_state WHERE id = 1`).Scan(
		&s.mode, &s.binlogFile, &s.binlogPos, &gtidSet,
		&s.eventsIndexed, &s.lastEventTime, &s.serverID, &bintrailID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query stream_state: %w", err)
	}
	if gtidSet.Valid {
		s.gtidSet = gtidSet.String
	}
	if bintrailID.Valid {
		s.bintrailID = bintrailID.String
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
	var bintrailIDArg any
	if state.bintrailID != "" {
		bintrailIDArg = state.bintrailID
	}
	_, err := db.Exec(`
		INSERT INTO stream_state
		    (id, mode, binlog_file, binlog_position, gtid_set,
		     events_indexed, last_event_time, last_checkpoint, server_id, bintrail_id)
		VALUES (1, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(), ?, ?)
		ON DUPLICATE KEY UPDATE
		    binlog_file     = VALUES(binlog_file),
		    binlog_position = VALUES(binlog_position),
		    gtid_set        = VALUES(gtid_set),
		    events_indexed  = VALUES(events_indexed),
		    last_event_time = VALUES(last_event_time),
		    last_checkpoint = UTC_TIMESTAMP(),
		    server_id       = VALUES(server_id),
		    bintrail_id     = VALUES(bintrail_id)`,
		state.mode, state.binlogFile, state.binlogPos, gtidSet,
		state.eventsIndexed, lastEventTime, state.serverID, bintrailIDArg)
	return err
}

// ─── TLS configuration ───────────────────────────────────────────────────────────────

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

// normalizeGTIDSet zero-pads each UUID in a GTID set to the standard
// 8-4-4-4-12 format. Some MySQL-compatible services (e.g. Amazon RDS) return
// GTIDs with leading zeros stripped from UUID segments, producing UUIDs shorter
// than 36 characters (e.g. "5512139-1432-11f1-8d8d-0693b428a89b" instead of
// "05512139-1432-11f1-8d8d-0693b428a89b"). The go-mysql library requires
// standard-length UUIDs, so this function normalizes before parsing.
func normalizeGTIDSet(s string) string {
	// Expected segment lengths in a UUID: 8-4-4-4-12.
	segLens := [5]int{8, 4, 4, 4, 12}

	// A GTID set is comma-separated entries like "uuid:intervals,uuid:intervals".
	entries := strings.Split(s, ",")
	for i, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		// Split "uuid:intervals" at the first colon.
		colon := strings.IndexByte(entry, ':')
		if colon < 0 {
			continue // malformed, let the parser handle it
		}
		uuid := entry[:colon]
		rest := entry[colon:] // includes the ":"

		parts := strings.Split(uuid, "-")
		if len(parts) != 5 {
			continue // not a UUID shape, let the parser handle it
		}

		changed := false
		for j, seg := range parts {
			if len(seg) < segLens[j] {
				parts[j] = strings.Repeat("0", segLens[j]-len(seg)) + seg
				changed = true
			}
		}
		if changed {
			entries[i] = strings.Join(parts, "-") + rest
		} else {
			entries[i] = entry
		}
	}
	return strings.Join(entries, ",")
}

// resolveStart determines the start position for replication. It returns the
// mode ("position" or "gtid"), file, GTID string, pos, and an optional
// pre-parsed MysqlGTIDSet (non-nil only in GTID mode).
func resolveStart(
	startFile, startGTID string, startPos uint32,
	saved *streamState,
) (mode, file, gtidStr string, pos uint32, accGTID *gomysql.MysqlGTIDSet, err error) {
	// Saved checkpoint takes priority — makes re-running the same command
	// idempotent (the user doesn't need to remove --start-file to resume).
	// Exception: if the user explicitly requests a *different* mode than the
	// saved checkpoint (e.g. saved=position but --start-gtid given, or
	// saved=gtid but --start-file given), honor the flag to allow seamless
	// mode switching without deleting stream_state.
	if saved != nil {
		if startFile != "" && startGTID != "" {
			return "", "", "", 0, nil, fmt.Errorf("--start-file and --start-gtid are mutually exclusive")
		}

		// Detect mode switch: user explicitly requests a different mode.
		switchToGTID := saved.mode == "position" && startGTID != "" && startFile == ""
		switchToPosition := saved.mode == "gtid" && startFile != "" && startGTID == ""

		if switchToGTID {
			slog.Warn("switching from position mode to GTID mode", "old_file", saved.binlogFile, "old_pos", saved.binlogPos)
			startGTID = normalizeGTIDSet(startGTID)
			gs, parseErr := gomysql.ParseMysqlGTIDSet(startGTID)
			if parseErr != nil {
				return "", "", "", 0, nil, fmt.Errorf("invalid --start-gtid: %w", parseErr)
			}
			return "gtid", "", startGTID, 0, gs.(*gomysql.MysqlGTIDSet), nil
		}
		if switchToPosition {
			slog.Warn("switching from GTID mode to position mode", "old_gtid_set", saved.gtidSet)
			return "position", startFile, "", startPos, nil, nil
		}

		// Same mode or no flags — resume from saved state.
		if startFile != "" || startGTID != "" {
			slog.Warn("checkpoint exists; ignoring --start-file/--start-gtid and resuming from saved state")
		}
		if saved.mode == "gtid" {
			normalized := normalizeGTIDSet(saved.gtidSet)
			slog.Info("resuming from GTID set", "gtid_set", normalized)
			gs, parseErr := gomysql.ParseMysqlGTIDSet(normalized)
			if parseErr != nil {
				return "", "", "", 0, nil, fmt.Errorf("invalid saved gtid_set %q: %w", saved.gtidSet, parseErr)
			}
			return "gtid", "", normalized, 0, gs.(*gomysql.MysqlGTIDSet), nil
		}
		slog.Info("resuming from position", "file", saved.binlogFile, "pos", saved.binlogPos)
		return "position", saved.binlogFile, "", uint32(saved.binlogPos), nil, nil
	}

	// No checkpoint — use flags for initial start position (first run).
	if startFile != "" && startGTID != "" {
		return "", "", "", 0, nil, fmt.Errorf("--start-file and --start-gtid are mutually exclusive")
	}
	if startGTID != "" {
		startGTID = normalizeGTIDSet(startGTID)
		gs, parseErr := gomysql.ParseMysqlGTIDSet(startGTID)
		if parseErr != nil {
			return "", "", "", 0, nil, fmt.Errorf("invalid --start-gtid: %w", parseErr)
		}
		return "gtid", "", startGTID, 0, gs.(*gomysql.MysqlGTIDSet), nil
	}
	if startFile != "" {
		return "position", startFile, "", startPos, nil, nil
	}

	return "", "", "", 0, nil, fmt.Errorf(
		"no start position specified and no saved stream state found; " +
			"provide --start-file or --start-gtid to begin streaming")
}

// ─── Gap detection ──────────────────────────────────────────────────────────────

// gapResult describes a binlog gap between the saved checkpoint and the source.
type gapResult struct {
	HasGap   bool   // true if a gap exists between checkpoint and source
	Fillable bool   // true if the gap can be filled (binlogs still available)
	Message  string // human-readable description of the gap

	// For unfillable gaps in position mode: the earliest available binlog.
	EarliestFile string
	EarliestPos  uint32

	// For unfillable gaps in GTID mode: the purged GTID set to use as base.
	PurgedGTIDSet string
}

// detectPositionGap queries the source MySQL for available binary logs and
// checks whether the checkpoint file still exists. Returns a gapResult.
// timeout caps how long SHOW BINARY LOGS may take — RDS instances with many
// binlog files can take >10s, so callers should pass a generous value.
func detectPositionGap(sourceDB *sql.DB, checkpointFile string, checkpointPos uint32, timeout time.Duration) (*gapResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	rows, err := sourceDB.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return nil, fmt.Errorf("SHOW BINARY LOGS: %w", err)
	}
	defer rows.Close()

	// SHOW BINARY LOGS returns varying columns across MySQL versions;
	// we only need the first two (Log_name, File_size).
	cols, colErr := rows.Columns()
	if colErr != nil {
		return nil, fmt.Errorf("SHOW BINARY LOGS columns: %w", colErr)
	}
	if len(cols) < 2 {
		return nil, fmt.Errorf("SHOW BINARY LOGS returned %d columns, expected at least 2", len(cols))
	}

	type binlogEntry struct {
		name string
		size int64
	}
	var logs []binlogEntry
	for rows.Next() {
		var name string
		var size int64
		vals := make([]any, len(cols))
		vals[0] = &name
		vals[1] = &size
		for i := 2; i < len(cols); i++ {
			vals[i] = new(sql.RawBytes)
		}
		if err := rows.Scan(vals...); err != nil {
			return nil, fmt.Errorf("scan SHOW BINARY LOGS: %w", err)
		}
		logs = append(logs, binlogEntry{name, size})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate SHOW BINARY LOGS: %w", err)
	}
	if len(logs) == 0 {
		return nil, fmt.Errorf("SHOW BINARY LOGS returned no results")
	}

	// Check if the checkpoint file exists in the list.
	for _, l := range logs {
		if l.name == checkpointFile {
			// File exists but checkpoint position exceeds file size — the file
			// may have been regenerated (e.g., RESET MASTER). Treat as unfillable.
			if int64(checkpointPos) > l.size {
				return &gapResult{
					HasGap:       true,
					Fillable:     false,
					EarliestFile: logs[0].name,
					EarliestPos:  4,
					Message: fmt.Sprintf(
						"binlog gap: file %s exists but checkpoint position %d exceeds file size %d; "+
							"file may have been regenerated after RESET MASTER",
						checkpointFile, checkpointPos, l.size),
				}, nil
			}
			// File still exists and position is valid — gap is fillable.
			currentFile := logs[len(logs)-1].name
			if checkpointFile == currentFile {
				return &gapResult{HasGap: false}, nil
			}
			return &gapResult{
				HasGap:   true,
				Fillable: true,
				Message: fmt.Sprintf(
					"gap detected: checkpoint is at %s:%d, source is at %s; replaying missed events",
					checkpointFile, checkpointPos, currentFile),
			}, nil
		}
	}

	// File not found — binlogs have been purged.
	earliest := logs[0]
	return &gapResult{
		HasGap:       true,
		Fillable:     false,
		EarliestFile: earliest.name,
		EarliestPos:  4, // first 4 bytes are the magic number header, not event data
		Message: fmt.Sprintf(
			"binlog gap detected but CANNOT be filled: required file %s has been purged; "+
				"earliest available binlog is %s; events between these positions are permanently lost",
			checkpointFile, earliest.name),
	}, nil
}

// gtidSetsEqual parses two GTID set strings and compares them structurally,
// avoiding false mismatches from formatting differences (UUID case, ordering).
// Returns false (not an error) if either string cannot be parsed as a valid
// GTID set — the caller should treat this as "not equal" and proceed with
// gap detection.
func gtidSetsEqual(a, b string) bool {
	if a == "" && b == "" {
		return true
	}
	ga, err := gomysql.ParseMysqlGTIDSet(normalizeGTIDSet(a))
	if err != nil {
		slog.Debug("gtidSetsEqual: failed to parse first GTID set", "gtid_set", a, "error", err)
		return false
	}
	gb, err := gomysql.ParseMysqlGTIDSet(normalizeGTIDSet(b))
	if err != nil {
		slog.Debug("gtidSetsEqual: failed to parse second GTID set", "gtid_set", b, "error", err)
		return false
	}
	return ga.Equal(gb)
}

// detectGTIDGap queries the source MySQL for @@gtid_purged and @@gtid_executed,
// then checks whether the checkpoint GTID set requires any purged GTIDs.
// timeout caps how long the GTID system-variable queries may take.
func detectGTIDGap(sourceDB *sql.DB, checkpointGTID string, timeout time.Duration) (*gapResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	checkpointGTID = normalizeGTIDSet(strings.TrimSpace(checkpointGTID))

	var purgedStr, executedStr string
	if err := sourceDB.QueryRowContext(ctx, "SELECT @@gtid_purged").Scan(&purgedStr); err != nil {
		return nil, fmt.Errorf("query @@gtid_purged: %w", err)
	}
	if err := sourceDB.QueryRowContext(ctx, "SELECT @@gtid_executed").Scan(&executedStr); err != nil {
		return nil, fmt.Errorf("query @@gtid_executed: %w", err)
	}

	purgedStr = normalizeGTIDSet(strings.TrimSpace(purgedStr))
	executedStr = normalizeGTIDSet(strings.TrimSpace(executedStr))

	// If purged is empty, all GTIDs are still available.
	if purgedStr == "" {
		if gtidSetsEqual(checkpointGTID, executedStr) {
			return &gapResult{HasGap: false}, nil
		}
		return &gapResult{
			HasGap:   true,
			Fillable: true,
			Message:  "gap detected: checkpoint GTID set is behind source @@gtid_executed; replaying missed events",
		}, nil
	}

	if checkpointGTID == "" {
		return nil, fmt.Errorf("checkpoint GTID set is empty; cannot perform gap detection")
	}

	// Parse both GTID sets.
	checkpoint, err := gomysql.ParseMysqlGTIDSet(checkpointGTID)
	if err != nil {
		return nil, fmt.Errorf("parse checkpoint GTID set: %w", err)
	}
	purged, err := gomysql.ParseMysqlGTIDSet(purgedStr)
	if err != nil {
		return nil, fmt.Errorf("parse @@gtid_purged: %w", err)
	}

	cpSet, ok := checkpoint.(*gomysql.MysqlGTIDSet)
	if !ok {
		return nil, fmt.Errorf("unexpected GTID set type %T from checkpoint parse", checkpoint)
	}
	purgedSet, ok := purged.(*gomysql.MysqlGTIDSet)
	if !ok {
		return nil, fmt.Errorf("unexpected GTID set type %T from purged parse", purged)
	}

	// Check if the stream would need any purged GTIDs on resume. MySQL sends
	// all GTIDs NOT in the checkpoint set, so we must check two directions:
	//
	// 1. Forward: for each UUID in the checkpoint, does the purged set contain
	//    any transaction IDs that the checkpoint doesn't have? If so, MySQL
	//    would need to send those GTIDs but they've been purged.
	// 2. Reverse: are there UUIDs in the purged set that the checkpoint has
	//    never seen? MySQL would try to send all of that UUID's GTIDs, but
	//    some have been purged.
	needsPurged := false

	// Direction 1: for each UUID in the purged set, check if the checkpoint
	// fully covers all purged intervals. If the checkpoint doesn't contain all
	// purged GTIDs for a UUID, MySQL would need to send them but they're gone.
	// This correctly handles non-contiguous intervals (e.g., checkpoint has
	// uuid:1-100,200-300 but purged has uuid:1-150 — GTIDs 101-150 are lost).
	//
	// Direction 2 (inline): if a purged UUID is absent from the checkpoint
	// entirely, MySQL would try to send all of that UUID's GTIDs.
	for uuid, purgedIntervals := range purgedSet.Sets {
		cpIntervals, exists := cpSet.Sets[uuid]
		if !exists {
			// Purged UUID the checkpoint has never seen.
			needsPurged = true
			break
		}
		if len(purgedIntervals.Intervals) == 0 {
			continue
		}
		if len(cpIntervals.Intervals) == 0 {
			needsPurged = true
			break
		}
		// Check if the checkpoint's intervals fully contain the purged intervals.
		// IntervalSlice.Contain(sub) returns true if sub is a subset of s.
		if !cpIntervals.Intervals.Contain(purgedIntervals.Intervals) {
			needsPurged = true
			break
		}
	}

	if needsPurged {
		return &gapResult{
			HasGap:        true,
			Fillable:      false,
			PurgedGTIDSet: purgedStr,
			Message: fmt.Sprintf(
				"GTID gap detected but CANNOT be filled: required GTIDs have been purged from the source; "+
					"purged set: %s; events in the purged range are permanently lost",
				purgedStr),
		}, nil
	}

	// Checkpoint is ahead of (or equal to) the purged set — gap is fillable.
	if gtidSetsEqual(checkpointGTID, executedStr) {
		return &gapResult{HasGap: false}, nil
	}
	return &gapResult{
		HasGap:   true,
		Fillable: true,
		Message:  "gap detected: checkpoint GTID set is behind source @@gtid_executed; replaying missed events",
	}, nil
}

// ─── Stream loop ────────────────────────────────────────────────────────────────

// streamLoop consumes parser events, flushes batches to MySQL, and writes
// checkpoints to stream_state at the given interval.
// onDDL is called when a DDL event is received (after flushing the current batch).
// It may be nil, in which case no handler callback is invoked (position/GTID
// tracking still occurs).
func streamLoop(
	ctx context.Context,
	events <-chan parser.Event,
	idx *indexer.Indexer,
	db *sql.DB,
	checkpointInterval time.Duration,
	state *streamState,
	onDDL func(parser.Event) error,
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

			// GTID-only events: position/GTID already tracked above, skip insertion.
			if ev.EventType == parser.EventGTID {
				continue
			}

			// DDL events: flush batch, invoke handler, skip insertion.
			if ev.EventType == parser.EventDDL {
				if err := flush(); err != nil {
					return err
				}
				if onDDL != nil {
					if err := onDDL(ev); err != nil {
						slog.Error("DDL handler failed", "error", err,
						"file", ev.BinlogFile, "pos", ev.EndPos)
					}
				}
				continue
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

// ─── runStream ───────────────────────────────────────────────────────────────────

func runStream(cmd *cobra.Command, args []string) error {
	if !cliutil.IsValidOutputFormat(strmFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", strmFormat)
	}

	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	// ── 1. Connect to index database ─────────────────────────────────────────
	indexDB, err := config.Connect(strmIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer indexDB.Close()

	if err := indexer.EnsureSchema(indexDB); err != nil {
		return fmt.Errorf("schema migration: %w", err)
	}

	// ── 2. Connect to source database: validate binlog_row_image ─────────────
	sourceDB, err := config.Connect(strmSourceDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to source MySQL: %w", err)
	}
	defer sourceDB.Close()

	if err := validateBinlogFormat(sourceDB); err != nil {
		return err
	}
	fmt.Println("Source: binlog_format=ROW \u2713")

	if err := validateBinlogRowImage(sourceDB); err != nil {
		return err
	}
	fmt.Println("Source: binlog_row_image=FULL \u2713")

	if err := validateNoFKCascades(sourceDB, parseSchemaList(strmSchemas)); err != nil {
		return err
	}
	fmt.Println("Source: no FK cascades \u2713")

	// ── 3. Resolve server identity ────────────────────────────────────────────
	bintrailID, err := resolveServerIdentity(ctx, sourceDB, indexDB, strmSourceDSN)
	if err != nil {
		if errors.Is(err, serverid.ErrConflict) {
			return fmt.Errorf("cannot stream: %w", err)
		}
		slog.Warn("server identity resolution failed; proceeding without bintrail_id", "error", err)
	} else {
		slog.Info("server identity resolved", "bintrail_id", bintrailID)
	}

	// ── 4. Schema snapshot + resolver ─────────────────────────────────────────
	resolver, err := ensureResolver(indexDB, sourceDB, parseSchemaList(strmSchemas))
	if err != nil {
		return err
	}
	fmt.Printf("Snapshot: id=%d, tables=%d\n", resolver.SnapshotID(), resolver.TableCount())

	// ── 5. Filters ────────────────────────────────────────────────────────────
	filters := buildIndexFilters(strmSchemas, strmTables)

	// ── 6. Determine start position ───────────────────────────────────────────
	saved, err := loadStreamState(indexDB)
	if err != nil {
		return fmt.Errorf("failed to load stream state: %w", err)
	}

	if strmReset {
		if saved != nil {
			if _, err := indexDB.Exec(`DELETE FROM stream_state WHERE id = 1`); err != nil {
				return fmt.Errorf("failed to reset stream state: %w", err)
			}
			slog.Warn("cleared saved checkpoint (--reset)", "old_mode", saved.mode,
				"old_file", saved.binlogFile, "old_pos", saved.binlogPos)
			saved = nil
		} else {
			slog.Info("--reset specified but no saved checkpoint exists; ignoring")
		}
	}

	mode, startFile, startGTIDStr, startPos, accGTID, err := resolveStart(
		strmStartFile, strmStartGTID, strmStartPos, saved)
	if err != nil {
		return err
	}

	// ── 6b. Detect binlog gap ────────────────────────────────────────────
	// Only check for gaps when resuming from a saved checkpoint (not on first run).
	if saved != nil {
		var gap *gapResult
		var gapErr error

		gapTimeout := time.Duration(strmGapTimeout) * time.Second

		switch mode {
		case "position":
			gap, gapErr = detectPositionGap(sourceDB, startFile, startPos, gapTimeout)
		case "gtid":
			gap, gapErr = detectGTIDGap(sourceDB, startGTIDStr, gapTimeout)
		default:
			slog.Warn("gap detection not implemented for mode", "mode", mode)
		}

		if gapErr != nil {
			// Gap detection failure means we cannot verify whether binlogs have
			// been purged. Fail hard — proceeding could mask data loss.
			slog.Error("gap detection failed", "error", gapErr)
			return fmt.Errorf("gap detection failed: %w (use --reset to skip gap detection and start from a new position)", gapErr)
		} else if gap != nil && gap.HasGap {
			if gap.Fillable {
				slog.Info(gap.Message)
				fmt.Println("Gap: fillable — replaying missed events before live tailing")
			} else {
				// Unfillable gap — binlogs/GTIDs have been purged.
				slog.Warn(gap.Message)

				if strmNoGapFill {
					return fmt.Errorf("binlog gap detected and --no-gap-fill is set: %s", gap.Message)
				}

				// Auto-advance past the gap.
				switch mode {
				case "position":
					slog.Warn("auto-advancing to earliest available binlog",
						"old_file", startFile, "old_pos", startPos,
						"new_file", gap.EarliestFile, "new_pos", gap.EarliestPos)
					fmt.Printf("Gap: UNFILLABLE — advancing from %s:%d to %s:%d (events in between are permanently lost)\n",
						startFile, startPos, gap.EarliestFile, gap.EarliestPos)
					startFile = gap.EarliestFile
					startPos = gap.EarliestPos

				case "gtid":
					slog.Warn("auto-advancing past purged GTIDs",
						"old_gtid_set", startGTIDStr,
						"purged_gtid_set", gap.PurgedGTIDSet)
					fmt.Printf("Gap: UNFILLABLE — checkpoint GTID set includes purged GTIDs; advancing past purged set (events are permanently lost)\n")
					// Use the purged set as the checkpoint GTID set — this tells
					// MySQL we have already seen all purged GTIDs, so it will
					// only send the non-purged GTIDs that remain in @@gtid_executed.
					startGTIDStr = normalizeGTIDSet(gap.PurgedGTIDSet)
					gs, parseErr := gomysql.ParseMysqlGTIDSet(startGTIDStr)
					if parseErr != nil {
						return fmt.Errorf("failed to parse purged GTID set for auto-advance: %w", parseErr)
					}
					parsed, ok := gs.(*gomysql.MysqlGTIDSet)
					if !ok {
						return fmt.Errorf("unexpected GTID set type %T after parsing purged set", gs)
					}
					accGTID = parsed
				}

				// Persist the advanced position immediately so that if the stream
				// crashes during startup, the next restart won't hit the same
				// purged-binlog error again.
				advancedState := &streamState{
					mode:          mode,
					binlogFile:    startFile,
					binlogPos:     uint64(startPos),
					gtidSet:       startGTIDStr,
					serverID:      strmServerID,
					bintrailID:    bintrailID,
					eventsIndexed: saved.eventsIndexed,
					lastEventTime: saved.lastEventTime,
				}
				if err := saveCheckpoint(indexDB, advancedState); err != nil {
					return fmt.Errorf("failed to save advanced checkpoint: %w", err)
				}
				slog.Info("saved advanced checkpoint after gap auto-advance",
					"file", startFile, "pos", startPos, "gtid_set", startGTIDStr)
			}
		}
	}

	state := &streamState{
		mode:       mode,
		serverID:   strmServerID,
		accGTID:    accGTID,
		bintrailID: bintrailID,
	}
	if saved != nil {
		state.eventsIndexed = saved.eventsIndexed
	}
	if startGTIDStr != "" {
		state.gtidSet = startGTIDStr
	}

	// ── 7. Parse source DSN for BinlogSyncer ─────────────────────────────
	host, port, user, password, err := parseSourceDSN(strmSourceDSN)
	if err != nil {
		return err
	}

	// ── 6b. Build TLS config ──────────────────────────────────────────────────
	tlsCfg, err := buildTLSConfig(strmSSLMode, strmSSLCA, strmSSLCert, strmSSLKey, host)
	if err != nil {
		return err
	}

	// ── 7. Create BinlogSyncer ────────────────────────────────────────────────────
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

	// ── 8. Start sync ───────────────────────────────────────────────────────────────
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

	// ── 9. Signal handler ────────────────────────────────────────────────────────────
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

	// ── 9b. Optional Prometheus metrics HTTP server ─────────────────────────
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

	// ── 10. Launch StreamParser in a goroutine ──────────────────────────────────
	sp := parser.NewStreamParser(resolver, filters, nil)
	idx := indexer.New(indexDB, strmBatchSize)

	events := make(chan parser.Event, 1000)
	parseErrCh := make(chan error, 1)

	go func() {
		defer close(events)
		parseErrCh <- sp.Run(ctx, streamer, events)
	}()

	// ── 11. DDL auto-snapshot handler ────────────────────────────────────────────
	schemas := parseSchemaList(strmSchemas)
	// ddlHandler performs best-effort snapshot + recording. It always returns nil
	// so that streaming continues even if the snapshot or recording fails.
	// TRUNCATE does not change schema structure, so skip snapshot for it.
	ddlHandler := func(ev parser.Event) error {
		if ev.DDLType == parser.DDLTruncateTable {
			slog.Info("DDL detected (no snapshot needed)",
				"file", ev.BinlogFile, "pos", ev.EndPos,
				"ddl_type", ev.DDLType, "schema", ev.Schema, "table", ev.Table)
			if err := insertSchemaChange(indexDB, ev, nil); err != nil {
				slog.Warn("failed to record schema change", "error", err)
			}
			return nil
		}

		slog.Info("DDL detected — taking auto-snapshot",
			"file", ev.BinlogFile, "pos", ev.EndPos,
			"ddl_type", ev.DDLType, "schema", ev.Schema, "table", ev.Table)

		stats, snapErr := metadata.TakeSnapshot(sourceDB, indexDB, schemas)
		var snapID *int
		if snapErr != nil {
			slog.Error("auto-snapshot after DDL failed; subsequent events may use stale schema",
				"error", snapErr, "ddl_type", ev.DDLType, "table", ev.Table)
		} else {
			snapID = &stats.SnapshotID
			newResolver, resolverErr := metadata.NewResolver(indexDB, stats.SnapshotID)
			if resolverErr != nil {
				slog.Warn("failed to load new resolver after DDL snapshot", "error", resolverErr)
			} else {
				sp.SwapResolver(newResolver)
				slog.Info("auto-snapshot taken; resolver updated",
					"snapshot_id", stats.SnapshotID,
					"tables", stats.TableCount,
					"columns", stats.ColumnCount)
			}
		}

		if err := insertSchemaChange(indexDB, ev, snapID); err != nil {
			slog.Warn("failed to record schema change", "error", err)
		}
		return nil
	}

	// ── 12. Run stream loop with checkpointing ──────────────────────────────────
	fmt.Printf("Streaming started (server-id=%d, checkpoint=%ds)\n", strmServerID, strmCheckpoint)
	loopErr := streamLoop(ctx, events, idx, indexDB,
		time.Duration(strmCheckpoint)*time.Second, state, ddlHandler)

	parseErr := <-parseErrCh

	// ── 12. Summary ───────────────────────────────────────────────────────────────
	if loopErr != nil {
		return loopErr
	}
	if parseErr != nil && !errors.Is(parseErr, context.Canceled) {
		return parseErr
	}

	if strmFormat == "json" {
		return outputJSON(struct {
			EventsIndexed int64  `json:"events_indexed"`
			LastFile      string `json:"last_file"`
			LastPosition  uint64 `json:"last_position"`
		}{
			EventsIndexed: state.eventsIndexed,
			LastFile:      state.binlogFile,
			LastPosition:  state.binlogPos,
		})
	}

	fmt.Printf("\nEvents indexed: %d\n", state.eventsIndexed)
	fmt.Printf("Last position:  %s:%d\n", state.binlogFile, state.binlogPos)
	return nil
}
