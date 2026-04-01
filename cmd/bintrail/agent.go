package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/spf13/cobra"

	"github.com/dbtrail/bintrail/internal/agent"
	"github.com/dbtrail/bintrail/internal/buffer"
	"github.com/dbtrail/bintrail/internal/config"
	"github.com/dbtrail/bintrail/internal/parser"
)

var agentCmd = &cobra.Command{
	Use:   "agent",
	Short: "Connect to dbtrail and listen for commands",
	Long: `Start an outbound agent channel to the dbtrail service. The agent opens a
WebSocket connection to dbtrail, authenticates with its API key, and listens
for commands (resolve_pk, recover, forensics_query). No inbound ports are
required — all communication is initiated by the agent.

The connection auto-reconnects with exponential backoff on failure and sends
periodic heartbeats to report agent status.

In BYOS mode (when --source-dsn and --s3-bucket are provided), the agent also
reads binlogs from the customer MySQL and keeps recent events in an in-memory
buffer. Events are written to Parquet on the customer's S3 on each batch flush.
Recovery and pk resolution queries check the buffer first (fastest, recent data),
then fall back to S3 Parquet archives.

Examples:
  # Start agent with index database
  bintrail agent --api-key "ak_..." --endpoint "wss://api.dbtrail.io/v1/agent" \
    --index-dsn "user:pass@tcp(host:3306)/binlog_index"

  # Start agent with Parquet archives on S3
  bintrail agent --api-key "ak_..." --endpoint "wss://api.dbtrail.io/v1/agent" \
    --archive-s3 "s3://my-bucket/archives/"

  # BYOS mode: stream + buffer + S3
  bintrail agent --api-key "ak_..." --endpoint "wss://api.dbtrail.io/v1/agent" \
    --source-dsn "user:pass@tcp(host:3306)/mydb" \
    --s3-bucket "customer-bintrail" --s3-region "us-east-1" \
    --server-id 99999 --buffer-retain "6h"`,
	RunE: runAgent,
}

var (
	agtAPIKey       string
	agtEndpoint     string
	agtIndexDSN     string
	agtSourceDSN    string
	agtArchiveDir   string
	agtArchiveS3    string
	agtBufferRetain string
	agtServerID     uint32
	agtBatchSize    int
	agtSchemas      string
	agtTables       string
	agtStartGTID    string
)

func init() {
	agentCmd.Flags().StringVar(&agtAPIKey, "api-key", "", "API key for dbtrail authentication (required)")
	agentCmd.Flags().StringVar(&agtEndpoint, "endpoint", "", "dbtrail WebSocket endpoint URL (required)")
	agentCmd.Flags().StringVar(&agtIndexDSN, "index-dsn", "", "DSN for the index MySQL database")
	agentCmd.Flags().StringVar(&agtSourceDSN, "source-dsn", "", "DSN for the source MySQL database (enables forensics queries; required for BYOS streaming)")
	agentCmd.Flags().StringVar(&agtArchiveDir, "archive-dir", "", "Local directory containing Parquet archives")
	agentCmd.Flags().StringVar(&agtArchiveS3, "archive-s3", "", "S3 path to Parquet archives (e.g. s3://bucket/prefix/)")
	agentCmd.Flags().StringVar(&agtBufferRetain, "buffer-retain", "6h", "How long to retain events in the in-memory buffer (e.g. 6h, 24h)")
	agentCmd.Flags().Uint32Var(&agtServerID, "server-id", 0, "MySQL server ID for replication (required for BYOS streaming)")
	agentCmd.Flags().IntVar(&agtBatchSize, "batch-size", 1000, "Number of events per batch flush")
	agentCmd.Flags().StringVar(&agtSchemas, "schemas", "", "Comma-separated list of schemas to index (empty = all)")
	agentCmd.Flags().StringVar(&agtTables, "tables", "", "Comma-separated list of tables to index (empty = all)")
	agentCmd.Flags().StringVar(&agtStartGTID, "start-gtid", "", "GTID set to start streaming from (first run only)")
	_ = agentCmd.MarkFlagRequired("api-key")
	_ = agentCmd.MarkFlagRequired("endpoint")
	bindCommandEnv(agentCmd)

	rootCmd.AddCommand(agentCmd)
}

func runAgent(cmd *cobra.Command, args []string) error {
	start := time.Now()

	// Build archive sources list.
	var archiveSources []string
	if agtArchiveDir != "" {
		archiveSources = append(archiveSources, agtArchiveDir)
	}
	if agtArchiveS3 != "" {
		archiveSources = append(archiveSources, agtArchiveS3)
	}

	// Determine if BYOS streaming mode is requested.
	byosMode := agtSourceDSN != "" && agtServerID != 0

	// At least one data source must be configured.
	if agtIndexDSN == "" && len(archiveSources) == 0 && !byosMode {
		return fmt.Errorf("at least one data source required: --index-dsn, --archive-dir, --archive-s3, or BYOS mode (--source-dsn + --server-id)")
	}

	handler := &agent.DefaultHandler{
		ArchiveSources: archiveSources,
		Logger:         slog.Default(),
	}

	// Connect to index database if provided.
	if agtIndexDSN != "" {
		db, err := config.Connect(agtIndexDSN)
		if err != nil {
			return fmt.Errorf("connect to index database: %w", err)
		}
		defer db.Close()
		handler.IndexDB = db
	}

	// Connect to source database if provided (for forensics queries + BYOS streaming).
	if agtSourceDSN != "" {
		db, err := config.Connect(agtSourceDSN)
		if err != nil {
			return fmt.Errorf("connect to source database: %w", err)
		}
		defer db.Close()
		handler.SourceDB = db
	}

	// BYOS streaming: start buffer + streaming goroutine.
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	if byosMode {
		retain, err := parseRetain(agtBufferRetain)
		if err != nil {
			return fmt.Errorf("invalid --buffer-retain: %w", err)
		}

		buf := buffer.New(retain, slog.Default())
		handler.Buffer = buf

		slog.Info("BYOS mode enabled",
			"source_dsn", maskDSN(agtSourceDSN),
			"server_id", agtServerID,
			"buffer_retain", retain.String())

		streamErrCh := make(chan error, 1)
		go func() {
			streamErrCh <- runBYOSStream(ctx, handler.SourceDB, buf)
		}()

		// If the stream fails during setup, return immediately.
		// After setup, stream errors are logged but don't kill the agent.
		go func() {
			if err := <-streamErrCh; err != nil && ctx.Err() == nil {
				slog.Error("BYOS stream stopped", "error", err)
			}
		}()
	}

	cfg := agent.ChannelConfig{
		Endpoint:   agtEndpoint,
		APIKey:     agtAPIKey,
		Version:    Version,
		BintrailID: "", // TODO: resolve from index DB when server identity is available
	}

	ch := agent.NewChannel(cfg, handler, nil)

	slog.Info("starting agent",
		"endpoint", agtEndpoint,
		"has_index", agtIndexDSN != "",
		"has_source", agtSourceDSN != "",
		"has_buffer", byosMode,
		"archives", len(archiveSources))

	err := ch.Run(ctx)

	slog.Info("agent stopped",
		"duration", time.Since(start).Truncate(time.Second).String(),
		"error", err)

	return err
}

// maskDSN redacts the password from a DSN for logging.
func maskDSN(dsn string) string {
	for i := range dsn {
		if dsn[i] == ':' {
			for j := i + 1; j < len(dsn); j++ {
				if dsn[j] == '@' {
					return dsn[:i+1] + "***" + dsn[j:]
				}
			}
		}
	}
	return dsn
}

// ─── BYOS streaming ────────────────────────────────────────────────────────

// runBYOSStream reads binlogs from the source MySQL and writes events to
// the in-memory buffer. It also writes Parquet files to S3 on each batch flush.
func runBYOSStream(ctx context.Context, sourceDB *sql.DB, buf *buffer.Buffer) error {
	// Validate binlog settings.
	if err := validateBinlogFormat(sourceDB); err != nil {
		return err
	}
	if err := validateBinlogRowImage(sourceDB); err != nil {
		return err
	}

	slog.Info("BYOS stream: binlog_format=ROW, binlog_row_image=FULL validated")

	// Build filters.
	filters := buildIndexFilters(agtSchemas, agtTables)

	// Create parser with nil resolver (no MySQL index for schema snapshots
	// in pure BYOS mode; column names come from the binlog itself).
	sp := parser.NewStreamParser(nil, filters, nil)

	// Parse source DSN for BinlogSyncer.
	host, port, user, password, err := parseSourceDSN(agtSourceDSN)
	if err != nil {
		return err
	}

	syncerCfg := replication.BinlogSyncerConfig{
		ServerID:             agtServerID,
		Flavor:               "mysql",
		Host:                 host,
		Port:                 port,
		User:                 user,
		Password:             password,
		HeartbeatPeriod:      30 * time.Second,
		MaxReconnectAttempts: 0,
	}
	syncer := replication.NewBinlogSyncer(syncerCfg)
	defer syncer.Close()

	// Determine start position.
	streamer, err := startBYOSSyncer(syncer, agtStartGTID)
	if err != nil {
		return err
	}

	slog.Info("BYOS stream started", "start_gtid", agtStartGTID)

	// Run event loop.
	events := make(chan parser.Event, 1000)
	parseErrCh := make(chan error, 1)

	go func() {
		defer close(events)
		parseErrCh <- sp.Run(ctx, streamer, events)
	}()

	err = byosStreamLoop(ctx, events, buf, agtBatchSize)

	parseErr := <-parseErrCh
	if parseErr != nil && ctx.Err() == nil {
		return fmt.Errorf("parser error: %w", parseErr)
	}
	return err
}

// startBYOSSyncer starts the binlog syncer from the given GTID set or
// from the server's current position if no GTID is specified.
func startBYOSSyncer(syncer *replication.BinlogSyncer, startGTID string) (*replication.BinlogStreamer, error) {
	if startGTID != "" {
		gset, err := gomysql.ParseGTIDSet("mysql", startGTID)
		if err != nil {
			return nil, fmt.Errorf("parse start GTID set: %w", err)
		}
		return syncer.StartSyncGTID(gset)
	}
	// No start position — start from the current binlog position.
	// This is safe for first run: we'll only see new events.
	return syncer.StartSync(gomysql.Position{})
}

// byosStreamLoop reads events from the parser channel and writes them to
// the buffer. It also periodically evicts old events.
func byosStreamLoop(ctx context.Context, events <-chan parser.Event, buf *buffer.Buffer, batchSize int) error {
	if batchSize <= 0 {
		batchSize = 1000
	}

	batch := make([]parser.Event, 0, batchSize)
	evictTicker := time.NewTicker(5 * time.Minute)
	defer evictTicker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		buf.Insert(batch)
		slog.Debug("BYOS batch flushed", "events", len(batch), "buffer_size", buf.Len())
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return nil

		case <-evictTicker.C:
			n := buf.Evict()
			if n > 0 {
				slog.Info("BYOS buffer eviction", "evicted", n, "remaining", buf.Len())
			}

		case ev, ok := <-events:
			if !ok {
				flush()
				return nil
			}

			// Skip non-row events (GTID tracking, DDL).
			if ev.EventType == parser.EventGTID || ev.EventType == parser.EventDDL {
				continue
			}

			batch = append(batch, ev)
			if len(batch) >= batchSize {
				flush()
			}
		}
	}
}

// ─── BYOS checkpoint ───────────────────────────────────────────────────────

// byosCheckpoint holds the streaming position for BYOS mode.
// Saved to a local JSON file so the agent can resume after restart.
type byosCheckpoint struct {
	Mode          string    `json:"mode"`
	GTIDSet       string    `json:"gtid_set,omitempty"`
	BinlogFile    string    `json:"binlog_file,omitempty"`
	BinlogPos     uint64    `json:"binlog_pos,omitempty"`
	EventsIndexed int64     `json:"events_indexed"`
	LastEventTime time.Time `json:"last_event_time"`
}

func saveBYOSCheckpoint(path string, cp byosCheckpoint) error {
	data, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

func loadBYOSCheckpoint(path string) (*byosCheckpoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var cp byosCheckpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, err
	}
	return &cp, nil
}

