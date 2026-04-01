package main

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/spf13/cobra"

	"github.com/dbtrail/bintrail/internal/agent"
	"github.com/dbtrail/bintrail/internal/buffer"
	"github.com/dbtrail/bintrail/internal/byos"
	"github.com/dbtrail/bintrail/internal/config"
	"github.com/dbtrail/bintrail/internal/metadata"
	"github.com/dbtrail/bintrail/internal/parser"
	"github.com/dbtrail/bintrail/internal/storage"
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

In BYOS mode (when --source-dsn and --server-id are provided), the agent also
reads binlogs from the customer MySQL and keeps recent events in an in-memory
buffer. Recovery and pk resolution queries check the buffer first (fastest,
recent data), then fall back to S3 Parquet archives.

Examples:
  # Start agent with index database
  bintrail agent --api-key "ak_..." --endpoint "wss://api.dbtrail.io/v1/agent" \
    --index-dsn "user:pass@tcp(host:3306)/binlog_index"

  # Start agent with Parquet archives on S3
  bintrail agent --api-key "ak_..." --endpoint "wss://api.dbtrail.io/v1/agent" \
    --archive-s3 "s3://my-bucket/archives/"

  # BYOS mode: stream + buffer
  bintrail agent --api-key "ak_..." --endpoint "wss://api.dbtrail.io/v1/agent" \
    --source-dsn "user:pass@tcp(host:3306)/mydb" \
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
	agtStartGTID     string
	agtS3Bucket      string
	agtS3Region      string
	agtS3Prefix      string
	agtFlushInterval string
	agtValidate      bool
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
	agentCmd.Flags().StringVar(&agtS3Bucket, "s3-bucket", "", "S3 bucket for BYOS payload storage")
	agentCmd.Flags().StringVar(&agtS3Region, "s3-region", "", "AWS region for the S3 bucket")
	agentCmd.Flags().StringVar(&agtS3Prefix, "s3-prefix", "bintrail/", "Key prefix within the S3 bucket")
	agentCmd.Flags().StringVar(&agtFlushInterval, "flush-interval", "5s", "Max time between metadata/payload flushes (e.g. 5s, 10s)")
	agentCmd.Flags().BoolVar(&agtValidate, "validate", false, "Run pre-flight checks and exit without starting the agent")
	_ = agentCmd.MarkFlagRequired("api-key")
	_ = agentCmd.MarkFlagRequired("endpoint")
	bindCommandEnv(agentCmd)

	rootCmd.AddCommand(agentCmd)
}

func runAgent(cmd *cobra.Command, args []string) error {
	if agtValidate {
		return runAgentValidate(cmd.Context())
	}

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

	var flushState *flushPipelineState

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

		// Initialize flush sinks if S3 bucket is configured.
		var metaClient *byos.MetadataClient
		var payloadWriter *byos.PayloadWriter

		if agtS3Bucket != "" {
			metaClient = byos.NewMetadataClient(wsEndpointToHTTP(agtEndpoint), agtAPIKey)

			s3Backend, err := storage.NewS3Backend(ctx, storage.S3Config{
				Bucket: agtS3Bucket,
				Region: agtS3Region,
				Prefix: agtS3Prefix,
			})
			if err != nil {
				return fmt.Errorf("initialize S3 backend: %w", err)
			}
			payloadWriter = byos.NewPayloadWriter(s3Backend, fmt.Sprint(agtServerID))

			slog.Info("BYOS flush pipeline initialized",
				"s3_bucket", agtS3Bucket,
				"s3_region", agtS3Region,
				"s3_prefix", agtS3Prefix)
		}

		flushInterval, err := time.ParseDuration(agtFlushInterval)
		if err != nil {
			return fmt.Errorf("invalid --flush-interval: %w", err)
		}

		flushState = &flushPipelineState{
			metadataStatus: "ok",
			payloadStatus:  "ok",
		}
		serverIDStr := fmt.Sprint(agtServerID)

		streamErrCh := make(chan error, 1)
		go func() {
			streamErrCh <- runBYOSStream(ctx, handler.SourceDB, buf, &byosFlushConfig{
				metaClient:    metaClient,
				payloadWriter: payloadWriter,
				serverID:      serverIDStr,
				flushInterval: flushInterval,
				state:         flushState,
			})
		}()

		// Wait briefly for fast setup failures (bad credentials, wrong
		// binlog_format, etc.) before starting the agent channel. If the
		// stream survives setup, monitor for runtime failures in background.
		select {
		case err := <-streamErrCh:
			if err != nil {
				return fmt.Errorf("BYOS stream failed: %w", err)
			}
		case <-time.After(3 * time.Second):
			// Stream survived setup — monitor for runtime failures.
			go func() {
				if err := <-streamErrCh; err != nil && ctx.Err() == nil {
					slog.Error("BYOS stream stopped unexpectedly", "error", err)
					cancel()
				}
			}()
		}
	}

	cfg := agent.ChannelConfig{
		Endpoint:   agtEndpoint,
		APIKey:     agtAPIKey,
		Version:    Version,
		BintrailID: "", // TODO: resolve from index DB when server identity is available
	}

	var statusFn func() *agent.FlushStatus
	if flushState != nil {
		statusFn = flushState.toFlushStatus
	}
	ch := agent.NewChannel(cfg, handler, nil, statusFn)

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

// wsEndpointToHTTP converts a WebSocket agent endpoint URL to an HTTP base
// URL suitable for the metadata API. For example:
//
//	"wss://api.dbtrail.io/v1/agent" → "https://api.dbtrail.io"
//	"ws://localhost:8080/v1/agent"  → "http://localhost:8080"
func wsEndpointToHTTP(endpoint string) string {
	// Convert scheme.
	s := strings.Replace(endpoint, "wss://", "https://", 1)
	s = strings.Replace(s, "ws://", "http://", 1)
	// Strip the path — MetadataClient appends /v1/events itself.
	if i := strings.Index(s, "://"); i != -1 {
		rest := s[i+3:]
		if j := strings.Index(rest, "/"); j != -1 {
			s = s[:i+3+j]
		}
	}
	return s
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

// ─── BYOS flush pipeline state ─────────────────────────────────────────────

// byosFlushConfig holds the sinks and settings for the BYOS flush pipeline.
// All fields are optional — when metaClient and payloadWriter are nil,
// the stream loop runs in buffer-only mode (hosted mode).
type byosFlushConfig struct {
	metaClient    *byos.MetadataClient
	payloadWriter *byos.PayloadWriter
	serverID      string
	flushInterval time.Duration
	state         *flushPipelineState
}

// flushPipelineState tracks the health of metadata/payload flushes.
// Written by byosStreamLoop, read by the heartbeat's StatusProvider.
type flushPipelineState struct {
	mu                sync.Mutex
	bufferEvents      int
	metadataStatus    string // "ok" or "degraded"
	payloadStatus     string // "ok" or "degraded"
	lastMetadataFlush *time.Time
	lastPayloadFlush  *time.Time
}

func (s *flushPipelineState) updateFlush(metaOK, payloadOK bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	if metaOK {
		s.metadataStatus = "ok"
		s.lastMetadataFlush = &now
	} else {
		s.metadataStatus = "degraded"
	}
	if payloadOK {
		s.payloadStatus = "ok"
		s.lastPayloadFlush = &now
	} else {
		s.payloadStatus = "degraded"
	}
}

func (s *flushPipelineState) setBufferLen(n int) {
	s.mu.Lock()
	s.bufferEvents = n
	s.mu.Unlock()
}

func (s *flushPipelineState) toFlushStatus() *agent.FlushStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.bufferEvents
	return &agent.FlushStatus{
		BufferEvents:      &n,
		MetadataStatus:    s.metadataStatus,
		PayloadStatus:     s.payloadStatus,
		LastMetadataFlush: s.lastMetadataFlush,
		LastPayloadFlush:  s.lastPayloadFlush,
	}
}

// ─── BYOS streaming ────────────────────────────────────────────────────────

// runBYOSStream reads binlogs from the source MySQL and writes events to
// the in-memory buffer, and optionally flushes metadata/payload to sinks.
func runBYOSStream(ctx context.Context, sourceDB *sql.DB, buf *buffer.Buffer, fc *byosFlushConfig) error {
	// Validate binlog settings.
	if err := validateBinlogFormat(sourceDB); err != nil {
		return err
	}
	if err := validateBinlogRowImage(sourceDB); err != nil {
		return err
	}

	slog.Info("BYOS stream: binlog_format=ROW, binlog_row_image=FULL validated")

	// Build schema resolver from the source DB's information_schema.
	// The resolver maps column indices to names so the parser can produce
	// named column maps (RowBefore, RowAfter) and identify PK columns.
	resolver, err := buildResolverFromSource(sourceDB, parseSchemaList(agtSchemas))
	if err != nil {
		return fmt.Errorf("build schema resolver: %w", err)
	}
	slog.Info("BYOS schema resolver built", "tables", resolver.TableCount())

	// Build filters.
	filters := buildIndexFilters(agtSchemas, agtTables)

	sp := parser.NewStreamParser(resolver, filters, nil)

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
	streamer, err := startBYOSSyncer(sourceDB, syncer, agtStartGTID)
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

	err = byosStreamLoop(ctx, events, buf, agtBatchSize, fc)

	parseErr := <-parseErrCh
	if parseErr != nil && ctx.Err() == nil {
		return fmt.Errorf("parser error: %w", parseErr)
	}
	return err
}

// startBYOSSyncer starts the binlog syncer from the given GTID set or
// from the server's current binlog position.
func startBYOSSyncer(sourceDB *sql.DB, syncer *replication.BinlogSyncer, startGTID string) (*replication.BinlogStreamer, error) {
	if startGTID != "" {
		gset, err := gomysql.ParseGTIDSet("mysql", startGTID)
		if err != nil {
			return nil, fmt.Errorf("parse start GTID set: %w", err)
		}
		return syncer.StartSyncGTID(gset)
	}
	// No start GTID — query current position from source and start there.
	var file string
	var pos uint32
	if err := sourceDB.QueryRow("SHOW MASTER STATUS").Scan(&file, &pos, new(string), new(string), new(string)); err != nil {
		return nil, fmt.Errorf("SHOW MASTER STATUS: %w", err)
	}
	slog.Info("starting from current binlog position", "file", file, "pos", pos)
	return syncer.StartSync(gomysql.Position{Name: file, Pos: pos})
}

// byosStreamLoop reads events from the parser channel and writes them to
// the buffer. When flush sinks are configured (fc.metaClient / fc.payloadWriter),
// it also splits events and flushes metadata to dbtrail and payload to S3.
func byosStreamLoop(ctx context.Context, events <-chan parser.Event, buf *buffer.Buffer, batchSize int, fc *byosFlushConfig) error {
	if batchSize <= 0 {
		batchSize = 1000
	}

	batch := make([]parser.Event, 0, batchSize)
	evictTicker := time.NewTicker(5 * time.Minute)
	defer evictTicker.Stop()

	flushInterval := 5 * time.Second
	if fc != nil && fc.flushInterval > 0 {
		flushInterval = fc.flushInterval
	}
	flushTicker := time.NewTicker(flushInterval)
	defer flushTicker.Stop()

	flushBatch := func() {
		if len(batch) == 0 {
			return
		}

		// Always insert into buffer.
		buf.Insert(batch)
		slog.Debug("BYOS batch flushed to buffer", "events", len(batch), "buffer_size", buf.Len())

		// Split and flush to sinks if configured. Skip on shutdown —
		// the sinks would fail immediately with context.Canceled and
		// produce misleading error logs.
		if fc != nil && (fc.metaClient != nil || fc.payloadWriter != nil) && ctx.Err() == nil {
			flushToSinks(ctx, batch, fc)
		}
		if fc != nil && fc.state != nil {
			fc.state.setBufferLen(buf.Len())
		}

		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flushBatch()
			return nil

		case <-evictTicker.C:
			n := buf.Evict()
			if n > 0 {
				slog.Info("BYOS buffer eviction", "evicted", n, "remaining", buf.Len())
			}
			if fc != nil && fc.state != nil {
				fc.state.setBufferLen(buf.Len())
			}

		case <-flushTicker.C:
			// Timer-based flush to bound metadata latency.
			flushBatch()

		case ev, ok := <-events:
			if !ok {
				flushBatch()
				return nil
			}

			// Skip non-row events (GTID tracking, DDL).
			if ev.EventType == parser.EventGTID || ev.EventType == parser.EventDDL {
				continue
			}

			batch = append(batch, ev)
			if len(batch) >= batchSize {
				flushBatch()
			}
		}
	}
}

// flushToSinks splits events via byos.SplitEvent and sends metadata to
// dbtrail and payload to S3. Retries each sink up to 3 times with
// exponential backoff. Failures are logged but never block the stream.
func flushToSinks(ctx context.Context, batch []parser.Event, fc *byosFlushConfig) {
	var metaBatch []byos.MetadataRecord
	var payloadBatch []byos.PayloadRecord

	for i := range batch {
		meta, payload, err := byos.SplitEvent(batch[i], fc.serverID)
		if err != nil {
			slog.Warn("BYOS split failed, skipping event",
				"error", err,
				"schema", batch[i].Schema,
				"table", batch[i].Table,
				"event_type", batch[i].EventType)
			continue
		}
		metaBatch = append(metaBatch, meta)
		payloadBatch = append(payloadBatch, payload)
	}

	if len(metaBatch) == 0 {
		return
	}

	metaOK := true
	payloadOK := true

	// Flush metadata to dbtrail API.
	if fc.metaClient != nil {
		if err := retryFlush(ctx, 3, func() error {
			return fc.metaClient.Send(ctx, metaBatch)
		}); err != nil {
			slog.Error("BYOS metadata flush failed after retries",
				"events", len(metaBatch), "error", err)
			metaOK = false
		}
	}

	// Flush payload to customer S3.
	if fc.payloadWriter != nil {
		if err := retryFlush(ctx, 3, func() error {
			return fc.payloadWriter.WriteRecords(ctx, payloadBatch)
		}); err != nil {
			slog.Error("BYOS payload flush failed after retries",
				"events", len(payloadBatch), "error", err)
			payloadOK = false
		}
	}

	if fc.state != nil {
		fc.state.updateFlush(metaOK, payloadOK)
	}
}

// retryFlush retries fn up to maxAttempts times with exponential backoff
// (1s, 2s, 4s). Returns the last error on persistent failure.
func retryFlush(ctx context.Context, maxAttempts int, fn func() error) error {
	var err error
	delay := time.Second
	for attempt := range maxAttempts {
		err = fn()
		if err == nil {
			return nil
		}
		if attempt == maxAttempts-1 {
			break
		}
		slog.Warn("BYOS flush attempt failed, retrying",
			"attempt", attempt+1, "delay", delay.String(), "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
	}
	return err
}

// ─── Schema resolver ────────────────────────────────────────────────────────

// buildResolverFromSource queries information_schema.COLUMNS on the source
// MySQL and builds an in-memory Resolver. This avoids requiring a MySQL index
// database for schema snapshots in BYOS mode.
func buildResolverFromSource(sourceDB *sql.DB, schemas []string) (*metadata.Resolver, error) {
	var q string
	var args []any

	if len(schemas) == 0 {
		q = `SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME,
		            ORDINAL_POSITION, COLUMN_KEY, DATA_TYPE, EXTRA
		     FROM information_schema.COLUMNS
		     WHERE TABLE_SCHEMA NOT IN ('information_schema','performance_schema','mysql','sys')
		     ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION`
	} else {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(schemas)), ",")
		q = fmt.Sprintf(`SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME,
		                         ORDINAL_POSITION, COLUMN_KEY, DATA_TYPE, EXTRA
		                  FROM information_schema.COLUMNS
		                  WHERE TABLE_SCHEMA IN (%s)
		                  ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION`, placeholders)
		for _, s := range schemas {
			args = append(args, s)
		}
	}

	rows, err := sourceDB.Query(q, args...)
	if err != nil {
		return nil, fmt.Errorf("query information_schema.COLUMNS: %w", err)
	}
	defer rows.Close()

	tables := make(map[string]*metadata.TableMeta)
	for rows.Next() {
		var schema, table, column, colKey, dataType, extra string
		var ordinal int
		if err := rows.Scan(&schema, &table, &column, &ordinal, &colKey, &dataType, &extra); err != nil {
			return nil, fmt.Errorf("scan column row: %w", err)
		}

		key := schema + "." + table
		tm, ok := tables[key]
		if !ok {
			tm = &metadata.TableMeta{Schema: schema, Table: table}
			tables[key] = tm
		}

		isGenerated := strings.Contains(extra, "STORED GENERATED") || strings.Contains(extra, "VIRTUAL GENERATED")
		isPK := colKey == "PRI"

		tm.Columns = append(tm.Columns, metadata.ColumnMeta{
			Name:            column,
			OrdinalPosition: ordinal,
			IsPK:            isPK,
			DataType:        dataType,
			IsGenerated:     isGenerated,
		})
		if isPK {
			tm.PKColumns = append(tm.PKColumns, column)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate columns: %w", err)
	}

	if len(tables) == 0 {
		return nil, fmt.Errorf("no tables found; check --schemas and source server permissions")
	}

	// Ensure columns are sorted by ordinal position (they should be from
	// ORDER BY, but be defensive).
	for _, tm := range tables {
		slices.SortFunc(tm.Columns, func(a, b metadata.ColumnMeta) int {
			return cmp.Compare(a.OrdinalPosition, b.OrdinalPosition)
		})
	}

	return metadata.NewResolverFromTables(0, tables), nil
}
