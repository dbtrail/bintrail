package main

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/bintrail/internal/agent"
	"github.com/dbtrail/bintrail/internal/config"
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

Examples:
  # Start agent with index database
  bintrail agent --api-key "ak_..." --endpoint "wss://api.dbtrail.io/v1/agent" \
    --index-dsn "user:pass@tcp(host:3306)/binlog_index"

  # Start agent with Parquet archives on S3
  bintrail agent --api-key "ak_..." --endpoint "wss://api.dbtrail.io/v1/agent" \
    --archive-s3 "s3://my-bucket/archives/"

  # Start agent with all data sources
  bintrail agent --api-key "ak_..." --endpoint "wss://api.dbtrail.io/v1/agent" \
    --index-dsn "..." --source-dsn "..." --archive-dir "/data/archives"`,
	RunE: runAgent,
}

var (
	agtAPIKey     string
	agtEndpoint   string
	agtIndexDSN   string
	agtSourceDSN  string
	agtArchiveDir string
	agtArchiveS3  string
)

func init() {
	agentCmd.Flags().StringVar(&agtAPIKey, "api-key", "", "API key for dbtrail authentication (required)")
	agentCmd.Flags().StringVar(&agtEndpoint, "endpoint", "", "dbtrail WebSocket endpoint URL (required)")
	agentCmd.Flags().StringVar(&agtIndexDSN, "index-dsn", "", "DSN for the index MySQL database")
	agentCmd.Flags().StringVar(&agtSourceDSN, "source-dsn", "", "DSN for the source MySQL database (enables forensics queries)")
	agentCmd.Flags().StringVar(&agtArchiveDir, "archive-dir", "", "Local directory containing Parquet archives")
	agentCmd.Flags().StringVar(&agtArchiveS3, "archive-s3", "", "S3 path to Parquet archives (e.g. s3://bucket/prefix/)")
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

	// At least one data source must be configured.
	if agtIndexDSN == "" && len(archiveSources) == 0 {
		return fmt.Errorf("at least one data source required: --index-dsn, --archive-dir, or --archive-s3")
	}

	handler := &agent.DefaultHandler{
		ArchiveSources: archiveSources,
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

	// Connect to source database if provided (for forensics queries).
	if agtSourceDSN != "" {
		db, err := config.Connect(agtSourceDSN)
		if err != nil {
			return fmt.Errorf("connect to source database: %w", err)
		}
		defer db.Close()
		handler.SourceDB = db
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
		"archives", len(archiveSources))

	err := ch.Run(cmd.Context())

	slog.Info("agent stopped",
		"duration", time.Since(start).Truncate(time.Second).String(),
		"error", err)

	return err
}
