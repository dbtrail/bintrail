package main

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/spf13/cobra"

	"github.com/bintrail/bintrail/internal/config"
	"github.com/bintrail/bintrail/internal/metadata"
	"github.com/bintrail/bintrail/internal/serverid"
)

var snapshotCmd = &cobra.Command{
	Use:   "snapshot",
	Short: "Take a schema snapshot from the source MySQL server",
	Long: `Reads column metadata from information_schema on the source server and stores
it in the index database. The snapshot is used by the index command to map
binlog column ordinals to column names and to compute pk_values.

Must be run before 'bintrail index', or index will run it automatically if no
snapshot exists.`,
	RunE: runSnapshot,
}

var (
	snapshotSourceDSN string
	snapshotIndexDSN  string
	snapshotSchemas   string
)

func init() {
	snapshotCmd.Flags().StringVar(&snapshotSourceDSN, "source-dsn", "", "DSN for the source MySQL server (required)")
	snapshotCmd.Flags().StringVar(&snapshotIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	snapshotCmd.Flags().StringVar(&snapshotSchemas, "schemas", "", "Comma-separated schemas to snapshot (default: all user schemas)")
	_ = snapshotCmd.MarkFlagRequired("source-dsn")
	_ = snapshotCmd.MarkFlagRequired("index-dsn")

	rootCmd.AddCommand(snapshotCmd)
}

func runSnapshot(cmd *cobra.Command, args []string) error {
	schemas := parseSchemaList(snapshotSchemas)

	sourceDB, err := config.Connect(snapshotSourceDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to source MySQL: %w", err)
	}
	defer sourceDB.Close()

	indexDB, err := config.Connect(snapshotIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer indexDB.Close()

	// Resolve server identity before doing any index work.
	bintrailID, err := resolveServerIdentity(cmd.Context(), sourceDB, indexDB, snapshotSourceDSN)
	if err != nil {
		if errors.Is(err, serverid.ErrConflict) {
			return fmt.Errorf("cannot snapshot: %w", err)
		}
		slog.Warn("server identity resolution failed; proceeding without bintrail_id", "error", err)
	} else {
		slog.Info("server identity resolved", "bintrail_id", bintrailID)
	}

	if len(schemas) > 0 {
		fmt.Printf("Snapshotting schemas: %s\n", strings.Join(schemas, ", "))
	} else {
		fmt.Println("Snapshotting all user schemas...")
	}

	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, schemas)
	if err != nil {
		return err
	}

	fmt.Printf("Snapshot complete.\n")
	fmt.Printf("  snapshot_id : %d\n", stats.SnapshotID)
	fmt.Printf("  tables      : %d\n", stats.TableCount)
	fmt.Printf("  columns     : %d\n", stats.ColumnCount)
	return nil
}

// parseSchemaList splits a comma-separated schema string into a trimmed slice,
// dropping empty entries. Returns nil if the input is empty.
func parseSchemaList(s string) []string {
	if s == "" {
		return nil
	}
	var result []string
	for part := range strings.SplitSeq(s, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}
