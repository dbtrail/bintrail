package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/bintrail/bintrail/internal/config"
	"github.com/bintrail/bintrail/internal/indexer"
	"github.com/bintrail/bintrail/internal/metadata"
	"github.com/bintrail/bintrail/internal/parser"
)

var indexCmd = &cobra.Command{
	Use:   "index",
	Short: "Parse binlog files and populate the index",
	Long: `Parses one or more MySQL ROW-format binlog files and writes every row event
into the binlog_events table with full before/after images.

If no schema snapshot exists, one is taken automatically using --source-dsn.
Files already marked 'completed' in index_state are skipped.`,
	RunE: runIndex,
}

var (
	idxIndexDSN  string
	idxSourceDSN string
	idxBinlogDir string
	idxFiles     string
	idxAll       bool
	idxBatchSize int
	idxSchemas   string
	idxTables    string
)

func init() {
	indexCmd.Flags().StringVar(&idxIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	indexCmd.Flags().StringVar(&idxSourceDSN, "source-dsn", "", "DSN for the source MySQL server (required for validation and auto-snapshot)")
	indexCmd.Flags().StringVar(&idxBinlogDir, "binlog-dir", "", "Directory containing binlog files (required)")
	indexCmd.Flags().StringVar(&idxFiles, "files", "", "Comma-separated binlog filenames (e.g. binlog.000042,binlog.000043)")
	indexCmd.Flags().BoolVar(&idxAll, "all", false, "Index all binlog files found in --binlog-dir")
	indexCmd.Flags().IntVar(&idxBatchSize, "batch-size", 1000, "Events per batch INSERT")
	indexCmd.Flags().StringVar(&idxSchemas, "schemas", "", "Only index events from these schemas (comma-separated)")
	indexCmd.Flags().StringVar(&idxTables, "tables", "", "Only index these tables (comma-separated, e.g. mydb.orders,mydb.items)")
	_ = indexCmd.MarkFlagRequired("index-dsn")
	_ = indexCmd.MarkFlagRequired("binlog-dir")

	rootCmd.AddCommand(indexCmd)
}

func runIndex(cmd *cobra.Command, args []string) error {
	if !idxAll && idxFiles == "" {
		return fmt.Errorf("either --files or --all must be specified")
	}

	ctx := cmd.Context()

	// ── 1. Source server: validate binlog_row_image ───────────────────────────
	var sourceDB *sql.DB
	if idxSourceDSN != "" {
		var err error
		sourceDB, err = config.Connect(idxSourceDSN)
		if err != nil {
			return fmt.Errorf("failed to connect to source MySQL: %w", err)
		}
		defer sourceDB.Close()

		if err := validateBinlogRowImage(sourceDB); err != nil {
			return err
		}
		fmt.Println("Source: binlog_row_image=FULL ✓")
	} else {
		log.Println("WARNING: --source-dsn not provided; skipping binlog_row_image validation")
	}

	// ── 2. Index database connection ──────────────────────────────────────────
	indexDB, err := config.Connect(idxIndexDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to index database: %w", err)
	}
	defer indexDB.Close()

	// ── 3. Schema snapshot ────────────────────────────────────────────────────
	resolver, err := ensureResolver(indexDB, sourceDB, parseSchemaList(idxSchemas))
	if err != nil {
		return err
	}
	fmt.Printf("Snapshot: id=%d, tables=%d\n", resolver.SnapshotID(), resolver.TableCount())

	// ── 4. Filters ────────────────────────────────────────────────────────────
	filters := buildIndexFilters(idxSchemas, idxTables)

	// ── 5. File list ──────────────────────────────────────────────────────────
	files, err := resolveFiles(idxBinlogDir, idxFiles, idxAll)
	if err != nil {
		return err
	}
	fmt.Printf("Files to process: %d\n\n", len(files))

	// ── 6. Index each file ────────────────────────────────────────────────────
	p := parser.New(idxBinlogDir, resolver, filters)
	idx := indexer.New(indexDB, idxBatchSize)

	var totalEvents int64
	for _, filename := range files {
		n, err := indexFile(ctx, p, idx, indexDB, idxBinlogDir, filename)
		totalEvents += n
		if err != nil {
			// Log and continue so --all processes remaining files.
			log.Printf("ERROR [%s]: %v", filename, err)
		}
	}

	fmt.Printf("\nTotal events indexed: %d\n", totalEvents)
	return nil
}

// indexFile processes a single binlog file with full index_state tracking.
func indexFile(
	ctx context.Context,
	p *parser.Parser,
	idx *indexer.Indexer,
	indexDB *sql.DB,
	binlogDir, filename string,
) (int64, error) {
	// ── a. Skip already-completed files ──────────────────────────────────────
	status, err := getFileStatus(indexDB, filename)
	if err != nil {
		return 0, fmt.Errorf("failed to query index_state: %w", err)
	}
	if status == "completed" {
		fmt.Printf("[%s] already indexed — skipping\n", filename)
		return 0, nil
	}

	// ── b. Check file exists ──────────────────────────────────────────────────
	info, err := os.Stat(filepath.Join(binlogDir, filename))
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("WARNING: binlog file not found: %s — skipping", filename)
			return 0, nil
		}
		return 0, fmt.Errorf("stat %s: %w", filename, err)
	}
	fileSize := info.Size()

	// ── b. Mark in_progress ───────────────────────────────────────────────────
	if err := upsertFileState(indexDB, filename, "in_progress", fileSize, 0, 0, ""); err != nil {
		return 0, fmt.Errorf("failed to mark in_progress: %w", err)
	}
	fmt.Printf("[%s] indexing...\n", filename)

	// ── c. Run parser + indexer concurrently ──────────────────────────────────
	// Use a child context so we can cancel the parser if the indexer fails,
	// avoiding a goroutine leak and the associated channel deadlock.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	events := make(chan parser.Event, 1000)
	parseErrCh := make(chan error, 1) // buffered: goroutine never blocks on send

	go func() {
		defer close(events)
		parseErrCh <- p.ParseFile(ctx, filename, events)
	}()

	count, idxErr := idx.Run(ctx, events)
	if idxErr != nil {
		cancel() // tell the parser goroutine to stop
	}

	parseErr := <-parseErrCh // wait for parser to finish

	// ── e/f. Update index_state ───────────────────────────────────────────────
	switch {
	case idxErr != nil:
		_ = upsertFileState(indexDB, filename, "failed", fileSize, 0, count, idxErr.Error())
		return count, idxErr

	case parseErr != nil && !errors.Is(parseErr, context.Canceled):
		_ = upsertFileState(indexDB, filename, "failed", fileSize, 0, count, parseErr.Error())
		return count, parseErr

	default:
		if err := upsertFileState(indexDB, filename, "completed", fileSize, fileSize, count, ""); err != nil {
			log.Printf("WARNING: failed to mark %s completed: %v", filename, err)
		}
		fmt.Printf("[%s] done — %d events\n", filename, count)
		return count, nil
	}
}

// ─── index_state helpers ──────────────────────────────────────────────────────

// getFileStatus returns the current status from index_state, or "" if no row exists.
func getFileStatus(db *sql.DB, filename string) (string, error) {
	var status string
	err := db.QueryRow("SELECT status FROM index_state WHERE binlog_file = ?", filename).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return status, err
}

// upsertFileState writes or updates an index_state row using INSERT … ON DUPLICATE KEY UPDATE.
// lastPos is the byte offset of the last processed position (0 = unknown/in-progress).
// eventsIndexed is the count of events written so far.
// errMsg is stored for failed status; pass "" otherwise.
func upsertFileState(db *sql.DB, filename, status string, fileSize, lastPos, eventsIndexed int64, errMsg string) error {
	var errMsgArg any
	if errMsg != "" {
		errMsgArg = errMsg
	}

	var completedAt any
	if status == "completed" || status == "failed" {
		completedAt = "NOW()"
	}
	_ = completedAt // handled inline below

	switch status {
	case "in_progress":
		_, err := db.Exec(`
			INSERT INTO index_state
				(binlog_file, file_size, last_position, events_indexed, status, started_at, completed_at, error_message)
			VALUES (?, ?, ?, ?, 'in_progress', NOW(), NULL, NULL)
			ON DUPLICATE KEY UPDATE
				file_size      = VALUES(file_size),
				last_position  = VALUES(last_position),
				events_indexed = VALUES(events_indexed),
				status         = 'in_progress',
				started_at     = NOW(),
				completed_at   = NULL,
				error_message  = NULL`,
			filename, fileSize, lastPos, eventsIndexed)
		return err

	case "completed":
		_, err := db.Exec(`
			UPDATE index_state
			SET last_position  = ?,
			    events_indexed = ?,
			    status         = 'completed',
			    completed_at   = NOW(),
			    error_message  = NULL
			WHERE binlog_file = ?`,
			lastPos, eventsIndexed, filename)
		return err

	case "failed":
		_, err := db.Exec(`
			UPDATE index_state
			SET last_position  = ?,
			    events_indexed = ?,
			    status         = 'failed',
			    completed_at   = NOW(),
			    error_message  = ?
			WHERE binlog_file = ?`,
			lastPos, eventsIndexed, errMsgArg, filename)
		return err
	}
	return fmt.Errorf("upsertFileState: unknown status %q", status)
}

// ─── Validation ───────────────────────────────────────────────────────────────

// validateBinlogRowImage checks that the source server has binlog_row_image=FULL.
func validateBinlogRowImage(db *sql.DB) error {
	var varName, val string
	err := db.QueryRow("SHOW VARIABLES LIKE 'binlog_row_image'").Scan(&varName, &val)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("binlog_row_image not found on source server; MySQL 5.6+ with binlog_row_image=FULL is required")
	}
	if err != nil {
		return fmt.Errorf("failed to query binlog_row_image: %w", err)
	}
	if !strings.EqualFold(val, "FULL") {
		return fmt.Errorf("source server has binlog_row_image=%q; bintrail requires FULL", val)
	}
	return nil
}

// ─── Snapshot bootstrap ───────────────────────────────────────────────────────

// ensureResolver returns a Resolver loaded from the latest snapshot, taking a
// new snapshot automatically if none exists (requires sourceDB != nil).
func ensureResolver(indexDB, sourceDB *sql.DB, schemas []string) (*metadata.Resolver, error) {
	var snapshotID int
	if err := indexDB.QueryRow(
		"SELECT COALESCE(MAX(snapshot_id), 0) FROM schema_snapshots",
	).Scan(&snapshotID); err != nil {
		return nil, fmt.Errorf("failed to query schema snapshots: %w", err)
	}

	if snapshotID == 0 {
		if sourceDB == nil {
			return nil, fmt.Errorf(
				"no schema snapshot exists and --source-dsn was not provided; " +
					"run `bintrail snapshot` first or add --source-dsn for auto-snapshot")
		}
		fmt.Println("No snapshot found; taking schema snapshot automatically...")
		stats, err := metadata.TakeSnapshot(sourceDB, indexDB, schemas)
		if err != nil {
			return nil, fmt.Errorf("auto-snapshot failed: %w", err)
		}
		fmt.Printf("  snapshot_id=%d, tables=%d, columns=%d\n",
			stats.SnapshotID, stats.TableCount, stats.ColumnCount)
		snapshotID = stats.SnapshotID
	}

	return metadata.NewResolver(indexDB, snapshotID)
}

// ─── Filter builder ───────────────────────────────────────────────────────────

func buildIndexFilters(schemas, tables string) parser.Filters {
	var f parser.Filters
	if schemas != "" {
		f.Schemas = make(map[string]bool)
		for s := range strings.SplitSeq(schemas, ",") {
			if s = strings.TrimSpace(s); s != "" {
				f.Schemas[s] = true
			}
		}
	}
	if tables != "" {
		f.Tables = make(map[string]bool)
		for t := range strings.SplitSeq(tables, ",") {
			if t = strings.TrimSpace(t); t != "" {
				f.Tables[t] = true
			}
		}
	}
	return f
}

// ─── File discovery ───────────────────────────────────────────────────────────

// binlogFileRe matches standard MySQL binlog filenames: any name ending in six
// or more decimal digits after a dot (e.g. binlog.000042, mysql-bin.000001).
var binlogFileRe = regexp.MustCompile(`\.\d{6,}$`)

// resolveFiles returns the list of binlog filenames to process.
func resolveFiles(binlogDir, filesStr string, all bool) ([]string, error) {
	if all {
		return findBinlogFiles(binlogDir)
	}
	var files []string
	for f := range strings.SplitSeq(filesStr, ",") {
		if f = strings.TrimSpace(f); f != "" {
			files = append(files, f)
		}
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("--files is empty; provide at least one filename")
	}
	return files, nil
}

// findBinlogFiles scans binlogDir and returns filenames that match the standard
// MySQL binlog naming pattern, sorted in ascending order.
func findBinlogFiles(binlogDir string) ([]string, error) {
	entries, err := os.ReadDir(binlogDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read binlog directory %q: %w", binlogDir, err)
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && binlogFileRe.MatchString(e.Name()) {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files) // ascending order = chronological for standard naming
	if len(files) == 0 {
		return nil, fmt.Errorf("no binlog files found in %q", binlogDir)
	}
	return files, nil
}
