// Package baseline converts mydumper output into Parquet files, enabling full
// audit reconstruction when combined with binlog change events.
package baseline

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Version is embedded in Parquet file metadata.
const Version = "0.1.0"

// Config holds all parameters for a baseline conversion run.
type Config struct {
	InputDir     string
	OutputDir    string
	Timestamp    time.Time // zero = read from mydumper metadata
	Tables       []string  // "db.table" filter; nil = all
	Compression  string    // "zstd", "snappy", "gzip", "none"
	RowGroupSize int       // rows per row group
	Retry        bool      // skip tables whose output Parquet file already exists
}

// Stats describes the outcome of a baseline run.
type Stats struct {
	TablesProcessed int
	RowsWritten     int64
	FilesWritten    int
}

// Run converts a mydumper output directory into Parquet files.
func Run(ctx context.Context, cfg Config) (Stats, error) {
	// Resolve timestamp and binlog position from mydumper metadata.
	var meta DumpMetadata
	ts := cfg.Timestamp
	if ts.IsZero() {
		var err error
		meta, err = ParseMetadata(cfg.InputDir)
		if err != nil {
			return Stats{}, fmt.Errorf("parse mydumper metadata: %w", err)
		}
		ts = meta.StartedAt
	} else {
		// Best-effort: try to get binlog position even with timestamp override.
		var metaErr error
		meta, metaErr = ParseMetadata(cfg.InputDir)
		if metaErr != nil {
			slog.Info("could not read mydumper metadata for binlog position — Parquet files will lack baseline position",
				"input_dir", cfg.InputDir, "error", metaErr)
		}
	}

	// Discover tables.
	tables, err := DiscoverTables(cfg.InputDir)
	if err != nil {
		return Stats{}, fmt.Errorf("discover tables: %w", err)
	}

	// Apply table filter.
	if len(cfg.Tables) > 0 {
		tables = filterTables(tables, cfg.Tables)
	}

	if len(tables) == 0 {
		return Stats{}, nil
	}

	// Timestamp string for directory name and metadata (colons → dashes for
	// filesystem compatibility).
	tsStr := ts.UTC().Format(time.RFC3339)
	tsDir := strings.ReplaceAll(tsStr, ":", "-")

	rowGroupSize := cfg.RowGroupSize
	if rowGroupSize <= 0 {
		rowGroupSize = 500_000
	}
	compression := cfg.Compression
	if compression == "" {
		compression = "zstd"
	}

	// Process tables in parallel with bounded concurrency.
	concurrency := runtime.NumCPU()
	if concurrency < 1 {
		concurrency = 1
	}
	sem := make(chan struct{}, concurrency)

	var (
		mu    sync.Mutex
		stats Stats
		errs  []error
	)

	var wg sync.WaitGroup
	for _, tf := range tables {
		tf := tf
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			if ctx.Err() != nil {
				return
			}

			outPath := filepath.Join(cfg.OutputDir, tsDir, tf.Database, tf.Table+".parquet")

			if cfg.Retry {
				if fi, err := os.Stat(outPath); err == nil && fi.Size() > 0 {
					slog.Info("skipping existing file (--retry)",
						"db", tf.Database, "table", tf.Table, "file", outPath)
					mu.Lock()
					stats.TablesProcessed++
					stats.FilesWritten++
					mu.Unlock()
					return
				}
			}

			md := map[string]string{
				"bintrail.snapshot_timestamp": tsStr,
				"bintrail.source_database":    tf.Database,
				"bintrail.source_table":       tf.Table,
				"bintrail.mydumper_format":    tf.Format,
				"bintrail.bintrail_version":   Version,
			}
			if meta.BinlogFile != "" {
				md[MetaKeyBinlogFile] = meta.BinlogFile
			}
			if meta.BinlogPos != 0 {
				md[MetaKeyBinlogPos] = strconv.FormatInt(meta.BinlogPos, 10)
			}
			if meta.GTIDSet != "" {
				md[MetaKeyGTIDSet] = meta.GTIDSet
			}
			// Embed the raw mydumper <db>.<table>-schema.sql bytes so that
			// full-table reconstruct (#187) can emit a faithful schema file
			// without re-synthesising from Parquet column types. Non-fatal:
			// an older baseline or a schema-file read error just leaves the
			// key absent and full-table reconstruct will abort with a clear
			// "re-run bintrail baseline" message.
			if rawSchema, schemaErr := os.ReadFile(tf.SchemaFile); schemaErr != nil {
				slog.Warn("could not read schema file for CREATE TABLE embed",
					"db", tf.Database, "table", tf.Table,
					"path", tf.SchemaFile, "error", schemaErr)
			} else {
				md[MetaKeyCreateTableSQL] = string(rawSchema)
			}
			writerCfg := WriterConfig{
				Compression:  compression,
				RowGroupSize: rowGroupSize,
				Metadata:     md,
			}

			n, err := processTable(ctx, tf, outPath, writerCfg)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				slog.Error("failed to process table",
					"db", tf.Database, "table", tf.Table, "error", err)
				errs = append(errs, fmt.Errorf("%s.%s: %w", tf.Database, tf.Table, err))
				return
			}
			stats.TablesProcessed++
			stats.RowsWritten += n
			stats.FilesWritten++
			slog.Info("table complete",
				"db", tf.Database, "table", tf.Table,
				"rows", n, "file", outPath)
		}()
	}
	wg.Wait()

	if len(errs) > 0 {
		return stats, errs[0] // return first error; others are logged
	}
	return stats, nil
}

// processTable converts a single table's mydumper files to Parquet.
// Returns the number of rows written.
func processTable(ctx context.Context, tf TableFiles, outPath string, cfg WriterConfig) (int64, error) {
	// Parse schema.
	cols, err := ParseSchema(tf.SchemaFile)
	if err != nil {
		return 0, fmt.Errorf("parse schema: %w", err)
	}

	// Create writer.
	w, err := NewWriter(outPath, cols, cfg)
	if err != nil {
		return 0, fmt.Errorf("create writer: %w", err)
	}
	// Close file on error — on success we close below and return any error.
	var closed bool
	defer func() {
		if !closed {
			w.Close() //nolint
			if err := os.Remove(outPath); err != nil && !os.IsNotExist(err) {
				slog.Warn("failed to remove partial file", "path", outPath, "error", err)
			}
		}
	}()

	var rowCount int64
	rowFn := func(values []string, nulls []bool) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := w.WriteRow(values, nulls); err != nil {
			return err
		}
		rowCount++
		return nil
	}

	for _, dataFile := range tf.DataFiles {
		if ctx.Err() != nil {
			return rowCount, ctx.Err()
		}
		switch tf.Format {
		case "tab":
			if err := ReadTabFile(dataFile, len(cols), rowFn); err != nil {
				return rowCount, fmt.Errorf("read tab file %s: %w", dataFile, err)
			}
		case "sql":
			if err := ReadSQLFile(dataFile, rowFn); err != nil {
				return rowCount, fmt.Errorf("read sql file %s: %w", dataFile, err)
			}
		default:
			return rowCount, fmt.Errorf("unknown format %q", tf.Format)
		}
	}

	closed = true
	if err := w.Close(); err != nil {
		os.Remove(outPath)
		return rowCount, fmt.Errorf("close writer: %w", err)
	}
	return rowCount, nil
}

// filterTables returns only tables that match the "db.table" filter list.
func filterTables(tables []TableFiles, filter []string) []TableFiles {
	set := make(map[string]bool, len(filter))
	for _, f := range filter {
		set[strings.ToLower(f)] = true
	}
	var result []TableFiles
	for _, tf := range tables {
		key := strings.ToLower(tf.Database + "." + tf.Table)
		if set[key] {
			result = append(result, tf)
		}
	}
	return result
}
