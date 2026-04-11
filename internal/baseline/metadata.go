package baseline

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2" // DuckDB driver for s3:// metadata reads
	"github.com/parquet-go/parquet-go"
)

// Parquet metadata keys for baseline binlog position and schema DDL.
const (
	MetaKeyBinlogFile     = "bintrail.baseline_binlog_file"
	MetaKeyBinlogPos      = "bintrail.baseline_binlog_position"
	MetaKeyGTIDSet        = "bintrail.baseline_gtid_set"
	MetaKeyCreateTableSQL = "bintrail.create_table_sql"
)

// DumpMetadata contains information parsed from a mydumper metadata file or
// from a baseline Parquet file's key-value metadata.
type DumpMetadata struct {
	StartedAt      time.Time
	BinlogFile     string
	BinlogPos      int64
	GTIDSet        string
	CreateTableSQL string // raw mydumper -schema.sql bytes; set for baselines written after #187
}

// ParseMetadata reads the mydumper "metadata" file in inputDir and returns the
// extracted dump timestamp and binlog position information.
//
// The metadata file looks like:
//
//	Started dump at: 2025-02-28 00:00:00
//	SHOW MASTER STATUS:
//	    Log: binlog.000042
//	    Pos: 12345
//	    GTID: 3e11fa47-...:1-100
//	Finished dump at: 2025-02-28 00:01:23
func ParseMetadata(inputDir string) (DumpMetadata, error) {
	path := filepath.Join(inputDir, "metadata")
	f, err := os.Open(path)
	if err != nil {
		return DumpMetadata{}, fmt.Errorf("open metadata file: %w", err)
	}
	defer f.Close()

	var m DumpMetadata
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()

		// New mydumper format (0.16+) prefixes lines with "# ".
		trimmed := strings.TrimPrefix(line, "# ")

		if after, ok := strings.CutPrefix(trimmed, "Started dump at: "); ok {
			t, err := time.ParseInLocation("2006-01-02 15:04:05", strings.TrimSpace(after), time.UTC)
			if err != nil {
				return DumpMetadata{}, fmt.Errorf("parse dump timestamp %q: %w", after, err)
			}
			m.StartedAt = t
		} else if after, ok := strings.CutPrefix(line, "\tLog: "); ok {
			m.BinlogFile = strings.TrimSpace(after)
		} else if after, ok := strings.CutPrefix(line, "\tPos: "); ok {
			pos, err := strconv.ParseInt(strings.TrimSpace(after), 10, 64)
			if err == nil {
				m.BinlogPos = pos
			}
		} else if after, ok := strings.CutPrefix(line, "\tGTID: "); ok {
			m.GTIDSet = strings.TrimSpace(after)
		} else if after, ok := strings.CutPrefix(trimmed, "SOURCE_LOG_FILE = "); ok {
			m.BinlogFile = unquote(strings.TrimSpace(after))
		} else if after, ok := strings.CutPrefix(trimmed, "SOURCE_LOG_POS = "); ok {
			pos, err := strconv.ParseInt(strings.TrimSpace(after), 10, 64)
			if err == nil {
				m.BinlogPos = pos
			}
		} else if after, ok := strings.CutPrefix(trimmed, "executed_gtid_set = "); ok {
			m.GTIDSet = unquote(strings.TrimSpace(after))
		}
	}
	if err := scanner.Err(); err != nil {
		return DumpMetadata{}, fmt.Errorf("read metadata file: %w", err)
	}
	if m.StartedAt.IsZero() {
		return DumpMetadata{}, fmt.Errorf("metadata file missing 'Started dump at:' line")
	}
	return m, nil
}

// ReadParquetMetadata opens a local Parquet file and extracts the baseline
// binlog position from its file-level key-value metadata. Returns a zero-value
// DumpMetadata (no error) when the file lacks position metadata (older baselines).
func ReadParquetMetadata(path string) (DumpMetadata, error) {
	f, err := os.Open(path)
	if err != nil {
		return DumpMetadata{}, fmt.Errorf("open baseline file: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return DumpMetadata{}, fmt.Errorf("stat baseline file: %w", err)
	}

	pf, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		return DumpMetadata{}, fmt.Errorf("open parquet file: %w", err)
	}

	var m DumpMetadata
	if v, ok := pf.Lookup(MetaKeyBinlogFile); ok {
		m.BinlogFile = v
	}
	if v, ok := pf.Lookup(MetaKeyBinlogPos); ok {
		pos, parseErr := strconv.ParseInt(v, 10, 64)
		if parseErr != nil {
			slog.Warn("corrupt baseline_binlog_position in Parquet metadata",
				"path", path, "raw_value", v, "error", parseErr)
		} else {
			m.BinlogPos = pos
		}
	}
	if v, ok := pf.Lookup(MetaKeyGTIDSet); ok {
		m.GTIDSet = v
	}
	if v, ok := pf.Lookup(MetaKeyCreateTableSQL); ok {
		m.CreateTableSQL = v
	}
	return m, nil
}

// ReadParquetMetadataAny reads baseline Parquet metadata from either a local
// path or an s3:// URL. For S3 it uses DuckDB's parquet_kv_metadata() table
// function through the httpfs extension, avoiding a direct AWS SDK dependency.
//
// Used by the full-table reconstruct path (#187) which needs baseline
// metadata (CreateTableSQL in particular) from S3-resident Parquet files.
func ReadParquetMetadataAny(ctx context.Context, path string) (DumpMetadata, error) {
	if !strings.HasPrefix(path, "s3://") {
		return ReadParquetMetadata(path)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return DumpMetadata{}, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, "INSTALL httpfs; LOAD httpfs;"); err != nil {
		return DumpMetadata{}, fmt.Errorf("load httpfs extension: %w", err)
	}

	safePath := strings.ReplaceAll(path, "'", "''")
	q := fmt.Sprintf("SELECT key, value FROM parquet_kv_metadata('%s')", safePath)
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return DumpMetadata{}, fmt.Errorf("query parquet metadata: %w", err)
	}
	defer rows.Close()

	var m DumpMetadata
	for rows.Next() {
		// DuckDB returns key/value as BLOB (BYTE_ARRAY) when the Parquet
		// metadata column stores raw bytes. Scan as []byte to be safe.
		var keyBytes, valBytes []byte
		if err := rows.Scan(&keyBytes, &valBytes); err != nil {
			return DumpMetadata{}, fmt.Errorf("scan metadata row: %w", err)
		}
		key := string(keyBytes)
		val := string(valBytes)
		switch key {
		case MetaKeyBinlogFile:
			m.BinlogFile = val
		case MetaKeyBinlogPos:
			if pos, parseErr := strconv.ParseInt(val, 10, 64); parseErr == nil {
				m.BinlogPos = pos
			} else {
				slog.Warn("corrupt baseline_binlog_position in S3 Parquet metadata",
					"path", path, "raw_value", val, "error", parseErr)
			}
		case MetaKeyGTIDSet:
			m.GTIDSet = val
		case MetaKeyCreateTableSQL:
			m.CreateTableSQL = val
		}
	}
	if err := rows.Err(); err != nil {
		return DumpMetadata{}, fmt.Errorf("iterate metadata rows: %w", err)
	}
	return m, nil
}

// unquote strips surrounding double quotes from s, if present.
func unquote(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}
