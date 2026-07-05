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

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
)

// Parquet metadata keys for baseline binlog position and schema DDL.
const (
	MetaKeyBinlogFile     = "bintrail.baseline_binlog_file"
	MetaKeyBinlogPos      = "bintrail.baseline_binlog_position"
	MetaKeyGTIDSet        = "bintrail.baseline_gtid_set"
	MetaKeyCreateTableSQL = "bintrail.create_table_sql"
	// MetaKeyLSN anchors a PostgreSQL-source baseline: the WAL LSN at which the
	// snapshot is consistent (the slot's consistent_point), stored as the decimal
	// string of the uint64 LSN. Deltas for the table start strictly after this
	// point. Absent on MySQL/MariaDB baselines (which use the binlog file/pos/GTID
	// keys above) and on PG baselines taken before #593 slice A. Part of #593.
	MetaKeyLSN = "bintrail.baseline_lsn"
	// MetaKeyRowCount and MetaKeyContentDigest record, per table, how many rows
	// the baseline ingested and an order-independent content fingerprint of them
	// (consistency.Hasher, version-tagged). The digest is byte-identical to a
	// live ConsistentTableChecksum of the same rows, so the verify capstone
	// (#634) can compare a baseline against the source. Part of epic #631 (#633).
	//
	// The digest certifies SOURCE fidelity (the dump captured the same rows as
	// the source), not Parquet-encoding fidelity: writer transforms such as
	// zero-date→NULL are invisible to it (that is #634's concern). TIMESTAMP
	// agreement assumes the dump used UTC (mydumper's default --tz-utc, which
	// matches ConsistentTableChecksum's UTC session); an externally produced
	// dump made with --skip-tz-utc on a non-UTC server would not match.
	//
	// Readers must treat RowCount as valid only when ContentDigest != ""; the
	// readers clear ContentDigest if RowCount cannot be parsed, so the two are
	// never returned in a trustworthy-digest / untrustworthy-count combination.
	MetaKeyRowCount      = "bintrail.baseline_row_count"
	MetaKeyContentDigest = "bintrail.baseline_content_digest"
)

// DumpMetadata contains information parsed from a mydumper metadata file or
// from a baseline Parquet file's key-value metadata.
type DumpMetadata struct {
	StartedAt      time.Time
	BinlogFile     string
	BinlogPos      int64
	GTIDSet        string
	CreateTableSQL string // raw mydumper -schema.sql bytes; set for baselines written after #187
	ContentDigest  string // version-tagged content fingerprint; set after #633, empty when absent (old baselines)
	RowCount       int64  // rows ingested into this table's baseline; valid only when ContentDigest != ""
	LSN            uint64 // PostgreSQL WAL LSN anchor (MetaKeyLSN); 0 = absent (MySQL baseline, or pre-#593 PG baseline)
}

// StartedAtMarkerFile is a bintrail-authored sidecar written into the mydumper
// output directory by `bintrail dump`, recording that process's own UTC
// wall-clock time captured immediately before invoking mydumper.
//
// mydumper's own "Started dump at" metadata line is written in the dump
// host's LOCAL time, but ParseMetadata parses it with ParseInLocation(...,
// time.UTC) — i.e. verbatim, as if it already were UTC. On a dump host whose
// clock isn't set to UTC, every reconstruct/verify/shim consumer that anchors
// replay at this timestamp skews by the host's UTC offset (#768). When this
// marker is present, ParseMetadata prefers it over the ambiguous mydumper
// line, sidestepping the timezone question entirely. It is absent for
// mydumper dumps produced outside `bintrail dump` (manual mydumper run, a
// different tool) — see docs/dump-and-baseline.md for that case.
const StartedAtMarkerFile = "bintrail_dump_started_at_utc"

// WriteStartedAtMarker records t (converted to UTC) into inputDir as the
// authoritative dump-start time. Called by `bintrail dump` right before
// invoking mydumper; best-effort on the caller's side — a write failure just
// means ParseMetadata falls back to mydumper's own local-time line.
func WriteStartedAtMarker(inputDir string, t time.Time) error {
	path := filepath.Join(inputDir, StartedAtMarkerFile)
	if err := os.WriteFile(path, []byte(t.UTC().Format(time.RFC3339Nano)+"\n"), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", StartedAtMarkerFile, err)
	}
	return nil
}

// readStartedAtMarker reads StartedAtMarkerFile from inputDir, if present and
// parseable. Returns ok=false (never an error) when the marker is absent or
// corrupt — callers fall back to mydumper's own metadata timestamp.
func readStartedAtMarker(inputDir string) (t time.Time, ok bool) {
	path := filepath.Join(inputDir, StartedAtMarkerFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return time.Time{}, false
	}
	t, err = time.Parse(time.RFC3339Nano, strings.TrimSpace(string(data)))
	if err != nil {
		slog.Warn("dump-start marker present but unparseable; falling back to mydumper's 'Started dump at' line",
			"path", path, "error", err)
		return time.Time{}, false
	}
	return t.UTC(), true
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
//
// If inputDir also carries StartedAtMarkerFile (written by `bintrail dump`),
// its process-captured UTC time is preferred over the "Started dump at" line
// above, which is ambiguous with respect to the dump host's timezone (#768).
func ParseMetadata(inputDir string) (DumpMetadata, error) {
	path := filepath.Join(inputDir, "metadata")
	f, err := os.Open(path)
	if err != nil {
		return DumpMetadata{}, fmt.Errorf("open metadata file: %w", err)
	}
	defer f.Close()

	var m DumpMetadata
	markerStartedAt, haveMarker := readStartedAtMarker(inputDir)
	if haveMarker {
		m.StartedAt = markerStartedAt
	}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()

		// New mydumper format (0.16+) prefixes lines with "# ".
		trimmed := strings.TrimPrefix(line, "# ")

		if after, ok := strings.CutPrefix(trimmed, "Started dump at: "); ok {
			if !haveMarker {
				t, err := time.ParseInLocation("2006-01-02 15:04:05", strings.TrimSpace(after), time.UTC)
				if err != nil {
					return DumpMetadata{}, fmt.Errorf("parse dump timestamp %q: %w", after, err)
				}
				m.StartedAt = t
			}
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
	if v, ok := pf.Lookup(MetaKeyLSN); ok {
		lsn, parseErr := strconv.ParseUint(v, 10, 64)
		if parseErr != nil {
			slog.Warn("corrupt baseline_lsn in Parquet metadata",
				"path", path, "raw_value", v, "error", parseErr)
		} else {
			m.LSN = lsn
		}
	}
	if v, ok := pf.Lookup(MetaKeyGTIDSet); ok {
		m.GTIDSet = v
	}
	if v, ok := pf.Lookup(MetaKeyCreateTableSQL); ok {
		m.CreateTableSQL = v
	}
	if v, ok := pf.Lookup(MetaKeyContentDigest); ok {
		m.ContentDigest = v
	}
	if v, ok := pf.Lookup(MetaKeyRowCount); ok {
		n, parseErr := strconv.ParseInt(v, 10, 64)
		if parseErr != nil {
			slog.Warn("corrupt baseline_row_count in Parquet metadata",
				"path", path, "raw_value", v, "error", parseErr)
			// A digest we can't pair with a trustworthy count is not usable:
			// clearing it keeps the "ContentDigest != \"\" ⇒ RowCount valid"
			// contract from ever being observed false (a 0 would otherwise read
			// as a verified-empty table).
			m.ContentDigest = ""
		} else {
			m.RowCount = n
		}
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

	if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
		return DumpMetadata{}, fmt.Errorf("load httpfs extension: %w", err)
	}
	duckdbutil.EnableS3CredentialChain(ctx, db)

	safePath := strings.ReplaceAll(path, "'", "''")
	q := fmt.Sprintf("SELECT key, value FROM parquet_kv_metadata('%s')", safePath)
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return DumpMetadata{}, fmt.Errorf("query parquet metadata: %w", err)
	}
	defer rows.Close()

	var m DumpMetadata
	var rowCountCorrupt bool
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
		case MetaKeyLSN:
			if lsn, parseErr := strconv.ParseUint(val, 10, 64); parseErr == nil {
				m.LSN = lsn
			} else {
				slog.Warn("corrupt baseline_lsn in S3 Parquet metadata",
					"path", path, "raw_value", val, "error", parseErr)
			}
		case MetaKeyGTIDSet:
			m.GTIDSet = val
		case MetaKeyCreateTableSQL:
			m.CreateTableSQL = val
		case MetaKeyContentDigest:
			m.ContentDigest = val
		case MetaKeyRowCount:
			if n, parseErr := strconv.ParseInt(val, 10, 64); parseErr == nil {
				m.RowCount = n
			} else {
				slog.Warn("corrupt baseline_row_count in S3 Parquet metadata",
					"path", path, "raw_value", val, "error", parseErr)
				rowCountCorrupt = true
			}
		}
	}
	if err := rows.Err(); err != nil {
		return DumpMetadata{}, fmt.Errorf("iterate metadata rows: %w", err)
	}
	// Applied after the loop: keys arrive as rows in arbitrary order, so the
	// digest may be set after the count row. A digest we can't pair with a
	// trustworthy count is not usable (see ReadParquetMetadata).
	if rowCountCorrupt {
		m.ContentDigest = ""
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
