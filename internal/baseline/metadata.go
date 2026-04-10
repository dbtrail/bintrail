package baseline

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
)

// Parquet metadata keys for baseline binlog position.
const (
	MetaKeyBinlogFile = "bintrail.baseline_binlog_file"
	MetaKeyBinlogPos  = "bintrail.baseline_binlog_position"
	MetaKeyGTIDSet    = "bintrail.baseline_gtid_set"
)

// DumpMetadata contains information parsed from a mydumper metadata file.
type DumpMetadata struct {
	StartedAt  time.Time
	BinlogFile string
	BinlogPos  int64
	GTIDSet    string
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
		m.BinlogPos, _ = strconv.ParseInt(v, 10, 64)
	}
	if v, ok := pf.Lookup(MetaKeyGTIDSet); ok {
		m.GTIDSet = v
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
