package baseline

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// BaselineInfo holds metadata about a discovered baseline Parquet file.
type BaselineInfo struct {
	SnapshotTime time.Time
	Database     string
	Table        string
	BinlogFile   string
	BinlogPos    int64
	GTIDSet      string
	Path         string
}

// DiscoverBaselines walks a baseline directory and returns metadata for each
// Parquet file found. The expected layout is:
//
//	<dir>/<timestamp>/<database>/<table>.parquet
//
// where <timestamp> is an RFC3339 string with colons replaced by hyphens
// (e.g. "2025-02-28T00-00-00Z"). Files that cannot be parsed are skipped.
func DiscoverBaselines(dir string) ([]BaselineInfo, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var results []BaselineInfo
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		ts, ok := parseBaselineDirTimestamp(entry.Name())
		if !ok {
			continue
		}

		snapshotDir := filepath.Join(dir, entry.Name())
		dbEntries, err := os.ReadDir(snapshotDir)
		if err != nil {
			slog.Warn("could not read baseline snapshot directory", "path", snapshotDir, "error", err)
			continue
		}
		for _, dbEntry := range dbEntries {
			if !dbEntry.IsDir() {
				continue
			}
			dbName := dbEntry.Name()
			tableDir := filepath.Join(snapshotDir, dbName)
			tableFiles, err := os.ReadDir(tableDir)
			if err != nil {
				slog.Warn("could not read baseline table directory", "path", tableDir, "error", err)
				continue
			}
			for _, tf := range tableFiles {
				if tf.IsDir() || !strings.HasSuffix(tf.Name(), ".parquet") {
					continue
				}
				tableName := strings.TrimSuffix(tf.Name(), ".parquet")
				filePath := filepath.Join(tableDir, tf.Name())

				info := BaselineInfo{
					SnapshotTime: ts,
					Database:     dbName,
					Table:        tableName,
					Path:         filePath,
				}

				// Best-effort: read binlog position from Parquet metadata.
				if meta, err := ReadParquetMetadata(filePath); err == nil {
					info.BinlogFile = meta.BinlogFile
					info.BinlogPos = meta.BinlogPos
					info.GTIDSet = meta.GTIDSet
				} else {
					slog.Warn("could not read Parquet metadata for baseline", "path", filePath, "error", err)
				}

				results = append(results, info)
			}
		}
	}
	return results, nil
}

// parseBaselineDirTimestamp converts a baseline directory name like
// "2025-02-28T00-00-00Z" to a time.Time. The format is RFC3339 with colons
// in the time portion replaced by hyphens for filesystem compatibility.
func parseBaselineDirTimestamp(name string) (time.Time, bool) {
	idx := strings.IndexByte(name, 'T')
	if idx < 0 {
		return time.Time{}, false
	}
	rfc := name[:idx+1] + strings.ReplaceAll(name[idx+1:], "-", ":")
	t, err := time.Parse(time.RFC3339, rfc)
	if err != nil {
		return time.Time{}, false
	}
	return t.UTC(), true
}
