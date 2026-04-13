package baseline

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// TableFiles groups all mydumper files for a single table.
type TableFiles struct {
	Database   string
	Table      string
	SchemaFile string   // <db>.<table>-schema.sql
	DataFiles  []string // absolute paths, sorted
	Format     string   // "sql" or "tab"
}

// DiscoverTables scans the mydumper output directory and groups files by table.
// It returns one TableFiles entry per table that has a schema file. Tables with
// no data files (empty at dump time) are included with an empty DataFiles slice
// so that downstream consumers can produce 0-row baselines. Views are skipped.
func DiscoverTables(inputDir string) ([]TableFiles, error) {
	entries, err := os.ReadDir(inputDir)
	if err != nil {
		return nil, fmt.Errorf("read input directory: %w", err)
	}

	type tableKey struct{ db, table string }
	schemas := make(map[tableKey]string) // key → schema file path
	data := make(map[tableKey][]string)  // key → data file paths
	formats := make(map[tableKey]string) // key → "sql" or "tab"

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		path := filepath.Join(inputDir, name)

		// Schema file: <db>.<table>-schema.sql
		if strings.HasSuffix(name, "-schema.sql") {
			base := strings.TrimSuffix(name, "-schema.sql")
			db, table, ok := splitDBTable(base)
			if !ok {
				continue
			}
			k := tableKey{db, table}
			schemas[k] = path
			continue
		}

		// Data file: <db>.<table>.<chunk>.sql, <db>.<table>.<chunk>.dat,
		// or <db>.<table>.sql / <db>.<table>.dat (mydumper 0.10.0 — no
		// chunk number; the table has a single data file). Both shapes
		// must be recognized so bintrail baseline works with the
		// apt-installed mydumper on Ubuntu 24.04 (#221).
		var ext string
		switch {
		case strings.HasSuffix(name, ".sql"):
			ext = "sql"
			name = strings.TrimSuffix(name, ".sql")
		case strings.HasSuffix(name, ".dat"):
			ext = "tab"
			name = strings.TrimSuffix(name, ".dat")
		default:
			continue
		}

		// Try the chunked format first: <db>.<table>.<chunk>
		// If the last dot-separated segment is numeric, split it off.
		// Otherwise fall through to the unchunked format: <db>.<table>.
		var db, table string
		var ok bool
		lastDot := strings.LastIndex(name, ".")
		if lastDot >= 0 {
			chunk := name[lastDot+1:]
			if isNumericChunk(chunk) {
				db, table, ok = splitDBTable(name[:lastDot])
			}
		}
		if !ok {
			// Unchunked format (mydumper 0.10.0): name is just <db>.<table>.
			db, table, ok = splitDBTable(name)
			if !ok {
				continue
			}
		}

		k := tableKey{db, table}
		data[k] = append(data[k], path)
		if formats[k] == "" {
			formats[k] = ext
		}
	}

	var result []TableFiles
	for k, schemaPath := range schemas {
		files, ok := data[k]
		if !ok {
			if isView(schemaPath) {
				continue // genuine view — no data to convert
			}
			// Empty table: schema exists but mydumper produced no data file
			// because the table had zero rows at dump time. Emit a 0-row
			// Parquet so that reconstruct can find a baseline for every table.
			result = append(result, TableFiles{
				Database:   k.db,
				Table:      k.table,
				SchemaFile: schemaPath,
				Format:     "sql",
			})
			continue
		}
		sort.Strings(files)
		result = append(result, TableFiles{
			Database:   k.db,
			Table:      k.table,
			SchemaFile: schemaPath,
			DataFiles:  files,
			Format:     formats[k],
		})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Database != result[j].Database {
			return result[i].Database < result[j].Database
		}
		return result[i].Table < result[j].Table
	})
	return result, nil
}

// splitDBTable splits a "<db>.<table>" string. Returns false if it doesn't
// contain exactly one dot separator.
func splitDBTable(s string) (db, table string, ok bool) {
	dot := strings.Index(s, ".")
	if dot < 0 || dot == len(s)-1 {
		return "", "", false
	}
	return s[:dot], s[dot+1:], true
}

// isView reads a mydumper schema SQL file and returns true if it contains a
// CREATE VIEW statement rather than CREATE TABLE. Views have no data to
// convert, while empty tables should still produce a 0-row Parquet file.
// Returns false (assume table) on errors — a harmless 0-row Parquet is
// better than silently dropping a real table.
func isView(schemaPath string) bool {
	f, err := os.Open(schemaPath)
	if err != nil {
		return false // can't read → assume table (safe: 0-row Parquet is harmless)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.ToUpper(strings.TrimSpace(scanner.Text()))
		if strings.HasPrefix(line, "CREATE") {
			// mydumper emits either "CREATE TABLE" or
			// "CREATE [ALGORITHM=...] [DEFINER=...] [SQL SECURITY ...] VIEW".
			return strings.Contains(line, " VIEW ")
		}
	}
	if err := scanner.Err(); err != nil {
		slog.Debug("I/O error reading schema file for view detection; assuming table",
			"path", schemaPath, "error", err)
	}
	// No CREATE statement found or I/O error — assume table rather than
	// silently skipping, which is the exact failure mode of issue #226.
	return false
}

// isNumericChunk returns true if s consists only of decimal digits (e.g. "00000").
func isNumericChunk(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
