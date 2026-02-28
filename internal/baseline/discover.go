package baseline

import (
	"fmt"
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
// It returns one TableFiles entry per table that has both a schema file and at
// least one data file.
func DiscoverTables(inputDir string) ([]TableFiles, error) {
	entries, err := os.ReadDir(inputDir)
	if err != nil {
		return nil, fmt.Errorf("read input directory: %w", err)
	}

	type tableKey struct{ db, table string }
	schemas := make(map[tableKey]string)       // key → schema file path
	data := make(map[tableKey][]string)        // key → data file paths
	formats := make(map[tableKey]string)       // key → "sql" or "tab"

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

		// Data file: <db>.<table>.<chunk>.sql or <db>.<table>.<chunk>.dat
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

		// The remaining name should be <db>.<table>.<chunk>
		lastDot := strings.LastIndex(name, ".")
		if lastDot < 0 {
			continue
		}
		chunk := name[lastDot+1:]
		if !isNumericChunk(chunk) {
			continue
		}
		dbTable := name[:lastDot]
		db, table, ok := splitDBTable(dbTable)
		if !ok {
			continue
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
			continue // schema-only table (e.g. view)
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
