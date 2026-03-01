package reconstruct

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

// FindBaseline finds the most recent baseline Parquet file at or before `at`
// for the given schema and table, returning its path and snapshot timestamp.
//
// source may be:
//   - A local directory path (parent of RFC3339-named snapshot subdirectories)
//   - An S3 URL prefix (e.g. "s3://bucket/baselines")
func FindBaseline(ctx context.Context, source, schema, table string, at time.Time) (path string, snapshotTime time.Time, err error) {
	if strings.HasPrefix(source, "s3://") {
		return findBaselineS3(ctx, source, schema, table, at)
	}
	return findBaselineLocal(source, schema, table, at)
}

// ReadBaselineRow opens the Parquet file at path using DuckDB and returns the
// first row matching pkFilter (column name → value string). Returns nil when
// no row matches. Loads the httpfs extension automatically for s3:// paths.
func ReadBaselineRow(ctx context.Context, path string, pkFilter map[string]string) (map[string]any, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	if strings.HasPrefix(path, "s3://") {
		if _, err := db.ExecContext(ctx, "INSTALL httpfs; LOAD httpfs;"); err != nil {
			return nil, fmt.Errorf("load httpfs extension: %w", err)
		}
	}

	// Build sorted conditions for deterministic SQL + arg ordering.
	conds := buildCondsList(pkFilter)
	safePath := strings.ReplaceAll(path, "'", "''")
	q := "SELECT * FROM read_parquet('" + safePath + "')"
	if len(conds) > 0 {
		parts := make([]string, len(conds))
		for i, c := range conds {
			parts[i] = quoteIdent(c.col) + " = ?"
		}
		q += " WHERE " + strings.Join(parts, " AND ")
	}
	q += " LIMIT 1"

	args := make([]any, len(conds))
	for i, c := range conds {
		args[i] = c.value
	}

	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("baseline query: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("baseline columns: %w", err)
	}
	if !rows.Next() {
		return nil, nil // no matching row
	}

	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, fmt.Errorf("scan baseline row: %w", err)
	}
	row := make(map[string]any, len(cols))
	for i, col := range cols {
		row[col] = vals[i]
	}
	return row, rows.Err()
}

// ExecSQL runs arbitrary SQL against an in-memory DuckDB instance and returns
// result rows and column names. The httpfs extension is loaded automatically
// when source or sqlStr references an s3:// URL.
func ExecSQL(ctx context.Context, source, sqlStr string) ([]map[string]any, []string, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	if strings.Contains(source, "s3://") || strings.Contains(sqlStr, "s3://") {
		if _, err := db.ExecContext(ctx, "INSTALL httpfs; LOAD httpfs;"); err != nil {
			return nil, nil, fmt.Errorf("load httpfs extension: %w", err)
		}
	}

	rows, err := db.QueryContext(ctx, sqlStr)
	if err != nil {
		return nil, nil, fmt.Errorf("execute SQL: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, fmt.Errorf("columns: %w", err)
	}
	var results []map[string]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, fmt.Errorf("scan row: %w", err)
		}
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			row[col] = vals[i]
		}
		results = append(results, row)
	}
	return results, cols, rows.Err()
}

// ─── local ────────────────────────────────────────────────────────────────────

func findBaselineLocal(baselineDir, schema, table string, at time.Time) (string, time.Time, error) {
	entries, err := os.ReadDir(baselineDir)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("read baseline directory %q: %w", baselineDir, err)
	}

	type candidate struct {
		t    time.Time
		path string
	}
	var candidates []candidate
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		t, ok := parseDirTimestamp(entry.Name())
		if !ok || t.After(at) {
			continue
		}
		p := filepath.Join(baselineDir, entry.Name(), schema, table+".parquet")
		if _, err := os.Stat(p); err != nil {
			continue // table not in this snapshot
		}
		candidates = append(candidates, candidate{t: t, path: p})
	}
	if len(candidates) == 0 {
		return "", time.Time{}, fmt.Errorf("no baseline snapshot found for %s.%s at or before %s in %q",
			schema, table, at.UTC().Format(time.RFC3339), baselineDir)
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].t.After(candidates[j].t) })
	return candidates[0].path, candidates[0].t, nil
}

// ─── S3 ───────────────────────────────────────────────────────────────────────

func findBaselineS3(ctx context.Context, s3URL, schema, table string, at time.Time) (string, time.Time, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return "", time.Time{}, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, "INSTALL httpfs; LOAD httpfs;"); err != nil {
		return "", time.Time{}, fmt.Errorf("load httpfs extension: %w", err)
	}

	prefix := strings.TrimSuffix(s3URL, "/")
	globPat := prefix + "/*/" + schema + "/" + table + ".parquet"
	safeGlob := strings.ReplaceAll(globPat, "'", "''")

	// Use DuckDB's glob() to enumerate matching S3 paths without downloading data.
	rows, err := db.QueryContext(ctx, "SELECT unnest(glob('"+safeGlob+"')) AS path")
	if err != nil {
		return "", time.Time{}, fmt.Errorf("list S3 baseline snapshots: %w", err)
	}
	defer rows.Close()

	type candidate struct {
		t    time.Time
		path string
	}
	var candidates []candidate
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			continue
		}
		t, ok := extractTimestampFromS3Path(path, prefix, schema, table)
		if !ok || t.After(at) {
			continue
		}
		candidates = append(candidates, candidate{t: t, path: path})
	}
	if err := rows.Err(); err != nil {
		return "", time.Time{}, fmt.Errorf("iterate S3 baseline list: %w", err)
	}
	if len(candidates) == 0 {
		return "", time.Time{}, fmt.Errorf("no baseline snapshot found for %s.%s at or before %s in %q",
			schema, table, at.UTC().Format(time.RFC3339), s3URL)
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].t.After(candidates[j].t) })
	return candidates[0].path, candidates[0].t, nil
}

// extractTimestampFromS3Path parses the snapshot timestamp from a full S3 path:
// s3://bucket/prefix/2026-02-28T00-00-00Z/mydb/orders.parquet
func extractTimestampFromS3Path(path, prefix, schema, table string) (time.Time, bool) {
	base := strings.TrimSuffix(prefix, "/") + "/"
	rest := strings.TrimPrefix(path, base)
	// rest: 2026-02-28T00-00-00Z/mydb/orders.parquet
	suffix := "/" + schema + "/" + table + ".parquet"
	dirName, ok := strings.CutSuffix(rest, suffix)
	if !ok {
		return time.Time{}, false
	}
	return parseDirTimestamp(dirName)
}

// ─── shared helpers ───────────────────────────────────────────────────────────

// parseDirTimestamp converts a baseline directory name like "2026-02-28T00-00-00Z"
// to a time.Time. The format is RFC3339 with colons in the time+offset portion
// replaced by hyphens for filesystem compatibility.
func parseDirTimestamp(name string) (time.Time, bool) {
	idx := strings.IndexByte(name, 'T')
	if idx < 0 {
		return time.Time{}, false
	}
	// Restore colons only in the portion after 'T'.
	rfc := name[:idx+1] + strings.ReplaceAll(name[idx+1:], "-", ":")
	t, err := time.Parse(time.RFC3339, rfc)
	if err != nil {
		return time.Time{}, false
	}
	return t.UTC(), true
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

type colCond struct {
	col   string
	value string
}

// buildCondsList returns sorted column conditions for deterministic SQL + arg order.
func buildCondsList(pkFilter map[string]string) []colCond {
	conds := make([]colCond, 0, len(pkFilter))
	for col, val := range pkFilter {
		conds = append(conds, colCond{col: col, value: val})
	}
	sort.Slice(conds, func(i, j int) bool { return conds[i].col < conds[j].col })
	return conds
}
