package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

// ErrNoBaseline is returned by FindBaseline when no baseline snapshot exists
// for the requested table at or before the target time.
var ErrNoBaseline = errors.New("no baseline snapshot found")

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
	if err := pinDuckDBSessionUTC(ctx, db); err != nil {
		return nil, err
	}

	if strings.HasPrefix(path, "s3://") {
		if _, err := db.ExecContext(ctx, "INSTALL httpfs; LOAD httpfs;"); err != nil {
			return nil, fmt.Errorf("load httpfs extension: %w", err)
		}
	}

	// Build sorted conditions for deterministic SQL + arg ordering.
	conds := buildCondsList(pkFilter)
	safePath := strings.ReplaceAll(path, "'", "''")
	q := "SELECT * FROM parquet_scan('" + safePath + "')"
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
		return nil, rows.Err() // nil when simply no rows; non-nil on iteration error
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
	if err := pinDuckDBSessionUTC(ctx, db); err != nil {
		return nil, nil, err
	}

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

// ─── listing ──────────────────────────────────────────────────────────────────

// BaselineFile is one table's Parquet file discovered in a baseline source
// listing. Path-derived only — no file contents are read — so listing stays
// cheap over both a local directory and an s3:// prefix. Binlog coordinates
// live in Parquet metadata and are the caller's (optional) enrichment step.
type BaselineFile struct {
	SnapshotTime time.Time
	Schema       string
	Table        string
	Path         string
}

// ListBaselines enumerates every baseline snapshot file under source (a local
// directory or an s3:// prefix), newest snapshot first (then schema/table for
// a stable render order). Entries that don't match the
// <timestamp>/<schema>/<table>.parquet layout are skipped, mirroring
// FindBaseline's tolerance; unreadable snapshot subdirectories are skipped
// WITH a warning — the listing is an observability surface, and a silently
// shrunken one would misreport "latest baseline" with nothing in any log.
// internal/baseline.DiscoverBaselines walks the same local layout (plus
// Parquet footers) for `bintrail status`; keep the two in sync if the layout
// ever changes.
func ListBaselines(ctx context.Context, source string) ([]BaselineFile, error) {
	if strings.HasPrefix(source, "s3://") {
		return listBaselinesS3(ctx, source)
	}
	return listBaselinesLocal(source)
}

func listBaselinesLocal(baselineDir string) ([]BaselineFile, error) {
	entries, err := os.ReadDir(baselineDir)
	if err != nil {
		return nil, fmt.Errorf("read baseline directory %q: %w", baselineDir, err)
	}
	var out []BaselineFile
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		ts, ok := parseDirTimestamp(entry.Name())
		if !ok {
			continue
		}
		snapDir := filepath.Join(baselineDir, entry.Name())
		dbDirs, err := os.ReadDir(snapDir)
		if err != nil {
			slog.Warn("baseline listing: skipping unreadable snapshot directory", "path", snapDir, "error", err)
			continue
		}
		for _, dbDir := range dbDirs {
			if !dbDir.IsDir() {
				continue
			}
			schemaDir := filepath.Join(snapDir, dbDir.Name())
			files, err := os.ReadDir(schemaDir)
			if err != nil {
				slog.Warn("baseline listing: skipping unreadable schema directory", "path", schemaDir, "error", err)
				continue
			}
			for _, f := range files {
				if f.IsDir() || !strings.HasSuffix(f.Name(), ".parquet") {
					continue
				}
				out = append(out, BaselineFile{
					SnapshotTime: ts,
					Schema:       dbDir.Name(),
					Table:        strings.TrimSuffix(f.Name(), ".parquet"),
					Path:         filepath.Join(schemaDir, f.Name()),
				})
			}
		}
	}
	sortBaselineFiles(out)
	return out, nil
}

func listBaselinesS3(ctx context.Context, s3URL string) ([]BaselineFile, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()
	if err := pinDuckDBSessionUTC(ctx, db); err != nil {
		return nil, err
	}
	if _, err := db.ExecContext(ctx, "INSTALL httpfs; LOAD httpfs;"); err != nil {
		return nil, fmt.Errorf("load httpfs extension: %w", err)
	}

	prefix := strings.TrimSuffix(s3URL, "/")
	safeGlob := strings.ReplaceAll(prefix+"/*/*/*.parquet", "'", "''")
	rows, err := db.QueryContext(ctx, "SELECT * FROM glob('"+safeGlob+"')")
	if err != nil {
		return nil, fmt.Errorf("list S3 baseline snapshots: %w", err)
	}
	defer rows.Close()

	var out []BaselineFile
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			// The glob returns exactly one VARCHAR column; a Scan failure is a
			// driver/DuckDB fault, never an expected layout condition — and
			// rows.Err() below would NOT catch it. Fail loud rather than let a
			// listing whose purpose is observability silently drop snapshots.
			return nil, fmt.Errorf("scan S3 baseline path: %w", err)
		}
		rest := strings.TrimPrefix(path, prefix+"/")
		parts := strings.Split(rest, "/")
		if len(parts) != 3 || !strings.HasSuffix(parts[2], ".parquet") {
			continue
		}
		ts, ok := parseDirTimestamp(parts[0])
		if !ok {
			continue
		}
		out = append(out, BaselineFile{
			SnapshotTime: ts,
			Schema:       parts[1],
			Table:        strings.TrimSuffix(parts[2], ".parquet"),
			Path:         path,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate S3 baseline list: %w", err)
	}
	sortBaselineFiles(out)
	return out, nil
}

func sortBaselineFiles(files []BaselineFile) {
	slices.SortFunc(files, func(a, b BaselineFile) int {
		if c := b.SnapshotTime.Compare(a.SnapshotTime); c != 0 {
			return c
		}
		if c := strings.Compare(a.Schema, b.Schema); c != 0 {
			return c
		}
		return strings.Compare(a.Table, b.Table)
	})
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
		return "", time.Time{}, fmt.Errorf("%w: %s.%s at or before %s in %q",
			ErrNoBaseline, schema, table, at.UTC().Format(time.RFC3339), baselineDir)
	}
	slices.SortFunc(candidates, func(a, b candidate) int { return b.t.Compare(a.t) })
	return candidates[0].path, candidates[0].t, nil
}

// ─── S3 ───────────────────────────────────────────────────────────────────────

func findBaselineS3(ctx context.Context, s3URL, schema, table string, at time.Time) (string, time.Time, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return "", time.Time{}, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()
	if err := pinDuckDBSessionUTC(ctx, db); err != nil {
		return "", time.Time{}, err
	}

	if _, err := db.ExecContext(ctx, "INSTALL httpfs; LOAD httpfs;"); err != nil {
		return "", time.Time{}, fmt.Errorf("load httpfs extension: %w", err)
	}

	prefix := strings.TrimSuffix(s3URL, "/")
	globPat := prefix + "/*/" + schema + "/" + table + ".parquet"
	safeGlob := strings.ReplaceAll(globPat, "'", "''")

	// Use DuckDB's glob() table function to enumerate matching S3 paths without downloading data.
	rows, err := db.QueryContext(ctx, "SELECT * FROM glob('"+safeGlob+"')")
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
		return "", time.Time{}, fmt.Errorf("%w: %s.%s at or before %s in %q",
			ErrNoBaseline, schema, table, at.UTC().Format(time.RFC3339), s3URL)
	}
	slices.SortFunc(candidates, func(a, b candidate) int { return b.t.Compare(a.t) })
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

// pinDuckDBSessionUTC pins a freshly-opened DuckDB connection's session time
// zone to UTC so string→timestamp casts are deterministic regardless of the
// host OS timezone (#359).
//
// bintrail stores all temporal values UTC-anchored: DATETIME/TIMESTAMP land in
// the baseline Parquet as micros-since-epoch (internal/baseline/schema.go),
// which DuckDB reads back as TIMESTAMP WITH TIME ZONE. Without this pin,
// ReadBaselineRow's `pkcol = ?` predicate binds the PK value as a string and
// DuckDB casts that literal to TIMESTAMPTZ using the *session* timezone
// inherited from the OS TZ. On a non-UTC host `'2020-01-01 00:00:00'` resolves
// to a different UTC instant than the stored micros, so a datetime/timestamp
// PK row silently fails to match. Pinning UTC makes the cast match the stored
// instant on every host. The other DuckDB-opening helpers here pin it too for
// consistency, so returned temporal values render as their UTC wall-clock.
func pinDuckDBSessionUTC(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, "SET TimeZone='UTC'"); err != nil {
		return fmt.Errorf("pin duckdb session to UTC: %w", err)
	}
	return nil
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
	slices.SortFunc(conds, func(a, b colCond) int { return strings.Compare(a.col, b.col) })
	return conds
}
