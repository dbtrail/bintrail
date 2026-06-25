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

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
)

// ErrNoBaseline is returned by FindBaseline when no baseline snapshot exists
// for the requested table at or before the target time.
var ErrNoBaseline = errors.New("no baseline snapshot found")

// StaleWarning describes a baseline that was selected by falling back to an
// older snapshot because the table is absent from a newer one (#461/#466).
// Empty Message means "not stale" — the table is present in the newest eligible
// snapshot. Both the local and S3 lookups populate it identically; callers that
// surface staleness (the console reconstruct response, #466) read Message,
// while callers that only log can ignore it.
type StaleWarning struct {
	Message        string    // human-readable, empty when not stale
	UsingSnapshot  time.Time // snapshot the table was actually read from
	NewestSnapshot time.Time // newest eligible snapshot (lacks the table)
}

// Stale reports whether the baseline was a stale fallback.
func (s StaleWarning) Stale() bool { return s.Message != "" }

// FindBaseline finds the most recent baseline Parquet file at or before `at`
// for the given schema and table, returning its path, snapshot timestamp, and a
// staleness warning (#466). The warning's Message is non-empty only when the
// table is absent from a newer eligible snapshot, meaning the result is an
// older-snapshot fallback — both the local and S3 paths report it now.
//
// source may be:
//   - A local directory path (parent of RFC3339-named snapshot subdirectories)
//   - An S3 URL prefix (e.g. "s3://bucket/baselines")
func FindBaseline(ctx context.Context, source, schema, table string, at time.Time) (path string, snapshotTime time.Time, stale StaleWarning, err error) {
	if strings.HasPrefix(source, "s3://") {
		return findBaselineS3(ctx, source, schema, table, at)
	}
	return findBaselineLocal(source, schema, table, at)
}

// ReadBaselineRow opens the Parquet file at path using DuckDB and returns the
// first row matching pkFilter (column name → value string). Returns nil when
// no row matches. Loads the httpfs extension automatically for s3:// paths.
func ReadBaselineRow(ctx context.Context, path string, pkFilter map[string]string) (map[string]any, error) {
	rows, err := ReadBaselineRows(ctx, path, pkFilter, 1)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return rows[0], nil
}

// ReadBaselineRows returns every baseline row matching filter (an arbitrary
// column→value map, AND-ed), up to limit rows (limit <= 0 = no cap). Unlike
// ReadBaselineRow it is not restricted to the primary key, so cascade Phase-2
// can scan a child snapshot by a foreign-key column and recover ALL matching
// children. Values are bound as query parameters; column names are quoted
// identifiers; the path is single-quote-escaped.
func ReadBaselineRows(ctx context.Context, path string, filter map[string]string, limit int) ([]map[string]any, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()
	if err := pinDuckDBSessionUTC(ctx, db); err != nil {
		return nil, err
	}

	if strings.HasPrefix(path, "s3://") {
		if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
			return nil, fmt.Errorf("load httpfs extension: %w", err)
		}
		duckdbutil.EnableS3CredentialChain(ctx, db)
	}

	// Build sorted conditions for deterministic SQL + arg ordering.
	conds := buildCondsList(filter)
	safePath := strings.ReplaceAll(path, "'", "''")
	q := "SELECT * FROM parquet_scan('" + safePath + "')"
	if len(conds) > 0 {
		parts := make([]string, len(conds))
		for i, c := range conds {
			parts[i] = quoteIdent(c.col) + " = ?"
		}
		q += " WHERE " + strings.Join(parts, " AND ")
	}
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}

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
	var out []map[string]any
	for rows.Next() {
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
		out = append(out, row)
	}
	return out, rows.Err()
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
		if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
			return nil, nil, fmt.Errorf("load httpfs extension: %w", err)
		}
		duckdbutil.EnableS3CredentialChain(ctx, db)
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
		// Skip a partially-converted snapshot (#467) so the listing doesn't
		// advertise an incomplete snapshot as the latest baseline.
		if !baseline.SnapshotComplete(snapDir) {
			slog.Warn("baseline listing: skipping incomplete snapshot", "path", snapDir)
			continue
		}
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
	if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
		return nil, fmt.Errorf("load httpfs extension: %w", err)
	}
	duckdbutil.EnableS3CredentialChain(ctx, db)

	prefix := strings.TrimSuffix(s3URL, "/")

	// Exclude partially-converted snapshots (#467) so the listing doesn't
	// advertise an incomplete snapshot as the latest baseline.
	incomplete, err := s3IncompleteSnapshots(ctx, db, prefix)
	if err != nil {
		return nil, err
	}

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
		if incomplete[ts.UTC().Format(time.RFC3339)] {
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

func findBaselineLocal(baselineDir, schema, table string, at time.Time) (string, time.Time, StaleWarning, error) {
	entries, err := os.ReadDir(baselineDir)
	if err != nil {
		return "", time.Time{}, StaleWarning{}, fmt.Errorf("read baseline directory %q: %w", baselineDir, err)
	}

	type candidate struct {
		t    time.Time
		path string
	}
	var candidates []candidate
	var newestSnap time.Time // newest eligible snapshot, whether or not it has the table
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		t, ok := parseDirTimestamp(entry.Name())
		if !ok || t.After(at) {
			continue
		}
		// Skip a partially-converted snapshot (#467): treating it as the newest
		// would reconstruct missing tables from an older one or hit ErrNoBaseline.
		if !baseline.SnapshotComplete(filepath.Join(baselineDir, entry.Name())) {
			continue
		}
		if t.After(newestSnap) {
			newestSnap = t
		}
		p := filepath.Join(baselineDir, entry.Name(), schema, table+".parquet")
		if _, err := os.Stat(p); err != nil {
			continue // table not in this snapshot
		}
		candidates = append(candidates, candidate{t: t, path: p})
	}
	if len(candidates) == 0 {
		return "", time.Time{}, StaleWarning{}, fmt.Errorf("%w: %s.%s at or before %s in %q",
			ErrNoBaseline, schema, table, at.UTC().Format(time.RFC3339), baselineDir)
	}
	slices.SortFunc(candidates, func(a, b candidate) int { return b.t.Compare(a.t) })
	best := candidates[0]
	stale := staleFallback(schema, table, best.t, newestSnap)
	return best.path, best.t, stale, nil
}

// staleFallback builds the staleness warning (and logs it) when the table is
// absent from a newer eligible snapshot, so the chosen snapshot is an older
// fallback (#461/#466). When newestSnap is not after using, the result is the
// zero StaleWarning ("not stale"). The local and S3 lookups share this so the
// server-side log message and the surfaced warning are identical.
func staleFallback(schema, table string, using, newestSnap time.Time) StaleWarning {
	if !newestSnap.After(using) {
		return StaleWarning{}
	}
	// The table dropped out of newer snapshots (dump filter change, lost SELECT
	// privilege, rename): falling back to an older snapshot is the designed
	// behavior, but doing it silently means reconstructing from ever-staler data
	// with no signal anywhere (#461). Warn server-side AND return the message so
	// callers (the console, #466) can surface it in-band.
	usingStr := using.UTC().Format(time.RFC3339)
	newestStr := newestSnap.UTC().Format(time.RFC3339)
	slog.Warn("baseline: table is absent from the newest snapshot; using an older one — re-dump to refresh it",
		"schema", schema, "table", table, "using", usingStr, "newest_snapshot", newestStr)
	return StaleWarning{
		Message: fmt.Sprintf("baseline for %s.%s is stale: the table is absent from the newest snapshot (%s); reconstructing from an older snapshot (%s) — re-dump to refresh it",
			schema, table, newestStr, usingStr),
		UsingSnapshot:  using,
		NewestSnapshot: newestSnap,
	}
}

// ─── S3 ───────────────────────────────────────────────────────────────────────

// findBaselineS3 mirrors findBaselineLocal over an s3:// prefix. It now also
// warns when the result is an older-snapshot fallback (#466): the prior
// table-scoped glob made snapshots lacking the table invisible, so it could
// never compute a "newest eligible snapshot" to compare against. We resolve
// that by running ONE broader listing (prefix/*/*/*.parquet, the same glob
// listBaselinesS3 uses) — bounding the listing cost to a single extra glob —
// to derive the newest complete snapshot at-or-before `at`, and ONE marker glob
// (prefix/*/_SUCCESS and _INCOMPLETE) to exclude partial snapshots (#467).
//
// The two glob steps differ in fatality: the marker glob (s3IncompleteSnapshots)
// is a CORRECTNESS filter — its error fails the lookup so a partial snapshot can
// never slip through — while the broad newest-snapshot glob is purely ADVISORY
// (staleWarningS3) and its error must NOT discard the already-located baseline
// (#524 review).
func findBaselineS3(ctx context.Context, s3URL, schema, table string, at time.Time) (string, time.Time, StaleWarning, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return "", time.Time{}, StaleWarning{}, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()
	if err := pinDuckDBSessionUTC(ctx, db); err != nil {
		return "", time.Time{}, StaleWarning{}, err
	}

	if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
		return "", time.Time{}, StaleWarning{}, fmt.Errorf("load httpfs extension: %w", err)
	}
	duckdbutil.EnableS3CredentialChain(ctx, db)

	prefix := strings.TrimSuffix(s3URL, "/")

	// Snapshot dirs flagged incomplete (#467) — excluded from both the
	// table-scoped candidate scan and the broad newest-snapshot scan.
	incomplete, err := s3IncompleteSnapshots(ctx, db, prefix)
	if err != nil {
		return "", time.Time{}, StaleWarning{}, err
	}

	// Table-scoped glob: the snapshots that actually contain this table.
	globPat := prefix + "/*/" + schema + "/" + table + ".parquet"
	safeGlob := strings.ReplaceAll(globPat, "'", "''")
	rows, err := db.QueryContext(ctx, "SELECT * FROM glob('"+safeGlob+"')")
	if err != nil {
		return "", time.Time{}, StaleWarning{}, fmt.Errorf("list S3 baseline snapshots: %w", err)
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
		if incomplete[t.UTC().Format(time.RFC3339)] {
			continue // partial snapshot (#467)
		}
		candidates = append(candidates, candidate{t: t, path: path})
	}
	if err := rows.Err(); err != nil {
		return "", time.Time{}, StaleWarning{}, fmt.Errorf("iterate S3 baseline list: %w", err)
	}
	if len(candidates) == 0 {
		return "", time.Time{}, StaleWarning{}, fmt.Errorf("%w: %s.%s at or before %s in %q",
			ErrNoBaseline, schema, table, at.UTC().Format(time.RFC3339), s3URL)
	}
	slices.SortFunc(candidates, func(a, b candidate) int { return b.t.Compare(a.t) })
	best := candidates[0]

	// `best` is a VALID located baseline. The staleness warning is ADVISORY, so
	// its computation must never fail the recovery (see staleWarningS3).
	stale := staleWarningS3(ctx, db, prefix, schema, table, best.t, at, incomplete)
	return best.path, best.t, stale, nil
}

// staleWarningS3 derives the advisory staleness warning for an already-located
// S3 baseline. The broad newest-snapshot glob (s3NewestSnapshot) lists every
// parquet across all snapshots and is the most likely of the lookup's globs to
// throttle/timeout on a large bucket; findBaselineS3 is on the per-request shim
// `_snapshot` / console reconstruct hot path (no caching). A transient S3 blip
// on this purely-advisory step must NOT throw away the baseline we already
// found — that would fail a recovery that pre-#466 succeeded, the inverse of
// the goal. So on error we warn and return the zero StaleWarning ("not stale");
// only the FATAL filters (incomplete-snapshot exclusion, the table-scoped glob)
// can fail the lookup (#524 review).
func staleWarningS3(ctx context.Context, db *sql.DB, prefix, schema, table string, using, at time.Time, incomplete map[string]bool) StaleWarning {
	// Broad scan for the newest complete snapshot at-or-before `at`, whether or
	// not it contains this table — the missing piece that let S3 fall back
	// silently (#466).
	newestSnap, err := s3NewestSnapshot(ctx, db, prefix, at, incomplete)
	if err != nil {
		slog.Warn("baseline: staleness check failed; returning the located baseline without a stale warning",
			"schema", schema, "table", table, "error", err)
		return StaleWarning{}
	}
	return staleFallback(schema, table, using, newestSnap)
}

// s3NewestSnapshot returns the newest complete snapshot timestamp at-or-before
// `at` across ALL tables under prefix, using the broad prefix/*/*/*.parquet
// glob. Snapshots in the incomplete set (#467) are excluded.
func s3NewestSnapshot(ctx context.Context, db *sql.DB, prefix string, at time.Time, incomplete map[string]bool) (time.Time, error) {
	safeGlob := strings.ReplaceAll(prefix+"/*/*/*.parquet", "'", "''")
	rows, err := db.QueryContext(ctx, "SELECT * FROM glob('"+safeGlob+"')")
	if err != nil {
		return time.Time{}, fmt.Errorf("list S3 baseline snapshots (broad): %w", err)
	}
	defer rows.Close()

	var newest time.Time
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			continue
		}
		rest := strings.TrimPrefix(path, prefix+"/")
		parts := strings.Split(rest, "/")
		if len(parts) != 3 || !strings.HasSuffix(parts[2], ".parquet") {
			continue
		}
		t, ok := parseDirTimestamp(parts[0])
		if !ok || t.After(at) {
			continue
		}
		if incomplete[t.UTC().Format(time.RFC3339)] {
			continue
		}
		if t.After(newest) {
			newest = t
		}
	}
	if err := rows.Err(); err != nil {
		return time.Time{}, fmt.Errorf("iterate S3 baseline list (broad): %w", err)
	}
	return newest, nil
}

// s3IncompleteSnapshots returns the set of snapshot timestamps (keyed by
// RFC3339 UTC) that carry an _INCOMPLETE marker without a _SUCCESS marker, so a
// partially-converted snapshot (#467) is excluded from S3 discovery. Pre-marker
// snapshots have neither and are complete-by-default (absent from this set).
//
// The glob is prefix/*/_* — DuckDB's glob() does NOT brace-expand
// {_SUCCESS,_INCOMPLETE} (verified empirically), and the only underscore-prefixed
// entries in the snapshot layout are these two markers; we still filter by exact
// basename so an unrelated _* file can't be mistaken for a marker.
func s3IncompleteSnapshots(ctx context.Context, db *sql.DB, prefix string) (map[string]bool, error) {
	markerGlob := strings.ReplaceAll(prefix+"/*/_*", "'", "''")
	rows, err := db.QueryContext(ctx, "SELECT * FROM glob('"+markerGlob+"')")
	if err != nil {
		return nil, fmt.Errorf("list S3 baseline markers: %w", err)
	}
	defer rows.Close()

	hasSuccess := map[string]bool{}
	hasIncomplete := map[string]bool{}
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			// This is a CORRECTNESS filter, not an observability listing: a
			// silently dropped row could be an _INCOMPLETE marker, which would
			// demote its partial snapshot to complete-by-default (residual #467).
			// Fail loud — the safe-on-error direction for a marker filter is
			// "treat as incomplete / surface the error", never silently complete.
			// Mirrors the hardened listBaselinesS3 Scan branch (#524 review).
			return nil, fmt.Errorf("scan S3 baseline marker path: %w", err)
		}
		rest := strings.TrimPrefix(path, prefix+"/")
		parts := strings.Split(rest, "/")
		if len(parts) != 2 {
			continue
		}
		t, ok := parseDirTimestamp(parts[0])
		if !ok {
			continue
		}
		key := t.UTC().Format(time.RFC3339)
		switch parts[1] {
		case baseline.SuccessMarker:
			hasSuccess[key] = true
		case baseline.IncompleteMarker:
			hasIncomplete[key] = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate S3 baseline markers: %w", err)
	}
	incomplete := map[string]bool{}
	for key := range hasIncomplete {
		if !hasSuccess[key] {
			incomplete[key] = true
		}
	}
	return incomplete, nil
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
