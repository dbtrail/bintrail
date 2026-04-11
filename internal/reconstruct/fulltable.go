package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/duckdb/duckdb-go/v2" // DuckDB driver for parquet_scan baseline streaming
	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/bintrail/internal/baseline"
	"github.com/dbtrail/bintrail/internal/config"
	"github.com/dbtrail/bintrail/internal/metadata"
	"github.com/dbtrail/bintrail/internal/parquetquery"
	"github.com/dbtrail/bintrail/internal/parser"
	"github.com/dbtrail/bintrail/internal/query"
)

// FullTableConfig drives ReconstructTables — the full-table merge-on-read
// entry point for #187. One Config instance covers one run; each table in
// Tables is reconstructed independently with bounded parallelism.
type FullTableConfig struct {
	IndexDSN    string        // DSN for the bintrail index database
	BaselineSrc string        // local directory or s3:// URL of baselines
	Tables      []string      // "db.table" entries
	At          time.Time     // target point-in-time
	OutputDir   string        // mydumper dump output directory (must exist)
	ChunkSize   int64         // per-chunk SQL file size (0 → 256 MiB)
	Parallelism int           // max concurrent tables (0 → runtime.NumCPU())
	AllowGaps   bool          // false = strict abort on gaps (default for reconstruct)
}

// TableReport carries the per-table outcome stats that the CLI summary prints.
type TableReport struct {
	Schema, Table  string
	BaselineRows   int64 // rows streamed through from the baseline unchanged
	EventsApplied  int64 // total events observed from the event index
	InsertsEmitted int64 // rows appended after the baseline pass (new PKs)
	UpdatesApplied int64 // baseline rows whose PK matched an UPDATE/INSERT event
	DeletesSkipped int64 // baseline rows whose PK matched a DELETE event
	Files          []string
	Duration       time.Duration
}

// ReconstructTables runs ReconstructTable concurrently for every entry in
// cfg.Tables, sharing a single *sql.DB + *query.Engine + *metadata.Resolver.
// Returns the list of reports in arbitrary order plus the first error
// encountered (others are logged via slog.Error).
func ReconstructTables(ctx context.Context, cfg FullTableConfig) ([]*TableReport, error) {
	if cfg.IndexDSN == "" {
		return nil, errors.New("FullTableConfig: IndexDSN is required")
	}
	if cfg.BaselineSrc == "" {
		return nil, errors.New("FullTableConfig: BaselineSrc is required")
	}
	if len(cfg.Tables) == 0 {
		return nil, errors.New("FullTableConfig: at least one table is required")
	}
	if cfg.OutputDir == "" {
		return nil, errors.New("FullTableConfig: OutputDir is required")
	}
	if cfg.At.IsZero() {
		cfg.At = time.Now().UTC()
	}
	if cfg.Parallelism <= 0 {
		cfg.Parallelism = runtime.NumCPU()
	}
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = 256 << 20
	}

	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return nil, fmt.Errorf("create output dir: %w", err)
	}

	db, err := config.Connect(cfg.IndexDSN)
	if err != nil {
		return nil, fmt.Errorf("connect to index DB: %w", err)
	}
	defer db.Close()
	// Give per-table goroutines enough connections for concurrent fetches.
	db.SetMaxOpenConns(2 * cfg.Parallelism)

	// Derive DBName for the query planner.
	var dbName string
	if dsnCfg, perr := mysqldriver.ParseDSN(cfg.IndexDSN); perr == nil {
		dbName = dsnCfg.DBName
	}

	// Load schema resolver once (latest snapshot). All PK encoding goes
	// through parser.BuildPKValues with the resolver's ColumnMetas so the
	// keys are byte-identical to what the indexer stored in pk_values.
	resolver, err := metadata.NewResolver(db, 0)
	if err != nil {
		return nil, fmt.Errorf("load schema resolver: %w; run `bintrail snapshot` first", err)
	}

	engine := query.New(db)

	// Resolve archive sources once — the same set is used for every table.
	archSources := query.ResolveArchiveSources(ctx, db)

	// Report slice is protected by a mutex for the parallel goroutines.
	reports := make([]*TableReport, 0, len(cfg.Tables))
	var (
		mu   sync.Mutex
		errs []error
	)

	sem := make(chan struct{}, cfg.Parallelism)
	var wg sync.WaitGroup

	for _, entry := range cfg.Tables {
		schema, table, ok := splitSchemaTable(entry)
		if !ok {
			return nil, fmt.Errorf("invalid --tables entry %q: must be schema.table", entry)
		}
		wg.Add(1)
		go func(schema, table string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			if ctx.Err() != nil {
				return
			}

			rep, err := ReconstructTable(ctx, cfg, schema, table, db, engine, archSources, resolver, dbName)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				slog.Error("full-table reconstruct failed",
					"schema", schema, "table", table, "error", err)
				errs = append(errs, fmt.Errorf("%s.%s: %w", schema, table, err))
				return
			}
			reports = append(reports, rep)
		}(schema, table)
	}
	wg.Wait()

	// Write the shared metadata file once after every table completes.
	// We use the metadata from the FIRST reconstructed table — all tables
	// share the same baseline set, so their positions should agree. If a
	// user somehow reconstructs tables from baselines of different ages,
	// the metadata file will reflect the first one and the rest will log
	// a warning in ReconstructTable itself.
	if len(reports) > 0 {
		// Re-read the metadata of the first successful table's baseline.
		firstTableName := reports[0].Schema + "." + reports[0].Table
		baselinePath, _, perr := FindBaseline(ctx, cfg.BaselineSrc, reports[0].Schema, reports[0].Table, cfg.At)
		if perr == nil {
			bmeta, merr := baseline.ReadParquetMetadataAny(ctx, baselinePath)
			if merr == nil {
				if err := WriteMetadataFile(cfg.OutputDir, cfg.At,
					bmeta.GTIDSet, bmeta.BinlogFile, bmeta.BinlogPos); err != nil {
					slog.Warn("could not write metadata file", "error", err)
				}
			} else {
				slog.Warn("could not read baseline metadata for metadata file",
					"table", firstTableName, "error", merr)
			}
		}
	}

	if len(errs) > 0 {
		return reports, errs[0]
	}
	return reports, nil
}

// ReconstructTable is the per-table worker. Safe to call concurrently with
// other ReconstructTable invocations that share the same db / engine /
// archSources / resolver.
func ReconstructTable(
	ctx context.Context,
	cfg FullTableConfig,
	schema, table string,
	db *sql.DB,
	engine *query.Engine,
	archSources []string,
	resolver *metadata.Resolver,
	dbName string,
) (*TableReport, error) {
	start := time.Now()
	rep := &TableReport{Schema: schema, Table: table}

	// ── 1. Find the baseline snapshot ──────────────────────────────────────
	baselinePath, snapshotTime, err := FindBaseline(ctx, cfg.BaselineSrc, schema, table, cfg.At)
	if err != nil {
		return nil, fmt.Errorf("find baseline: %w", err)
	}
	slog.Debug("baseline selected",
		"schema", schema, "table", table,
		"path", baselinePath, "snapshot_time", snapshotTime.UTC().Format(time.RFC3339))

	// ── 2. Read baseline Parquet metadata ──────────────────────────────────
	bmeta, err := baseline.ReadParquetMetadataAny(ctx, baselinePath)
	if err != nil {
		return nil, fmt.Errorf("read baseline metadata: %w", err)
	}
	if bmeta.CreateTableSQL == "" {
		return nil, fmt.Errorf(
			"baseline at %s lacks bintrail.create_table_sql metadata; "+
				"re-run `bintrail baseline` to embed the CREATE TABLE statement",
			baselinePath)
	}

	// ── 3. Resolve PK columns from the schema resolver ─────────────────────
	tm, err := resolver.Resolve(schema, table)
	if err != nil {
		return nil, fmt.Errorf("resolve schema for %s.%s: %w; run `bintrail snapshot` to refresh", schema, table, err)
	}
	pkCols := tm.PKColumnMetas()
	if len(pkCols) == 0 {
		return nil, fmt.Errorf("%s.%s has no primary key in the loaded snapshot; full-table reconstruct requires a PK", schema, table)
	}

	// ── 4. Fetch events via the shared helper (gap-aware) ──────────────────
	fetchOpts := query.Options{
		Schema: schema,
		Table:  table,
		Since:  &snapshotTime,
		Until:  &cfg.At,
		// No PKValues filter — we want every event for this table.
	}
	archFetcher := query.ArchiveFetcher(parquetquery.Fetch)
	events, _, err := query.FetchMerged(ctx, db, engine, query.FetchMergedOptions{
		Opts:           fetchOpts,
		DBName:         dbName,
		NoArchive:      len(archSources) == 0, // nothing to discover → skip archive fetch
		AllowGaps:      cfg.AllowGaps,
		ArchiveFetcher: archFetcher,
	})
	if err != nil {
		return nil, fmt.Errorf("fetch events: %w", err)
	}
	rep.EventsApplied = int64(len(events))

	// ── 5. Build the change map: PK string → last event for that PK ───────
	// events is already sorted by (event_timestamp, event_id) via
	// query.MergeResults, so the last write wins naturally.
	changes := make(map[string]*query.ResultRow, len(events))
	for i := range events {
		changes[events[i].PKValues] = &events[i]
	}

	// ── 6. Materialize the baseline locally for DuckDB streaming ───────────
	localPath, cleanup, err := materializeBaselineLocal(ctx, baselinePath)
	if err != nil {
		return nil, fmt.Errorf("materialize baseline: %w", err)
	}
	defer cleanup()

	// ── 7-9. Merge baseline + changes into the mydumper writer ────────────
	// The merge loop is extracted so it can be unit-tested without MySQL.
	if err := mergeBaselineIntoWriter(ctx, mergeInput{
		LocalBaselinePath: localPath,
		CreateTableSQL:    bmeta.CreateTableSQL,
		Schema:            schema,
		Table:             table,
		PKCols:            pkCols,
		Changes:           changes,
		OutputDir:         cfg.OutputDir,
		ChunkSize:         cfg.ChunkSize,
	}, rep); err != nil {
		return nil, err
	}
	rep.Duration = time.Since(start)

	slog.Info("table reconstructed",
		"schema", schema, "table", table,
		"baseline_rows", rep.BaselineRows,
		"events_applied", rep.EventsApplied,
		"updates_applied", rep.UpdatesApplied,
		"inserts_emitted", rep.InsertsEmitted,
		"deletes_skipped", rep.DeletesSkipped,
		"duration_ms", rep.Duration.Milliseconds())
	return rep, nil
}

// mergeInput bundles everything mergeBaselineIntoWriter needs. Extracted so
// unit tests can exercise the merge loop without standing up MySQL.
type mergeInput struct {
	LocalBaselinePath string
	CreateTableSQL    string
	Schema            string
	Table             string
	PKCols            []metadata.ColumnMeta
	Changes           map[string]*query.ResultRow
	OutputDir         string
	ChunkSize         int64
}

// mergeBaselineIntoWriter streams the local baseline Parquet via DuckDB,
// applies the change map to produce the final row set, and writes the result
// through a MydumperWriter. Updates counters on rep in place. Drains the
// Changes map: after this function returns, entries still present are rows
// that were NOT found in the baseline (appended as new INSERTs).
func mergeBaselineIntoWriter(ctx context.Context, in mergeInput, rep *TableReport) error {
	colNames, err := readBaselineColumns(ctx, in.LocalBaselinePath)
	if err != nil {
		return fmt.Errorf("read baseline columns: %w", err)
	}
	mw, err := NewMydumperWriter(in.OutputDir, in.Schema, in.Table, colNames, in.ChunkSize)
	if err != nil {
		return fmt.Errorf("open mydumper writer: %w", err)
	}
	if err := mw.WriteSchema(in.CreateTableSQL); err != nil {
		return err
	}

	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	defer ddb.Close()

	safePath := strings.ReplaceAll(in.LocalBaselinePath, "'", "''")
	q := fmt.Sprintf("SELECT * FROM parquet_scan('%s')", safePath)
	drows, err := ddb.QueryContext(ctx, q)
	if err != nil {
		return fmt.Errorf("duckdb baseline query: %w", err)
	}
	defer drows.Close()

	dcols, err := drows.Columns()
	if err != nil {
		return fmt.Errorf("duckdb columns: %w", err)
	}

	scan := make([]any, len(dcols))
	ptrs := make([]any, len(dcols))
	for i := range scan {
		ptrs[i] = &scan[i]
	}

	for drows.Next() {
		if err := drows.Scan(ptrs...); err != nil {
			return fmt.Errorf("scan baseline row: %w", err)
		}
		rowMap := zipMap(dcols, scan)
		pk := parser.BuildPKValues(in.PKCols, rowMap)

		if ev, ok := in.Changes[pk]; ok {
			delete(in.Changes, pk) // mark consumed
			switch ev.EventType {
			case parser.EventDelete:
				rep.DeletesSkipped++
				continue
			case parser.EventUpdate, parser.EventInsert:
				ordered := rowAfterOrdered(ev.RowAfter, dcols, in.Schema, in.Table)
				if err := mw.WriteRow(ordered); err != nil {
					return fmt.Errorf("write update row: %w", err)
				}
				rep.UpdatesApplied++
			}
		} else {
			// Pass-through baseline row. Copy the scan buffer because
			// database/sql reuses it across Scan calls.
			rowCopy := make([]any, len(scan))
			copy(rowCopy, scan)
			if err := mw.WriteRow(rowCopy); err != nil {
				return fmt.Errorf("write baseline row: %w", err)
			}
			rep.BaselineRows++
		}
	}
	if err := drows.Err(); err != nil {
		return fmt.Errorf("iterate baseline rows: %w", err)
	}

	// Append events for PKs that weren't in the baseline (rows inserted
	// after the snapshot). Deterministic order: sort by PK string so tests
	// can assert on the output without flakiness.
	newPKs := make([]string, 0, len(in.Changes))
	for pk := range in.Changes {
		newPKs = append(newPKs, pk)
	}
	sort.Strings(newPKs)
	for _, pk := range newPKs {
		ev := in.Changes[pk]
		if ev.EventType == parser.EventDelete {
			continue
		}
		ordered := rowAfterOrdered(ev.RowAfter, dcols, in.Schema, in.Table)
		if err := mw.WriteRow(ordered); err != nil {
			return fmt.Errorf("write new-row insert: %w", err)
		}
		rep.InsertsEmitted++
	}

	if err := mw.Close(); err != nil {
		return err
	}
	rep.Files = mw.Files()
	return nil
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// splitSchemaTable parses "db.table" into (db, table, true). Rejects entries
// with zero or more than one dot.
func splitSchemaTable(entry string) (string, string, bool) {
	parts := strings.SplitN(entry, ".", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	if parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	if strings.Contains(parts[1], ".") {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// materializeBaselineLocal ensures the baseline Parquet is available on the
// local filesystem. Local paths are returned as-is with a no-op cleanup. S3
// URLs are downloaded to a temp file via DuckDB's httpfs + COPY so DuckDB
// can then query the resulting local file without an outbound connection.
func materializeBaselineLocal(ctx context.Context, path string) (string, func(), error) {
	if !strings.HasPrefix(path, "s3://") {
		return path, func() {}, nil
	}
	// Download via DuckDB httpfs. Keep the temp file around until cleanup().
	tmpDir, err := os.MkdirTemp("", "bintrail-baseline-*")
	if err != nil {
		return "", nil, fmt.Errorf("mkdir temp: %w", err)
	}
	tmpPath := filepath.Join(tmpDir, "baseline.parquet")

	db, err := sql.Open("duckdb", "")
	if err != nil {
		os.RemoveAll(tmpDir)
		return "", nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, "INSTALL httpfs; LOAD httpfs;"); err != nil {
		os.RemoveAll(tmpDir)
		return "", nil, fmt.Errorf("load httpfs: %w", err)
	}
	safeSrc := strings.ReplaceAll(path, "'", "''")
	safeDst := strings.ReplaceAll(tmpPath, "'", "''")
	copyQ := fmt.Sprintf("COPY (SELECT * FROM parquet_scan('%s')) TO '%s' (FORMAT PARQUET)", safeSrc, safeDst)
	if _, err := db.ExecContext(ctx, copyQ); err != nil {
		os.RemoveAll(tmpDir)
		return "", nil, fmt.Errorf("download s3 baseline: %w", err)
	}

	cleanup := func() { os.RemoveAll(tmpDir) }
	return tmpPath, cleanup, nil
}

// readBaselineColumns opens the local Parquet file with DuckDB and returns
// the column names in the order parquet_scan() emits them. This order is
// the canonical column order for the emitted INSERT statements.
func readBaselineColumns(ctx context.Context, localPath string) ([]string, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	safePath := strings.ReplaceAll(localPath, "'", "''")
	q := fmt.Sprintf("SELECT * FROM parquet_scan('%s') LIMIT 0", safePath)
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("describe baseline: %w", err)
	}
	defer rows.Close()
	return rows.Columns()
}

// zipMap pairs cols with vals to produce a map[string]any view of a row.
// Used only for PK key construction — not for downstream writes.
func zipMap(cols []string, vals []any) map[string]any {
	out := make(map[string]any, len(cols))
	for i, c := range cols {
		out[c] = vals[i]
	}
	return out
}

// rowAfterOrdered walks colNames and looks up each name in rowAfter (a
// map[string]any from a binlog event's row_after image), returning a slice
// of values aligned to the baseline Parquet column order. Missing columns
// become nil (SQL NULL) with an slog.Warn — this covers the schema drift
// case where a column was added to the source table between baseline time
// and target time.
func rowAfterOrdered(rowAfter map[string]any, colNames []string, schema, table string) []any {
	out := make([]any, len(colNames))
	for i, col := range colNames {
		v, ok := rowAfter[col]
		if !ok {
			slog.Warn("event row_after missing column present in baseline; emitting NULL",
				"schema", schema, "table", table, "column", col)
			out[i] = nil
			continue
		}
		out[i] = v
	}
	return out
}
