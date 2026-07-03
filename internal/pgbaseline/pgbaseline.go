// Package pgbaseline produces Parquet baseline snapshots directly from a live
// PostgreSQL source (#593 slice C) — the PG sibling of the mydumper→Parquet
// converter in internal/baseline, with no dump step: it COPYs each published
// table inside one REPEATABLE READ snapshot and streams the rows into
// internal/baseline.Writer.
//
// CAPTURE SIDE ONLY. This package links pgx/pglogrepl (via internal/pgcapture)
// and therefore must never be imported by the read layer (internal/baseline,
// internal/reconstruct, query/recover/shim/console) — the #528 guard
// (internal/event.TestReadLayerDoesNotLinkGoMySQL) enforces that boundary.
// The dependency points the other way: pgbaseline imports internal/baseline
// for the Writer, markers, and metadata keys.
//
// Anchoring: the output embeds baseline.MetaKeyLSN — pg_current_wal_lsn()
// captured in the SAME statement that establishes the MVCC snapshot — so
// deltas strictly after that LSN, applied by reconstruct, yield the table
// state at any later time. The replication slot is ensured to exist BEFORE
// the snapshot opens (ordering invariant: slot consistent_point ≤ anchor LSN);
// the overlap is redelivered and is harmless because reconstruct's merge is
// last-write-wins idempotent.
//
// Values are stored as raw PostgreSQL text (Column.RawText — COPY text output,
// unescaped): identical to the pgoutput text rendering the delta path indexes,
// so the PK join between baseline and deltas is an identity string match. No
// type conversion, ever.
package pgbaseline

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

// Config holds all parameters for a PostgreSQL baseline run.
type Config struct {
	// QueryDSN is an ordinary connection string: catalog reads, the snapshot
	// transaction, and the COPY transfers all run on it. Required.
	QueryDSN string
	// ReplDSN is a REPLICATION connection string (replication=database), used
	// ONLY to create the slot when it does not exist yet. Empty is fine when
	// the slot already exists (e.g. `bintrail-pg stream` created it); a
	// missing slot with an empty ReplDSN is an actionable fatal error, never
	// a silent skip — a baseline without a slot has no delta stream to anchor.
	ReplDSN string
	// SlotName and Publication mirror `bintrail-pg stream`'s. The publication
	// defines the table set (narrowed by Filters); the slot is the ordering
	// anchor.
	SlotName    string
	Publication string
	// Filters narrows the published set client-side (same semantics as
	// stream's --schemas/--tables via cliutil.BuildIndexFilters).
	Filters event.Filters

	OutputDir    string
	Compression  string // "zstd" (default), "snappy", "gzip", "none"
	RowGroupSize int    // rows per row group; <=0 → 500_000
	Parallelism  int    // concurrent table COPYs; <=0 → runtime.NumCPU()
	Retry        bool   // skip tables whose output Parquet file already exists

	Logger *slog.Logger // nil → slog.Default()
}

// Stats describes the outcome of a baseline run.
type Stats struct {
	TablesProcessed int
	RowsWritten     int64
	FilesWritten    int
	AnchorLSN       uint64    // pg_current_wal_lsn() at snapshot establishment
	SnapshotTime    time.Time // DB now() at snapshot establishment (UTC)
	SlotCreated     bool      // true when this run created the replication slot
}

// Run takes a consistent baseline snapshot of every table the publication
// streams and writes one Parquet file per table under
// <OutputDir>/<timestamp>/<schema>/<table>.parquet, mirroring
// internal/baseline.Run's marker/manifest discipline exactly (_INCOMPLETE
// before work, manifest + _SUCCESS only on full success).
func Run(ctx context.Context, cfg Config) (Stats, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if err := baseline.ValidateCodec(cfg.Compression); err != nil {
		return Stats{}, err
	}
	rowGroupSize := cfg.RowGroupSize
	if rowGroupSize <= 0 {
		rowGroupSize = 500_000
	}
	compression := cfg.Compression
	if compression == "" {
		compression = "zstd"
	}

	conn, err := pgx.Connect(ctx, cfg.QueryDSN)
	if err != nil {
		return Stats{}, fmt.Errorf("pgbaseline: connect (query DSN): %w", err)
	}
	defer conn.Close(context.Background())

	// Ensure the slot exists BEFORE the snapshot transaction opens — the
	// ordering invariant that makes the baseline anchorable: the slot's
	// consistent_point ≤ the anchor LSN read below, so no delta between them
	// can be missed (overlap is redelivered; the merge is idempotent).
	var replConnect func(context.Context) (*pgconn.PgConn, error)
	if cfg.ReplDSN != "" {
		replConnect = func(ctx context.Context) (*pgconn.PgConn, error) {
			return pgconn.Connect(ctx, cfg.ReplDSN)
		}
	}
	created, err := pgcapture.EnsureSlotExists(ctx, conn, cfg.SlotName, replConnect)
	if err != nil {
		return Stats{}, err
	}
	if created {
		logger.Info("pgbaseline: created replication slot", "slot", cfg.SlotName)
	}

	// Open the snapshot transaction. The FIRST statement both establishes the
	// REPEATABLE READ MVCC snapshot and reads the WAL anchor + snapshot time,
	// so (anchorLSN, snapshotTime, visible data) are fixed atomically.
	if _, err := conn.Exec(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY"); err != nil {
		return Stats{}, fmt.Errorf("pgbaseline: begin snapshot transaction: %w", err)
	}
	defer func() {
		// Best-effort: the transaction is READ ONLY, rollback cannot lose data.
		_, _ = conn.Exec(context.Background(), "ROLLBACK")
	}()

	var anchorText string
	var snapshotTime time.Time
	if err := conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text, now()").Scan(&anchorText, &snapshotTime); err != nil {
		return Stats{}, fmt.Errorf("pgbaseline: read snapshot anchor (pg_current_wal_lsn): %w", err)
	}
	anchorLSN, err := pglogrepl.ParseLSN(anchorText)
	if err != nil {
		return Stats{}, fmt.Errorf("pgbaseline: parse anchor LSN %q: %w", anchorText, err)
	}
	snapshotTime = snapshotTime.UTC()

	// Discovery + column resolution run INSIDE the snapshot transaction, so
	// the schema the Parquet files carry is consistent with the copied data.
	tables, err := discoverTables(ctx, conn, cfg.Publication, cfg.Filters, logger)
	if err != nil {
		return Stats{}, err
	}
	if len(tables) == 0 {
		// Never publish an empty baseline (mirrors baseline.Run's #461 guard):
		// success here would surface weeks later as ErrNoBaseline — or as
		// Time-travel silently reconstructing from an older snapshot.
		return Stats{}, fmt.Errorf("pgbaseline: publication %q streams no tables matching the filters — the baseline would be empty; check the publication's table list and the --schemas/--tables filters", cfg.Publication)
	}
	for i := range tables {
		cols, err := loadColumns(ctx, conn, tables[i].Schema, tables[i].Table, logger)
		if err != nil {
			return Stats{}, err
		}
		tables[i].Columns = cols
	}

	concurrency := cfg.Parallelism
	if concurrency <= 0 {
		concurrency = runtime.NumCPU()
	}
	if concurrency > len(tables) {
		concurrency = len(tables)
	}
	if concurrency < 1 {
		concurrency = 1
	}

	// Parallel workers each need their own connection sharing THIS snapshot:
	// export it while the anchor transaction is open (pg_export_snapshot is
	// only valid until the exporting transaction ends, so the anchor
	// transaction stays open until all workers finish).
	var snapshotID string
	if concurrency > 1 {
		if err := conn.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&snapshotID); err != nil {
			return Stats{}, fmt.Errorf("pgbaseline: export snapshot for parallel workers: %w", err)
		}
	}

	tsStr := snapshotTime.Format(time.RFC3339)
	tsDir := strings.ReplaceAll(tsStr, ":", "-")

	// Crash-safety (#467), mirroring baseline.Run: create the snapshot
	// directory and flag it _INCOMPLETE BEFORE any table conversion; only
	// full success replaces the marker with _SUCCESS.
	snapDir := filepath.Join(cfg.OutputDir, tsDir)
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		return Stats{}, fmt.Errorf("pgbaseline: create snapshot directory %s: %w", snapDir, err)
	}
	if err := baseline.WriteIncompleteMarker(snapDir); err != nil {
		logger.Warn("pgbaseline: could not write incomplete-snapshot marker", "dir", snapDir, "error", err)
	}

	stats := Stats{AnchorLSN: uint64(anchorLSN), SnapshotTime: snapshotTime, SlotCreated: created}

	var (
		mu   sync.Mutex
		errs []error
	)
	work := make(chan tableInfo)
	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Worker connection: the anchor conn itself when serial, else a
			// fresh conn adopting the exported snapshot.
			wconn := conn
			if concurrency > 1 {
				var err error
				wconn, err = openWorkerConn(ctx, cfg.QueryDSN, snapshotID)
				if err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
					return
				}
				defer func() {
					_, _ = wconn.Exec(context.Background(), "ROLLBACK")
					wconn.Close(context.Background())
				}()
			}
			for t := range work {
				if ctx.Err() != nil {
					return
				}
				n, skipped, err := processTable(ctx, wconn, t, cfg.OutputDir, tsDir, tsStr, uint64(anchorLSN), compression, rowGroupSize, cfg.Retry, logger)
				mu.Lock()
				if err != nil {
					logger.Error("pgbaseline: failed to process table",
						"schema", t.Schema, "table", t.Table, "error", err)
					errs = append(errs, fmt.Errorf("%s.%s: %w", t.Schema, t.Table, err))
				} else {
					stats.TablesProcessed++
					stats.FilesWritten++
					stats.RowsWritten += n
					if !skipped {
						logger.Info("pgbaseline: table complete",
							"schema", t.Schema, "table", t.Table, "rows", n)
					}
				}
				mu.Unlock()
			}
		}()
	}
	for _, t := range tables {
		work <- t
	}
	close(work)
	wg.Wait()

	// Every early return below leaves the _INCOMPLETE marker in place; only
	// full success publishes the snapshot (manifest, then _SUCCESS) — exactly
	// baseline.Run's close-out discipline.
	if err := ctx.Err(); err != nil {
		return stats, err
	}
	if len(errs) > 0 {
		if len(errs) > 1 {
			return stats, fmt.Errorf("pgbaseline: %d of %d tables failed (others logged); first: %w", len(errs), len(tables), errs[0])
		}
		return stats, errs[0]
	}
	if err := baselineintegrity.WriteManifest(snapDir); err != nil {
		return stats, fmt.Errorf("pgbaseline: snapshot complete but could not write integrity manifest: %w", err)
	}
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		return stats, fmt.Errorf("pgbaseline: snapshot complete but could not write %s marker: %w", baseline.SuccessMarker, err)
	}
	return stats, nil
}

// openWorkerConn opens a connection whose transaction adopts the exported
// snapshot, so a parallel worker sees exactly the anchor transaction's data.
func openWorkerConn(ctx context.Context, dsn, snapshotID string) (*pgx.Conn, error) {
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgbaseline: connect parallel worker: %w", err)
	}
	if _, err := c.Exec(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY"); err != nil {
		c.Close(context.Background())
		return nil, fmt.Errorf("pgbaseline: begin worker transaction: %w", err)
	}
	// The snapshot ID comes from pg_export_snapshot(), not user input, but is
	// still quoted through a literal escape for defense in depth.
	if _, err := c.Exec(ctx, "SET TRANSACTION SNAPSHOT '"+strings.ReplaceAll(snapshotID, "'", "''")+"'"); err != nil {
		c.Close(context.Background())
		return nil, fmt.Errorf("pgbaseline: adopt exported snapshot %q: %w", snapshotID, err)
	}
	return c, nil
}

// processTable COPYs one table into its Parquet file. Returns rows written and
// whether the table was retry-skipped.
func processTable(ctx context.Context, conn *pgx.Conn, t tableInfo, outputDir, tsDir, tsStr string, anchorLSN uint64, compression string, rowGroupSize int, retry bool, logger *slog.Logger) (int64, bool, error) {
	outPath := filepath.Join(outputDir, tsDir, t.Schema, t.Table+".parquet")

	if retry {
		if fi, err := os.Stat(outPath); err == nil && fi.Size() > 0 {
			logger.Info("pgbaseline: skipping existing file (--retry)",
				"schema", t.Schema, "table", t.Table, "file", outPath)
			return 0, true, nil
		}
	}

	md := map[string]string{
		"bintrail.snapshot_timestamp": tsStr,
		"bintrail.source_database":    t.Schema,
		"bintrail.source_table":       t.Table,
		"bintrail.bintrail_version":   baseline.Version,
		// The LSN anchor (#593 slice A): deltas for this table start strictly
		// after this point. MetaKeyCreateTableSQL is deliberately absent — it
		// exists for full-table mydumper reconstruct, out of scope for PG.
		baseline.MetaKeyLSN: strconv.FormatUint(anchorLSN, 10),
	}

	cols := make([]baseline.Column, len(t.Columns))
	for i, name := range t.Columns {
		cols[i] = baseline.Column{Name: name, RawText: true}
	}
	w, err := baseline.NewWriter(outPath, cols, baseline.WriterConfig{
		Compression:  compression,
		RowGroupSize: rowGroupSize,
		Metadata:     md,
	})
	if err != nil {
		return 0, false, fmt.Errorf("create writer: %w", err)
	}
	var closed bool
	defer func() {
		if !closed {
			_ = w.Close()
			if err := os.Remove(outPath); err != nil && !os.IsNotExist(err) {
				logger.Warn("pgbaseline: failed to remove partial file", "path", outPath, "error", err)
			}
		}
	}()

	sink := newCopyTextSink(len(t.Columns), func(values []string, nulls []bool) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return w.WriteRow(values, nulls)
	})

	// COPY (SELECT <explicit columns> ...) rather than COPY <table>:
	//   - a partitioned parent (relkind='p') rejects direct COPY but the
	//     SELECT form reads all partitions under the parent name;
	//   - the explicit column list pins catalog order and excludes generated
	//     columns (loadColumns), keeping the column set identical to deltas.
	// Identifiers are quoted with PostgreSQL rules via pgx.Identifier.
	colList := make([]string, len(t.Columns))
	for i, name := range t.Columns {
		colList[i] = pgx.Identifier{name}.Sanitize()
	}
	copySQL := fmt.Sprintf("COPY (SELECT %s FROM %s) TO STDOUT (FORMAT text)",
		strings.Join(colList, ", "), pgx.Identifier{t.Schema, t.Table}.Sanitize())
	if _, err := conn.PgConn().CopyTo(ctx, sink, copySQL); err != nil {
		return sink.rows, false, fmt.Errorf("copy %s.%s: %w", t.Schema, t.Table, err)
	}
	if err := sink.Flush(); err != nil {
		return sink.rows, false, fmt.Errorf("copy %s.%s (final row): %w", t.Schema, t.Table, err)
	}

	closed = true
	if err := w.Close(); err != nil {
		os.Remove(outPath)
		return sink.rows, false, fmt.Errorf("close writer: %w", err)
	}
	return sink.rows, false, nil
}
