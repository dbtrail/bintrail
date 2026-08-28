package icebergexport

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/table"
	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// Config drives one export run.
type Config struct {
	IndexDSN    string
	BaselineSrc string   // local directory or s3:// prefix of baseline snapshots
	Warehouse   string   // local directory the Iceberg tables live under
	Tables      []string // schema.table entries
	At          time.Time

	// FetchBatchSize is the event page size (0 = query.DefaultStreamBatchSize).
	FetchBatchSize int
	// LoadBatchRows is how many baseline rows form one Arrow batch on the
	// first load (0 = defaultLoadBatchRows).
	LoadBatchRows int
	// ArchiveFetcher reads archived events; nil = parquetquery.Fetch.
	ArchiveFetcher query.ArchiveFetcher
	// DuckDBTuning bounds the baseline scan; the zero value is DefaultTuning.
	DuckDBTuning duckdbutil.Tuning
}

const defaultLoadBatchRows = 50_000

// Verdict is one table's outcome.
type Verdict string

const (
	VerdictLoaded     Verdict = "loaded"      // first load committed (deltas since the anchor folded in the same run)
	VerdictExported   Verdict = "exported"    // deltas committed
	VerdictUnchanged  Verdict = "unchanged"   // no events since the cursor
	VerdictRefusedGap Verdict = "refused-gap" // a capture gap or unarchived rotated hours inside the window
	VerdictRefusedDDL Verdict = "refused-ddl" // the table changed shape, or a destructive DDL sits in the window
	VerdictRefused    Verdict = "refused"     // any other refusal; Detail says which
	VerdictSkipped    Verdict = "skipped"     // the run ended before this table was reached
)

// OK reports whether the verdict left the table current.
func (v Verdict) OK() bool {
	return v == VerdictLoaded || v == VerdictExported || v == VerdictUnchanged
}

// Outcome is one table's result.
type Outcome struct {
	Schema, Table string
	Verdict       Verdict
	Detail        string
	Err           error

	RowsLoaded int64
	Events     int64
	Upserts    int64
	Deletes    int64
	SnapshotID int64
	Cursor     string
	Location   string
}

// deps is what every table run shares.
type deps struct {
	cfg      Config
	db       *sql.DB
	dbName   string
	engine   *query.Engine
	resolver *metadata.Resolver
	fetcher  query.ArchiveFetcher
	cat      *hadoop.Catalog
	mem      memory.Allocator
}

// Run exports every configured table. The returned error is a run-level
// failure (nothing was attempted); per-table refusals are Outcomes.
func Run(ctx context.Context, cfg Config) ([]Outcome, error) {
	if cfg.IndexDSN == "" {
		return nil, errors.New("index DSN is required")
	}
	if cfg.BaselineSrc == "" {
		return nil, errors.New("a baseline source is required")
	}
	if cfg.Warehouse == "" {
		return nil, errors.New("a warehouse directory is required")
	}
	if len(cfg.Tables) == 0 {
		return nil, errors.New("no tables to export")
	}
	if cfg.At.IsZero() {
		cfg.At = time.Now().UTC()
	}
	cfg.At = cfg.At.UTC()

	cat, release, err := openWarehouse(ctx, cfg.Warehouse)
	if err != nil {
		return nil, err
	}
	defer release()

	db, err := config.Connect(cfg.IndexDSN)
	if err != nil {
		return nil, fmt.Errorf("connect to index: %w", err)
	}
	defer db.Close()
	// Every reader of binlog_events runs the migration first so the SELECT
	// list matches the schema (the #699 columns); this is the CLI-typed DSN,
	// where that one DDL belongs.
	if err := indexer.EnsureSchema(db); err != nil {
		return nil, fmt.Errorf("ensure index schema: %w", err)
	}
	parsed, err := drivermysql.ParseDSN(cfg.IndexDSN)
	if err != nil {
		return nil, fmt.Errorf("parse index DSN: %w", err)
	}
	if err := refuseMultiSource(ctx, db); err != nil {
		return nil, err
	}
	resolver, err := metadata.NewResolver(db, 0)
	if err != nil {
		return nil, fmt.Errorf("load schema snapshot: %w", err)
	}
	fetcher := cfg.ArchiveFetcher
	if fetcher == nil {
		fetcher = parquetquery.Fetch
	}
	d := &deps{
		cfg:      cfg,
		db:       db,
		dbName:   parsed.DBName,
		engine:   query.New(db),
		resolver: resolver,
		fetcher:  fetcher,
		cat:      cat,
		mem:      memory.DefaultAllocator,
	}

	outcomes := make([]Outcome, 0, len(cfg.Tables))
	for _, entry := range cfg.Tables {
		schema, tbl, ok := strings.Cut(entry, ".")
		if !ok || schema == "" || tbl == "" {
			outcomes = append(outcomes, Outcome{Schema: schema, Table: tbl, Verdict: VerdictRefused,
				Detail: fmt.Sprintf("table entry %q must be schema.table", entry)})
			continue
		}
		if ctx.Err() != nil {
			outcomes = append(outcomes, Outcome{Schema: schema, Table: tbl, Verdict: VerdictSkipped,
				Detail: "the run ended before this table was reached"})
			continue
		}
		outcomes = append(outcomes, d.runTable(ctx, schema, tbl))
	}
	return outcomes, nil
}

// refuseMultiSource refuses an index that holds more than one source.
// Nothing downstream of archive_state scopes events by source (live
// partitions carry no per-row source column at all), so two sources with the
// same schema.table would interleave in one Iceberg table under one key
// space. Refusing is the only honest answer until events can be attributed.
func refuseMultiSource(ctx context.Context, db *sql.DB) error {
	var n int64
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM bintrail_servers`).Scan(&n)
	if err != nil {
		if strings.Contains(err.Error(), "doesn't exist") || strings.Contains(err.Error(), "1146") {
			return nil
		}
		return fmt.Errorf("count index sources: %w", err)
	}
	if n > 1 {
		return fmt.Errorf("the index holds %d sources (bintrail_servers), and the export cannot attribute events to one of them; export from a single-source index", n)
	}
	return nil
}

// classify turns a per-table error into its verdict, on the sentinels the
// reconstruct package exports rather than on message text.
func classify(err error) Verdict {
	var gap *query.GapError
	switch {
	case errors.Is(err, reconstruct.ErrCaptureGap), errors.As(err, &gap):
		return VerdictRefusedGap
	case errors.Is(err, reconstruct.ErrSchemaChanged), errors.Is(err, reconstruct.ErrDestructiveDDL):
		return VerdictRefusedDDL
	default:
		return VerdictRefused
	}
}

func refusal(schema, tbl string, err error) Outcome {
	return Outcome{Schema: schema, Table: tbl, Verdict: classify(err), Detail: err.Error(), Err: err}
}

// runTable runs one table end to end: first load when the table has no
// cursor, then the deltas from the cursor to the run's cut.
func (d *deps) runTable(ctx context.Context, schema, tbl string) Outcome {
	tm, err := d.resolver.Resolve(schema, tbl)
	if err != nil {
		return refusal(schema, tbl, fmt.Errorf("resolve %s.%s in the schema snapshot: %w", schema, tbl, err))
	}
	pkCols := tm.PKColumnMetas()
	if len(pkCols) == 0 {
		return refusal(schema, tbl, fmt.Errorf("%s.%s has no primary key; the export needs one to name rows in equality deletes", schema, tbl))
	}
	if c, bad := reconstruct.FirstUnsupportedPKType(pkCols); bad {
		return refusal(schema, tbl, fmt.Errorf("%s.%s has a primary key column of type %s (%s), which the export cannot key rows by", schema, tbl, c.DataType, c.Name))
	}

	if err := ensureNamespace(ctx, d.cat, schema); err != nil {
		return refusal(schema, tbl, err)
	}
	ident := catalog.ToIdentifier(schema, tbl)
	icetbl, exists, err := loadTable(ctx, d.cat, ident)
	if err != nil {
		return refusal(schema, tbl, err)
	}
	var cur *cursor
	if exists {
		cur, err = readCursor(icetbl.Properties())
		if err != nil {
			return refusal(schema, tbl, err)
		}
	}

	out := Outcome{Schema: schema, Table: tbl}
	if cur == nil {
		var loaded int64
		icetbl, cur, loaded, err = d.firstLoad(ctx, schema, tbl, tm, pkCols, ident, icetbl)
		if err != nil {
			return refusal(schema, tbl, err)
		}
		out.RowsLoaded = loaded
		out.Verdict = VerdictLoaded
	}

	res, err := d.increment(ctx, schema, tbl, tm, pkCols, icetbl, cur)
	if err != nil {
		o := refusal(schema, tbl, err)
		o.RowsLoaded = out.RowsLoaded
		if out.Verdict == VerdictLoaded {
			o.Detail = fmt.Sprintf("the first load committed %d rows, then the deltas refused: %s", out.RowsLoaded, err.Error())
		}
		return o
	}
	out.Events, out.Upserts, out.Deletes = res.events, res.upserts, res.deletes
	out.SnapshotID = res.snapshotID
	out.Cursor = res.cursor.String()
	out.Location = res.location
	switch {
	case out.Verdict == VerdictLoaded:
		out.Detail = fmt.Sprintf("%d rows from the baseline, then %d events folded", out.RowsLoaded, res.events)
	case res.events == 0:
		out.Verdict = VerdictUnchanged
		out.Detail = "no events since the cursor"
	default:
		out.Verdict = VerdictExported
		out.Detail = fmt.Sprintf("%d events folded into %d upserts and %d deletes", res.events, res.upserts, res.deletes)
	}
	return out
}

// firstLoad seeds the Iceberg table from the newest baseline snapshot and
// stamps the baseline's binlog anchor as the cursor. The table is created
// here when the catalog has none; a table that exists without a cursor is a
// load that never committed, and is loaded again.
func (d *deps) firstLoad(ctx context.Context, schema, tbl string, tm *metadata.TableMeta, pkCols []metadata.ColumnMeta,
	ident table.Identifier, existing *table.Table) (*table.Table, *cursor, int64, error) {

	path, snapTime, stale, err := reconstruct.FindBaseline(ctx, d.cfg.BaselineSrc, schema, tbl, d.cfg.At)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("no usable baseline snapshot for %s.%s under %s: %w", schema, tbl, d.cfg.BaselineSrc, err)
	}
	if stale.Stale() {
		slog.Warn("iceberg export: seeding from an older snapshot", "schema", schema, "table", tbl, "detail", stale.Message)
	}
	meta, err := baseline.ReadParquetMetadataAny(ctx, path)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("read baseline metadata %s: %w", path, err)
	}
	if meta.BinlogFile == "" || meta.BinlogPos <= 0 {
		return nil, nil, 0, fmt.Errorf("baseline %s carries no binlog position, so the export cannot tell where its deltas start; take the snapshot with `bintrail dump` on a source that exposes the position (see docs/dump-and-baseline.md)", path)
	}
	if strings.TrimSpace(meta.CreateTableSQL) == "" {
		return nil, nil, 0, fmt.Errorf("baseline %s predates the embedded CREATE TABLE; take a new snapshot", path)
	}
	if err := reconstruct.CheckBaselineSchemaCurrent(meta.CreateTableSQL, tm, schema, tbl); err != nil {
		return nil, nil, 0, err
	}
	bcols, err := baseline.ParseSchemaText(meta.CreateTableSQL)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("parse the baseline's CREATE TABLE for %s.%s: %w", schema, tbl, err)
	}
	cols, err := buildColumns(bcols, tm.PKColumns)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("%s.%s: %w", schema, tbl, err)
	}

	icetbl := existing
	if icetbl == nil {
		icetbl, err = d.cat.CreateTable(ctx, ident, icebergSchema(cols), catalog.WithProperties(tableProperties()))
		if err != nil {
			return nil, nil, 0, fmt.Errorf("create Iceberg table %s.%s: %w", schema, tbl, err)
		}
	} else if err := sameColumns(icetbl.Schema(), cols); err != nil {
		return nil, nil, 0, fmt.Errorf("Iceberg table at %s exists without an export cursor and %w; remove the table directory to reload it", icetbl.Location(), err)
	}
	arrowSchema, err := table.SchemaToArrowSchema(icetbl.Schema(), nil, true, false)
	if err != nil {
		return nil, nil, 0, err
	}

	local, cleanup, err := reconstruct.MaterializeBaselineLocal(ctx, path, d.cfg.DuckDBTuning)
	if err != nil {
		return nil, nil, 0, err
	}
	defer cleanup()

	files, rows, err := d.writeBaselineRows(ctx, icetbl, arrowSchema, cols, pkCols, local)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("load baseline %s: %w", path, err)
	}

	cur := &cursor{File: meta.BinlogFile, Pos: uint64(meta.BinlogPos), At: snapTime.UTC()}
	tx := icetbl.NewTransaction()
	if len(files) > 0 {
		rd := tx.NewRowDelta(iceberg.Properties{
			summaryRowsLoaded:  strconv.FormatInt(rows, 10),
			summaryWindowUntil: cur.At.Format(time.RFC3339Nano),
		})
		rd.AddRows(files...)
		if err := rd.Commit(ctx); err != nil {
			return nil, nil, 0, fmt.Errorf("stage the first load of %s.%s: %w", schema, tbl, err)
		}
	}
	props := cur.properties()
	props[propBaseline] = path
	if err := tx.SetProperties(props); err != nil {
		return nil, nil, 0, err
	}
	icetbl, err = tx.Commit(ctx)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("commit the first load of %s.%s: %w", schema, tbl, err)
	}
	slog.Info("iceberg export: first load committed", "schema", schema, "table", tbl, "rows", rows, "cursor", cur.String(), "location", icetbl.Location())
	return icetbl, cur, rows, nil
}

// writeBaselineRows scans the baseline Parquet through DuckDB and writes it as
// Iceberg data files, batch by batch, so memory is bounded by the batch and
// not by the table. Primary key columns are canonicalized on the way so they
// spell exactly what the row events will (a fixed BINARY(n) key is trimmed of
// its storage padding, #1155): an equality delete only matches an equal value.
func (d *deps) writeBaselineRows(ctx context.Context, icetbl *table.Table, arrowSchema *arrow.Schema, cols []column,
	pkCols []metadata.ColumnMeta, local string) ([]iceberg.DataFile, int64, error) {

	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, 0, fmt.Errorf("open duckdb: %w", err)
	}
	defer ddb.Close()
	d.cfg.DuckDBTuning.Apply(ctx, ddb)

	q := "SELECT * FROM parquet_scan('" + strings.ReplaceAll(local, "'", "''") + "')"
	drows, err := ddb.QueryContext(ctx, q)
	if err != nil {
		return nil, 0, fmt.Errorf("duckdb baseline query: %w", err)
	}
	defer drows.Close()
	dcols, err := drows.Columns()
	if err != nil {
		return nil, 0, err
	}
	if err := sameNames(dcols, cols); err != nil {
		return nil, 0, fmt.Errorf("baseline Parquet columns %w", err)
	}

	batch := d.cfg.LoadBatchRows
	if batch <= 0 {
		batch = defaultLoadBatchRows
	}
	var rows int64
	var scanErr error
	seq := func(yield func(arrow.RecordBatch, error) bool) {
		app, err := newRowAppender(d.mem, arrowSchema, cols)
		if err != nil {
			yield(nil, err)
			return
		}
		defer app.release()
		scan := make([]any, len(dcols))
		ptrs := make([]any, len(dcols))
		for i := range scan {
			ptrs[i] = &scan[i]
		}
		for drows.Next() {
			if err := drows.Scan(ptrs...); err != nil {
				yield(nil, fmt.Errorf("scan baseline row: %w", err))
				return
			}
			row := make(map[string]any, len(dcols))
			for i, name := range dcols {
				row[name] = scan[i]
			}
			canon, err := reconstruct.CanonicalizePKMap(row, pkCols)
			if err != nil {
				yield(nil, fmt.Errorf("canonicalize baseline primary key: %w", err))
				return
			}
			for _, c := range pkCols {
				if v, ok := canon[c.Name]; ok {
					row[c.Name] = v
				}
			}
			if err := app.append(row); err != nil {
				yield(nil, err)
				return
			}
			rows++
			if app.n >= batch {
				if !yield(app.flush(), nil) {
					return
				}
			}
		}
		if err := drows.Err(); err != nil {
			yield(nil, err)
			return
		}
		if app.n > 0 {
			yield(app.flush(), nil)
		}
	}
	var files []iceberg.DataFile
	for df, err := range table.WriteRecords(ctx, icetbl, arrowSchema, seq) {
		if err != nil {
			scanErr = err
			break
		}
		files = append(files, df)
	}
	if scanErr != nil {
		return nil, 0, scanErr
	}
	return files, rows, nil
}

// incrementResult is what one delta window produced.
type incrementResult struct {
	events, upserts, deletes int64
	snapshotID               int64
	cursor                   cursor
	location                 string
}

// increment folds the events between cur and the run's cut into one commit.
//
// The window is positional on both ends: SincePos is the cursor (the previous
// run's cut, or the baseline anchor), UntilPos is this run's cut from
// ResolveSnapshotCut, and the two partition the binlog exactly, so no event is
// folded twice or missed. The time bounds beside them only prune partitions
// and archive files, the same pairing `baseline refresh` uses.
func (d *deps) increment(ctx context.Context, schema, tbl string, tm *metadata.TableMeta, pkCols []metadata.ColumnMeta,
	icetbl *table.Table, cur *cursor) (*incrementResult, error) {

	res := &incrementResult{cursor: *cur, location: icetbl.Location()}
	if snap := icetbl.CurrentSnapshot(); snap != nil {
		res.snapshotID = snap.SnapshotID
	}
	at := d.cfg.At
	if !at.After(cur.At) {
		return nil, fmt.Errorf("--at %s is not after the table's cursor (%s); the export only moves forward", at.Format(time.RFC3339), cur.String())
	}

	bcols := make(map[string]baseline.Column, len(tm.Columns))
	for _, c := range tm.Columns {
		bcols[strings.ToLower(c.Name)] = baseline.Column{Name: c.Name, MySQLType: c.DataType, Unsigned: strings.Contains(strings.ToLower(c.ColumnType), "unsigned")}
	}
	cols, err := columnsFromSchema(icetbl.Schema(), bcols)
	if err != nil {
		return nil, err
	}
	if err := sameTableColumns(icetbl.Schema(), tm, schema, tbl); err != nil {
		return nil, err
	}
	if err := reconstruct.CheckDestructiveDDL(ctx, d.db, schema, tbl, cur.At, at); err != nil {
		return nil, err
	}
	if _, err := reconstruct.CheckCaptureGapStatus(ctx, d.db, schema, tbl, cur.At, at, false); err != nil {
		return nil, err
	}
	cut, err := reconstruct.ResolveSnapshotCut(ctx, d.db, at)
	if err != nil {
		return nil, fmt.Errorf("resolve the binlog cut for %s: %w", at.Format(time.RFC3339), err)
	}
	if cut == nil || (cut.File == cur.File && cut.Pos == cur.Pos) {
		// No indexed event past the cursor: nothing to fold and nothing to
		// commit. The cursor stays, which is correct and cheap: the next run
		// re-checks the same empty window.
		return res, nil
	}

	since, until := cur.At, at
	opts := query.Options{
		Schema:   schema,
		Table:    tbl,
		Since:    &since,
		Until:    &until,
		SincePos: &query.BinlogPos{File: cur.File, Pos: cur.Pos},
		UntilPos: cut,
		Order:    "ASC",
	}
	dec := reconstruct.NewEventDecoder(d.db, schema, tbl, d.resolver)
	f := newFold(schema, tbl, pkCols)
	var foldErr error
	_, err = query.FetchMergedStream(ctx, d.db, d.engine, query.FetchMergedOptions{
		Opts:           opts,
		DBName:         d.dbName,
		NoArchive:      false,
		AllowGaps:      false,
		ArchiveFetcher: d.fetcher,
	}, d.cfg.FetchBatchSize, func(page []query.ResultRow) error {
		if len(page) == 0 {
			return nil
		}
		dec.DecodePage(page)
		if err := f.addPage(page); err != nil {
			foldErr = err
			return err
		}
		return nil
	})
	if err != nil {
		if foldErr != nil {
			return nil, foldErr
		}
		return nil, fmt.Errorf("fetch events for %s.%s: %w", schema, tbl, err)
	}
	if !dec.Typed() && hasBinaryOrText(tm) {
		return nil, fmt.Errorf("%s.%s: BLOB/TEXT values in the window could not be typed against a schema snapshot, so they would be written as their stored base64 text; run `bintrail snapshot` and retry", schema, tbl)
	}

	newCur := cursor{File: cut.File, Pos: cut.Pos, At: at}
	res.events = f.events
	ops := f.touched()
	for _, op := range ops {
		if op.Row != nil {
			res.upserts++
		} else {
			res.deletes++
		}
	}
	committed, err := writeDelta(ctx, d.mem, icetbl, cols, ops, iceberg.Properties{
		summaryEvents:      strconv.FormatInt(res.events, 10),
		summaryUpserts:     strconv.FormatInt(res.upserts, 10),
		summaryDeletes:     strconv.FormatInt(res.deletes, 10),
		summaryWindowSince: cur.String(),
		summaryWindowUntil: newCur.String(),
	}, newCur)
	if err != nil {
		return nil, fmt.Errorf("commit deltas for %s.%s: %w", schema, tbl, err)
	}
	res.cursor = newCur
	res.location = committed.Location()
	if snap := committed.CurrentSnapshot(); snap != nil {
		res.snapshotID = snap.SnapshotID
	}
	slog.Info("iceberg export: deltas committed", "schema", schema, "table", tbl,
		"events", res.events, "upserts", res.upserts, "deletes", res.deletes, "cursor", newCur.String())
	return res, nil
}

// writeDelta commits the net change of one window as ONE snapshot and moves
// the cursor in the same commit: an equality-delete file naming every touched
// key, a data file with the after-image of every key that still exists. With
// no ops it commits the cursor alone (no snapshot is added), so the next run
// starts where this one looked, not where the last change was.
//
// Both files share the commit's sequence number, and an equality delete only
// applies to data files with a strictly LOWER one: the delete removes the
// key's previous row from every earlier snapshot and cannot touch the row
// written beside it, which is what makes delete-plus-insert an update. It is
// also why ops must already be folded to one per key (fold.go): two rows for
// one key in the same data file would both survive.
func writeDelta(ctx context.Context, mem memory.Allocator, icetbl *table.Table, cols []column, ops []*netOp,
	summary iceberg.Properties, newCur cursor) (*table.Table, error) {

	tx := icetbl.NewTransaction()
	if len(ops) > 0 {
		arrowSchema, err := table.SchemaToArrowSchema(icetbl.Schema(), nil, true, false)
		if err != nil {
			return nil, err
		}
		delFiles, err := tx.WriteEqualityDeletes(ctx, pkFieldIDs(cols), deleteBatches(mem, arrowSchema, cols, ops))
		if err != nil {
			return nil, fmt.Errorf("write equality deletes: %w", err)
		}
		var dataFiles []iceberg.DataFile
		var writeErr error
		for df, err := range table.WriteRecords(ctx, icetbl, arrowSchema, upsertBatches(mem, arrowSchema, cols, ops)) {
			if err != nil {
				writeErr = err
				break
			}
			dataFiles = append(dataFiles, df)
		}
		if writeErr != nil {
			return nil, fmt.Errorf("write data files: %w", writeErr)
		}
		rd := tx.NewRowDelta(summary)
		rd.AddDeletes(delFiles...).AddRows(dataFiles...)
		if err := rd.Commit(ctx); err != nil {
			return nil, fmt.Errorf("stage the delta commit: %w", err)
		}
	}
	if err := tx.SetProperties(newCur.properties()); err != nil {
		return nil, err
	}
	return tx.Commit(ctx)
}

// deleteBatches yields the equality-delete rows: the key columns of every
// touched primary key, whether it still exists or not. A delete on a key that
// was never present matches nothing, which is why an INSERT needs no special
// case.
func deleteBatches(mem memory.Allocator, arrowSchema *arrow.Schema, cols []column, ops []*netOp) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		var fields []arrow.Field
		var pk []column
		for i, c := range cols {
			if c.PK {
				fields = append(fields, arrowSchema.Field(i))
				pk = append(pk, c)
			}
		}
		delSchema := arrow.NewSchema(fields, nil)
		app, err := newRowAppender(mem, delSchema, pk)
		if err != nil {
			yield(nil, err)
			return
		}
		defer app.release()
		for _, op := range ops {
			if err := app.append(op.PK); err != nil {
				yield(nil, fmt.Errorf("equality delete key: %w", err))
				return
			}
			if app.n >= defaultLoadBatchRows {
				if !yield(app.flush(), nil) {
					return
				}
			}
		}
		if app.n > 0 {
			yield(app.flush(), nil)
		}
	}
}

// upsertBatches yields the after-image of every key that exists at the cut.
func upsertBatches(mem memory.Allocator, arrowSchema *arrow.Schema, cols []column, ops []*netOp) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		app, err := newRowAppender(mem, arrowSchema, cols)
		if err != nil {
			yield(nil, err)
			return
		}
		defer app.release()
		for _, op := range ops {
			if op.Row == nil {
				continue
			}
			if err := app.append(op.Row); err != nil {
				yield(nil, err)
				return
			}
			if app.n >= defaultLoadBatchRows {
				if !yield(app.flush(), nil) {
					return
				}
			}
		}
		if app.n > 0 {
			yield(app.flush(), nil)
		}
	}
}

// sameNames checks that a Parquet column list and the export columns name the
// same set, case-insensitively.
func sameNames(names []string, cols []column) error {
	have := make(map[string]bool, len(names))
	for _, n := range names {
		have[strings.ToLower(n)] = true
	}
	want := make(map[string]bool, len(cols))
	for _, c := range cols {
		want[strings.ToLower(c.Name)] = true
	}
	var missing, extra []string
	for n := range want {
		if !have[n] {
			missing = append(missing, n)
		}
	}
	for n := range have {
		if !want[n] {
			extra = append(extra, n)
		}
	}
	if len(missing) == 0 && len(extra) == 0 {
		return nil
	}
	return fmt.Errorf("differ from its CREATE TABLE (missing: %v, unexpected: %v)", missing, extra)
}

// sameColumns checks an existing Iceberg schema against the export columns.
func sameColumns(sc *iceberg.Schema, cols []column) error {
	names := make([]string, 0, len(sc.Fields()))
	for _, f := range sc.Fields() {
		names = append(names, f.Name)
	}
	if err := sameNames(names, cols); err != nil {
		return fmt.Errorf("its columns %w", err)
	}
	return nil
}

// sameTableColumns refuses, with ErrSchemaChanged, when the current schema
// snapshot has columns the Iceberg table lacks or the reverse. Schema
// evolution is a later slice; a table exported under the old shape and then
// fed events of the new one would be wrong in every row the ALTER touched.
func sameTableColumns(sc *iceberg.Schema, tm *metadata.TableMeta, schema, tbl string) error {
	inTable := make(map[string]bool, len(sc.Fields()))
	for _, f := range sc.Fields() {
		inTable[strings.ToLower(f.Name)] = true
	}
	current := make(map[string]bool, len(tm.Columns))
	for _, c := range tm.Columns {
		if c.IsGenerated {
			continue
		}
		current[strings.ToLower(c.Name)] = true
	}
	var added, dropped []string
	for n := range current {
		if !inTable[n] {
			added = append(added, n)
		}
	}
	for n := range inTable {
		if !current[n] {
			dropped = append(dropped, n)
		}
	}
	if len(added) == 0 && len(dropped) == 0 {
		return nil
	}
	return fmt.Errorf("%s.%s changed shape since it was exported (added: %v, gone: %v); the export does not evolve the Iceberg schema yet, so remove the table directory to reload it from a fresh baseline: %w",
		schema, tbl, added, dropped, reconstruct.ErrSchemaChanged)
}

// hasBinaryOrText reports whether the table has a column the capture stores
// base64-encoded, i.e. one whose value depends on the epoch decoder having
// resolved.
func hasBinaryOrText(tm *metadata.TableMeta) bool {
	for _, c := range tm.Columns {
		switch strings.ToLower(c.DataType) {
		case "tinytext", "text", "mediumtext", "longtext",
			"tinyblob", "blob", "mediumblob", "longblob",
			"binary", "varbinary":
			return true
		}
	}
	return false
}
