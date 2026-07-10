package verify

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// BaselineConfig wires the dependencies for baseline-anchored verify (#642): the
// index (for binlog events) and a schema resolver. There is deliberately no live
// source — both sides of the comparison are at-rest baselines, which is what
// makes it drift-free.
type BaselineConfig struct {
	IndexDB        *sql.DB
	Resolver       *metadata.Resolver
	IndexDBName    string
	NoArchive      bool
	ArchiveFetcher query.ArchiveFetcher
}

// BaselinePair is one table's previous + new baseline, with the new baseline's
// recorded binlog anchor and the previous baseline's snapshot time — everything
// VerifyBaselinePair needs to reconstruct prev→anchor and compare to new.
type BaselinePair struct {
	Schema, Table string
	PrevPath      string
	NewPath       string
	PrevSnapshot  time.Time
	NewSnapshot   time.Time // new baseline's snapshot time — the coarse time bound paired with NewAnchor
	NewAnchor     query.BinlogPos
}

// VerifyBaselinePair proves, drift-free, that the recovery chain reproduces a
// fresh baseline. It reconstructs the previous baseline forward to the new
// baseline's exact anchor G (baseline + binlog events up to G) and compares the
// resulting content digest to the new baseline's own content digest.
//
// BOTH digests are produced by reconstructDigest over the SAME column set, so
// they are byte-comparable by construction — immune to the column-set mismatch
// the live-source path had to guard (the new-baseline digest is recomputed here,
// not taken from #633's persisted value). Neither side reads the live source, so
// there is no snapshot drift, no off-peak requirement, and no production impact.
func VerifyBaselinePair(ctx context.Context, cfg BaselineConfig, p BaselinePair) (TableResult, error) {
	res := TableResult{Schema: p.Schema, Table: p.Table,
		Anchor: fmt.Sprintf("%s:%d", p.NewAnchor.File, p.NewAnchor.Pos)}

	tm, err := cfg.Resolver.Resolve(p.Schema, p.Table)
	if err != nil {
		return res, fmt.Errorf("resolve %s.%s: %w", p.Schema, p.Table, err)
	}
	pkCols := tm.PKColumnMetas()
	if len(pkCols) == 0 {
		return inconclusive(res, "table has no primary key"), nil
	}
	for _, c := range pkCols {
		if !reconstruct.SupportedPKType(c.DataType) {
			return inconclusive(res, fmt.Sprintf("primary-key column %q has type %q unsupported by the baseline canonicalizer", c.Name, c.DataType)), nil
		}
	}
	// A real baseline anchor is never at binlog position 0; an empty file or a
	// zero position means the anchor wasn't recorded (pre-#633) or its metadata
	// is corrupt (the reader keeps the file but zeroes an unparseable position).
	// Bounding the reconstruction at position 0 would cut it short and could pass
	// a window that touched no rows as a (false) match — refuse instead.
	if p.NewAnchor.File == "" || p.NewAnchor.Pos == 0 {
		return inconclusive(res, "new baseline has no usable binlog anchor (missing or zero position); cannot bound the reconstruction"), nil
	}
	if !p.PrevSnapshot.Before(p.NewSnapshot) {
		return inconclusive(res, "baseline pair is not in prev→new order (prev snapshot is not before new)"), nil
	}

	// Hash exactly the columns the baseline Parquet holds. mydumper excludes true
	// STORED/VIRTUAL generated columns but keeps ordinary DEFAULT_GENERATED ones
	// (created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, …), so the Parquet schema is
	// the correct set. Deriving it from the snapshot's is_generated flag would hit
	// the DEFAULT_GENERATED trap (consistency.ConsistentTableChecksum's comment)
	// and silently drop those real columns from the fingerprint — under-verifying
	// them on BOTH sides, a false-match exposure.
	colNames, err := reconstruct.ReadBaselineColumns(ctx, p.NewPath)
	if err != nil {
		return res, fmt.Errorf("read new baseline columns %s.%s: %w", p.Schema, p.Table, err)
	}
	colByName := make(map[string]metadata.ColumnMeta, len(tm.Columns))
	for _, c := range tm.Columns {
		colByName[c.Name] = c
	}
	orderedCols := make([]metadata.ColumnMeta, 0, len(colNames))
	for _, name := range colNames {
		cm, ok := colByName[name]
		if !ok {
			return inconclusive(res, fmt.Sprintf("baseline column %q is absent from the index schema snapshot; re-run bintrail snapshot", name)), nil
		}
		orderedCols = append(orderedCols, cm)
	}

	// Latest event per PK in (prev snapshot, new anchor]. Lower bound by the
	// previous snapshot's wall-clock TIME (the baseline supersedes older events);
	// upper bound by the exact binlog anchor (#641) so the reconstruction lands
	// precisely on the new baseline's point.
	//
	// The lower bound is the prev snapshot's directory timestamp, NOT its exact
	// binlog anchor (a position lower bound is a follow-up). So a reported
	// MISMATCH could in principle be a lower-bound artifact — a change committed
	// between the prev baseline's true anchor and its directory timestamp. A
	// reported MATCH is unaffected (a too-wide lower bound can only add a
	// superseded older event, never drop a real one).
	engine := query.New(cfg.IndexDB)
	rows, _, err := query.FetchMerged(ctx, cfg.IndexDB, engine, query.FetchMergedOptions{
		Opts: query.Options{
			Schema:     p.Schema,
			Table:      p.Table,
			Since:      &p.PrevSnapshot,
			Until:      &p.NewSnapshot, // coarse time bound: partition pruning + gap planner
			UntilPos:   &p.NewAnchor,   // exact cut at the new baseline's binlog point
			LimitPerPK: 1,
		},
		DBName:         cfg.IndexDBName,
		NoArchive:      cfg.NoArchive,
		ArchiveFetcher: cfg.ArchiveFetcher,
	})
	if err != nil {
		var gap *query.GapError
		if errors.As(err, &gap) {
			return inconclusive(res, "coverage gap in the reconstruction window: "+gap.Error()), nil
		}
		return res, fmt.Errorf("fetch changes %s.%s: %w", p.Schema, p.Table, err)
	}
	// BLOB/TEXT base64 → real value, epoch-aware (#672). See verify.go's
	// VerifyTable for the same wiring and its rationale.
	reconstruct.DecodeEventBinaries(cfg.IndexDB, p.Schema, p.Table, rows)
	changes := make(map[string]*query.ResultRow, len(rows))
	for i := range rows {
		changes[rows[i].PKValues] = &rows[i]
	}

	// Recovery side: reconstruct the previous baseline forward to the anchor.
	// renderCellNormalized (not plain renderCell): both operands here come
	// from this package, so canonicalizing a JSON object/array value the SAME
	// way on both sides closes the false-mismatch gap a TEXT/JSON column's
	// event-image round-trip can otherwise open (see its doc comment) without
	// risking the live-source comparison, which this function is not used for.
	reconDigest, reconCount, err := reconstructDigest(ctx, p.PrevPath, p.Schema, p.Table, pkCols, changes, rows, orderedCols, renderCellNormalized)
	if err != nil {
		return res, fmt.Errorf("reconstruct prev %s.%s: %w", p.Schema, p.Table, err)
	}
	// Truth side: the new baseline as-is (no events), via the same path.
	newDigest, newCount, err := reconstructDigest(ctx, p.NewPath, p.Schema, p.Table, pkCols, map[string]*query.ResultRow{}, nil, orderedCols, renderCellNormalized)
	if err != nil {
		return res, fmt.Errorf("read new baseline %s.%s: %w", p.Schema, p.Table, err)
	}

	res.SourceDigest = newDigest // "truth" = the fresh baseline
	res.SourceRows = newCount
	res.ReconstructDigest = reconDigest
	res.ReconstructRows = reconCount

	res.Status, res.Detail = classify(newDigest, newCount, reconDigest, reconCount, deferredReprChanged(orderedCols, changes))
	return res, nil
}

// deferredReprChanged reports whether an ENUM/SET/JSON/binary column was actually
// changed by an event in the window — the only case where the event-image renders
// differently than the baseline reads it (ordinal vs label, base64 vs raw bytes,
// canonical JSON). Gating on actual participation, not merely "the table contains
// such a column and saw any change", keeps a real divergence on an unrelated
// non-deferred column reportable as a mismatch instead of masking it inconclusive.
func deferredReprChanged(cols []metadata.ColumnMeta, changes map[string]*query.ResultRow) bool {
	deferred := make(map[string]bool)
	for _, c := range cols {
		if isDeferredType(c.DataType) {
			deferred[c.Name] = true
		}
	}
	if len(deferred) == 0 {
		return false
	}
	for _, ev := range changes {
		switch ev.EventType {
		case event.EventInsert:
			return true // an insert sets every column, including the deferred one
		case event.EventUpdate:
			for _, col := range ev.ChangedColumns {
				if deferred[col] {
					return true
				}
			}
		}
		// EventDelete removes the row from both sides — no value is rendered, so
		// it cannot introduce a representation difference.
	}
	return false
}

// AnyBaseline reports whether at least one complete baseline snapshot exists
// under source. The CLI uses it on the "nothing to verify" path to tell two
// physically different causes apart: a misconfigured or empty source (no
// baselines at all — a broken baseline job → fail loud) from a legitimate first
// run (exactly one baseline, no predecessor yet → genuinely nothing to compare).
// FindBaselinePair collapses both into nil, nil, nil, so the distinction has to
// be recovered here.
// EverBaselinedTables returns the set of "schema.table" keys that appear in
// AT LEAST ONE baseline snapshot under source, at ANY snapshot time — not
// just the two most recent ones FindBaselinePair uses to build its
// pairs/unpaired/prevOnly sets. A table absent from that top-2 window but
// present here still has an older snapshot on disk/S3: reconstruct.FindBaseline
// (the function `bintrail reconstruct` and the shim's `_snapshot` actually use)
// will fall back to it and return a StaleWarning, so the table IS recoverable
// via reconstruct — just not verifiable against the current two-snapshot
// window. Callers use this to distinguish that from a table with zero
// baselines at any point in time, which reconstruct genuinely cannot serve.
func EverBaselinedTables(ctx context.Context, source string) (map[string]bool, error) {
	files, err := reconstruct.ListBaselines(ctx, source)
	if err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(files))
	for _, f := range files {
		out[f.Schema+"."+f.Table] = true
	}
	return out, nil
}

// AnyBaseline reports whether at least one complete baseline snapshot exists
// under source. The CLI uses it on the "nothing to verify" path to tell two
// physically different causes apart: a misconfigured or empty source (no
// baselines at all — a broken baseline job → fail loud) from a legitimate first
// run (exactly one baseline, no predecessor yet → genuinely nothing to compare).
// FindBaselinePair collapses both into nil, nil, nil, so the distinction has to
// be recovered here.
func AnyBaseline(ctx context.Context, source string) (bool, error) {
	files, err := reconstruct.ListBaselines(ctx, source)
	if err != nil {
		return false, err
	}
	return len(files) > 0, nil
}

// FindBaselinePair builds the verifiable table pairs from the two most recent
// baseline snapshots under source: for every table present in both, the prev +
// new Parquet paths, the prev snapshot time, and the new baseline's binlog
// anchor (read from its Parquet metadata).
//
// It also surfaces the two asymmetric "can't pair" sets so the caller reports
// them instead of silently dropping either — an operator must be able to tell
// "verified" from "not present to verify":
//   - unpaired: present in the NEW snapshot, absent from the prev (new since the
//     prev snapshot — no predecessor image to reconstruct from).
//   - prevOnly: present in the PREV snapshot, absent from the new. Either a table
//     dropped between the snapshots, or the newest baseline was a subset (e.g.
//     "bintrail baseline --tables") that didn't re-snapshot it. Without this the
//     table would produce no report row at all on a default all-tables run — a
//     silent omission that could let "recovery verified" hide untouched tables.
//
// Returns nil, nil, nil, nil (nothing to verify) when fewer than two snapshots
// exist.
func FindBaselinePair(ctx context.Context, source string) (pairs []BaselinePair, unpaired, prevOnly []query.SchemaTable, err error) {
	files, err := reconstruct.ListBaselines(ctx, source)
	if err != nil {
		return nil, nil, nil, err
	}
	// files are newest-snapshot-first; find the two most recent distinct times.
	var tNew, tPrev time.Time
	for _, f := range files {
		if tNew.IsZero() {
			tNew = f.SnapshotTime
			continue
		}
		if !f.SnapshotTime.Equal(tNew) {
			tPrev = f.SnapshotTime
			break
		}
	}
	if tNew.IsZero() || tPrev.IsZero() {
		return nil, nil, nil, nil // fewer than two snapshots: nothing to verify yet
	}

	newByTable := map[string]reconstruct.BaselineFile{}
	prevByTable := map[string]reconstruct.BaselineFile{}
	for _, f := range files {
		key := f.Schema + "." + f.Table
		switch {
		case f.SnapshotTime.Equal(tNew):
			newByTable[key] = f
		case f.SnapshotTime.Equal(tPrev):
			prevByTable[key] = f
		}
	}

	for key, nf := range newByTable {
		pf, ok := prevByTable[key]
		if !ok {
			// New since the previous snapshot: no predecessor image to compare.
			unpaired = append(unpaired, query.SchemaTable{Schema: nf.Schema, Table: nf.Table})
			continue
		}
		meta, err := baseline.ReadParquetMetadataAny(ctx, nf.Path)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("read new baseline metadata %s: %w", nf.Path, err)
		}
		pairs = append(pairs, BaselinePair{
			Schema:       nf.Schema,
			Table:        nf.Table,
			PrevPath:     pf.Path,
			NewPath:      nf.Path,
			PrevSnapshot: tPrev,
			NewSnapshot:  tNew,
			NewAnchor:    query.BinlogPos{File: meta.BinlogFile, Pos: uint64(meta.BinlogPos)},
		})
	}
	// Symmetric to unpaired: tables in the prev snapshot the new one no longer
	// carries. Reported, never silently dropped.
	for key, pf := range prevByTable {
		if _, ok := newByTable[key]; ok {
			continue
		}
		prevOnly = append(prevOnly, query.SchemaTable{Schema: pf.Schema, Table: pf.Table})
	}
	sortSchemaTables(unpaired)
	sortSchemaTables(prevOnly)
	return pairs, unpaired, prevOnly, nil
}

func sortSchemaTables(s []query.SchemaTable) {
	sort.Slice(s, func(i, j int) bool {
		if s[i].Schema != s[j].Schema {
			return s[i].Schema < s[j].Schema
		}
		return s[i].Table < s[j].Table
	})
}
