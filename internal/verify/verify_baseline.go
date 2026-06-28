package verify

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
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
		GTID: fmt.Sprintf("%s:%d", p.NewAnchor.File, p.NewAnchor.Pos)}

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
	if p.NewAnchor.File == "" {
		return inconclusive(res, "new baseline has no recorded binlog anchor (pre-#633 baseline); cannot bound the reconstruction"), nil
	}

	// Non-generated columns in ordinal order. Using IsGenerated is safe here:
	// both sides hash the same set, so an over/under-inclusion is symmetric and
	// cannot produce a false mismatch (unlike the live-source path, where the
	// source and reconstruct derived the set independently).
	orderedCols := make([]metadata.ColumnMeta, 0, len(tm.Columns))
	for _, c := range tm.Columns {
		if c.IsGenerated {
			continue
		}
		orderedCols = append(orderedCols, c)
	}

	// Latest event per PK in (prev snapshot, new anchor]. Lower bound by the
	// previous snapshot time (the baseline supersedes older events); upper bound
	// by the exact binlog anchor (#641) so the reconstruction lands precisely on
	// the new baseline's point.
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
	changes := make(map[string]*query.ResultRow, len(rows))
	for i := range rows {
		changes[rows[i].PKValues] = &rows[i]
	}

	// Recovery side: reconstruct the previous baseline forward to the anchor.
	reconDigest, reconCount, err := reconstructDigest(ctx, p.PrevPath, p.Schema, p.Table, pkCols, changes, orderedCols)
	if err != nil {
		return res, fmt.Errorf("reconstruct prev %s.%s: %w", p.Schema, p.Table, err)
	}
	// Truth side: the new baseline as-is (no events), via the same path.
	newDigest, newCount, err := reconstructDigest(ctx, p.NewPath, p.Schema, p.Table, pkCols, map[string]*query.ResultRow{}, orderedCols)
	if err != nil {
		return res, fmt.Errorf("read new baseline %s.%s: %w", p.Schema, p.Table, err)
	}

	res.SourceDigest = newDigest // "truth" = the fresh baseline
	res.SourceRows = newCount
	res.ReconstructDigest = reconDigest
	res.ReconstructRows = reconCount

	deferredRepr := hasDeferredRepr(orderedCols) && len(changes) > 0
	res.Status, res.Detail = classify(newDigest, newCount, reconDigest, reconCount, deferredRepr)
	return res, nil
}

// FindBaselinePair builds the verifiable table pairs from the two most recent
// baseline snapshots under source: for every table present in both, the prev +
// new Parquet paths, the prev snapshot time, and the new baseline's binlog
// anchor (read from its Parquet metadata). Returns nil (nothing to verify) when
// fewer than two snapshots exist — the first baseline has no predecessor.
func FindBaselinePair(ctx context.Context, source string) ([]BaselinePair, error) {
	files, err := reconstruct.ListBaselines(ctx, source)
	if err != nil {
		return nil, err
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
		return nil, nil // fewer than two snapshots: nothing to verify yet
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

	var pairs []BaselinePair
	for key, nf := range newByTable {
		pf, ok := prevByTable[key]
		if !ok {
			continue // table is new since the previous snapshot: no predecessor image
		}
		meta, err := baseline.ReadParquetMetadataAny(ctx, nf.Path)
		if err != nil {
			return nil, fmt.Errorf("read new baseline metadata %s: %w", nf.Path, err)
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
	return pairs, nil
}
