package verify

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// maxExplainRows caps how many differing rows the drill-down prints in detail.
// It runs only on an already-flagged mismatch (rare, opt-in via --explain); a
// pathological all-rows-differ table would otherwise dump the whole table. The
// total count is still reported, so nothing is silently hidden.
const maxExplainRows = 100

const (
	diffChanged = "changed" // both sides have the PK; some columns differ
	diffMissing = "missing" // in the new baseline, not reproduced by the recovery
	diffExtra   = "extra"   // reconstructed by the recovery, absent from the new baseline
)

// CellDiff is one column whose reconstructed value diverged from the new
// baseline's, rendered in the SAME canonical text form the content digest
// compares — so a reported diff is exactly what made the digests differ.
type CellDiff struct {
	Column   string
	Recovery string // reconstruct(prev baseline + binlog → anchor)
	Baseline string // the new baseline (the "truth" side)
}

// RowDiff is one primary key whose reconstructed row differs from the new
// baseline. Kind is changed/missing/extra (see the diff* constants).
type RowDiff struct {
	PK    string
	Kind  string
	Cells []CellDiff // populated only for Kind == diffChanged
}

// MismatchExplanation is the row-level drill-down for one mismatched table: which
// primary keys diverged and how. It is the "which rows" the one-way content
// digest cannot give — recomputed from the SAME reconstructed row streams the
// digest used, so the diff and the verdict can never disagree. No live source,
// no scratch DB, no external tool: an in-memory full-outer-join on the PK.
type MismatchExplanation struct {
	Schema, Table, Anchor string
	Diffs                 []RowDiff
	Total                 int // total differing rows (Diffs is capped at maxExplainRows)
}

func (ex *MismatchExplanation) add(d RowDiff) {
	ex.Total++
	if len(ex.Diffs) < maxExplainRows {
		ex.Diffs = append(ex.Diffs, d)
	}
}

// rowCells is one reconstructed row reduced to its canonical per-column bytes
// (the digest's comparison form) plus a human-readable primary-key label.
type rowCells struct {
	pk    string
	cells map[string][]byte
}

// ExplainBaselinePairMismatch drills into a table VerifyBaselinePair already
// reported as StatusMismatch, producing a row-level diff between the recovery
// side (prev baseline reconstructed forward to the new baseline's anchor) and the
// new baseline (truth). It reuses the SAME SnapshotFullTableImages row stream the
// digest is built from, so the diff is guaranteed consistent with the verdict.
//
// It re-derives the setup VerifyBaselinePair computed (resolver/columns/changes).
// Because it runs only after a confirmed mismatch, the guards VerifyBaselinePair
// applied (PK present and supported, usable anchor, prev→new order) have already
// passed, so any failure here is a hard error, not an inconclusive verdict.
func ExplainBaselinePairMismatch(ctx context.Context, cfg BaselineConfig, p BaselinePair) (*MismatchExplanation, error) {
	tm, err := cfg.Resolver.Resolve(p.Schema, p.Table)
	if err != nil {
		return nil, fmt.Errorf("resolve %s.%s: %w", p.Schema, p.Table, err)
	}
	pkCols := tm.PKColumnMetas()
	if len(pkCols) == 0 {
		return nil, fmt.Errorf("%s.%s has no primary key", p.Schema, p.Table)
	}

	// Same column set as the digest: exactly what the baseline Parquet holds, via
	// ReadBaselineColumns (not the is_generated flag — see VerifyBaselinePair).
	colNames, err := reconstruct.ReadBaselineColumns(ctx, p.NewPath)
	if err != nil {
		return nil, fmt.Errorf("read new baseline columns %s.%s: %w", p.Schema, p.Table, err)
	}
	colByName := make(map[string]metadata.ColumnMeta, len(tm.Columns))
	for _, c := range tm.Columns {
		colByName[c.Name] = c
	}
	orderedCols := make([]metadata.ColumnMeta, 0, len(colNames))
	for _, name := range colNames {
		cm, ok := colByName[name]
		if !ok {
			return nil, fmt.Errorf("baseline column %q absent from the index schema snapshot", name)
		}
		orderedCols = append(orderedCols, cm)
	}

	// Latest event per PK in (prev snapshot, new anchor] — the same window
	// VerifyBaselinePair reconstructs the recovery side over.
	engine := query.New(cfg.IndexDB)
	rows, _, err := query.FetchMerged(ctx, cfg.IndexDB, engine, query.FetchMergedOptions{
		Opts: query.Options{
			Schema:     p.Schema,
			Table:      p.Table,
			Since:      &p.PrevSnapshot,
			Until:      &p.NewSnapshot,
			UntilPos:   &p.NewAnchor,
			LimitPerPK: 1,
		},
		DBName:         cfg.IndexDBName,
		NoArchive:      cfg.NoArchive,
		ArchiveFetcher: cfg.ArchiveFetcher,
	})
	if err != nil {
		return nil, fmt.Errorf("fetch changes %s.%s: %w", p.Schema, p.Table, err)
	}
	changes := make(map[string]*query.ResultRow, len(rows))
	for i := range rows {
		changes[rows[i].PKValues] = &rows[i]
	}

	// Truth side held fully (the new baseline as-is); recovery streamed against it
	// so only one side is materialized in memory at a time.
	truth, err := collectRowsByPK(ctx, p.NewPath, p.Schema, p.Table, pkCols, orderedCols, map[string]*query.ResultRow{})
	if err != nil {
		return nil, fmt.Errorf("read new baseline %s.%s: %w", p.Schema, p.Table, err)
	}

	ex := &MismatchExplanation{Schema: p.Schema, Table: p.Table, Anchor: fmt.Sprintf("%s:%d", p.NewAnchor.File, p.NewAnchor.Pos)}
	seen := make(map[string]bool, len(truth))
	err = streamRowsByPK(ctx, p.PrevPath, p.Schema, p.Table, pkCols, orderedCols, changes, func(key string, rec rowCells) error {
		seen[key] = true
		t, ok := truth[key]
		if !ok {
			ex.add(RowDiff{PK: rec.pk, Kind: diffExtra})
			return nil
		}
		var cells []CellDiff
		for _, c := range orderedCols {
			if !bytes.Equal(rec.cells[c.Name], t.cells[c.Name]) {
				cells = append(cells, CellDiff{Column: c.Name, Recovery: displayCell(rec.cells[c.Name]), Baseline: displayCell(t.cells[c.Name])})
			}
		}
		if len(cells) > 0 {
			ex.add(RowDiff{PK: rec.pk, Kind: diffChanged, Cells: cells})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("reconstruct prev %s.%s: %w", p.Schema, p.Table, err)
	}

	// Truth rows the recovery never produced (deterministic order for stable output).
	missing := make([]rowCells, 0)
	for key, t := range truth {
		if !seen[key] {
			missing = append(missing, t)
		}
	}
	sort.Slice(missing, func(i, j int) bool { return missing[i].pk < missing[j].pk })
	for _, m := range missing {
		ex.add(RowDiff{PK: m.pk, Kind: diffMissing})
	}

	return ex, nil
}

// streamRowsByPK reconstructs baselinePath+changes and invokes emit once per row
// with its PK key (for joining) and canonical per-column bytes. It is the digest
// row stream (reconstructDigest) re-pointed at a diff instead of a hash.
func streamRowsByPK(ctx context.Context, baselinePath, schema, table string, pkCols, orderedCols []metadata.ColumnMeta, changes map[string]*query.ResultRow, emit func(key string, r rowCells) error) error {
	return reconstruct.SnapshotFullTableImages(ctx, reconstruct.SnapshotFullTableInput{
		BaselinePath: baselinePath,
		Schema:       schema,
		Table:        table,
		PKCols:       pkCols,
		Changes:      changes,
	}, func(rowMap map[string]any) error {
		key, pk := pkKeyAndDisplay(rowMap, pkCols)
		cells := make(map[string][]byte, len(orderedCols))
		for _, c := range orderedCols {
			cells[c.Name] = renderCell(rowMap[c.Name], c)
		}
		return emit(key, rowCells{pk: pk, cells: cells})
	})
}

func collectRowsByPK(ctx context.Context, baselinePath, schema, table string, pkCols, orderedCols []metadata.ColumnMeta, changes map[string]*query.ResultRow) (map[string]rowCells, error) {
	out := make(map[string]rowCells)
	err := streamRowsByPK(ctx, baselinePath, schema, table, pkCols, orderedCols, changes, func(key string, r rowCells) error {
		out[key] = r
		return nil
	})
	return out, err
}

// pkKeyAndDisplay builds a join key (canonical PK bytes, NUL-joined so multi-
// column keys can't collide) and a human label ("col=val, …"). Both sides render
// the PK through the same renderCell, so equal logical keys produce equal keys.
func pkKeyAndDisplay(rowMap map[string]any, pkCols []metadata.ColumnMeta) (key, display string) {
	keyParts := make([]string, len(pkCols))
	dispParts := make([]string, len(pkCols))
	for i, c := range pkCols {
		v := renderCell(rowMap[c.Name], c)
		keyParts[i] = string(v)
		dispParts[i] = c.Name + "=" + displayCell(v)
	}
	return strings.Join(keyParts, "\x00"), strings.Join(dispParts, ", ")
}

// displayCell renders a canonical cell for human output: NULL for an absent
// value, the text form otherwise. Differing columns in a mismatch are
// non-deferred types (ENUM/SET/JSON/binary changes are classified inconclusive,
// never mismatch), so the text form is clean here.
func displayCell(b []byte) string {
	if b == nil {
		return "NULL"
	}
	return string(b)
}

// Write prints the drill-down: one line per differing primary key.
func (ex *MismatchExplanation) Write(w io.Writer) {
	fmt.Fprintf(w, "\n--- mismatch drill-down: %s.%s @ %s ---\n", ex.Schema, ex.Table, ex.Anchor)
	fmt.Fprintln(w, "  recovery = previous baseline reconstructed to the anchor; baseline = the new baseline (truth)")
	if ex.Total == 0 {
		// A mismatch can be flagged on row COUNT while every matched PK lines up
		// cell-for-cell (e.g. a duplicate-PK pathology collapsing in the join).
		// Say so rather than print nothing.
		fmt.Fprintln(w, "  no per-row content differences found — the mismatch is in row count, not row content")
		return
	}
	for _, d := range ex.Diffs {
		switch d.Kind {
		case diffChanged:
			parts := make([]string, len(d.Cells))
			for i, c := range d.Cells {
				parts[i] = fmt.Sprintf("%s: recovery=%s baseline=%s", c.Column, c.Recovery, c.Baseline)
			}
			fmt.Fprintf(w, "  ~ %s\t%s\n", d.PK, strings.Join(parts, "; "))
		case diffMissing:
			fmt.Fprintf(w, "  + %s\t(in the new baseline, NOT reproduced by the recovery)\n", d.PK)
		case diffExtra:
			fmt.Fprintf(w, "  - %s\t(reconstructed by the recovery, absent from the new baseline)\n", d.PK)
		}
	}
	if ex.Total > len(ex.Diffs) {
		fmt.Fprintf(w, "  … and %d more differing row(s)\n", ex.Total-len(ex.Diffs))
	}
}
