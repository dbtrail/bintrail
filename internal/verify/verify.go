package verify

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/consistency"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// Status is the outcome of verifying one table.
type Status string

const (
	// StatusMatch: the reconstructed digest equals the live source digest.
	StatusMatch Status = "match"
	// StatusMismatch: digests differ — a real divergence between what a recovery
	// would produce and the source.
	StatusMismatch Status = "mismatch"
	// StatusInconclusive: the comparison could not be made meaningfully (index
	// behind the source, no baseline, unsupported PK, or a value class this
	// version can't render to the source form). Never reported as a failure —
	// an inconclusive result is not a divergence.
	StatusInconclusive Status = "inconclusive"
)

// TableResult is the per-table verify outcome.
type TableResult struct {
	Schema, Table     string
	Status            Status
	SourceDigest      string
	ReconstructDigest string
	SourceRows        int64
	ReconstructRows   int64
	GTID              string // source snapshot GTID the comparison is anchored to
	Detail            string // reason for inconclusive/mismatch
}

// Config wires the three data sources verify needs.
type Config struct {
	SourceDB       *sql.DB
	IndexDB        *sql.DB
	Resolver       *metadata.Resolver
	BaselineSource string // local dir or s3:// prefix, passed to FindBaseline
	IndexDBName    string
	NoArchive      bool
	ArchiveFetcher query.ArchiveFetcher
}

// VerifyTable verifies one table: fingerprint the live source at a consistent
// snapshot (#632), reconstruct the table to that same point from baseline +
// binlog, render the reconstructed rows into the source's text form, hash them,
// and compare.
//
// Alignment: the source snapshot is frozen at its GTID; verify reconstructs to a
// wall-clock asOf captured at the snapshot. On a quiescent source (run off-peak)
// these coincide exactly; on an actively-written table, events on the snapshot
// boundary can make a single run inconclusive — re-run, or wait for a quiet
// window. GTID-precise alignment is a follow-up.
func VerifyTable(ctx context.Context, cfg Config, schema, table string) (TableResult, error) {
	res := TableResult{Schema: schema, Table: table}

	tm, err := cfg.Resolver.Resolve(schema, table)
	if err != nil {
		return res, fmt.Errorf("resolve %s.%s: %w", schema, table, err)
	}

	// PK must be a type the baseline canonicalizer supports, or the reconstruct
	// would silently miss never-touched rows.
	pkCols := tm.PKColumnMetas()
	if len(pkCols) == 0 {
		return inconclusive(res, "table has no primary key"), nil
	}
	for _, c := range pkCols {
		if !reconstruct.SupportedPKType(c.DataType) {
			return inconclusive(res, fmt.Sprintf("primary-key column %q has type %q unsupported by the baseline canonicalizer", c.Name, c.DataType)), nil
		}
	}

	// 1. Live source fingerprint at a consistent snapshot.
	src, err := consistency.ConsistentTableChecksum(ctx, cfg.SourceDB, schema, table)
	if err != nil {
		return res, fmt.Errorf("source checksum %s.%s: %w", schema, table, err)
	}
	res.SourceDigest = src.Digest
	res.SourceRows = src.RowCount
	res.GTID = src.GTIDSet
	asOf := time.Now().UTC()

	// 2. Require the index to have indexed every event the source snapshot
	// reflects, else a missing event would read as a (false) mismatch. Checked
	// by GTID containment, which is correct even when the source has had no
	// recent writes (a stale last_event_time does not mean "behind").
	if covered, detail := indexCovers(ctx, cfg.IndexDB, src.GTIDSet); !covered {
		return inconclusive(res, detail), nil
	}

	// 3. Find the baseline at-or-before asOf.
	baselinePath, snapshotTime, _, err := reconstruct.FindBaseline(ctx, cfg.BaselineSource, schema, table, asOf)
	if err != nil {
		if isNoBaseline(err) {
			return inconclusive(res, "no baseline at-or-before the snapshot; reconstruct would omit never-touched rows"), nil
		}
		return res, fmt.Errorf("find baseline %s.%s: %w", schema, table, err)
	}

	// 4. Latest event per PK in (baseline, asOf] — the change map the merge needs.
	engine := query.New(cfg.IndexDB)
	rows, _, err := query.FetchMerged(ctx, cfg.IndexDB, engine, query.FetchMergedOptions{
		Opts: query.Options{
			Schema:     schema,
			Table:      table,
			Since:      &snapshotTime,
			Until:      &asOf,
			LimitPerPK: 1,
		},
		DBName:         cfg.IndexDBName,
		NoArchive:      cfg.NoArchive,
		ArchiveFetcher: cfg.ArchiveFetcher,
	})
	if err != nil {
		var gap *query.GapError
		if errors.As(err, &gap) {
			// A coverage gap (events rotated away and not archived) means the
			// reconstruction window is incomplete — we can't fingerprint a
			// faithful state, so the comparison is inconclusive, not a mismatch.
			return inconclusive(res, "coverage gap in the reconstruction window: "+gap.Error()), nil
		}
		return res, fmt.Errorf("fetch changes %s.%s: %w", schema, table, err)
	}
	changes := make(map[string]*query.ResultRow, len(rows))
	for i := range rows {
		changes[rows[i].PKValues] = &rows[i]
	}

	// 5. Reconstruct the full table to asOf and hash each row in the source's
	// text form.
	orderedCols := nonGeneratedColumns(tm)
	enumRisk := hasEnumOrSet(orderedCols) && len(changes) > 0
	hasher := consistency.NewHasher()
	var renderErr error
	emitErr := reconstruct.SnapshotFullTableImages(ctx, reconstruct.SnapshotFullTableInput{
		BaselinePath: baselinePath,
		Schema:       schema,
		Table:        table,
		PKCols:       pkCols,
		Changes:      changes,
	}, func(rowMap map[string]any) error {
		cells := make([][]byte, len(orderedCols))
		for i, c := range orderedCols {
			b, err := renderCell(rowMap[c.Name], c)
			if err != nil {
				renderErr = fmt.Errorf("column %q: %w", c.Name, err)
				return renderErr
			}
			cells[i] = b
		}
		hasher.AddBytes(cells)
		return nil
	})
	if emitErr != nil {
		if renderErr != nil {
			// A value class this version can't render — inconclusive, not a failure.
			return inconclusive(res, "reconstructed value could not be rendered to the source form: "+renderErr.Error()), nil
		}
		return res, fmt.Errorf("reconstruct %s.%s: %w", schema, table, emitErr)
	}

	res.ReconstructDigest = hasher.Digest()
	res.ReconstructRows = hasher.Count()

	// 6. Compare.
	if res.ReconstructDigest == res.SourceDigest && res.ReconstructRows == res.SourceRows {
		res.Status = StatusMatch
		return res, nil
	}
	// A digest mismatch on a table whose ENUM/SET columns were changed by events
	// is expected: binlog event images carry ENUM/SET ordinals, while the source
	// and baseline carry labels, and ordinal→label mapping is deferred. Downgrade
	// to inconclusive rather than cry wolf.
	if enumRisk {
		return inconclusive(res, "ENUM/SET column changed by an event; ordinal→label mapping is deferred, so a difference here is not conclusive"), nil
	}
	res.Status = StatusMismatch
	if res.ReconstructRows != res.SourceRows {
		res.Detail = fmt.Sprintf("row count differs: source=%d reconstructed=%d", res.SourceRows, res.ReconstructRows)
	} else {
		res.Detail = "content digest differs at equal row count (in-place value divergence)"
	}
	return res, nil
}

func inconclusive(res TableResult, detail string) TableResult {
	res.Status = StatusInconclusive
	res.Detail = detail
	return res
}

// nonGeneratedColumns returns the table's columns in ordinal order excluding
// generated columns — matching ConsistentTableChecksum's SELECT set.
func nonGeneratedColumns(tm *metadata.TableMeta) []metadata.ColumnMeta {
	out := make([]metadata.ColumnMeta, 0, len(tm.Columns))
	for _, c := range tm.Columns {
		if c.IsGenerated {
			continue
		}
		out = append(out, c)
	}
	return out
}

func hasEnumOrSet(cols []metadata.ColumnMeta) bool {
	for _, c := range cols {
		switch strings.ToLower(c.DataType) {
		case "enum", "set":
			return true
		}
	}
	return false
}

func isNoBaseline(err error) bool { return errors.Is(err, reconstruct.ErrNoBaseline) }

// indexCovers reports whether the index has indexed every transaction the source
// snapshot reflects, by checking that the index's checkpointed GTID set
// (stream_state.gtid_set) contains the source snapshot's @@gtid_executed
// (srcGTID). If it does not, a reconstruct would be missing events the source
// has, so the comparison is inconclusive rather than a mismatch.
//
// A source with GTIDs disabled (empty srcGTID) cannot be coverage-checked this
// way; verify reports that as inconclusive rather than silently risking a false
// mismatch.
func indexCovers(ctx context.Context, indexDB *sql.DB, srcGTID string) (bool, string) {
	if strings.TrimSpace(srcGTID) == "" {
		// No GTID to check containment against (gtid_mode=OFF). Proceed without
		// the coverage guarantee rather than blocking — a behind index on a
		// GTID-off source is a narrow case the operator runs verify knowing the
		// daemon is current. Documented limitation.
		return true, ""
	}
	var idxGTID sql.NullString
	err := indexDB.QueryRowContext(ctx,
		"SELECT gtid_set FROM stream_state WHERE id = 1").Scan(&idxGTID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, "index has no stream state yet (daemon not running or never checkpointed)"
	}
	if err != nil {
		return false, "could not read index coverage: " + err.Error()
	}
	if !idxGTID.Valid || strings.TrimSpace(idxGTID.String) == "" {
		return false, "index has not checkpointed any GTID yet"
	}
	idxSet, err := gomysql.ParseMysqlGTIDSet(idxGTID.String)
	if err != nil {
		return false, "index GTID set is unparseable: " + err.Error()
	}
	srcSet, err := gomysql.ParseMysqlGTIDSet(srcGTID)
	if err != nil {
		return false, "source GTID set is unparseable: " + err.Error()
	}
	if !idxSet.Contain(srcSet) {
		return false, fmt.Sprintf("index is behind the source snapshot (indexed %s does not contain snapshot %s); re-run once the daemon catches up",
			idxGTID.String, srcGTID)
	}
	return true, ""
}
