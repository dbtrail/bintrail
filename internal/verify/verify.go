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
	// StatusError: verifying this table hit a hard error (e.g. the source read
	// failed). Recorded per table so one table's error does not abort the run;
	// the overall run still fails.
	StatusError Status = "error"
)

// TableResult is the per-table verify outcome.
type TableResult struct {
	Schema, Table     string
	Status            Status
	SourceDigest      string
	ReconstructDigest string
	SourceRows        int64
	ReconstructRows   int64
	Anchor            string // the point the comparison is anchored to: a GTID set (live-source path) or a binlog coordinate file:pos (baseline-pair path)
	Detail            string // reason for inconclusive/mismatch, or a note carried on a match (e.g. coverage-unverified)
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
// Alignment (load-bearing precondition): the source digest is anchored at the
// GTID captured when the snapshot opens (T0, inside ConsistentTableChecksum);
// asOf is wall-clock captured AFTER the full-table scan returns (T1). Any write
// committed in the window (T0, T1] enters the reconstruct (Until=asOf) but not
// the frozen source snapshot, so it surfaces as a divergence — a row-count or
// content MISMATCH that FAILS the run (not a soft "inconclusive"). That window
// spans the whole source scan (seconds to minutes on a large table), so verify
// is only reliable on a quiescent source — run it off-peak. GTID-precise
// alignment (reconstruct to exactly the snapshot's GTID) is a follow-up.
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

	// 1. Live source fingerprint at a consistent snapshot. Normalized (see
	// renderCellNormalized) so it stays symmetric with step 5's reconstruct
	// digest: both sides must agree on what counts as a representation-only
	// difference, or normalizing only one would trade one false mismatch for
	// another.
	src, err := consistency.ConsistentTableChecksumNormalized(ctx, cfg.SourceDB, schema, table, normalizeRenderedBytes)
	if err != nil {
		return res, fmt.Errorf("source checksum %s.%s: %w", schema, table, err)
	}
	res.SourceDigest = src.Digest
	res.SourceRows = src.RowCount
	res.Anchor = src.GTIDSet
	asOf := time.Now().UTC()

	// 2. Require the index to have indexed every event the source snapshot
	// reflects, else a missing event would read as a (false) mismatch. Checked
	// by GTID containment, which is correct even when the source has had no
	// recent writes (a stale last_event_time does not mean "behind"). A GTID-off
	// source can't be checked this way — verify proceeds but flags the result
	// as coverage-unverified rather than blocking.
	covered, coverageNote := indexCovers(ctx, cfg.IndexDB, src.GTIDSet)
	if !covered {
		return inconclusive(res, coverageNote), nil
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
	// BLOB/TEXT base64 → real value, epoch-aware (#672; same helper #668 wired
	// into the offline reconstruct writer path). SnapshotFullTableImages itself
	// never decodes — every caller is responsible for decoding its own changes
	// map first, same as the shim's runSnapshotFullTable does via mapEventImages.
	reconstruct.DecodeEventBinaries(cfg.IndexDB, schema, table, rows)
	changes := make(map[string]*query.ResultRow, len(rows))
	for i := range rows {
		changes[rows[i].PKValues] = &rows[i]
	}

	// 5. Reconstruct the full table to asOf and hash each row in the source's
	// text form. Hash exactly the column set ConsistentTableChecksum hashed, in
	// its order — re-deriving the non-generated set from the schema snapshot
	// risks a different generated-column membership (the DEFAULT_GENERATED trap)
	// and a spurious mismatch.
	colByName := make(map[string]metadata.ColumnMeta, len(tm.Columns))
	for _, c := range tm.Columns {
		colByName[c.Name] = c
	}
	orderedCols := make([]metadata.ColumnMeta, 0, len(src.Columns))
	for _, name := range src.Columns {
		cm, ok := colByName[name]
		if !ok {
			return inconclusive(res, fmt.Sprintf("source column %q is absent from the index schema snapshot; re-run bintrail snapshot", name)), nil
		}
		orderedCols = append(orderedCols, cm)
	}
	// ENUM/SET, JSON and binary columns whose value was changed by an event in
	// the window can render differently on the event side than the source reads
	// them (ENUM ordinal vs label, MySQL-canonical JSON text, base64 vs raw
	// bytes); their faithful event-image normalization is deferred. When such a
	// table mismatches at EQUAL row count, the difference is not conclusive. A
	// row-count difference is always conclusive (real loss/gain) and is never
	// masked by this.
	deferredRepr := hasDeferredRepr(orderedCols) && len(changes) > 0

	reconDigest, reconCount, emitErr := reconstructDigest(ctx, baselinePath, schema, table, pkCols, changes, orderedCols, renderCellNormalized)
	if emitErr != nil {
		return res, fmt.Errorf("reconstruct %s.%s: %w", schema, table, emitErr)
	}
	res.ReconstructDigest = reconDigest
	res.ReconstructRows = reconCount
	if coverageNote != "" {
		res.Detail = coverageNote
	}

	// 6. Compare.
	status, detail := classify(res.SourceDigest, res.SourceRows, res.ReconstructDigest, res.ReconstructRows, deferredRepr)
	res.Status = status
	if detail != "" {
		res.Detail = detail // a real reason overrides the coverage note
	}
	return res, nil
}

// classify is the pure comparison core. Row count is checked first: a difference
// is always a conclusive mismatch (real row loss/gain) and is NEVER downgraded by
// deferredRepr — that ordering is the guard against masking data loss on a table
// that merely contains an ENUM/SET/JSON/binary column. At equal row count a
// content difference is a mismatch, except when deferredRepr is set (a deferred
// column may have changed and its event image isn't normalized yet), where it is
// inconclusive rather than a false alarm.
func classify(srcDigest string, srcRows int64, reconDigest string, reconRows int64, deferredRepr bool) (Status, string) {
	if reconRows != srcRows {
		return StatusMismatch, fmt.Sprintf("row count differs: source=%d reconstructed=%d", srcRows, reconRows)
	}
	// Equal row count — the byte comparison below is only meaningful when both
	// digests were produced under the SAME contract version (consistency.
	// DigestVersion). A version skew (e.g. a persisted pre-charset-pin v1 baseline
	// digest against a current v2 scan, issue #792) byte-differs even on identical
	// data, so it must degrade to a needs-rebaseline signal, never a false
	// MISMATCH. Row count is version-independent, so it stays checked first: real
	// row loss/gain is still conclusive under a version skew.
	if sv, rv := consistency.DigestVersionOf(srcDigest), consistency.DigestVersionOf(reconDigest); sv != rv {
		return StatusInconclusive, fmt.Sprintf("digest contract skew (%q vs %q): the compared digests were produced under different versions; regenerate the baseline (bintrail baseline) so both sides use the current contract", sv, rv)
	}
	if reconDigest == srcDigest {
		return StatusMatch, ""
	}
	if deferredRepr {
		return StatusInconclusive, "an ENUM/SET, JSON or binary column was changed by an event; its event-image normalization is deferred, so this content difference is not conclusive"
	}
	return StatusMismatch, "content digest differs at equal row count (in-place value divergence)"
}

// reconstructDigest reconstructs a table from baselinePath merged with changes,
// renders each row's columns (in orderedCols order) via render, and returns the
// order-independent content digest + row count. Shared by the live-source
// verify (VerifyTable) and the baseline-pair verify (#642) — both now pass
// renderCellNormalized (see its doc comment for why the normalization it
// applies is safe, and how each caller keeps it symmetric with the OTHER
// side of its own comparison): both sides of any one comparison must be
// produced with the SAME render func so the digests are byte-comparable by
// construction.
func reconstructDigest(ctx context.Context, baselinePath, schema, table string, pkCols []metadata.ColumnMeta, changes map[string]*query.ResultRow, orderedCols []metadata.ColumnMeta, render func(any, metadata.ColumnMeta) []byte) (string, int64, error) {
	hasher := consistency.NewHasher()
	err := reconstruct.SnapshotFullTableImages(ctx, reconstruct.SnapshotFullTableInput{
		BaselinePath: baselinePath,
		Schema:       schema,
		Table:        table,
		PKCols:       pkCols,
		Changes:      changes,
	}, func(rowMap map[string]any) error {
		cells := make([][]byte, len(orderedCols))
		for i, c := range orderedCols {
			cells[i] = render(rowMap[c.Name], c)
		}
		hasher.AddBytes(cells)
		return nil
	})
	if err != nil {
		return "", 0, err
	}
	return hasher.Digest(), hasher.Count(), nil
}

func inconclusive(res TableResult, detail string) TableResult {
	res.Status = StatusInconclusive
	res.Detail = detail
	return res
}

// hasDeferredRepr reports whether any column's event-image representation can
// differ from how the source renders it in a way this version does not yet
// normalize: ENUM/SET (ordinal vs label), JSON (MySQL-canonical text), and
// binary families (base64 in the event image vs raw bytes from the source).
func hasDeferredRepr(cols []metadata.ColumnMeta) bool {
	for _, c := range cols {
		if isDeferredType(c.DataType) {
			return true
		}
	}
	return false
}

// isDeferredType reports whether a column's event-image representation can differ
// from how the baseline/source renders it in a way this version doesn't yet
// normalize: ENUM/SET (ordinal vs label), JSON (MySQL-canonical text), binary
// families (base64 in the event image vs raw bytes), BIT, and the spatial and
// VECTOR types — whose values are binary (WKB / packed floats) in the event
// image and so carry the same base64-vs-raw representation gap as BLOB (#793).
//
// TEXT is deliberately NOT here, despite being decoded by the same
// DecodeEventBinaries call as BLOB (#672): once decoded, a TEXT value is just
// a string, directly comparable to the baseline/source text — unlike
// ENUM/JSON/binary, decoding doesn't leave a representation gap to defer.
// Deferring it anyway would mask genuine TEXT divergences as Inconclusive on
// every table with a TEXT column (the common case, e.g. wp_options), which
// defeats the point of decoding it in the first place. The narrow remaining
// risk — an unresolvable epoch leaving a value as stored base64 — is the same
// accepted risk DecodeEventBinaries already carries for its other callers
// (recover, shim, reconstruct); it is not given a broader safety net here.
func isDeferredType(dataType string) bool {
	switch strings.ToLower(dataType) {
	case "enum", "set", "json",
		"binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob", "bit",
		// Spatial family (DATA_TYPE as reported by information_schema.COLUMNS)
		// plus MySQL 9.0+ VECTOR: binary in the event image, no normalization yet.
		"geometry", "point", "linestring", "polygon",
		"multipoint", "multilinestring", "multipolygon",
		// MySQL 8.0.11+ (WL#2388) reports a GEOMETRYCOLLECTION column's DATA_TYPE
		// as "geomcollection"; MariaDB and pre-8.0.11 report "geometrycollection".
		"geometrycollection", "geomcollection",
		"vector":
		return true
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
// way; verify proceeds without the coverage guarantee, flagging the result as
// coverage-unverified (rather than blocking or reporting inconclusive) — it
// returns (true, note).
func indexCovers(ctx context.Context, indexDB *sql.DB, srcGTID string) (bool, string) {
	if strings.TrimSpace(srcGTID) == "" {
		// No GTID to check containment against (gtid_mode=OFF). Proceed without
		// the coverage guarantee rather than blocking — a behind index on a
		// GTID-off source is a narrow case the operator runs verify knowing the
		// daemon is current. The note surfaces the weaker guarantee in the result.
		return true, "coverage unverified (source GTIDs disabled): assuming the index is current"
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
