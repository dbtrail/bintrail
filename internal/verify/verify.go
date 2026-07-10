package verify

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/consistency"
	"github.com/dbtrail/dbtrail/internal/event"
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
	// ENUM/SET ordinals → labels, epoch-aware — the same pass every other
	// reconstruction surface (reconstruct, shim, console) runs before folding
	// events onto a baseline (#769/#791): with row_image=FULL an UPDATE's
	// row_after carries EVERY ENUM/SET column, so an unmapped ordinal would
	// digest-differ from the source's label even when the column never changed.
	reconstruct.MapEventEnumLabels(cfg.IndexDB, cfg.Resolver, schema, table, rows)
	// BLOB/TEXT base64 → real value, epoch-aware (#672; same helper #668 wired
	// into the offline reconstruct writer path). SnapshotFullTableImages itself
	// never decodes — every caller is responsible for decoding its own changes
	// map first, same as the shim's runSnapshotFullTable does via mapEventImages.
	// binariesTyped=false means some event's binary values may still be stored
	// base64; the deferred gate below keeps such a comparison inconclusive.
	binariesTyped := reconstruct.DecodeEventBinaries(cfg.IndexDB, schema, table, rows)
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
	// A deferred-type value an event carried that the normalization passes
	// above provably could NOT resolve to the source's representation makes a
	// content difference at equal row count inconclusive. Merely CONTAINING a
	// deferred column no longer keeps the gate on (#791): that masked a real
	// divergence on an unrelated non-deferred column as Inconclusive whenever
	// any event existed in the window. A row-count difference is always
	// conclusive (real loss/gain) and is never masked by this.
	deferredRepr := deferredReprUnresolved(orderedCols, changes, binariesTyped)

	reconDigest, reconCount, emitErr := reconstructDigest(ctx, baselinePath, schema, table, pkCols, changes, rows, orderedCols, renderCellNormalized)
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
		return StatusInconclusive, "an event carried an ENUM/SET, JSON, binary or BIT value that could not be normalized to the baseline/source representation, so this content difference is not conclusive"
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
func reconstructDigest(ctx context.Context, baselinePath, schema, table string, pkCols []metadata.ColumnMeta, changes map[string]*query.ResultRow, events []query.ResultRow, orderedCols []metadata.ColumnMeta, render func(any, metadata.ColumnMeta) []byte) (string, int64, error) {
	hasher := consistency.NewHasher()
	err := reconstruct.SnapshotFullTableImages(ctx, reconstruct.SnapshotFullTableInput{
		BaselinePath: baselinePath,
		Schema:       schema,
		Table:        table,
		PKCols:       pkCols,
		Changes:      changes,
		Events:       events,
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

// deferredReprUnresolved reports whether some change's row image carries a
// deferred-representation value that could NOT be normalized to the form the
// baseline/source renders — the only remaining case where a content-digest
// difference at equal row count is not conclusive.
//
// This replaces two earlier, broader gates (#769/#791): gating on "the table
// contains a deferred column and any change exists" (live mode) masked a REAL
// divergence on an unrelated non-deferred column as Inconclusive, while gating
// on ev.ChangedColumns (baseline mode) missed that a FULL row image carries
// every column — a carried-but-unchanged ENUM ordinal digest-differed and read
// as a false MISMATCH in the default mode. With the event side now normalized
// at the root (MapEventEnumLabels, BIT → raw bytes, DecodeEventBinaries,
// canonicalizeJSONContainer), the gate only stays on for a value those passes
// provably could not resolve.
//
// Only INSERT/UPDATE row_after images matter: a DELETE removes the row from
// both sides, so none of its values is rendered. binariesTyped is
// DecodeEventBinaries' report — false means some event's BLOB/BINARY values
// may still be stored base64 (epoch typing unavailable), so any non-nil
// binary-family value stays unresolved.
func deferredReprUnresolved(cols []metadata.ColumnMeta, changes map[string]*query.ResultRow, binariesTyped bool) bool {
	var deferredCols []metadata.ColumnMeta
	for _, c := range cols {
		if isDeferredType(c.DataType) {
			deferredCols = append(deferredCols, c)
		}
	}
	if len(deferredCols) == 0 {
		return false
	}
	for _, ev := range changes {
		if ev.EventType != event.EventInsert && ev.EventType != event.EventUpdate {
			continue
		}
		for _, c := range deferredCols {
			v, present := ev.RowAfter[c.Name]
			if !present || v == nil {
				continue // absent/NULL renders identically on both sides
			}
			if deferredValueUnresolved(v, c, binariesTyped) {
				return true
			}
		}
	}
	return false
}

// deferredValueUnresolved reports whether one deferred-typed value is still in
// a representation the render side cannot make byte-faithful to the
// baseline/source form. Unsure means unresolved — Inconclusive beats a false
// MISMATCH — but a resolvable value must not keep the gate on, or a real
// divergence elsewhere in the table stays masked (#791).
func deferredValueUnresolved(v any, c metadata.ColumnMeta, binariesTyped bool) bool {
	switch strings.ToLower(c.DataType) {
	case "enum", "set":
		// MapEventEnumLabels maps ordinals conservatively; a value still
		// numeric is an ordinal it could not label (definition drift, epoch
		// gap). A non-ASCII label is byte-comparable only when the table
		// charset matches the snapshot's utf8 — not provable here.
		s, ok := v.(string)
		return !ok || !isASCII(s)
	case "bit":
		if _, ok := v.([]byte); ok {
			return false // already the raw byte form both sides render
		}
		_, ok := renderBitBytes(v, c)
		return !ok
	case "json":
		// JSON is one of the base64StoredKind text-decode targets
		// DecodeEventBinaries degrades (internal/reconstruct/fulltable.go);
		// an untyped epoch may leave the value as the raw base64 string it
		// was stored as. jsonRenderConclusive happens to reject most such
		// strings (base64's alphabet almost never forms valid bare JSON),
		// but a base64 string equal to the literal "true"/"false"/"null"
		// would slip through undetected as resolved — guard explicitly,
		// matching the binary-family branch below.
		if !binariesTyped {
			return true // may still be stored base64 — DecodeEventBinaries degraded
		}
		// Key order is canonicalized on both sides; number literals are NOT
		// provably faithful: go-mysql renders a JSONB double 1.0 as "1" at
		// capture (the parser does not set UseFloatWithTrailingZero) while the
		// source/baseline text keeps "1.0" — and after the round-trip the
		// integer form is indistinguishable from a genuine integer.
		return !jsonRenderConclusive(renderCell(v, c))
	case "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob":
		if !binariesTyped {
			return true // may still be stored base64 — DecodeEventBinaries degraded
		}
		switch v.(type) {
		case string, []byte:
			return false // decoded to the raw value; byte-comparable
		default:
			return true // #736 mis-promotion leftover the decode pass could not restore
		}
	default:
		// Spatial family + VECTOR: []byte (WKB / packed floats) → stored
		// base64, and no event-side decode exists for them yet (#793).
		return true
	}
}

// jsonRenderConclusive reports whether a rendered JSON value is byte-faithful
// to how MySQL renders the same document: valid JSON, canonicalizable when a
// container (so key order cannot differ), and free of number literals (see
// deferredValueUnresolved's json case).
func jsonRenderConclusive(b []byte) bool {
	t := bytes.TrimSpace(b)
	if len(t) == 0 || !json.Valid(t) {
		return false
	}
	if t[0] == '{' || t[0] == '[' {
		if _, ok := canonicalizeJSONContainer(t); !ok {
			return false
		}
	}
	return !jsonContainsNumber(t)
}

// jsonContainsNumber walks an already-valid JSON document's token stream and
// reports whether any number literal appears (object keys are always strings,
// so only value positions produce a json.Number token).
func jsonContainsNumber(data []byte) bool {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	for {
		tok, err := dec.Token()
		if err != nil {
			return false // io.EOF ends the walk; validity was pre-checked
		}
		if _, isNum := tok.(json.Number); isNum {
			return true
		}
	}
}

// isASCII reports whether s contains only 7-bit bytes.
func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
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
