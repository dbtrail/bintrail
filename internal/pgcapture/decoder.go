package pgcapture

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/dbtrail/dbtrail/internal/event"
)

// UnchangedToastKey is the single key of the structurally-distinct marker the
// decoder emits for an unchanged-TOAST value ('u') that could NOT be resolved from
// the before-image. It is a one-key map[string]any, deliberately NOT a plain
// string, so it can never collide with a real text column that legitimately holds
// the literal "<unchanged-toast>": the indexer serializes a map as a JSON object,
// while every real text value is a Go string, so the two are structurally distinct
// on disk and any consumer can detect the marker by a type switch on the reserved
// key.
//
// Under REPLICA IDENTITY FULL — the mode bintrail requires (#531) — the before-
// image always carries the real unchanged value, so the decoder resolves 'u' from
// it (RowAfter[col] = RowBefore[col]) and this marker is never persisted. The
// marker is only reachable under a weaker replica identity, where it keeps the
// column visible rather than silently dropped (the never-drop floor).
const UnchangedToastKey = "__bintrail_unchanged_toast__"

// Decoder turns a pgoutput logical-replication message stream into source-neutral
// event.Event values. It is stateful across messages within a stream: it caches
// each RelationMessage (column names + types + primary key) and the in-flight
// transaction's commit LSN and timestamp.
//
// Decode performs NO I/O. The one piece of state that needs the database — a
// relation's primary-key columns — is supplied by an injected PKResolver, so the
// decode logic is fully unit-testable without a live PostgreSQL: the capturer wires
// a catalog-query PKResolver, tests stub it.
type Decoder struct {
	resolvePK PKResolver
	filters   event.Filters
	logger    *slog.Logger

	relations map[uint32]*relationInfo
	txn       txnContext
}

// txnContext carries the in-flight transaction's commit metadata. pgoutput puts
// the commit LSN and timestamp on the Begin/Commit messages, never on the row
// messages, so the decoder records them at Begin and stamps every row event in the
// transaction with them. All rows of a logical-replication transaction are
// delivered at commit and share its timestamp — the correct event_timestamp for
// partitioning (binlog_events partitions on TO_SECONDS(event_timestamp); a zero
// timestamp would land rows in the wrong partition).
type txnContext struct {
	commitLSN  pglogrepl.LSN
	commitTime time.Time
	open       bool
}

// NewDecoder constructs a Decoder. resolvePK supplies primary-key columns per
// relation (catalog-backed in the capturer, stubbed in tests) and must be non-nil.
// filters restricts which schema/table produce events (the zero Filters accepts
// all). logger may be nil (slog.Default() is used).
func NewDecoder(resolvePK PKResolver, filters event.Filters, logger *slog.Logger) *Decoder {
	if logger == nil {
		logger = slog.Default()
	}
	return &Decoder{
		resolvePK: resolvePK,
		filters:   filters,
		logger:    logger,
		relations: make(map[uint32]*relationInfo),
	}
}

// Decode processes one pgoutput message. It returns:
//   - (event, true, nil) when the message produces a row or commit event;
//   - (zero, false, nil) when the message is consumed for internal state only
//     (Begin/Relation) or is deliberately not indexed (Truncate/Type/Origin);
//   - (zero, false, err) on a decode-invariant violation (unknown relation, a row
//     outside a transaction, a binary tuple datum, a tuple column-count mismatch,
//     or a primary-key lookup failure). The caller must treat a non-nil error as
//     fatal — never skip the message and continue, or the stream desynchronizes.
func (d *Decoder) Decode(msg pglogrepl.Message) (event.Event, bool, error) {
	switch m := msg.(type) {
	case *pglogrepl.BeginMessage:
		// FinalLSN is the transaction's commit LSN; CommitTime its commit timestamp.
		d.txn = txnContext{commitLSN: m.FinalLSN, commitTime: m.CommitTime, open: true}
		return event.Event{}, false, nil

	case *pglogrepl.RelationMessage:
		if err := d.cacheRelation(m); err != nil {
			return event.Event{}, false, err
		}
		return event.Event{}, false, nil

	case *pglogrepl.InsertMessage:
		return d.decodeInsert(m)

	case *pglogrepl.UpdateMessage:
		return d.decodeUpdate(m)

	case *pglogrepl.DeleteMessage:
		return d.decodeDelete(m)

	case *pglogrepl.CommitMessage:
		// The commit boundary: the ONLY event the durable cursor advances on (the
		// #491 invariant in PostgreSQL clothing). The commit LSN travels in GTID —
		// the field the consumer reads to advance stream_state.gtid_set.
		ev := event.Event{
			BinlogFile: m.CommitLSN.String(),
			EndPos:     uint64(m.CommitLSN),
			Timestamp:  m.CommitTime,
			GTID:       m.CommitLSN.String(),
			EventType:  event.EventCommit,
		}
		d.txn = txnContext{}
		return ev, true, nil

	case *pglogrepl.TruncateMessage:
		// DDL replay on the PostgreSQL path is out of #530 scope; surface a TRUNCATE
		// loudly so it is never silently invisible in the index. (Open question:
		// map to EventDDL/DDLTruncateTable later.)
		d.logger.Warn("pgcapture: TRUNCATE not indexed (DDL replay out of scope)",
			"relations", len(m.RelationIDs))
		return event.Event{}, false, nil

	case *pglogrepl.TypeMessage, *pglogrepl.OriginMessage:
		// No row data. Type/Origin are not needed for capture fidelity under
		// proto_version 1 (column names + text values arrive regardless).
		return event.Event{}, false, nil

	default:
		// proto_version 1 emits no in-progress-transaction streaming messages, so
		// any other type is unexpected; log rather than fail (it carries no row
		// data we could be silently dropping).
		d.logger.Debug("pgcapture: ignoring unhandled message", "type", fmt.Sprintf("%T", msg))
		return event.Event{}, false, nil
	}
}

// cacheRelation records (or refreshes) a relation's columns + primary key. PG emits
// a fresh RelationMessage before the first change to a relation in a session and
// again after a relation's shape changes, so this doubles as cache invalidation —
// the analog of the MySQL parser's SwapResolver-on-DDL.
func (d *Decoder) cacheRelation(m *pglogrepl.RelationMessage) error {
	cols := make([]relColumn, len(m.Columns))
	for i, c := range m.Columns {
		cols[i] = relColumn{name: c.Name, typeOID: c.DataType, typeMod: c.TypeModifier}
	}

	pkCols, err := d.resolvePK(m.RelationID, m.Namespace, m.RelationName)
	if err != nil {
		return fmt.Errorf("pgcapture: primary-key lookup for %s.%s (oid %d): %w",
			m.Namespace, m.RelationName, m.RelationID, err)
	}

	// The catalog PK lookup reads the CURRENT schema on a separate connection, while
	// the in-band column names belong to this RelationMessage. If a PK column name is
	// absent from the relation's columns, the two have diverged (a PK-changing DDL
	// during stream lag) — fail loud rather than build a wrong/empty PK.
	colSet := make(map[string]bool, len(cols))
	for _, c := range cols {
		colSet[c.name] = true
	}
	for _, pk := range pkCols {
		if !colSet[pk.Name] {
			return fmt.Errorf("pgcapture: primary-key column %q absent from relation %s.%s columns (schema drift?)",
				pk.Name, m.Namespace, m.RelationName)
		}
	}

	d.relations[m.RelationID] = &relationInfo{
		schema:  m.Namespace,
		table:   m.RelationName,
		columns: cols,
		pkCols:  pkCols,
	}
	return nil
}

func (d *Decoder) decodeInsert(m *pglogrepl.InsertMessage) (event.Event, bool, error) {
	rel, err := d.relationFor(m.RelationID)
	if err != nil {
		return event.Event{}, false, err
	}
	if !d.filters.Matches(rel.schema, rel.table) {
		return event.Event{}, false, nil
	}
	// INSERT carries only a new tuple, fully written — it never contains a 'u'
	// (unchanged-TOAST) marker, so there is no before-image to resolve from.
	after, err := d.decodeTuple(rel, m.Tuple, nil)
	if err != nil {
		return event.Event{}, false, err
	}
	return d.rowEvent(rel, event.EventInsert, nil, after), true, nil
}

func (d *Decoder) decodeUpdate(m *pglogrepl.UpdateMessage) (event.Event, bool, error) {
	rel, err := d.relationFor(m.RelationID)
	if err != nil {
		return event.Event{}, false, err
	}
	if !d.filters.Matches(rel.schema, rel.table) {
		return event.Event{}, false, nil
	}
	// OldTuple is present as a full tuple ('O') only under REPLICA IDENTITY FULL, or
	// as a key-only tuple ('K') when a replica-identity column changed; either form
	// carries real values (never 'u'). It is absent when an UPDATE under RI DEFAULT
	// left the key unchanged.
	var before map[string]any
	if m.OldTuple != nil {
		before, err = d.decodeTuple(rel, m.OldTuple, nil)
		if err != nil {
			return event.Event{}, false, err
		}
	}
	// Resolve any 'u' in the new tuple from the before-image (Option B): under RI
	// FULL the unchanged column's real value is in the before-image, so the after-
	// image holds the TRUE post-update row state — making changed_columns correct
	// and needing zero downstream handling.
	after, err := d.decodeTuple(rel, m.NewTuple, before)
	if err != nil {
		return event.Event{}, false, err
	}
	return d.rowEvent(rel, event.EventUpdate, before, after), true, nil
}

func (d *Decoder) decodeDelete(m *pglogrepl.DeleteMessage) (event.Event, bool, error) {
	rel, err := d.relationFor(m.RelationID)
	if err != nil {
		return event.Event{}, false, err
	}
	if !d.filters.Matches(rel.schema, rel.table) {
		return event.Event{}, false, nil
	}
	before, err := d.decodeTuple(rel, m.OldTuple, nil)
	if err != nil {
		return event.Event{}, false, err
	}
	return d.rowEvent(rel, event.EventDelete, before, nil), true, nil
}

// relationFor returns the cached relation for a row message, erroring if no
// RelationMessage preceded it (a protocol invariant) or if no transaction is open
// (a row must arrive between Begin and Commit; without an open txn the event would
// get a zero commit timestamp and land in the wrong partition).
func (d *Decoder) relationFor(oid uint32) (*relationInfo, error) {
	if !d.txn.open {
		return nil, fmt.Errorf("pgcapture: row event for relation OID %d outside a transaction (no preceding Begin)", oid)
	}
	rel, ok := d.relations[oid]
	if !ok {
		return nil, fmt.Errorf("pgcapture: row event for unknown relation OID %d (no preceding Relation message)", oid)
	}
	return rel, nil
}

// decodeTuple converts a pgoutput tuple to a column-name→value map in the cached
// relation's column order:
//   - 'n' (null)   → Go nil (SQL NULL);
//   - 't' (text)   → Go string (lossless; type-faithful rendering is #533's);
//   - 'u' (toast)  → resolveFrom[col] when present (Option B), else the structurally
//     distinct unchanged-TOAST marker (never a plain string);
//   - 'b' (binary) → error: pgcapture streams text format only, so a binary datum is
//     a misconfiguration we refuse rather than silently mishandle.
//
// resolveFrom is the before-image for an UPDATE's new tuple, and nil otherwise (a
// before-image and an INSERT contain no 'u').
func (d *Decoder) decodeTuple(rel *relationInfo, t *pglogrepl.TupleData, resolveFrom map[string]any) (map[string]any, error) {
	if t == nil {
		return nil, nil
	}
	if len(t.Columns) != len(rel.columns) {
		return nil, fmt.Errorf("pgcapture: tuple has %d columns but relation %s.%s has %d",
			len(t.Columns), rel.schema, rel.table, len(rel.columns))
	}
	row := make(map[string]any, len(t.Columns))
	for i, col := range t.Columns {
		name := rel.columns[i].name
		switch col.DataType {
		case pglogrepl.TupleDataTypeNull:
			row[name] = nil
		case pglogrepl.TupleDataTypeText:
			row[name] = string(col.Data)
		case pglogrepl.TupleDataTypeToast:
			if resolveFrom != nil {
				if v, ok := resolveFrom[name]; ok {
					row[name] = v
					continue
				}
			}
			row[name] = unchangedToastMarker()
		case pglogrepl.TupleDataTypeBinary:
			return nil, fmt.Errorf("pgcapture: binary tuple datum for column %q in %s.%s — pgcapture streams text format only",
				name, rel.schema, rel.table)
		default:
			return nil, fmt.Errorf("pgcapture: unknown tuple data type %q for column %q in %s.%s",
				col.DataType, name, rel.schema, rel.table)
		}
	}
	return row, nil
}

// rowEvent assembles a row event.Event, stamping it with the in-flight
// transaction's commit LSN and timestamp.
func (d *Decoder) rowEvent(rel *relationInfo, typ event.EventType, before, after map[string]any) event.Event {
	// PKValues from the before-image for UPDATE/DELETE (under RI FULL it is complete
	// and free of the unchanged-TOAST marker, so a TOASTed-but-unchanged PK column
	// still yields its real value), from the after-image for INSERT (no before-image)
	// — mirroring the MySQL parser (parser.go emitInserts/emitDeletes/emitUpdates).
	pkSource := before
	if pkSource == nil {
		pkSource = after
	}
	var pkValues string
	if len(rel.pkCols) > 0 {
		pkValues = event.BuildPKValues(rel.pkCols, pkSource)
	}

	lsn := d.txn.commitLSN.String()
	return event.Event{
		BinlogFile: lsn,
		StartPos:   uint64(d.txn.commitLSN),
		EndPos:     uint64(d.txn.commitLSN),
		Timestamp:  d.txn.commitTime,
		GTID:       lsn,
		Schema:     rel.schema,
		Table:      rel.table,
		EventType:  typ,
		PKValues:   pkValues,
		RowBefore:  before,
		RowAfter:   after,
	}
}

// unchangedToastMarker is the structurally-distinct stand-in for an unchanged-TOAST
// value that could not be resolved from the before-image. See UnchangedToastKey.
func unchangedToastMarker() map[string]any {
	return map[string]any{UnchangedToastKey: true}
}
