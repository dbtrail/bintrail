package metadata

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"
)

// ErrNoSnapshots signals "schema_snapshots is queryable but empty"
// — a benign first-install state, not a real failure. Callers that
// degrade gracefully when no snapshot exists (e.g. the shim's
// columnOrderFor falling back to alphabetical column ordering) can
// errors.Is against this sentinel to distinguish "operator hasn't
// run `bintrail snapshot` yet" from a genuine DB-side failure
// (table dropped post-upgrade, permissions revoked, connection
// lost) — which deserve a louder log channel.
var ErrNoSnapshots = errors.New("no snapshots found; run `bintrail snapshot` first")

// ─── Types ───────────────────────────────────────────────────────────────────

// ColumnMeta holds metadata for a single table column.
type ColumnMeta struct {
	Name            string
	OrdinalPosition int
	IsPK            bool
	DataType        string // e.g. "int", "datetime", "varchar" (information_schema.COLUMNS.DATA_TYPE)
	ColumnType      string // full type declaration, e.g. "int(11) unsigned", "datetime(6)" (COLUMN_TYPE). Empty on pre-#212 snapshots.
	IsGenerated     bool   // true for STORED or VIRTUAL generated columns
}

// TableMeta holds the column mapping for a table, derived from a schema snapshot.
// Columns are in ordinal_position order (matching the binlog row value order).
type TableMeta struct {
	Schema    string
	Table     string
	Columns   []ColumnMeta // ordered by ordinal_position
	PKColumns []string     // PK column names in ordinal order
}

// PKColumnMetas returns the ColumnMeta entries for primary key columns,
// preserving their ordinal order. Used by BuildPKValues.
func (t *TableMeta) PKColumnMetas() []ColumnMeta {
	var pks []ColumnMeta
	for _, c := range t.Columns {
		if c.IsPK {
			pks = append(pks, c)
		}
	}
	return pks
}

// SnapshotStats is returned by TakeSnapshot with counts of what was captured.
type SnapshotStats struct {
	SnapshotID  int
	TableCount  int
	ColumnCount int
	FKCount     int
}

// ─── Resolver ────────────────────────────────────────────────────────────────

// Resolver provides table metadata lookups from a single schema snapshot.
// It holds the full snapshot in memory for fast per-event lookups during indexing.
type Resolver struct {
	snapshotID int
	tables     map[string]*TableMeta // key: "schema.table"
}

// NewResolver loads all table metadata for the given snapshot from the index
// database. Pass snapshotID=0 to load the most recent snapshot automatically.
func NewResolver(db *sql.DB, snapshotID int) (*Resolver, error) {
	if snapshotID == 0 {
		row := db.QueryRow("SELECT COALESCE(MAX(snapshot_id), 0) FROM schema_snapshots")
		if err := row.Scan(&snapshotID); err != nil {
			return nil, fmt.Errorf("failed to query latest snapshot ID: %w", err)
		}
		if snapshotID == 0 {
			return nil, ErrNoSnapshots
		}
	}

	// column_type was added in #212; existing snapshots may lack the column
	// entirely. COALESCE handles the post-ALTER default empty string; for
	// databases still on the pre-ALTER schema, the caller must run
	// indexer.EnsureSchema first.
	rows, err := db.Query(`
		SELECT schema_name, table_name, column_name, ordinal_position,
		       column_key, data_type, COALESCE(column_type, '') AS column_type,
		       is_generated
		FROM schema_snapshots
		WHERE snapshot_id = ?
		ORDER BY schema_name, table_name, ordinal_position`,
		snapshotID)
	if err != nil {
		return nil, fmt.Errorf("failed to query snapshot %d: %w", snapshotID, err)
	}
	defer rows.Close()

	r := &Resolver{snapshotID: snapshotID, tables: make(map[string]*TableMeta)}
	sawColumnType := false
	sawDataType := false

	for rows.Next() {
		var schemaName, tableName, columnName, columnKey, dataType, columnType string
		var ordinalPosition int
		var isGenerated bool

		if err := rows.Scan(&schemaName, &tableName, &columnName, &ordinalPosition, &columnKey, &dataType, &columnType, &isGenerated); err != nil {
			return nil, fmt.Errorf("failed to scan snapshot row: %w", err)
		}

		key := schemaName + "." + tableName
		tm, ok := r.tables[key]
		if !ok {
			tm = &TableMeta{Schema: schemaName, Table: tableName}
			r.tables[key] = tm
		}

		col := ColumnMeta{
			Name:            columnName,
			OrdinalPosition: ordinalPosition,
			IsPK:            columnKey == "PRI",
			DataType:        dataType,
			ColumnType:      columnType,
			IsGenerated:     isGenerated,
		}
		if columnType != "" {
			sawColumnType = true
		}
		if dataType != "" {
			sawDataType = true
		}
		tm.Columns = append(tm.Columns, col)
		if col.IsPK {
			tm.PKColumns = append(tm.PKColumns, columnName)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate snapshot rows: %w", err)
	}

	// Pre-#212 snapshots have no column_type, so coerceUnsigned cannot tell which
	// integer columns are UNSIGNED and silently indexes them as-is. Warn once (not
	// per row) so an operator who upgraded for the unsigned fix (#490) isn't misled
	// into thinking a stale snapshot is corrected.
	//
	// Gate on sawDataType so this MySQL-only warning never fires for a PostgreSQL
	// snapshot (#533): WritePGSnapshot leaves both data_type AND column_type empty
	// (PG carries no MySQL DATA_TYPE/COLUMN_TYPE, and UNSIGNED sign-correction is a
	// MySQL-only concern coerceUnsigned never runs for PG rows). A genuine pre-#212
	// MySQL snapshot always has a non-empty data_type (from information_schema), so
	// it still trips the warning; only the all-empty-data_type PG signature is
	// suppressed.
	if len(r.tables) > 0 && sawDataType && !sawColumnType {
		slog.Warn("snapshot predates column_type capture (#212); UNSIGNED integer "+
			"columns cannot be sign-corrected and are indexed with the wrong value when "+
			"the high bit is set (unsigned PKs also corrupt pk_hash) — re-run "+
			"`bintrail snapshot` to enable the fix",
			"snapshot_id", snapshotID)
	}

	return r, nil
}

// NewResolverFromTables creates a Resolver directly from a pre-built table map.
// The map key must be "schema.table". Primarily useful for testing.
func NewResolverFromTables(snapshotID int, tables map[string]*TableMeta) *Resolver {
	return &Resolver{snapshotID: snapshotID, tables: tables}
}

// SnapshotID returns the snapshot ID this resolver was loaded from.
func (r *Resolver) SnapshotID() int { return r.snapshotID }

// TableCount returns the number of tables in this resolver.
func (r *Resolver) TableCount() int { return len(r.tables) }

// Tables returns every TableMeta whose schema matches the argument,
// sorted by table name for deterministic output. Used by the shim to
// answer SHOW TABLES FROM _flashback/_diff/_snapshot (#315) and by
// any future caller needing a list view of the snapshot.
//
// Returns an empty slice (not nil) when the schema is unknown — that's
// the same shape MySQL itself returns for SHOW TABLES FROM <empty db>,
// so callers can treat it as "nothing to display" without a nil check.
func (r *Resolver) Tables(schema string) []*TableMeta {
	out := make([]*TableMeta, 0)
	for _, t := range r.tables {
		if t.Schema == schema {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Table < out[j].Table })
	return out
}

// Resolve returns metadata for a given schema.table.
// Returns an error if the table is not found in the snapshot.
func (r *Resolver) Resolve(schema, table string) (*TableMeta, error) {
	key := schema + "." + table
	tm, ok := r.tables[key]
	if !ok {
		return nil, fmt.Errorf("table %s.%s not found in snapshot %d; consider re-running `bintrail snapshot`",
			schema, table, r.snapshotID)
	}
	return tm, nil
}

// MapRow maps a binlog row ([]any in column ordinal order) to a named
// map using column metadata from the snapshot. The binlog provides values by
// position — MapRow attaches the correct column names.
//
// Returns an error if the row length does not match the snapshot column count.
func (r *Resolver) MapRow(schema, table string, row []any) (map[string]any, error) {
	tm, err := r.Resolve(schema, table)
	if err != nil {
		return nil, err
	}
	if len(row) != len(tm.Columns) {
		return nil, fmt.Errorf(
			"column count mismatch for %s.%s: binlog has %d columns, snapshot has %d — consider re-running `bintrail snapshot`",
			schema, table, len(row), len(tm.Columns))
	}
	named := make(map[string]any, len(row))
	for i, col := range tm.Columns {
		named[col.Name] = coerceUnsigned(row[i], col)
	}
	return named, nil
}

// coerceUnsigned reinterprets an integer value decoded by go-mysql into the
// correct unsigned value. go-mysql always decodes INT-family columns as SIGNED
// (int8/int16/int32/int64) — it parses the TABLE_MAP SIGNEDNESS bitmap but never
// applies it (UnsignedMap is used only in its Dump output). So a column declared
// UNSIGNED whose value has the high bit set comes back negative (e.g.
// BIGINT UNSIGNED 18446744073709551615 → int64(-1)). Left uncorrected, the wrong
// value lands in the index and, for unsigned PKs, corrupts pk_values/pk_hash.
//
// Signedness is taken from the snapshot's ColumnType ("... unsigned"), NOT the
// TABLE_MAP SIGNEDNESS bitmap: coerceUnsigned runs inside MapRow, which works
// only off the resolver's snapshot and never sees the live TableMapEvent, so the
// bitmap is not reachable at this layer (and go-mysql would not apply it anyway —
// see above). ColumnType is always loaded by the resolver, independent of the
// source's binlog_row_metadata setting. Width is taken from DataType so the
// reinterpretation masks to the column's real size — MEDIUMINT is 3 bytes but
// go-mysql returns it as int32, so it must be masked to 24 bits (else
// MEDIUMINT UNSIGNED -1 would become 2^32-1 instead of 2^24-1).
//
// No-op when the column is not unsigned, when ColumnType is empty (pre-#212
// snapshots can't express signedness — NewResolver warns once in that case), or
// when the value is not a signed integer (NULL, string, and DECIMAL/FLOAT/DOUBLE
// UNSIGNED — which go-mysql returns as string/float — are returned unchanged).
// BIT is also reinterpreted here: go-mysql decodes it as a signed int64, so a
// BIT(64) with the high bit set comes back negative; it's mapped to uint64 (#497).
func coerceUnsigned(v any, col ColumnMeta) any {
	// BIT is an unsigned bit string. go-mysql decodes BIT(N) as int64, so BIT(64)
	// with the high bit set comes back negative; reinterpret as uint64 — identity
	// for BIT(1..63) (the value is non-negative as int64, so uint64() preserves
	// it). BIT's ColumnType is "bit(N)" (no "unsigned"), so handle it before the
	// unsigned gate below.
	if strings.ToLower(col.DataType) == "bit" {
		if i, ok := v.(int64); ok {
			return uint64(i)
		}
		// A NULL BIT arrives as nil and passes through here. Otherwise go-mysql
		// always decodes BIT as int64, so a non-nil non-int64 value can't occur
		// today; if a future go-mysql/MariaDB path delivered BIT as []byte/string,
		// leave it uninterpreted rather than mis-coerce — the original value.
		return v
	}
	if !strings.Contains(strings.ToLower(col.ColumnType), "unsigned") {
		return v
	}
	var signed int64
	switch x := v.(type) {
	case int8:
		signed = int64(x)
	case int16:
		signed = int64(x)
	case int32:
		signed = int64(x)
	case int64:
		signed = x
	default:
		return v // not a signed integer (NULL/string/decimal/...) — leave as-is
	}
	switch strings.ToLower(col.DataType) {
	case "tinyint":
		return uint8(signed)
	case "smallint":
		return uint16(signed)
	case "mediumint":
		return uint32(signed) & 0xFFFFFF
	case "int":
		return uint32(signed)
	case "bigint":
		return uint64(signed)
	default:
		return v
	}
}

// ─── PostgreSQL schema oracle (#533) ─────────────────────────────────────────

// PGRelationColumn is one column of a PostgreSQL relation's shape, decoded in-band
// from a pgoutput RelationMessage (no information_schema). Ordinal is the 1-based
// table-column position; IsPK marks a primary-key column; TypeOID/TypeMod are the
// pg_type OID and atttypmod, persisted now for the (deferred #533) type-faithful
// renderer even though slice-1 stores values as text and does not read them.
type PGRelationColumn struct {
	Name    string
	Ordinal int
	IsPK    bool
	TypeOID uint32
	TypeMod int32
}

// PGRelationSchema is a PostgreSQL relation's shape as seen on the logical-
// replication stream — the in-band, source-neutral payload an event.EventRelation
// carries from the pgcapture decoder to the consumer, which persists it via
// WritePGSnapshot. Columns are in table-ordinal order (so a primary key declared
// out of column order still yields ordinal-order pk_values matching the resolver —
// the pgcapture decoder reorders its catalog-key-order PK to match).
//
// It lives here, not in internal/event, because event imports metadata (so the
// reverse would be an import cycle) and because WritePGSnapshot — its only
// consumer — belongs next to TakeSnapshot.
type PGRelationSchema struct {
	Schema  string
	Table   string
	Columns []PGRelationColumn // table-ordinal order
}

// WritePGSnapshot persists one PostgreSQL relation's shape as a schema_snapshots
// snapshot and returns the allocated snapshot_id. It is the PostgreSQL,
// stream-time sibling of TakeSnapshot: where TakeSnapshot reads MySQL
// information_schema for a whole schema, WritePGSnapshot takes one relation's
// in-band shape (decoded from a pgoutput RelationMessage) so the offline recover
// path can build a PK-scoped WHERE without a live PostgreSQL connection — closing
// the remainder of #531 (PK-aware recovery on the PG path).
//
// Each call writes ONE table under a fresh snapshot_id (MAX+1), unlike MySQL where
// one snapshot covers a whole schema: PostgreSQL relations arrive one
// RelationMessage at a time, interleaved with rows, so each is its own snapshot and
// each PG row is stamped (by the consumer) with its table's snapshot_id.
// Consequence to respect in later slices: NewResolver(db, 0) (latest) yields a
// SINGLE table for a PG index, not a whole-schema view — recovery is unaffected (it
// loads each row's own snapshot_id), but a whole-schema consumer (console Tables(),
// shim SHOW TABLES) must not assume MAX(snapshot_id) is the full schema.
//
// PG columns leave the MySQL-only fields empty/NULL: data_type and is_nullable are
// the empty string (both NOT NULL columns, so empty string not NULL), column_type and
// column_default NULL, is_generated 0. The PostgreSQL type identity rides the nullable
// pg_type_oid/pg_type_mod columns for the deferred type-faithful renderer.
func WritePGSnapshot(db *sql.DB, rel *PGRelationSchema) (int, error) {
	if rel == nil || len(rel.Columns) == 0 {
		return 0, fmt.Errorf("metadata: WritePGSnapshot requires a relation with at least one column")
	}

	tx, err := db.Begin()
	if err != nil {
		return 0, fmt.Errorf("metadata: WritePGSnapshot begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// Allocate the next snapshot_id inside the transaction (same scheme as
	// TakeSnapshot). MySQL and PG snapshots coexist in one table with distinct ids.
	var nextID int
	if err = tx.QueryRow("SELECT COALESCE(MAX(snapshot_id), 0) + 1 FROM schema_snapshots").Scan(&nextID); err != nil {
		return 0, fmt.Errorf("metadata: WritePGSnapshot allocate snapshot_id: %w", err)
	}

	snapshotTime := time.Now().UTC()
	valClause := strings.TrimRight(strings.Repeat("(?,?,?,?,?,?,?,?,?,?,?,?,?,?),", len(rel.Columns)), ",")
	insertSQL := "INSERT INTO schema_snapshots " +
		"(snapshot_id, snapshot_time, schema_name, table_name, column_name, " +
		"ordinal_position, column_key, data_type, column_type, is_nullable, column_default, is_generated, " +
		"pg_type_oid, pg_type_mod) VALUES " + valClause

	args := make([]any, 0, len(rel.Columns)*14)
	for _, c := range rel.Columns {
		columnKey := ""
		if c.IsPK {
			columnKey = "PRI"
		}
		args = append(args,
			nextID, snapshotTime, rel.Schema, rel.Table, c.Name,
			c.Ordinal, columnKey, "", nil, "", nil, false,
			c.TypeOID, c.TypeMod,
		)
	}
	if _, err = tx.Exec(insertSQL, args...); err != nil {
		return 0, fmt.Errorf("metadata: WritePGSnapshot insert %s.%s: %w", rel.Schema, rel.Table, err)
	}

	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("metadata: WritePGSnapshot commit: %w", err)
	}
	committed = true
	return nextID, nil
}

// ─── TakeSnapshot ────────────────────────────────────────────────────────────

// columnRow holds a single row from information_schema.COLUMNS as fetched by TakeSnapshot.
type columnRow struct {
	schemaName, tableName, columnName string
	ordinalPosition                   int
	columnKey, dataType, isNullable   string
	columnType                        string // full COLUMN_TYPE (e.g. "datetime(6)"); needed by full-table reconstruct for PK precision
	extra                             string
	columnDefault                     sql.NullString
}

// fkRow holds a single foreign key column mapping as fetched from
// INFORMATION_SCHEMA.KEY_COLUMN_USAGE joined with REFERENTIAL_CONSTRAINTS.
type fkRow struct {
	constraintName       string
	schemaName           string
	tableName            string
	columnName           string
	ordinalPosition      int
	referencedSchemaName string
	referencedTableName  string
	referencedColumnName string
	deleteRule           string // ON DELETE rule (CASCADE/RESTRICT/SET NULL/NO ACTION)
	updateRule           string // ON UPDATE rule
}

// TakeSnapshot reads column metadata and foreign key constraints from
// information_schema on the source server and writes them atomically into
// schema_snapshots and fk_constraints in the index database.
//
// If schemas is empty, all non-system schemas are captured. The new snapshot_id
// is allocated inside the transaction via MAX(snapshot_id)+1, so concurrent
// snapshot runs (rare in CLI usage) won't collide.
func TakeSnapshot(sourceDB, indexDB *sql.DB, schemas []string) (SnapshotStats, error) {
	// ── 1. Query information_schema on the source server ─────────────────────
	var (
		query string
		args  []any
	)

	if len(schemas) == 0 {
		query = `
			SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME,
			       ORDINAL_POSITION, COLUMN_KEY, DATA_TYPE, COLUMN_TYPE,
			       IS_NULLABLE, COLUMN_DEFAULT, EXTRA
			FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA NOT IN ('information_schema','performance_schema','mysql','sys')
			ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION`
	} else {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(schemas)), ",")
		query = fmt.Sprintf(`
			SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME,
			       ORDINAL_POSITION, COLUMN_KEY, DATA_TYPE, COLUMN_TYPE,
			       IS_NULLABLE, COLUMN_DEFAULT, EXTRA
			FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA IN (%s)
			ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION`, placeholders)
		for _, s := range schemas {
			args = append(args, s)
		}
	}

	srcRows, err := sourceDB.Query(query, args...)
	if err != nil {
		return SnapshotStats{}, fmt.Errorf("failed to query information_schema.COLUMNS: %w", err)
	}
	defer srcRows.Close()

	var columns []columnRow
	seenTables := make(map[string]struct{})

	for srcRows.Next() {
		var c columnRow
		if err := srcRows.Scan(
			&c.schemaName, &c.tableName, &c.columnName,
			&c.ordinalPosition, &c.columnKey, &c.dataType, &c.columnType,
			&c.isNullable, &c.columnDefault, &c.extra,
		); err != nil {
			return SnapshotStats{}, fmt.Errorf("failed to scan column row: %w", err)
		}
		columns = append(columns, c)
		seenTables[c.schemaName+"."+c.tableName] = struct{}{}
	}
	if err := srcRows.Err(); err != nil {
		return SnapshotStats{}, fmt.Errorf("failed to iterate source columns: %w", err)
	}
	srcRows.Close() // close early before the write transaction

	if len(columns) == 0 {
		return SnapshotStats{}, fmt.Errorf(
			"no columns found for the requested schemas — if the schema has no tables yet, " +
				"create at least one table first; otherwise check --schemas and source server permissions")
	}

	// ── 1b. Validate: all tables must be InnoDB with explicit PKs ────────────
	if err := validateTables(sourceDB, schemas, columns); err != nil {
		return SnapshotStats{}, err
	}

	// ── 1c. Query FK constraints from the source server ─────────────────────
	fkRows, err := queryFKConstraints(sourceDB, schemas)
	if err != nil {
		return SnapshotStats{}, err
	}

	// ── 2. Write snapshot atomically into the index database ─────────────────
	tx, err := indexDB.Begin()
	if err != nil {
		return SnapshotStats{}, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// Allocate the next snapshot_id inside the transaction. The SELECT…FOR
	// UPDATE is not needed here because snapshot runs are serial in CLI usage,
	// but even concurrent runs would simply get the same next ID and produce
	// two snapshots with distinct row content but same ID — acceptable.
	var nextID int
	if err = tx.QueryRow(
		"SELECT COALESCE(MAX(snapshot_id), 0) + 1 FROM schema_snapshots",
	).Scan(&nextID); err != nil {
		return SnapshotStats{}, fmt.Errorf("failed to allocate snapshot_id: %w", err)
	}

	snapshotTime := time.Now().UTC()

	// Batch in groups of 500 rows to stay within default max_allowed_packet.
	const batchSize = 500
	for i := 0; i < len(columns); i += batchSize {
		batch := columns[i:min(i+batchSize, len(columns))]

		valClause := strings.TrimRight(strings.Repeat("(?,?,?,?,?,?,?,?,?,?,?,?),", len(batch)), ",")
		insertSQL := "INSERT INTO schema_snapshots " +
			"(snapshot_id, snapshot_time, schema_name, table_name, column_name, " +
			"ordinal_position, column_key, data_type, column_type, is_nullable, column_default, is_generated) VALUES " +
			valClause

		insertArgs := make([]any, 0, len(batch)*12)
		for _, c := range batch {
			var def any
			if c.columnDefault.Valid {
				def = c.columnDefault.String
			}
			isGenerated := strings.Contains(strings.ToUpper(c.extra), "GENERATED")
			insertArgs = append(insertArgs,
				nextID, snapshotTime, c.schemaName, c.tableName, c.columnName,
				c.ordinalPosition, c.columnKey, c.dataType, c.columnType, c.isNullable, def, isGenerated,
			)
		}

		if _, err = tx.Exec(insertSQL, insertArgs...); err != nil {
			return SnapshotStats{}, fmt.Errorf("failed to insert snapshot batch: %w", err)
		}
	}

	// ── 2b. Insert FK constraint rows ───────────────────────────────────────
	// Check that the fk_constraints table exists before inserting. Existing
	// installations that upgrade without re-running `bintrail init` would
	// otherwise fail the entire snapshot (including column metadata) because
	// the transaction rolls back.
	var fkTableExists bool
	if err = tx.QueryRow(
		"SELECT COUNT(*) > 0 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'fk_constraints'",
	).Scan(&fkTableExists); err != nil {
		return SnapshotStats{}, fmt.Errorf("failed to check fk_constraints table: %w", err)
	}

	fkCount := 0
	if !fkTableExists {
		if len(fkRows) > 0 {
			slog.Warn("fk_constraints table does not exist; skipping FK capture (run 'bintrail init' to create it)")
		}
	} else {
		for i := 0; i < len(fkRows); i += batchSize {
			batch := fkRows[i:min(i+batchSize, len(fkRows))]

			valClause := strings.TrimRight(strings.Repeat("(?,?,?,?,?,?,?,?,?,?,?),", len(batch)), ",")
			insertSQL := "INSERT INTO fk_constraints " +
				"(snapshot_id, constraint_name, schema_name, table_name, column_name, " +
				"ordinal_position, referenced_schema_name, referenced_table_name, referenced_column_name, " +
				"delete_rule, update_rule) VALUES " +
				valClause

			insertArgs := make([]any, 0, len(batch)*11)
			for _, fk := range batch {
				insertArgs = append(insertArgs,
					nextID, fk.constraintName, fk.schemaName, fk.tableName, fk.columnName,
					fk.ordinalPosition, fk.referencedSchemaName, fk.referencedTableName, fk.referencedColumnName,
					fk.deleteRule, fk.updateRule,
				)
			}

			if _, err = tx.Exec(insertSQL, insertArgs...); err != nil {
				return SnapshotStats{}, fmt.Errorf("failed to insert fk_constraints batch: %w", err)
			}
		}
		fkCount = len(fkRows)
	}

	if err = tx.Commit(); err != nil {
		return SnapshotStats{}, fmt.Errorf("failed to commit snapshot: %w", err)
	}

	return SnapshotStats{
		SnapshotID:  nextID,
		TableCount:  len(seenTables),
		ColumnCount: len(columns),
		FKCount:     fkCount,
	}, nil
}

// queryFKConstraints reads foreign key constraint metadata from the source
// database by joining KEY_COLUMN_USAGE with REFERENTIAL_CONSTRAINTS. The result
// includes one row per FK column mapping. Returns nil (not an error) when the
// source has no foreign keys.
func queryFKConstraints(sourceDB *sql.DB, schemas []string) ([]fkRow, error) {
	var (
		query string
		args  []any
	)

	if len(schemas) == 0 {
		query = `
			SELECT kcu.CONSTRAINT_NAME, kcu.TABLE_SCHEMA, kcu.TABLE_NAME,
			       kcu.COLUMN_NAME, kcu.ORDINAL_POSITION,
			       kcu.REFERENCED_TABLE_SCHEMA, kcu.REFERENCED_TABLE_NAME,
			       kcu.REFERENCED_COLUMN_NAME, rc.DELETE_RULE, rc.UPDATE_RULE
			FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
			JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
			    ON rc.CONSTRAINT_SCHEMA = kcu.TABLE_SCHEMA
			    AND rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
			WHERE kcu.TABLE_SCHEMA NOT IN ('information_schema','performance_schema','mysql','sys')
			    AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
			ORDER BY kcu.TABLE_SCHEMA, kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION`
	} else {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(schemas)), ",")
		query = fmt.Sprintf(`
			SELECT kcu.CONSTRAINT_NAME, kcu.TABLE_SCHEMA, kcu.TABLE_NAME,
			       kcu.COLUMN_NAME, kcu.ORDINAL_POSITION,
			       kcu.REFERENCED_TABLE_SCHEMA, kcu.REFERENCED_TABLE_NAME,
			       kcu.REFERENCED_COLUMN_NAME, rc.DELETE_RULE, rc.UPDATE_RULE
			FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
			JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
			    ON rc.CONSTRAINT_SCHEMA = kcu.TABLE_SCHEMA
			    AND rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
			WHERE kcu.TABLE_SCHEMA IN (%s)
			    AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
			ORDER BY kcu.TABLE_SCHEMA, kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION`, placeholders)
		for _, s := range schemas {
			args = append(args, s)
		}
	}

	rows, err := sourceDB.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query FK constraints: %w", err)
	}
	defer rows.Close()

	var fks []fkRow
	for rows.Next() {
		var fk fkRow
		if err := rows.Scan(
			&fk.constraintName, &fk.schemaName, &fk.tableName,
			&fk.columnName, &fk.ordinalPosition,
			&fk.referencedSchemaName, &fk.referencedTableName, &fk.referencedColumnName,
			&fk.deleteRule, &fk.updateRule,
		); err != nil {
			return nil, fmt.Errorf("failed to scan FK row: %w", err)
		}
		fks = append(fks, fk)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate FK rows: %w", err)
	}

	return fks, nil
}

// validateTables checks that all base tables in scope use InnoDB and have an
// explicit primary key. Bintrail requires InnoDB for row-format binary log
// support and needs primary keys to build pk_values for each event.
// Returns an error listing all violations; returns nil when all tables pass.
func validateTables(sourceDB *sql.DB, schemas []string, columns []columnRow) error {
	var (
		tabQuery string
		tabArgs  []any
	)
	if len(schemas) == 0 {
		tabQuery = `
			SELECT TABLE_SCHEMA, TABLE_NAME, ENGINE
			FROM information_schema.TABLES
			WHERE TABLE_SCHEMA NOT IN ('information_schema','performance_schema','mysql','sys')
			  AND TABLE_TYPE = 'BASE TABLE'
			ORDER BY TABLE_SCHEMA, TABLE_NAME`
	} else {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(schemas)), ",")
		tabQuery = fmt.Sprintf(`
			SELECT TABLE_SCHEMA, TABLE_NAME, ENGINE
			FROM information_schema.TABLES
			WHERE TABLE_SCHEMA IN (%s)
			  AND TABLE_TYPE = 'BASE TABLE'
			ORDER BY TABLE_SCHEMA, TABLE_NAME`, placeholders)
		for _, s := range schemas {
			tabArgs = append(tabArgs, s)
		}
	}

	tabRows, err := sourceDB.Query(tabQuery, tabArgs...)
	if err != nil {
		return fmt.Errorf("failed to query information_schema.TABLES: %w", err)
	}
	defer tabRows.Close()

	baseTables := make(map[string]struct{})
	var nonInnoDB []string

	for tabRows.Next() {
		var schemaName, tableName string
		var engine sql.NullString
		if err := tabRows.Scan(&schemaName, &tableName, &engine); err != nil {
			return fmt.Errorf("failed to scan table row: %w", err)
		}
		key := schemaName + "." + tableName
		baseTables[key] = struct{}{}
		if !engine.Valid || !strings.EqualFold(engine.String, "InnoDB") {
			nonInnoDB = append(nonInnoDB, key)
		}
	}
	if err := tabRows.Err(); err != nil {
		return fmt.Errorf("failed to iterate tables: %w", err)
	}

	// Build the set of tables that have at least one PK column.
	tablesWithPK := make(map[string]bool)
	for _, c := range columns {
		if c.columnKey == "PRI" {
			tablesWithPK[c.schemaName+"."+c.tableName] = true
		}
	}

	// Find base tables with no PK column.
	var noPK []string
	for key := range baseTables {
		if !tablesWithPK[key] {
			noPK = append(noPK, key)
		}
	}

	sort.Strings(nonInnoDB)
	sort.Strings(noPK)

	if len(nonInnoDB) == 0 && len(noPK) == 0 {
		return nil
	}

	var msgs []string
	if len(nonInnoDB) > 0 {
		msgs = append(msgs, fmt.Sprintf("tables not using InnoDB: %s", strings.Join(nonInnoDB, ", ")))
	}
	if len(noPK) > 0 {
		msgs = append(msgs, fmt.Sprintf("tables without a primary key: %s", strings.Join(noPK, ", ")))
	}
	return fmt.Errorf(
		"snapshot validation failed — bintrail requires all tables to use InnoDB with an explicit primary key\n%s",
		strings.Join(msgs, "\n"),
	)
}

// ─── Source pre-flight ──────────────────────────────────────────────────────

// ValidateBinlogFormat checks that the source server has binlog_format=ROW.
func ValidateBinlogFormat(db *sql.DB) error {
	var varName, val string
	err := db.QueryRow("SHOW VARIABLES LIKE 'binlog_format'").Scan(&varName, &val)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("binlog_format not found on source server")
	}
	if err != nil {
		return fmt.Errorf("failed to query binlog_format: %w", err)
	}
	if !strings.EqualFold(val, "ROW") {
		return fmt.Errorf("source server has binlog_format=%q; bintrail requires ROW", val)
	}
	return nil
}

// ValidateBinlogRowImage checks that the source server has binlog_row_image=FULL.
func ValidateBinlogRowImage(db *sql.DB) error {
	var varName, val string
	err := db.QueryRow("SHOW VARIABLES LIKE 'binlog_row_image'").Scan(&varName, &val)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("binlog_row_image not found on source server; MySQL 5.6+ with binlog_row_image=FULL is required")
	}
	if err != nil {
		return fmt.Errorf("failed to query binlog_row_image: %w", err)
	}
	if !strings.EqualFold(val, "FULL") {
		return fmt.Errorf("source server has binlog_row_image=%q; bintrail requires FULL", val)
	}
	return nil
}

// DetectFlavor reports the source server flavor by inspecting VERSION():
// "mariadb" when the version string contains "MariaDB" (case-insensitive),
// otherwise "mysql" (which also covers Percona). On a query error it returns ""
// (unknown) rather than fabricating a flavor — detection is advisory (surfacing a
// --source-flavor mismatch), and callers must treat "" as "could not determine"
// instead of asserting a false mismatch. Returns bare strings to keep
// internal/metadata free of a go-mysql dependency.
func DetectFlavor(db *sql.DB) string {
	var version string
	if err := db.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		return ""
	}
	if strings.Contains(strings.ToLower(version), "mariadb") {
		return "mariadb"
	}
	return "mysql"
}

// buildFKCascadeQuery returns the REFERENTIAL_CONSTRAINTS query (and its args)
// used to find CASCADE foreign keys. When schemas is non-empty the scan is
// scoped to exactly those schemas — the operator explicitly asked us to index
// them, so we police them as named. When schemas is empty we scan every schema
// except (a) MySQL's own system schemas and (b) bintrail's own index schemas.
//
// A bintrail index schema is recognised structurally — by the signature tables
// `bintrail init` creates (binlog_events, schema_snapshots and stream_state must
// all be present) — not by name. This is what lets the pre-flight skip
// bintrail's own `access_rules`→`profiles` ON DELETE CASCADE so an agent does
// not fatal-fail on its own (or another agent's) index DB sharing the source
// MySQL (#347), while still flagging a genuine user schema whatever it is named
// (#365). The signature tables are created before access_rules, so any schema
// carrying that cascade necessarily carries the signature too.
func buildFKCascadeQuery(schemas []string) (string, []any) {
	query := `SELECT CONSTRAINT_SCHEMA, CONSTRAINT_NAME, DELETE_RULE, UPDATE_RULE
		FROM information_schema.REFERENTIAL_CONSTRAINTS
		WHERE (DELETE_RULE = 'CASCADE' OR UPDATE_RULE = 'CASCADE')`

	var args []any
	if len(schemas) > 0 {
		placeholders := strings.Repeat("?,", len(schemas))
		query += " AND CONSTRAINT_SCHEMA IN (" + placeholders[:len(placeholders)-1] + ")"
		for _, s := range schemas {
			args = append(args, s)
		}
	} else {
		// Skip bintrail's own index schemas, identified by their signature tables
		// rather than by name: a schema counts as bintrail-internal only if it
		// holds ALL of binlog_events, schema_snapshots and stream_state (HAVING
		// = 3), so a user schema that merely shares one of those names is still
		// scanned.
		query += " AND CONSTRAINT_SCHEMA NOT IN ('mysql','information_schema','performance_schema','sys')" +
			" AND CONSTRAINT_SCHEMA NOT IN (" +
			"SELECT TABLE_SCHEMA FROM information_schema.TABLES" +
			" WHERE TABLE_TYPE = 'BASE TABLE'" +
			" AND TABLE_NAME IN ('binlog_events','schema_snapshots','stream_state')" +
			" GROUP BY TABLE_SCHEMA HAVING COUNT(DISTINCT TABLE_NAME) = 3)"
	}
	return query, args
}

// ErrFKCascadesFound is wrapped into the error ValidateNoFKCascades returns when
// the source carries FK CASCADE constraints — as opposed to an operational
// failure (a dropped connection, a permissions error reading
// information_schema). Call sites use errors.Is to tell the two apart: a cascade
// finding is now a warn-and-proceed signal, while a genuine query failure must
// still abort. Without this distinction a real fault would be silently
// downgraded to a warning and mislabeled as "cascades present".
var ErrFKCascadesFound = errors.New("FK cascade constraints present on source")

// ValidateNoFKCascades checks that none of the targeted schemas contain foreign
// key constraints with CASCADE rules. When schemas is empty, all non-system,
// non-bintrail-internal schemas are checked (see buildFKCascadeQuery). FK
// cascades produce invisible side-effect row changes that make reversal SQL
// unreliable. A cascade finding is returned wrapped in ErrFKCascadesFound; any
// other returned error is an operational failure.
func ValidateNoFKCascades(db *sql.DB, schemas []string) error {
	query, args := buildFKCascadeQuery(schemas)

	// The unscoped scan skips schemas that look like a bintrail index — those
	// holding all of bintrail's signature tables (see buildFKCascadeQuery) — so a
	// clean result does not cover them. Disclose the rule, naming the signature
	// tables rather than asserting the skipped schemas are definitely bintrail's:
	// a user schema that replicated those table names would be skipped too, and
	// the operator should be able to recognise that case.
	if len(schemas) == 0 {
		slog.Info("FK cascade pre-flight skips schemas that look like a bintrail index DB " +
			"(those containing binlog_events, schema_snapshots and stream_state); a clean result does not cover them")
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return fmt.Errorf("failed to query FK cascades: %w", err)
	}
	defer rows.Close()

	type cascade struct{ schema, name, deleteRule, updateRule string }
	var found []cascade
	for rows.Next() {
		var c cascade
		if err := rows.Scan(&c.schema, &c.name, &c.deleteRule, &c.updateRule); err != nil {
			return fmt.Errorf("failed to scan FK cascade row: %w", err)
		}
		found = append(found, c)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate FK cascade rows: %w", err)
	}

	if len(found) > 0 {
		for _, c := range found {
			slog.Warn("FK cascade constraint found on source",
				"source_schema", c.schema, "constraint", c.name,
				"delete_rule", c.deleteRule, "update_rule", c.updateRule)
		}
		return fmt.Errorf("%d FK cascade constraint(s) found on source; reversal SQL from `recover` may not correctly handle cascade side-effects: %w", len(found), ErrFKCascadesFound)
	}
	return nil
}

// FKCascadeEdge describes a CASCADE foreign-key edge recorded in the index's
// fk_constraints table (latest snapshot).
type FKCascadeEdge struct {
	Schema          string
	Table           string
	Column          string
	ReferencedTable string
	DeleteRule      string
	UpdateRule      string
}

// CascadeConstraintsInIndex returns the CASCADE foreign-key edges recorded in
// the latest snapshot that captured FK rows (MAX(snapshot_id) in fk_constraints),
// optionally scoped to schemas. Unlike ValidateNoFKCascades (which queries the
// source's information_schema), this reads from the INDEX, so the source-less
// `recover` path can warn that cascade-deleted child rows are not reversible by
// plain recover. (If a newer snapshot recorded zero FKs, this can surface a
// cascade warning from an older snapshot — acceptable for a warn-only path.)
//
// Returns nil when fk_constraints is absent (index predates it) or carries no
// cascade rules — including pre-cascade-recovery snapshots whose delete_rule/
// update_rule columns are empty.
func CascadeConstraintsInIndex(indexDB *sql.DB, schemas []string) ([]FKCascadeEdge, error) {
	var exists bool
	if err := indexDB.QueryRow(
		"SELECT COUNT(*) > 0 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'fk_constraints'",
	).Scan(&exists); err != nil {
		return nil, fmt.Errorf("failed to check fk_constraints table: %w", err)
	}
	if !exists {
		return nil, nil
	}

	query := `SELECT schema_name, table_name, column_name, referenced_table_name, delete_rule, update_rule
		FROM fk_constraints
		WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM fk_constraints)
		  AND (delete_rule = 'CASCADE' OR update_rule = 'CASCADE')`
	var args []any
	if len(schemas) > 0 {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(schemas)), ",")
		query += " AND schema_name IN (" + placeholders + ")"
		for _, s := range schemas {
			args = append(args, s)
		}
	}
	query += " ORDER BY schema_name, table_name, column_name"

	rows, err := indexDB.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query cascade FK constraints: %w", err)
	}
	defer rows.Close()

	var out []FKCascadeEdge
	for rows.Next() {
		var e FKCascadeEdge
		if err := rows.Scan(&e.Schema, &e.Table, &e.Column, &e.ReferencedTable, &e.DeleteRule, &e.UpdateRule); err != nil {
			return nil, fmt.Errorf("failed to scan cascade FK row: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// EnsureResolver returns a Resolver loaded from the latest snapshot, taking a
// new snapshot automatically if none exists (requires sourceDB != nil).
func EnsureResolver(indexDB, sourceDB *sql.DB, schemas []string) (*Resolver, error) {
	var snapshotID int
	if err := indexDB.QueryRow(
		"SELECT COALESCE(MAX(snapshot_id), 0) FROM schema_snapshots",
	).Scan(&snapshotID); err != nil {
		return nil, fmt.Errorf("failed to query schema snapshots: %w", err)
	}

	if snapshotID == 0 {
		if sourceDB == nil {
			return nil, fmt.Errorf(
				"no schema snapshot exists and --source-dsn was not provided; " +
					"run `bintrail snapshot` first or add --source-dsn for auto-snapshot")
		}
		fmt.Println("No snapshot found; taking schema snapshot automatically...")
		stats, err := TakeSnapshot(sourceDB, indexDB, schemas)
		if err != nil {
			return nil, fmt.Errorf("auto-snapshot failed: %w", err)
		}
		fmt.Printf("  snapshot_id=%d, tables=%d, columns=%d\n",
			stats.SnapshotID, stats.TableCount, stats.ColumnCount)
		snapshotID = stats.SnapshotID
	}

	return NewResolver(indexDB, snapshotID)
}

// HasReplPrivileges checks a list of SHOW GRANTS output lines for REPLICATION
// SLAVE and REPLICATION CLIENT privileges. A pure source pre-flight parser
// shared by `bintrail agent --validate` and the doctor's replication-grants
// check, so it lives in metadata next to the other source validators rather
// than in either consumer.
func HasReplPrivileges(grants []string) (slave, client bool) {
	for _, grant := range grants {
		upper := strings.ToUpper(grant)
		if strings.Contains(upper, "ALL PRIVILEGES") {
			return true, true
		}
		if strings.Contains(upper, "REPLICATION SLAVE") {
			slave = true
		}
		if strings.Contains(upper, "REPLICATION CLIENT") {
			client = true
		}
	}
	return
}
