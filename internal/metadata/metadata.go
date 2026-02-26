package metadata

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// ─── Types ───────────────────────────────────────────────────────────────────

// ColumnMeta holds metadata for a single table column.
type ColumnMeta struct {
	Name            string
	OrdinalPosition int
	IsPK            bool
	DataType        string
	IsGenerated     bool // true for STORED or VIRTUAL generated columns
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
			return nil, fmt.Errorf("no snapshots found; run `bintrail snapshot` first")
		}
	}

	rows, err := db.Query(`
		SELECT schema_name, table_name, column_name, ordinal_position, column_key, data_type, is_generated
		FROM schema_snapshots
		WHERE snapshot_id = ?
		ORDER BY schema_name, table_name, ordinal_position`,
		snapshotID)
	if err != nil {
		return nil, fmt.Errorf("failed to query snapshot %d: %w", snapshotID, err)
	}
	defer rows.Close()

	r := &Resolver{snapshotID: snapshotID, tables: make(map[string]*TableMeta)}

	for rows.Next() {
		var schemaName, tableName, columnName, columnKey, dataType string
		var ordinalPosition int
		var isGenerated bool

		if err := rows.Scan(&schemaName, &tableName, &columnName, &ordinalPosition, &columnKey, &dataType, &isGenerated); err != nil {
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
			IsGenerated:     isGenerated,
		}
		tm.Columns = append(tm.Columns, col)
		if col.IsPK {
			tm.PKColumns = append(tm.PKColumns, columnName)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate snapshot rows: %w", err)
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
		named[col.Name] = row[i]
	}
	return named, nil
}

// ─── TakeSnapshot ────────────────────────────────────────────────────────────

// TakeSnapshot reads column metadata from information_schema on the source
// server and writes it atomically into schema_snapshots in the index database.
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
			       ORDINAL_POSITION, COLUMN_KEY, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA
			FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA NOT IN ('information_schema','performance_schema','mysql','sys')
			ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION`
	} else {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(schemas)), ",")
		query = fmt.Sprintf(`
			SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME,
			       ORDINAL_POSITION, COLUMN_KEY, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA
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

	type columnRow struct {
		schemaName, tableName, columnName string
		ordinalPosition                   int
		columnKey, dataType, isNullable   string
		extra                             string
		columnDefault                     sql.NullString
	}

	var columns []columnRow
	seenTables := make(map[string]struct{})

	for srcRows.Next() {
		var c columnRow
		if err := srcRows.Scan(
			&c.schemaName, &c.tableName, &c.columnName,
			&c.ordinalPosition, &c.columnKey, &c.dataType, &c.isNullable, &c.columnDefault, &c.extra,
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
			"no columns found for the requested schemas; check --schemas and source server permissions")
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

		valClause := strings.TrimRight(strings.Repeat("(?,?,?,?,?,?,?,?,?,?,?),", len(batch)), ",")
		insertSQL := "INSERT INTO schema_snapshots " +
			"(snapshot_id, snapshot_time, schema_name, table_name, column_name, " +
			"ordinal_position, column_key, data_type, is_nullable, column_default, is_generated) VALUES " +
			valClause

		insertArgs := make([]any, 0, len(batch)*11)
		for _, c := range batch {
			var def any
			if c.columnDefault.Valid {
				def = c.columnDefault.String
			}
			isGenerated := strings.Contains(strings.ToUpper(c.extra), "GENERATED")
			insertArgs = append(insertArgs,
				nextID, snapshotTime, c.schemaName, c.tableName, c.columnName,
				c.ordinalPosition, c.columnKey, c.dataType, c.isNullable, def, isGenerated,
			)
		}

		if _, err = tx.Exec(insertSQL, insertArgs...); err != nil {
			return SnapshotStats{}, fmt.Errorf("failed to insert snapshot batch: %w", err)
		}
	}

	if err = tx.Commit(); err != nil {
		return SnapshotStats{}, fmt.Errorf("failed to commit snapshot: %w", err)
	}

	return SnapshotStats{
		SnapshotID:  nextID,
		TableCount:  len(seenTables),
		ColumnCount: len(columns),
	}, nil
}
