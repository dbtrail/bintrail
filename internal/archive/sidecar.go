package archive

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// MetaSidecarName is the durable-state sidecar written next to a source's
// archives (directly under <root>/bintrail_id=<id>/). The archive tier holds
// every event, but a rebuilt index also needs schema_snapshots (to resolve
// row images) and the server identity — neither lives in the event files, so
// rotation persists this small JSON alongside them (#1196) and restore-index
// reads it back. stream_state/index_state are deliberately NOT here: a
// replication position that survived an index loss is stale and trusting it
// would fake continuity — the runbook restarts the stream cleanly instead.
const MetaSidecarName = "index-meta.json"

// MetaSidecar is the sidecar's content: generic row dumps so the format
// follows the tables without a parallel schema. Values round-trip through
// JSON (numbers as float64 — fine for these tables' small integers).
type MetaSidecar struct {
	WrittenAt       time.Time        `json:"written_at"`
	SchemaSnapshots []map[string]any `json:"schema_snapshots"`
	BintrailServers []map[string]any `json:"bintrail_servers"`
}

// WriteMetaSidecar dumps schema_snapshots + bintrail_servers into
// dir/index-meta.json (atomic tmp+rename). Callers treat failures as
// best-effort — a sidecar write must never fail rotation.
func WriteMetaSidecar(ctx context.Context, db *sql.DB, dir string) error {
	snaps, err := dumpTable(ctx, db, "schema_snapshots")
	if err != nil {
		return err
	}
	servers, err := dumpTable(ctx, db, "bintrail_servers")
	if err != nil {
		return err
	}
	m := MetaSidecar{WrittenAt: time.Now().UTC(), SchemaSnapshots: snaps, BintrailServers: servers}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, MetaSidecarName)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// ReadMetaSidecar parses a sidecar file.
func ReadMetaSidecar(path string) (*MetaSidecar, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m MetaSidecar
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse sidecar %s: %w", path, err)
	}
	return &m, nil
}

// RestoreMetaSidecar inserts the sidecar's rows into the (fresh) index,
// atomically: a mid-restore failure must leave ZERO rows — a partially
// restored snapshot group would let the resolver decode row images against
// an incomplete table set. Row keys are filtered against the target table's
// actual columns, so a sidecar written by a different binary version
// degrades to the shared columns instead of erroring on a renamed one.
func RestoreMetaSidecar(ctx context.Context, db *sql.DB, m *MetaSidecar) (snapshots, servers int64, err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit
	snapshots, err = restoreRows(ctx, tx, "schema_snapshots", m.SchemaSnapshots)
	if err != nil {
		return 0, 0, err
	}
	servers, err = restoreRows(ctx, tx, "bintrail_servers", m.BintrailServers)
	if err != nil {
		return 0, 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, err
	}
	return snapshots, servers, nil
}

func dumpTable(ctx context.Context, db *sql.DB, table string) ([]map[string]any, error) {
	rows, err := db.QueryContext(ctx, "SELECT * FROM `"+table+"`")
	if err != nil {
		return nil, fmt.Errorf("dump %s: %w", table, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out []map[string]any
	for rows.Next() {
		scan := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range scan {
			ptrs[i] = &scan[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("dump %s: %w", table, err)
		}
		row := make(map[string]any, len(cols))
		for i, c := range cols {
			v := scan[i]
			if b, ok := v.([]byte); ok {
				v = string(b)
			}
			row[c] = v
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func restoreRows(ctx context.Context, tx *sql.Tx, table string, rowsIn []map[string]any) (int64, error) {
	if len(rowsIn) == 0 {
		return 0, nil
	}
	// Insertable columns of the target table, with their types. Generated
	// columns are excluded via GENERATION_EXPRESSION, NOT `EXTRA LIKE
	// '%GENERATED%'` — EXTRA also reports DEFAULT_GENERATED for ordinary
	// expression-default columns (bintrail_servers' created_at/updated_at),
	// and the substring match would silently drop those real data columns
	// (the trap consistency/checksum.go and reconstruct/fulltable.go both
	// document).
	colRows, err := tx.QueryContext(ctx, `
		SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
		  AND (GENERATION_EXPRESSION IS NULL OR GENERATION_EXPRESSION = '')`, table)
	if err != nil {
		return 0, fmt.Errorf("introspect %s: %w", table, err)
	}
	defer colRows.Close()
	target := map[string]string{}
	for colRows.Next() {
		var name, dataType string
		if err := colRows.Scan(&name, &dataType); err != nil {
			return 0, err
		}
		target[name] = strings.ToLower(dataType)
	}
	if err := colRows.Err(); err != nil {
		return 0, err
	}

	var n int64
	for _, row := range rowsIn {
		var cols []string
		var args []any
		for k, v := range row {
			dataType, ok := target[k]
			if !ok {
				continue
			}
			// time.Time values were JSON-marshaled as RFC3339; MySQL's
			// DATETIME parser rejects the T/Z form. Conversion is driven by
			// the COLUMN type — probing every string would silently rewrite
			// a VARCHAR whose content merely looks like a timestamp.
			if dataType == "datetime" || dataType == "timestamp" {
				if s, ok := v.(string); ok {
					if ts, err := time.Parse(time.RFC3339, s); err == nil {
						v = ts.UTC().Format("2006-01-02 15:04:05")
					}
				}
			}
			cols = append(cols, "`"+k+"`")
			args = append(args, v)
		}
		if len(cols) == 0 {
			continue
		}
		stmt := "INSERT INTO `" + table + "` (" + strings.Join(cols, ", ") + ") VALUES (" +
			strings.TrimSuffix(strings.Repeat("?,", len(cols)), ",") + ")"
		if _, err := tx.ExecContext(ctx, stmt, args...); err != nil {
			return n, fmt.Errorf("restore %s row: %w", table, err)
		}
		n++
	}
	return n, nil
}
