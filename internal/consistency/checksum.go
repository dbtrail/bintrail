// Package consistency provides primitives that prove a Parquet snapshot
// faithfully represents the MySQL table it was taken from.
//
// The foundation is ConsistentTableChecksum: a point-in-time, order-independent,
// type-canonical fingerprint of a source table. Fidelity is only provable at a
// frozen consistent point — you cannot compare a checksum of the Parquet against
// a live table that has moved on. So the fingerprint is computed inside
// START TRANSACTION WITH CONSISTENT SNAPSHOT and bound to the @@gtid_executed
// captured at that same point. The baseline writer (issue #633) and the verify
// capstone (issue #634) both call this primitive — at dump time and at audit
// time respectively — and compare the result against a Parquet-derived digest.
//
// Part of the data-consistency guarantee epic (#631).
package consistency

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"strings"
)

// TableChecksum is a point-in-time fingerprint of a single source table.
//
// GTIDSet is @@gtid_executed captured inside the consistent-snapshot transaction
// — the exact point against which a Parquet snapshot can later be compared. It is
// empty on a server with GTIDs disabled (gtid_mode=OFF) or absent (MariaDB);
// callers that need a position anchor on such servers capture it separately.
//
// Digest is a hex-encoded, order-independent multiset hash of the row contents.
// Two tables holding the same rows in any physical or primary-key order produce
// the same Digest; a single changed byte produces a different one; a
// representation-only difference (e.g. JSON whitespace, which the server
// normalizes) produces the same one.
type TableChecksum struct {
	Schema   string
	Table    string
	GTIDSet  string
	RowCount int64
	Digest   string
}

// ConsistentTableChecksum computes a TableChecksum for schema.table against the
// live source db. The whole computation — GTID capture, column introspection,
// and the table scan — runs on a single pinned connection inside
// START TRANSACTION WITH CONSISTENT SNAPSHOT, so the digest, the row count, and
// the captured GTID all describe the exact same snapshot of the data.
//
// The canonical form of every value is MySQL's text-protocol rendering with the
// session time zone pinned to UTC. That rendering is already type-exact —
// UNSIGNED integers print unsigned, DATETIME/TIMESTAMP carry their declared
// fractional precision, DECIMAL is pre-formatted, JSON is normalized — so no
// per-type canonicalization is reimplemented here. The Parquet side of the
// comparison (#634) must reproduce this same contract: "MySQL text rendering,
// session time zone UTC".
//
// Generated columns (VIRTUAL/STORED) are excluded: mydumper does not dump them,
// so they are absent from the baseline Parquet and must be absent here too.
func ConsistentTableChecksum(ctx context.Context, db *sql.DB, schema, table string) (TableChecksum, error) {
	res := TableChecksum{Schema: schema, Table: table}

	conn, err := db.Conn(ctx)
	if err != nil {
		return res, fmt.Errorf("pin connection: %w", err)
	}
	defer conn.Close()

	// Pin the session time zone so TIMESTAMP values render deterministically
	// (TIMESTAMP is stored in UTC and rendered in the session zone).
	if _, err := conn.ExecContext(ctx, "SET SESSION time_zone = '+00:00'"); err != nil {
		return res, fmt.Errorf("set session time_zone: %w", err)
	}

	// Open the consistent snapshot. Everything below reads the same view.
	if _, err := conn.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
		return res, fmt.Errorf("start consistent snapshot: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			// Read-only transaction; rollback is best-effort cleanup.
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()

	// Capture the GTID anchor inside the snapshot.
	res.GTIDSet, err = capturedGTID(ctx, conn)
	if err != nil {
		return res, err
	}

	// Introspect the non-generated columns, in ordinal order.
	cols, err := tableColumns(ctx, conn, schema, table)
	if err != nil {
		return res, err
	}
	if len(cols) == 0 {
		return res, fmt.Errorf("table %s.%s has no columns (does it exist?)", schema, table)
	}

	// Scan every row, hashing as we stream — no full-table buffering.
	selectList := make([]string, len(cols))
	for i, c := range cols {
		selectList[i] = quoteIdent(c)
	}
	query := fmt.Sprintf("SELECT %s FROM %s.%s",
		strings.Join(selectList, ","), quoteIdent(schema), quoteIdent(table))

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return res, fmt.Errorf("scan %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	dest := make([]sql.RawBytes, len(cols))
	ptrs := make([]any, len(cols))
	for i := range dest {
		ptrs[i] = &dest[i]
	}
	values := make([][]byte, len(cols))

	hasher := newRowHasher()
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return res, fmt.Errorf("scan row of %s.%s: %w", schema, table, err)
		}
		for i := range dest {
			// nil RawBytes is SQL NULL; a non-nil empty slice is an empty value.
			if dest[i] == nil {
				values[i] = nil
			} else {
				values[i] = dest[i]
			}
		}
		hasher.add(values)
	}
	if err := rows.Err(); err != nil {
		return res, fmt.Errorf("iterate rows of %s.%s: %w", schema, table, err)
	}

	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return res, fmt.Errorf("commit snapshot: %w", err)
	}
	committed = true

	res.RowCount = hasher.count()
	res.Digest = hasher.digest()
	return res, nil
}

// capturedGTID reads @@gtid_executed inside the snapshot. MySQL always exposes
// this variable (empty string when gtid_mode=OFF); MariaDB does not have it, in
// which case the GTID anchor is reported as empty rather than erroring — a
// missing GTID is a legitimate server configuration, not a checksum failure.
func capturedGTID(ctx context.Context, conn *sql.Conn) (string, error) {
	var gtid sql.NullString
	err := conn.QueryRowContext(ctx, "SELECT @@global.gtid_executed").Scan(&gtid)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unknown system variable") {
			return "", nil
		}
		return "", fmt.Errorf("read @@gtid_executed: %w", err)
	}
	return strings.TrimSpace(gtid.String), nil
}

// tableColumns returns the non-generated column names of schema.table in ordinal
// order. Generated columns are excluded because mydumper omits them from the
// dump, so they never reach the baseline Parquet.
func tableColumns(ctx context.Context, conn *sql.Conn, schema, table string) ([]string, error) {
	const q = `
		SELECT COLUMN_NAME, EXTRA
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION`
	rows, err := conn.QueryContext(ctx, q, schema, table)
	if err != nil {
		return nil, fmt.Errorf("introspect columns of %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var name, extra string
		if err := rows.Scan(&name, &extra); err != nil {
			return nil, fmt.Errorf("scan column metadata of %s.%s: %w", schema, table, err)
		}
		// EXTRA reads e.g. "VIRTUAL GENERATED" / "STORED GENERATED".
		if strings.Contains(strings.ToUpper(extra), "GENERATED") {
			continue
		}
		cols = append(cols, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate column metadata of %s.%s: %w", schema, table, err)
	}
	return cols, nil
}

// quoteIdent backtick-quotes a MySQL identifier, doubling any embedded backtick.
func quoteIdent(id string) string {
	return "`" + strings.ReplaceAll(id, "`", "``") + "`"
}

// rowHasher accumulates an order-independent multiset hash over rows.
//
// Each row is hashed independently with FNV-1a/64 (deterministic, no random
// seed — unlike hash/maphash), then folded into the accumulator by addition.
// Addition is commutative (so row order does not matter) and, unlike XOR, does
// not cancel out two identical rows. Each field is length-prefixed and tagged so
// a SQL NULL (tag 0x00) is distinct from an empty value (tag 0x01, length 0) and
// no field-value boundary is ambiguous.
type rowHasher struct {
	h   hash.Hash64
	acc uint64
	cnt int64
}

func newRowHasher() *rowHasher { return &rowHasher{h: fnv.New64a()} }

func (r *rowHasher) add(values [][]byte) {
	r.h.Reset()
	var lb [binary.MaxVarintLen64]byte
	for _, v := range values {
		if v == nil {
			_, _ = r.h.Write([]byte{0x00})
			continue
		}
		_, _ = r.h.Write([]byte{0x01})
		n := binary.PutUvarint(lb[:], uint64(len(v)))
		_, _ = r.h.Write(lb[:n])
		_, _ = r.h.Write(v)
	}
	r.acc += r.h.Sum64()
	r.cnt++
}

func (r *rowHasher) digest() string { return fmt.Sprintf("%016x", r.acc) }
func (r *rowHasher) count() int64   { return r.cnt }
