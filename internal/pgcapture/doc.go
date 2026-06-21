// Package pgcapture decodes a PostgreSQL logical-replication (pgoutput) stream
// into the source-neutral event.Event consumed by the indexer and the rest of
// bintrail's value stack — the PostgreSQL analog of the go-mysql binlog parser in
// internal/parser. It shares zero code with that parser.
//
// This is the capture layer: it links the PostgreSQL replication libraries —
// github.com/jackc/pglogrepl in this slice, with github.com/jackc/pgx/v5 becoming
// a direct dependency when the capturer (slice 2) opens the connection. The
// read/value stack (query, recover, reconstruct, shim, console) must NEVER import
// this package —
// it consumes event.Event, which links no capture library. That boundary is
// enforced by internal/event's TestReadLayerDoesNotLinkGoMySQL, which bans pgx and
// pglogrepl from the read side alongside go-mysql.
//
// Issue #530. The package is built in two slices:
//   - decoder.go (+ lsn.go, relation.go): the pure message→Event logic and the
//     LSN/Event contract — fully unit-testable without a live PostgreSQL.
//   - capturer.go (+ slot.go): the replication connection, slot/publication
//     lifecycle, and the standby-status / confirmed_flush_lsn feedback loop.
//
// Design record: drafts/postgres-pgcapture-blueprint-2026-06-21.md.
package pgcapture
