package pgcapture

import "github.com/jackc/pglogrepl"

// EncodeLSN renders a PostgreSQL LSN as the canonical "X/Y" upper-case hex string
// PostgreSQL itself uses (e.g. "0/19DF9E8"). This is the form stored as the
// durable replication checkpoint (stream_state.gtid_set) and carried in an
// event.Event's BinlogFile field and in EventCommit's GTID field. It is the
// PostgreSQL analog of a MySQL GTID string: an opaque, comparable cursor the
// consumer persists at commit boundaries and resumes from.
func EncodeLSN(lsn pglogrepl.LSN) string {
	return lsn.String()
}

// DecodeLSN parses an "X/Y" hex LSN string (as produced by EncodeLSN) back into a
// pglogrepl.LSN. It is the exact inverse of EncodeLSN; the consumer uses it to
// turn a saved checkpoint back into the LSN it resumes replication from.
func DecodeLSN(s string) (pglogrepl.LSN, error) {
	return pglogrepl.ParseLSN(s)
}
