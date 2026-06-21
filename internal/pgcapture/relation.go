package pgcapture

import "github.com/dbtrail/dbtrail/internal/metadata"

// relColumn is one column of a cached relation, in ordinal (tuple) order. It
// retains the pgoutput per-column type identity (typeOID/typeMod) even though #530
// stores every value in text form: the RelationMessage is the only in-band source
// of PostgreSQL column types, and discarding them here would leave a future
// type-faithful renderer (#533) with no oracle to key on. #530 itself does not
// interpret these fields.
type relColumn struct {
	name    string
	typeOID uint32
	typeMod int32
}

// relationInfo is the decoder's cached knowledge of one relation, keyed by its
// pgoutput relation OID. It is the PostgreSQL analog of a metadata.Resolver entry,
// but built entirely from the in-band RelationMessage (column names arrive in the
// stream) plus a one-time catalog primary-key lookup — no schema_snapshots needed.
type relationInfo struct {
	schema  string
	table   string
	columns []relColumn           // ordinal order, matching the tuple column order
	pkCols  []metadata.ColumnMeta // primary-key columns in ordinal order (from a PKResolver)
}

// PKResolver returns the primary-key columns of a relation, in ordinal order, used
// to build event.Event.PKValues.
//
// It must source the PK from the catalog (pg_index.indisprimary), NOT from the
// RelationMessage's per-column "key" flag: under REPLICA IDENTITY FULL — the mode
// bintrail requires (#531) — pgoutput sends an empty key bitmap, so the flag would
// yield an empty PK for every table; under REPLICA IDENTITY USING INDEX it can mark
// a non-PK unique index.
//
// A relation with no primary key returns an empty slice — the event still indexes
// under an empty PK string, mirroring the MySQL path. A genuine lookup failure must
// return a non-nil error: the decoder fails loud rather than index rows under a
// wrong or missing PK.
//
// The capturer implements this with a catalog query on a second, non-replication
// connection (the replication connection is in CopyBoth streaming mode and cannot
// run queries); unit tests stub it.
type PKResolver func(relationID uint32, schema, table string) ([]metadata.ColumnMeta, error)
