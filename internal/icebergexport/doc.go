// Package icebergexport writes the current state of indexed tables to Apache
// Iceberg tables, incrementally (#1466).
//
// It is an OUTPUT of bintrail, never its storage: the archives, the baselines
// and the index stay what they are (#1467). Two rules follow from that and are
// enforced mechanically rather than promised:
//
//   - This package is the ONLY importer of the Iceberg and Arrow libraries;
//     the cliapp root and the bintrail binary link them through it and
//     nothing else may. cliapp/icebergfree_test.go derives the guarded set
//     from `go list` over the module minus that three-entry allowlist, so a
//     new package is guarded by default, and the console, MCP and pg
//     binaries must never link either library.
//   - It READS through paths that already exist (baseline discovery, the merged
//     event stream, the per-epoch decoders, the same refusals `baseline
//     refresh` applies) and WRITES somewhere new. Nothing on the recovery path
//     gained a branch for it; internal/reconstruct/exportseam.go is names only.
//
// Shape of the output, per table: one Iceberg v2 table at
// <warehouse>/<schema>/<table>/ managed by a filesystem catalog (version-hint,
// no service), with the table's primary key as the identifier fields.
//
// The first run loads the newest baseline snapshot as data files. Every run
// then folds the events between the table's own cursor and the run's binlog
// cut into the NET change per primary key and commits it as ONE snapshot: an
// equality-delete file naming every touched key plus one or more data files
// with the after-image of every key that still exists. Iceberg applies an equality
// delete to data files with a strictly lower sequence number, which is exactly
// what makes "delete then insert in one commit" an update. It is also why the
// fold is not optional: two changes to one key emitted event by event into the
// same commit would land two rows, because the delete cannot see a data file
// of its own sequence number.
//
// The cursor is the run's binlog cut (file and position, the same coordinate
// `baseline refresh` anchors a snapshot on), stored as table properties in the
// same commit as the data. The table carries its own state; nothing is written
// to the index, and a run that dies before committing leaves the previous
// snapshot intact and resumes from it.
package icebergexport
