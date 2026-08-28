package icebergexport

import (
	"fmt"
	"strconv"
	"time"

	"github.com/apache/iceberg-go"
)

// Table properties the export owns. They are written in the same commit as
// the data they describe, so the table is its own state: a reader can see
// where the next run resumes with `iceberg_table_properties()`, and a run
// that dies before committing leaves the previous cursor with the previous
// snapshot.
const (
	propVersion  = "bintrail.export.version"
	propFile     = "bintrail.export.binlog_file"
	propPos      = "bintrail.export.binlog_position"
	propAt       = "bintrail.export.at"
	propBaseline = "bintrail.export.source_baseline"

	exportVersion = "1"
)

// Snapshot summary properties, informational, one set per commit.
const (
	summaryRowsLoaded  = "bintrail.export.rows_loaded"
	summaryEvents      = "bintrail.export.events"
	summaryUpserts     = "bintrail.export.upserts"
	summaryDeletes     = "bintrail.export.deletes"
	summaryWindowSince = "bintrail.export.window_since"
	summaryWindowUntil = "bintrail.export.window_until"
)

// cursor is where the table's deltas resume: a binlog coordinate (the run's
// positional cut, or the baseline anchor right after the first load) and the
// wall-clock instant the run targeted, which the next run uses as the time
// floor for pruning partitions and archives.
type cursor struct {
	File string
	Pos  uint64
	At   time.Time
}

// readCursor reads the cursor from table properties. A table with none of
// the keys is a table whose first load has not committed (nil, nil); a table
// with some of them is corrupt.
func readCursor(props iceberg.Properties) (*cursor, error) {
	file, hasFile := props[propFile]
	pos, hasPos := props[propPos]
	at, hasAt := props[propAt]
	if !hasFile && !hasPos && !hasAt {
		return nil, nil
	}
	if !hasFile || !hasPos || !hasAt {
		return nil, fmt.Errorf("table properties carry a partial export cursor (%s=%q %s=%q %s=%q); remove the table directory to reload it",
			propFile, file, propPos, pos, propAt, at)
	}
	p, err := strconv.ParseUint(pos, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("table property %s=%q is not a position: %w", propPos, pos, err)
	}
	t, err := time.Parse(time.RFC3339Nano, at)
	if err != nil {
		return nil, fmt.Errorf("table property %s=%q is not a time: %w", propAt, at, err)
	}
	return &cursor{File: file, Pos: p, At: t.UTC()}, nil
}

func (c cursor) properties() iceberg.Properties {
	return iceberg.Properties{
		propFile: c.File,
		propPos:  strconv.FormatUint(c.Pos, 10),
		propAt:   c.At.UTC().Format(time.RFC3339Nano),
	}
}

func (c cursor) String() string {
	return fmt.Sprintf("%s:%d at %s", c.File, c.Pos, c.At.UTC().Format(time.RFC3339))
}
