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
	propOrigin   = "bintrail.export.cursor_origin" // "baseline" after a load, "cut" after a delta
	propBaseline = "bintrail.export.source_baseline"

	// exportVersion is the newest table layout this build writes and reads.
	// A table stamped with a higher one was written by a newer bintrail, and
	// its cursor may mean something this build would misread; refusing is
	// the one check that cannot be added to a binary after it ships.
	exportVersion = "1"

	originBaseline = "baseline"
	originCut      = "cut"
)

// cursorKeys are the properties that make up a cursor: all present or none.
var cursorKeys = [...]string{propFile, propPos, propAt}

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
// floor for pruning partitions and archives. FromBaseline says which of the
// two the coordinate is; the first window after a load gets one extra check
// that later windows do not.
type cursor struct {
	File         string
	Pos          uint64
	At           time.Time
	FromBaseline bool
}

// readCursor reads the cursor from table properties. A table with none of
// the keys is a table whose first load has not committed (nil, nil); a table
// with some of them is corrupt; a table stamped by a newer export is refused.
func readCursor(props iceberg.Properties) (*cursor, error) {
	if v, ok := props[propVersion]; ok && v != exportVersion {
		return nil, fmt.Errorf("table was written by a newer export (%s=%s, this build reads %s); upgrade bintrail", propVersion, v, exportVersion)
	}
	present := 0
	for _, k := range cursorKeys {
		if _, ok := props[k]; ok {
			present++
		}
	}
	if present == 0 {
		return nil, nil
	}
	if present != len(cursorKeys) {
		return nil, fmt.Errorf("table properties carry a partial export cursor (%s=%q %s=%q %s=%q); remove the table directory to reload it",
			propFile, props[propFile], propPos, props[propPos], propAt, props[propAt])
	}
	p, err := strconv.ParseUint(props[propPos], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("table property %s=%q is not a position: %w", propPos, props[propPos], err)
	}
	t, err := time.Parse(time.RFC3339Nano, props[propAt])
	if err != nil {
		return nil, fmt.Errorf("table property %s=%q is not a time: %w", propAt, props[propAt], err)
	}
	return &cursor{File: props[propFile], Pos: p, At: t.UTC(), FromBaseline: props[propOrigin] == originBaseline}, nil
}

// properties renders the cursor for a delta commit.
func (c cursor) properties() iceberg.Properties {
	origin := originCut
	if c.FromBaseline {
		origin = originBaseline
	}
	return iceberg.Properties{
		propFile:   c.File,
		propPos:    strconv.FormatUint(c.Pos, 10),
		propAt:     c.At.UTC().Format(time.RFC3339Nano),
		propOrigin: origin,
	}
}

// loadProperties renders the cursor for the first load, which also records
// the baseline it was seeded from.
func (c cursor) loadProperties(baselinePath string) iceberg.Properties {
	p := c.properties()
	p[propBaseline] = baselinePath
	return p
}

func (c cursor) String() string {
	return fmt.Sprintf("%s:%d at %s", c.File, c.Pos, c.At.UTC().Format(time.RFC3339))
}
