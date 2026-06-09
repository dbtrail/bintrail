package testutil

import (
	"database/sql"
	"fmt"
	"testing"
	"time"
)

// SetupPartitionedTable reorganizes binlog_events' p_future into hourly
// partitions covering the given hours (plus a fresh p_future catch-all), so a
// test can insert data into specific hours and then rotate them away.
//
// Kept dependency-free on purpose — it inlines the "p_2006010215" partition
// name format rather than calling indexer.PartitionName — so testutil stays a
// leaf. internal/rotation's own tests import testutil; a testutil→rotation edge
// (or testutil→indexer, whose tests import testutil) would create an import
// cycle. Shared by the rotation, archive, and reconstruct integration tests.
func SetupPartitionedTable(t *testing.T, db *sql.DB, dbName string, hours []time.Time) {
	t.Helper()
	parts := ""
	for i, h := range hours {
		nextHour := h.Add(time.Hour)
		if i > 0 {
			parts += ",\n"
		}
		parts += fmt.Sprintf(
			"PARTITION %s VALUES LESS THAN (TO_SECONDS('%s'))",
			h.UTC().Format("p_2006010215"),
			nextHour.UTC().Format("2006-01-02 15:04:05"),
		)
	}
	parts += ",\nPARTITION p_future VALUES LESS THAN MAXVALUE"

	MustExec(t, db, fmt.Sprintf(
		"ALTER TABLE `%s`.`binlog_events` REORGANIZE PARTITION p_future INTO (\n%s\n)",
		dbName, parts,
	))
}
