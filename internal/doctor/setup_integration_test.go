//go:build integration

package doctor

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/dbtrail/bintrail/internal/indexer"
	"github.com/dbtrail/bintrail/internal/testutil"
)

// setupPartitionedTable creates binlog_events partitions covering the given
// hours so the capacity check has measurable per-hour samples. Mirrors the
// cmd-layer test helper of the same name (cmd/bintrail/rotate_integration_test.go);
// duplicated rather than shared to keep this package's test deps minimal.
func setupPartitionedTable(t *testing.T, db *sql.DB, dbName string, hours []time.Time) {
	t.Helper()
	parts := ""
	for i, h := range hours {
		nextHour := h.Add(time.Hour)
		if i > 0 {
			parts += ",\n"
		}
		parts += fmt.Sprintf(
			"PARTITION %s VALUES LESS THAN (TO_SECONDS('%s'))",
			indexer.PartitionName(h),
			nextHour.UTC().Format("2006-01-02 15:04:05"),
		)
	}
	parts += ",\nPARTITION p_future VALUES LESS THAN MAXVALUE"
	testutil.MustExec(t, db, fmt.Sprintf(
		"ALTER TABLE `%s`.`binlog_events` REORGANIZE PARTITION p_future INTO (\n%s\n)",
		dbName, parts,
	))
}
