package indexer

import (
	"strings"
	"time"
)

// PartitionDate parses the hour from a partition name like "p_2026021914".
// Returns the time and true on success; zero and false for p_future or other names.
func PartitionDate(name string) (time.Time, bool) {
	if len(name) != 12 || !strings.HasPrefix(name, "p_") {
		return time.Time{}, false
	}
	t, err := time.ParseInLocation("p_2006010215", name, time.UTC)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// PartitionName returns the partition name for a given hour ("p_YYYYMMDDHH").
func PartitionName(d time.Time) string {
	return d.UTC().Format("p_2006010215")
}
