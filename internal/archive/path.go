package archive

import (
	"fmt"
	"path/filepath"
	"regexp"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
)

// archivePathRe matches the Hive-partitioned archive path pattern produced by
// rotate --archive-dir: bintrail_id=<uuid>/event_date=YYYY-MM-DD/event_hour=HH/events.parquet
// archivePathRe accepts ANY non-empty id segment — exactly what the writer
// produces: rotate's --bintrail-id takes an arbitrary string verbatim, and
// the discovery side (extractBasePath, buildGlob) matches on the
// `bintrail_id=` marker alone. A scanner stricter than the writer (this
// regex used to demand a 36-char lowercase UUID) silently skipped every
// file under an uppercase or human-named id — harmless for upload's
// UPDATE-only flow, but blinding for `archive reconcile`, where a blind
// scan plus --prune would wipe the registry of healthy archives (#392
// review).
var archivePathRe = regexp.MustCompile(
	`bintrail_id=([^/]+)/event_date=(\d{4}-\d{2}-\d{2})/event_hour=(0[0-9]|1[0-9]|2[0-3])/[^/]+\.parquet$`,
)

// ParseArchivePath extracts the bintrail_id and partition name from a
// Hive-partitioned archive file path. Returns empty strings if the path
// does not match the expected pattern. It is the inverse of the writer's
// Hive layout (rotation.HiveArchivePath) and is shared by the `upload` and
// `archive reconcile` scanners so both derive the same partition name from a
// scanned file.
func ParseArchivePath(path string) (bintrailID, partName string) {
	// Normalise to forward slashes so the regex works on Windows too.
	m := archivePathRe.FindStringSubmatch(filepath.ToSlash(path))
	if m == nil {
		return "", ""
	}
	id := m[1]
	date := m[2]
	hour := m[3]
	t, err := time.Parse("2006-01-02", date)
	if err != nil {
		return "", ""
	}
	h := 0
	fmt.Sscanf(hour, "%d", &h)
	t = t.Add(time.Duration(h) * time.Hour)
	return id, indexer.PartitionName(t)
}
