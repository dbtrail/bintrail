package status

import (
	"bytes"
	"database/sql"
	"strings"
	"testing"
	"time"
)

// TestStatusSurfacesIndexAndBaselineSize pins the #351/#406-residual display:
// the MySQL index storage size shows in the Restore Coverage section, and the
// per-table baseline Parquet size shows in the Baselines section — both in text
// and JSON.
func TestStatusSurfacesIndexAndBaselineSize(t *testing.T) {
	const (
		indexBytes    = int64(5 * 1024 * 1024)  // 5 MiB
		baselineBytes = int64(12 * 1024 * 1024) // 12 MiB
	)
	d := &StatusData{
		Coverage: &CoverageInfo{
			EarliestEvent:  sql.NullTime{Time: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true},
			LatestEvent:    sql.NullTime{Time: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), Valid: true},
			TotalEvents:    100,
			IndexSizeBytes: indexBytes,
		},
		Baselines: []BaselineInfo{
			{
				SnapshotTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
				Database:     "shop",
				Table:        "orders",
				Size:         baselineBytes,
			},
		},
	}

	var text bytes.Buffer
	d.Write(&text)
	ts := text.String()
	if !strings.Contains(ts, "Index size:") {
		t.Errorf("text status missing 'Index size:' line:\n%s", ts)
	}
	if !strings.Contains(ts, formatBytes(indexBytes)) {
		t.Errorf("text status missing index size value %q:\n%s", formatBytes(indexBytes), ts)
	}
	if !strings.Contains(ts, formatBytes(baselineBytes)) {
		t.Errorf("text status missing baseline size value %q:\n%s", formatBytes(baselineBytes), ts)
	}

	var jsonBuf bytes.Buffer
	if err := d.WriteJSON(&jsonBuf); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}
	js := jsonBuf.String()
	if !strings.Contains(js, `"index_size_bytes":`) {
		t.Errorf("JSON missing index_size_bytes:\n%s", js)
	}
	if !strings.Contains(js, `"size_bytes":`) {
		t.Errorf("JSON missing baseline size_bytes:\n%s", js)
	}
}
