//go:build integration

package status_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #816: LoadCoverage degraded EVERY archive_state failure to live-only
// coverage with nothing but a slog.Warn. The two causes are not the same
// fact — "this index has no archive tier" is true and describable, "I could
// not read the archive tier" is not — and collapsing them makes `status`
// print a restore window SHORTER than reality. An operator reads it and
// concludes an old incident is unrecoverable while the Parquet covering it is
// sitting in the bucket.
//
// This goes to a real database because the discrimination is on a driver
// error code; a fixture would assert the mapping we wrote rather than the one
// MySQL produces.
func TestLoadCoverage_DistinguishesUnreadableArchivesFromNone(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, _ := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	exec := func(q string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}

	// Healthy: a real archive row extends the window and nothing is flagged.
	exec(`INSERT INTO archive_state (partition_name, bintrail_id, row_count)
	      VALUES ('p_2020010203', 'src', 41)`)
	c, err := status.LoadCoverage(ctx, db)
	if err != nil {
		t.Fatalf("LoadCoverage: %v", err)
	}
	if c.ArchiveUnavailable {
		t.Error("a readable archive_state was reported as unavailable")
	}
	if c.ArchiveTotalRows != 41 || !c.ArchiveEarliestHour.Valid {
		t.Errorf("archive coverage not read: rows=%d earliest=%v", c.ArchiveTotalRows, c.ArchiveEarliestHour)
	}

	// No archive tier at all: the zeros ARE the truth, so no flag. This is
	// the case the old blanket warn was written for, and it must not regress
	// into a scary banner on every pre-archive index.
	exec(`DROP TABLE archive_state`)
	c, err = status.LoadCoverage(ctx, db)
	if err != nil {
		t.Fatalf("LoadCoverage (no table): %v", err)
	}
	if c.ArchiveUnavailable {
		t.Error("an index with no archive_state was flagged unavailable; 'no archive tier' is a fact, not a failure")
	}
	if c.ArchiveTotalRows != 0 || c.ArchiveEarliestHour.Valid {
		t.Errorf("archive fields should be zero with no table: rows=%d earliest=%v", c.ArchiveTotalRows, c.ArchiveEarliestHour)
	}

	// Present but unreadable — here a legacy shape missing row_count (1054),
	// which is exactly the "table exists, query fails" class. THE regression:
	// before #816 this was indistinguishable from the case above.
	exec(`CREATE TABLE archive_state (
	        partition_name VARCHAR(64) NOT NULL,
	        bintrail_id    VARCHAR(64) NOT NULL,
	        PRIMARY KEY (partition_name, bintrail_id))`)
	exec(`INSERT INTO archive_state (partition_name, bintrail_id) VALUES ('p_2019010203', 'src')`)
	c, err = status.LoadCoverage(ctx, db)
	if err != nil {
		t.Fatalf("LoadCoverage must stay non-fatal — coverage is a report, not a gate: %v", err)
	}
	if !c.ArchiveUnavailable {
		t.Fatal("an unreadable archive_state was reported as 'no archives'; the restore window silently understates reality")
	}
	if c.ArchiveError == "" {
		t.Error("no reason recorded — an operator cannot act on 'unavailable' alone")
	}

	// Both renderings must carry it. The text report is what an operator
	// reads during an incident; the JSON is what a monitor keys on.
	var buf strings.Builder
	data := &status.StatusData{Coverage: c}
	data.Write(&buf)
	out := buf.String()
	if !strings.Contains(out, "NOT READ") {
		t.Errorf("the text report does not say the archives were not read:\n%s", out)
	}
	if !strings.Contains(out, "LOWER BOUND") {
		t.Errorf("the text report does not qualify the restore window:\n%s", out)
	}
	if strings.Contains(out, "(includes archives)") {
		t.Errorf("the text report claims the window includes archives it could not read:\n%s", out)
	}

	var jbuf strings.Builder
	if err := data.WriteJSON(&jbuf); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}
	var parsed struct {
		Coverage struct {
			ArchivesUnavailable bool   `json:"archives_unavailable"`
			ArchivesError       string `json:"archives_error"`
		} `json:"coverage"`
	}
	if err := json.Unmarshal([]byte(jbuf.String()), &parsed); err != nil {
		t.Fatalf("status JSON: %v\n%s", err, jbuf.String())
	}
	if !parsed.Coverage.ArchivesUnavailable || parsed.Coverage.ArchivesError == "" {
		t.Errorf("the JSON report does not carry the unavailable verdict: %s", jbuf.String())
	}
}
