//go:build integration

package status_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #1323: CollectStatus swallowed a LoadCoverage or LoadArchiveStats failure
// with a slog.Warn, leaving the field nil — the whole section then vanished
// from both renderings, and a consumer read the absence as an affirmative
// fact ("no archives exist", "nothing to restore from"). The same class #816
// closed one layer down, at the frame above.
//
// This goes to a real database because the discrimination is on driver error
// codes (a missing table must NOT alarm; anything else must), and because the
// wiring under test is CollectStatus itself — the unit render tests set the
// fields by hand and would pass with the swallow restored.
func TestCollectStatus_readFailuresSurfaceNotVanish(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	exec := func(q string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	collect := func() *status.StatusData {
		t.Helper()
		d, err := status.CollectStatus(ctx, db, dbName)
		if err != nil {
			t.Fatalf("CollectStatus must stay non-fatal — status is a report, not a gate: %v", err)
		}
		return d
	}

	// Healthy index: both sections load, neither error field is set.
	d := collect()
	if d.Coverage == nil || d.CoverageErr != nil {
		t.Fatalf("healthy index: coverage=%v err=%v", d.Coverage, d.CoverageErr)
	}
	if d.ArchivesErr != nil {
		t.Fatalf("healthy index: ArchivesErr = %v", d.ArchivesErr)
	}

	// No archive tier at all (ER_NO_SUCH_TABLE): the absent section IS the
	// truth — no error, no tombstone. This is every pre-archive index, and it
	// must not start crying wolf (#816's stance, applied to LoadArchiveStats).
	exec(`DROP TABLE archive_state`)
	d = collect()
	if d.ArchivesErr != nil {
		t.Errorf("an index with no archive_state was flagged unreadable; 'no archive tier' is a fact, not a failure: %v", d.ArchivesErr)
	}
	if d.Archives != nil {
		t.Errorf("no archive_state but Archives = %+v", d.Archives)
	}

	// Present but unreadable — a legacy shape missing row_count (1054), the
	// same "table exists, query fails" class the #816 test uses. THE bug: this
	// used to be indistinguishable from the case above.
	exec(`CREATE TABLE archive_state (
	        partition_name VARCHAR(64) NOT NULL,
	        bintrail_id    VARCHAR(64) NOT NULL,
	        PRIMARY KEY (partition_name, bintrail_id))`)
	d = collect()
	if d.ArchivesErr == nil {
		t.Fatal("an unreadable archive_state left ArchivesErr nil; the section will silently vanish")
	}
	if d.Archives != nil {
		t.Errorf("unreadable archive_state but Archives = %+v", d.Archives)
	}
	var text bytes.Buffer
	d.Write(&text)
	if !strings.Contains(text.String(), "=== Archives ===") || !strings.Contains(text.String(), "NOT READ") {
		t.Errorf("the text report has no Archives tombstone:\n%s", text.String())
	}
	var js bytes.Buffer
	if err := d.WriteJSON(&js); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}
	type sectionKeys struct {
		Archives      *json.RawMessage `json:"archives"`
		ArchivesError *struct {
			Error string `json:"error"`
		} `json:"archives_error"`
		Coverage      *json.RawMessage `json:"coverage"`
		CoverageError *struct {
			Error string `json:"error"`
		} `json:"coverage_error"`
	}
	var parsed sectionKeys
	if err := json.Unmarshal(js.Bytes(), &parsed); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, js.String())
	}
	if parsed.ArchivesError == nil || parsed.ArchivesError.Error == "" {
		t.Errorf("JSON lacks archives_error; a consumer reads the absent archives key as 'no archives exist':\n%s", js.String())
	}

	// LoadCoverage failing outright — here binlog_events gone (the aggregate
	// scan is the statement in this package most likely to die at scale; any
	// error there must land in CoverageErr, there is no benign sub-case).
	// index_state and the partition listing still load, so CollectStatus
	// itself succeeds and the failure has to be carried, not returned.
	exec(`DROP TABLE binlog_events`)
	d = collect()
	if d.CoverageErr == nil {
		t.Fatal("a failed LoadCoverage left CoverageErr nil; the coverage section will silently vanish")
	}
	if d.Coverage != nil {
		t.Errorf("failed LoadCoverage but Coverage = %+v", d.Coverage)
	}
	text.Reset()
	d.Write(&text)
	if !strings.Contains(text.String(), "=== Restore Coverage ===") || !strings.Contains(text.String(), "NOT READ") {
		t.Errorf("the text report has no coverage tombstone:\n%s", text.String())
	}
	js.Reset()
	if err := d.WriteJSON(&js); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}
	parsed = sectionKeys{} // Unmarshal leaves absent keys untouched; a stale Coverage would mask its omission
	if err := json.Unmarshal(js.Bytes(), &parsed); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, js.String())
	}
	if parsed.Coverage != nil {
		t.Errorf("a read error must not fabricate a coverage object:\n%s", js.String())
	}
	if parsed.CoverageError == nil || !strings.Contains(parsed.CoverageError.Error, "binlog_events") {
		t.Errorf("JSON lacks coverage_error with the underlying cause:\n%s", js.String())
	}
}
