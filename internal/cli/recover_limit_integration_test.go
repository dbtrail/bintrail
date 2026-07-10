//go:build integration

package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// seedRecoverUpdates seeds a chain of 4 UPDATE events on the same row
// (v0→v1→v2→v3→v4) at one-minute intervals, so a truncating --limit has an
// unambiguous "newest suffix" (v2→v3, v3→v4) and "oldest prefix" (v0→v1,
// v1→v2). The values v0/v1 appear ONLY in the oldest two events, making
// their presence in a reversal script a direct proof of oldest-prefix
// truncation (#785).
func seedRecoverUpdates(t *testing.T) (dbName, dsn string) {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	for i := 1; i <= 4; i++ {
		ts := h.Add(time.Duration(i) * time.Minute).Format("2006-01-02 15:04:05")
		before := fmt.Sprintf(`{"id":1,"v":"v%d"}`, i-1)
		after := fmt.Sprintf(`{"id":1,"v":"v%d"}`, i)
		testutil.InsertEvent(t, db, "binlog.000001", uint64(i*100), uint64(i*100+50), ts, nil,
			dbName, "orders", 2 /* UPDATE */, "1",
			[]byte(`["v"]`), []byte(before), []byte(after))
	}
	return dbName, testutil.IntegrationDSN(dbName)
}

// resetRecoverGlobals snapshots every recover command global, restores them on
// cleanup, and sets flag-default values for a clean programmatic run.
func resetRecoverGlobals(t *testing.T) {
	t.Helper()
	sIndexDSN, sSchema, sTable, sPK := rIndexDSN, rSchema, rTable, rPK
	sPKs, sLimitPerPK, sEventType, sGTID := rPKs, rLimitPerPK, rEventType, rGTID
	sSince, sUntil, sFlag, sOutput := rSince, rUntil, rFlag, rOutput
	sDryRun, sLimit, sProfile, sFormat := rDryRun, rLimit, rProfile, rFormat
	sNoArchive, sColumnEq, sMaxScriptBytes, sAllowGaps := rNoArchive, rColumnEq, rMaxScriptBytes, rAllowGaps
	t.Cleanup(func() {
		rIndexDSN, rSchema, rTable, rPK = sIndexDSN, sSchema, sTable, sPK
		rPKs, rLimitPerPK, rEventType, rGTID = sPKs, sLimitPerPK, sEventType, sGTID
		rSince, rUntil, rFlag, rOutput = sSince, sUntil, sFlag, sOutput
		rDryRun, rLimit, rProfile, rFormat = sDryRun, sLimit, sProfile, sFormat
		rNoArchive, rColumnEq, rMaxScriptBytes, rAllowGaps = sNoArchive, sColumnEq, sMaxScriptBytes, sAllowGaps
	})

	rIndexDSN, rSchema, rTable, rPK = "", "", "", ""
	rPKs, rLimitPerPK, rEventType, rGTID = nil, 0, "", ""
	rSince, rUntil, rFlag, rOutput = "", "", "", ""
	rDryRun, rLimit, rProfile, rFormat = false, 1000, "", "text"
	rNoArchive, rColumnEq, rMaxScriptBytes, rAllowGaps = true, nil, "2GB", false
}

func newRecoverTestCmd() *cobra.Command {
	c := &cobra.Command{}
	c.SetContext(context.Background())
	AddDuckDBTuningFlags(c)
	return c
}

// TestRecover_limitKeepsNewestEvents pins #785: when --limit truncates the
// matched window, the reversal script must undo the most RECENT events (a
// rollback to a consistent intermediate point), not the oldest prefix that the
// old ASC fetch kept — undoing old events underneath later un-reverted ones
// maps to no state that ever existed.
func TestRecover_limitKeepsNewestEvents(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	dbName, dsn := seedRecoverUpdates(t)
	resetRecoverGlobals(t)

	out := t.TempDir() + "/recovery.sql"
	rIndexDSN, rSchema, rTable = dsn, dbName, "orders"
	rOutput, rLimit = out, 2

	if err := runRecover(newRecoverTestCmd(), nil); err != nil {
		t.Fatalf("runRecover: %v", err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	script := string(b)

	// Newest two events are v2→v3 and v3→v4; their reversals reference v2..v4.
	if !strings.Contains(script, `'v3'`) || !strings.Contains(script, `'v2'`) {
		t.Errorf("expected reversals of the two NEWEST updates (v2→v3, v3→v4), got:\n%s", script)
	}
	// v0/v1 exist only in the oldest two events — any occurrence means the
	// truncation kept the oldest prefix (the #785 bug).
	if strings.Contains(script, `'v0'`) || strings.Contains(script, `'v1'`) {
		t.Errorf("script reversed the OLDEST events; --limit must keep the newest suffix of the window:\n%s", script)
	}
	// Most-recent undone first: the reverse of v3→v4 (its WHERE carries 'v4')
	// must precede the reverse of v2→v3 (its SET carries 'v2').
	i4, i2 := strings.Index(script, `'v4'`), strings.Index(script, `'v2'`)
	if i4 == -1 || i2 == -1 || i4 > i2 {
		t.Errorf("expected the newest event's reversal first (most-recent undone first), got:\n%s", script)
	}
}

// TestRecover_jsonCarriesTruncatedFlag pins the observability half of #785:
// --format json must carry a `truncated` flag (the old stderr-only warning was
// emitted after the JSON branch returned, so automated consumers never saw it),
// and the truncated dry-run SQL must also keep the newest suffix.
func TestRecover_jsonCarriesTruncatedFlag(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	dbName, dsn := seedRecoverUpdates(t)

	run := func(limit int) (statements int, truncated bool, sqlText string) {
		t.Helper()
		resetRecoverGlobals(t)
		rIndexDSN, rSchema, rTable = dsn, dbName, "orders"
		rDryRun, rFormat, rLimit = true, "json", limit

		old := os.Stdout
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatalf("pipe: %v", err)
		}
		os.Stdout = w
		runErr := runRecover(newRecoverTestCmd(), nil)
		w.Close()
		os.Stdout = old
		outBytes, readErr := io.ReadAll(r)
		if runErr != nil {
			t.Fatalf("runRecover: %v", runErr)
		}
		if readErr != nil {
			t.Fatalf("read captured stdout: %v", readErr)
		}
		var payload struct {
			Statements int    `json:"statements"`
			DryRun     bool   `json:"dry_run"`
			Truncated  bool   `json:"truncated"`
			SQL        string `json:"sql"`
		}
		if err := json.Unmarshal(outBytes, &payload); err != nil {
			t.Fatalf("unmarshal JSON output %q: %v", outBytes, err)
		}
		return payload.Statements, payload.Truncated, payload.SQL
	}

	n, trunc, sqlText := run(2)
	if n != 2 || !trunc {
		t.Errorf("limit=2 over 4 events: got statements=%d truncated=%v, want 2/true", n, trunc)
	}
	if strings.Contains(sqlText, `'v0'`) || strings.Contains(sqlText, `'v1'`) {
		t.Errorf("truncated dry-run reversed the OLDEST events, want newest suffix:\n%s", sqlText)
	}

	n, trunc, _ = run(1000)
	if n != 4 || trunc {
		t.Errorf("limit=1000 over 4 events: got statements=%d truncated=%v, want 4/false", n, trunc)
	}
}
