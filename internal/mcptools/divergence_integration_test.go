//go:build integration

package mcptools

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/internal/buffer"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationMergeDivergenceWarning drives the REAL query and recover
// tools against a live index plus a registered Parquet archive that holds
// (a) the same event_id as the live row with a DISAGREEING row image and
// (b) one archive-only event. (b) is the non-vacuous precondition: its data
// must appear in the result, proving the archive was actually read and merged
// — without it, a silently-unread archive would make the warning assertion
// pass or fail for the wrong reason (the #1321 lesson).
//
// Mutating MakeQueryTool to call MergeResults instead of MergeResultsReport,
// dropping the EventDivergenceNotice append, or dropping the recover tool's
// SQL-comment warning turns this red while every unit test stays green.
func TestIntegrationMergeDivergenceWarning(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	testutil.InsertEvent(t, db, "bin.000001", 4, 40, "2026-06-01 12:00:00", nil,
		"app", "users", 1 /*INSERT*/, "1", nil, nil, []byte(`{"id":1,"name":"alice"}`))
	var eventID uint64
	if err := db.QueryRow(`SELECT event_id FROM binlog_events LIMIT 1`).Scan(&eventID); err != nil {
		t.Fatalf("read event_id: %v", err)
	}

	cfg := Config{
		Resolve: func(context.Context, string) (*Target, error) {
			return &Target{DB: db}, nil
		},
	}

	// Cry-wolf control: with no archive registered there is nothing to merge
	// and no divergence warning may render.
	q0, _, err := MakeQueryTool(cfg)(context.Background(), &mcp.CallToolRequest{}, QueryArgs{Schema: "app", Table: "users"})
	if err != nil {
		t.Fatalf("query handler (no archive): %v", err)
	}
	if txt := resultText(q0); strings.Contains(txt, "event_divergence") {
		t.Errorf("no archive → no divergence warning, got: %s", txt)
	}

	// A valid archive: the live event's id with a mutated row image, plus an
	// archive-only sibling.
	ts := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	diverging := query.ResultRow{
		EventID: eventID, BinlogFile: "bin.000001", StartPos: 4, EndPos: 40,
		EventTimestamp: ts, SchemaName: "app", TableName: "users",
		EventType: 1, PKValues: "1",
		RowAfter: map[string]any{"id": 1, "name": "MUTATED"},
	}
	archiveOnly := query.ResultRow{
		EventID: eventID + 1000, BinlogFile: "bin.000001", StartPos: 41, EndPos: 80,
		EventTimestamp: ts.Add(time.Minute), SchemaName: "app", TableName: "users",
		EventType: 1, PKValues: "2",
		RowAfter: map[string]any{"id": 2, "name": "bob-from-archive"},
	}
	base := filepath.Join(t.TempDir(), "bintrail_id=div-test")
	hourDir := filepath.Join(base, "date=2026-06-01", "hour=12")
	if err := os.MkdirAll(hourDir, 0o755); err != nil {
		t.Fatal(err)
	}
	pq := filepath.Join(hourDir, "events.parquet")
	if _, err := buffer.WriteParquet([]query.ResultRow{diverging, archiveOnly}, pq, "none"); err != nil {
		t.Fatalf("WriteParquet: %v", err)
	}
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count, s3_bucket, s3_key, s3_uploaded_at)
		VALUES ('p_2026060112', 'div-test', ?, 2, NULL, NULL, NULL)`, pq)

	// Query tool: the merged result must carry the archive-only row
	// (precondition) AND the divergence warning.
	qres, _, err := MakeQueryTool(cfg)(context.Background(), &mcp.CallToolRequest{}, QueryArgs{Schema: "app", Table: "users"})
	if err != nil {
		t.Fatalf("query handler: %v", err)
	}
	if qres.IsError {
		t.Fatalf("query tool errored: %s", resultText(qres))
	}
	txt := resultText(qres)
	if !strings.Contains(txt, "bob-from-archive") {
		t.Fatalf("archive-only row missing — the archive was not merged, so the warning assertion below would be vacuous: %s", txt)
	}
	if !strings.Contains(txt, "event_divergence: 1 duplicate event(s) disagreed") {
		t.Errorf("query result must warn about the diverging duplicate, got: %s", txt)
	}

	// Recover tool: divergence is a WARNING, not a refusal — both copies
	// exist and the live index's is used, so the script is not incomplete.
	// But the reviewing operator must see the finding inside the script text.
	rres, _, err := MakeRecoverTool(cfg)(context.Background(), &mcp.CallToolRequest{}, RecoverArgs{Schema: "app", Table: "users"})
	if err != nil {
		t.Fatalf("recover handler: %v", err)
	}
	if rres.IsError {
		t.Fatalf("recover must still generate on divergence (warning, not refusal), got: %s", resultText(rres))
	}
	rtxt := resultText(rres)
	if !strings.Contains(rtxt, "DELETE FROM") {
		t.Fatalf("no reversal statements generated — the divergence assertion below would be vacuous: %s", rtxt)
	}
	if !strings.Contains(rtxt, "-- Warning: event_divergence: 1 duplicate event(s) disagreed") {
		t.Errorf("recover script must carry the divergence warning as a SQL comment, got: %s", rtxt)
	}
}
