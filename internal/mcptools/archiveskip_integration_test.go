//go:build integration

package mcptools

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationArchiveSkipWarning pins the #1285 wiring for BOTH tools: a
// registered archive source whose fetch fails (its only file is not valid
// Parquet) must surface as an archive_source_skipped warning in the tool
// RESULT, not only in the server log — an MCP client sees nothing else, so a
// response missing every event held by a source would otherwise read as
// complete. Deleting the skippedArchives append or the ArchiveSkipNotice call
// in either tool turns this red.
func TestIntegrationArchiveSkipWarning(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// A discoverable local archive whose only file is garbage:
	// ResolveArchiveSources returns the bintrail_id base (the directory
	// exists, so the local path is preferred), then parquetquery.Fetch fails
	// on the corrupt file and the tool must skip-and-warn.
	base := filepath.Join(t.TempDir(), "bintrail_id=skip-test")
	hourDir := filepath.Join(base, "date=2026-06-01", "hour=12")
	if err := os.MkdirAll(hourDir, 0o755); err != nil {
		t.Fatal(err)
	}
	garbage := filepath.Join(hourDir, "events.parquet")
	if err := os.WriteFile(garbage, []byte("not parquet"), 0o644); err != nil {
		t.Fatal(err)
	}
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count, s3_bucket, s3_key, s3_uploaded_at)
		VALUES ('p_2026060112', 'skip-test', ?, 1, NULL, NULL, NULL)`, garbage)

	cfg := Config{
		Resolve: func(context.Context, string) (*Target, error) {
			return &Target{DB: db}, nil
		},
	}

	qres, _, err := MakeQueryTool(cfg)(context.Background(), &mcp.CallToolRequest{}, QueryArgs{Schema: "app", Table: "orders"})
	if err != nil {
		t.Fatalf("query handler: %v", err)
	}
	if qres.IsError {
		t.Fatalf("query tool errored: %s", resultText(qres))
	}
	if txt := resultText(qres); !strings.Contains(txt, "archive_source_skipped") || !strings.Contains(txt, base) {
		t.Errorf("query result must warn about the skipped source, got: %s", txt)
	}

	rres, _, err := MakeRecoverTool(cfg)(context.Background(), &mcp.CallToolRequest{}, RecoverArgs{Schema: "app", Table: "orders"})
	if err != nil {
		t.Fatalf("recover handler: %v", err)
	}
	if rres.IsError {
		t.Fatalf("recover tool errored: %s", resultText(rres))
	}
	if txt := resultText(rres); !strings.Contains(txt, "-- Warning: archive_source_skipped") || !strings.Contains(txt, base) {
		t.Errorf("recover result must warn about the skipped source as a SQL comment, got: %s", txt)
	}
}
