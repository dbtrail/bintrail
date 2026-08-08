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

// TestIntegrationArchiveSkipWarning pins the #1285 wiring for both tools and
// both failure shapes. Per-source fetch failure (a registered archive whose
// only file is not valid Parquet): the query tool must carry an
// archive_source_skipped warning in the RESULT — an MCP client sees nothing
// else — and the recover tool must REFUSE (a knowingly incomplete reversal
// script is never returned; no_archive is the escape hatch, and it must
// actually work). Discovery failure (archive_state unreadable): query warns
// archive_discovery_failed, recover refuses. Deleting the skippedArchives
// append, the ArchiveSkipNotice call, the discoveryFailed plumbing, or either
// refusal turns a branch of this red.
func TestIntegrationArchiveSkipWarning(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	cfg := Config{
		Resolve: func(context.Context, string) (*Target, error) {
			return &Target{DB: db}, nil
		},
	}

	// Cry-wolf guard: a healthy target (empty archive_state, no archive
	// trouble) must carry NO archive warnings.
	q0, _, err := MakeQueryTool(cfg)(context.Background(), &mcp.CallToolRequest{}, QueryArgs{Schema: "app", Table: "orders"})
	if err != nil {
		t.Fatalf("query handler (healthy): %v", err)
	}
	if txt := resultText(q0); strings.Contains(txt, "archive_") {
		t.Errorf("healthy target must render no archive warnings, got: %s", txt)
	}

	// A discoverable local archive whose only file is garbage:
	// ResolveArchiveSources returns the bintrail_id base (the directory
	// exists, so the local path is preferred), then parquetquery.Fetch fails
	// on the corrupt file.
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
	if !rres.IsError {
		t.Fatalf("recover must REFUSE on a failed archive source, got a script: %s", resultText(rres))
	}
	if txt := resultText(rres); !strings.Contains(txt, base) || !strings.Contains(txt, "no_archive") {
		t.Errorf("recover refusal must name the failed source and the no_archive escape hatch, got: %s", txt)
	}

	// The escape hatch must actually work: no_archive skips discovery and
	// generates from the live index only.
	rok, _, err := MakeRecoverTool(cfg)(context.Background(), &mcp.CallToolRequest{}, RecoverArgs{Schema: "app", Table: "orders", NoArchive: true})
	if err != nil {
		t.Fatalf("recover handler (no_archive): %v", err)
	}
	if rok.IsError {
		t.Errorf("recover with no_archive must succeed, got: %s", resultText(rok))
	}

	// Discovery failure: archive_state present but unreadable (wrong shape →
	// the SELECT errors). "Table absent" is deliberately NOT failure (1146 =
	// pre-archive index); a broken read is.
	testutil.MustExec(t, db, `DROP TABLE archive_state`)
	testutil.MustExec(t, db, `CREATE TABLE archive_state (wrong_shape INT)`)

	qres2, _, err := MakeQueryTool(cfg)(context.Background(), &mcp.CallToolRequest{}, QueryArgs{Schema: "app", Table: "orders"})
	if err != nil {
		t.Fatalf("query handler (discovery): %v", err)
	}
	if qres2.IsError {
		t.Fatalf("query tool errored: %s", resultText(qres2))
	}
	if txt := resultText(qres2); !strings.Contains(txt, "archive_discovery_failed") {
		t.Errorf("query result must warn about failed discovery, got: %s", txt)
	}

	rres2, _, err := MakeRecoverTool(cfg)(context.Background(), &mcp.CallToolRequest{}, RecoverArgs{Schema: "app", Table: "orders"})
	if err != nil {
		t.Fatalf("recover handler (discovery): %v", err)
	}
	if !rres2.IsError {
		t.Fatalf("recover must REFUSE on failed discovery, got a script: %s", resultText(rres2))
	}
	if txt := resultText(rres2); !strings.Contains(txt, "discovery failed") || !strings.Contains(txt, "no_archive") {
		t.Errorf("recover refusal must name discovery and the no_archive escape hatch, got: %s", txt)
	}

	// Standalone posture: EnvArchiveDiscovery reaches state discovery only
	// through EnvArchiveSources' fallthrough — the discovery signal must
	// survive that hop (#1288 review: discarding the bool there recreates the
	// #1285 bug on the shipped default surface with everything else green).
	t.Setenv("BINTRAIL_ARCHIVE_S3", "")
	t.Setenv("BINTRAIL_ID", "")
	cfgEnv := Config{
		Resolve: func(context.Context, string) (*Target, error) {
			return &Target{DB: db, EnvArchiveDiscovery: true}, nil
		},
	}
	rres3, _, err := MakeRecoverTool(cfgEnv)(context.Background(), &mcp.CallToolRequest{}, RecoverArgs{Schema: "app", Table: "orders"})
	if err != nil {
		t.Fatalf("recover handler (env discovery): %v", err)
	}
	if !rres3.IsError || !strings.Contains(resultText(rres3), "discovery failed") {
		t.Errorf("standalone env-discovery posture must refuse on failed state discovery, got: %s", resultText(rres3))
	}
}