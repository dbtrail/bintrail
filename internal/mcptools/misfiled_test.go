package mcptools

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// mockDiscoverableSource creates a real bintrail_id base directory (so
// ResolveArchiveSources prefers the local path) and queues the discovery
// SELECT on mock. Returns the base the tools will fetch from.
func mockDiscoverableSource(t *testing.T, mock sqlmock.Sqlmock) string {
	t.Helper()
	base := filepath.Join(t.TempDir(), "bintrail_id=m1")
	if err := os.MkdirAll(base, 0o755); err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery("GROUP BY bintrail_id").WillReturnRows(
		sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
			AddRow("m1", filepath.Join(base, "date=2026-06-01", "hour=12", "events.parquet"), nil, nil))
	return base
}

// TestRecoverToolMisfiledScanFailureRefuses pins the third recover gate
// (#1288 review): sources discovered fine, but the misfiled-archive registry
// scan (#1037) fails, so time-scoped pruning could silently skip backfilled
// files — recover must refuse, not emit a possibly-incomplete script. The
// gate only arms under a time filter (no filter = nothing pruned), hence the
// since arg.
func TestRecoverToolMisfiledScanFailureRefuses(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mockDiscoverableSource(t, mock)
	mock.ExpectQuery("archive_state").WillReturnError(errors.New("simulated registry failure"))

	cfg := Config{
		Resolve: func(context.Context, string) (*Target, error) {
			return &Target{DB: db, ResolverLoaded: true}, nil
		},
	}
	res, _, err := MakeRecoverTool(cfg)(context.Background(), &mcp.CallToolRequest{},
		RecoverArgs{Schema: "app", Table: "orders", Since: "2026-06-01 00:00:00"})
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if !res.IsError {
		t.Fatalf("recover must refuse when the misfiled-archive scan fails, got: %s", resultText(res))
	}
	if txt := resultText(res); !strings.Contains(txt, "misfiled") || !strings.Contains(txt, "no_archive") {
		t.Errorf("refusal must name the misfiled scan and the no_archive escape hatch, got: %s", txt)
	}
}

// TestQueryToolMisfiledScanFailureWarns is the query-tool half: same failure,
// browsing surface — degrade, but say so in the result.
func TestQueryToolMisfiledScanFailureWarns(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mockDiscoverableSource(t, mock)
	mock.ExpectQuery("archive_state").WillReturnError(errors.New("simulated registry failure"))
	mock.ExpectQuery("binlog_events").WillReturnRows(sqlmock.NewRows(recoverToolMockCols))

	cfg := Config{
		Resolve: func(context.Context, string) (*Target, error) {
			return &Target{DB: db, ResolverLoaded: true}, nil
		},
	}
	res, _, err := MakeQueryTool(cfg)(context.Background(), &mcp.CallToolRequest{},
		QueryArgs{Schema: "app", Table: "orders", Since: "2026-06-01 00:00:00"})
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if res.IsError {
		t.Fatalf("query must degrade, not error, got: %s", resultText(res))
	}
	if txt := resultText(res); !strings.Contains(txt, "archive_scan_incomplete") {
		t.Errorf("query result must carry the archive_scan_incomplete warning, got: %s", txt)
	}
}