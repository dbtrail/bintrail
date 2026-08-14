//go:build integration

package mcptools

import (
	"context"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestRecoverTool_pksAndLimitPerPK is the wiring proof for the #962 params:
// the unit tests pin BuildQueryOptions, but only a real fetch shows the
// options reach the SQL (an unplumbed PKValuesIn returns the whole table and
// the reversal script covers events nobody named — the exact failure mode
// the recover tool exists to avoid).
func TestRecoverTool_pksAndLimitPerPK(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// pk 1 has TWO updates (limit_per_pk=1 must keep only the newer),
	// pk 2 has one, pk 9 is outside the pks filter entirely.
	testutil.InsertEvent(t, db, "bin.000001", 100, 200, "2026-06-01 12:00:00", nil,
		"app", "orders", 2, "1", nil,
		[]byte(`{"id":1,"status":"old1"}`), []byte(`{"id":1,"status":"mid1"}`))
	testutil.InsertEvent(t, db, "bin.000001", 200, 300, "2026-06-01 12:00:01", nil,
		"app", "orders", 2, "1", nil,
		[]byte(`{"id":1,"status":"mid1"}`), []byte(`{"id":1,"status":"new1"}`))
	testutil.InsertEvent(t, db, "bin.000001", 300, 400, "2026-06-01 12:00:02", nil,
		"app", "orders", 2, "2", nil,
		[]byte(`{"id":2,"status":"old2"}`), []byte(`{"id":2,"status":"new2"}`))
	testutil.InsertEvent(t, db, "bin.000001", 400, 500, "2026-06-01 12:00:03", nil,
		"app", "orders", 2, "9", nil,
		[]byte(`{"id":9,"status":"old9"}`), []byte(`{"id":9,"status":"new9"}`))

	cfg := Config{
		Resolve: func(context.Context, string) (*Target, error) {
			return &Target{DB: db, NoArchive: true}, nil
		},
	}
	res, _, err := MakeRecoverTool(cfg)(context.Background(), &mcp.CallToolRequest{}, RecoverArgs{
		Schema: "app", Table: "orders",
		PKs:        []string{"1", "2"},
		LimitPerPK: 1,
	})
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if res.IsError {
		t.Fatalf("recover refused: %s", resultText(res))
	}
	script := resultText(res)

	// Latest event per selected pk is reversed: pk 1 rolls back to mid1
	// (NOT old1 — that would mean limit_per_pk was ignored and the older
	// event was reversed too), pk 2 to old2.
	if !strings.Contains(script, "mid1") {
		t.Errorf("script does not reverse pk 1's latest event (missing mid1):\n%s", script)
	}
	if strings.Contains(script, "old1") {
		t.Errorf("script reverses pk 1's OLDER event despite limit_per_pk=1 (found old1):\n%s", script)
	}
	if !strings.Contains(script, "old2") {
		t.Errorf("script does not reverse pk 2's event (missing old2):\n%s", script)
	}
	if strings.Contains(script, "old9") || strings.Contains(script, "new9") {
		t.Errorf("script touches pk 9, which the pks filter never named:\n%s", script)
	}
	if !strings.Contains(script, "2 reversal statement(s) generated") {
		t.Errorf("expected exactly 2 reversal statements, got:\n%s", script)
	}
}
