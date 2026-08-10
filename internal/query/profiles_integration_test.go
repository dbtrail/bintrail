//go:build integration

package query

import (
	"context"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationListProfilesSurfacesNon1146 pins that the missing-table
// swallow is narrow. Widened to "any error is an empty list", a dead
// connection or a denied SELECT would report "no profiles exist" — and a
// caller that offers a picker would render an empty one, which is strictly
// worse than the free-text fallback it takes on a real error.
func TestIntegrationListProfilesSurfacesNon1146(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Sanity: the happy path is an empty list with no error.
	if names, err := ListProfiles(context.Background(), db); err != nil || len(names) != 0 {
		t.Fatalf("ListProfiles on a fresh index = %v, %v", names, err)
	}

	db.Close()
	names, err := ListProfiles(context.Background(), db)
	if err == nil {
		t.Fatalf("ListProfiles on a closed pool returned %v and no error", names)
	}
	if !strings.Contains(err.Error(), "list profiles") {
		t.Fatalf("error lost its context: %v", err)
	}
}
