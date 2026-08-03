//go:build integration

package cli

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationDrill_guardRefusesSecondRun proves the emptiness guard is
// actually WIRED into runDrill (not just unit-tested in isolation): the
// first rehearsal populates the scratch schema, so a second run against the
// same target must be refused before touching anything.
func TestIntegrationDrill_guardRefusesSecondRun(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srcSchema := fmt.Sprintf("drillsrc2_%d", time.Now().UnixNano())
	dsn, baseDir, at := drillContractFixture(t, srcSchema)
	resetDrillGlobals(t)
	drlIndexDSN, drlBaselineDir = dsn, baseDir
	drlTables = srcSchema + ".orders"
	drlTargetDSN = testutil.BaseDSN() + "/"
	drlAt = at.Format("2006-01-02 15:04:05")
	drlFormat = "json"

	if err := runDrill(newQueryTestCmd(), nil); err != nil {
		t.Fatalf("first drill must pass: %v", err)
	}
	err := runDrill(newQueryTestCmd(), nil)
	if err == nil || !strings.Contains(err.Error(), "already has table") {
		t.Fatalf("second run against the now-populated target must be refused by the guard: %v", err)
	}
}
