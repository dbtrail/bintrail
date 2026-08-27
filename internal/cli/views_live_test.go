package cli

import (
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/views"
)

// TestLiveIndexFromDSN_dropsThePassword guards the boundary the password
// actually crosses.
//
// The index DSN carries it, liveIndexFromDSN is the only place it is in scope,
// and everything downstream renders into a file the operator is meant to share.
// The views package has its own structural guard that LiveIndex grows no
// credential field; this one proves the value is dropped on the way in, which
// is the half that guard cannot see.
func TestLiveIndexFromDSN_dropsThePassword(t *testing.T) {
	const pw = "s3cr3t-index-password"
	li, err := liveIndexFromDSN("bintrail:" + pw + "@tcp(db.internal:3307)/bintrail_index")
	if err != nil {
		t.Fatalf("liveIndexFromDSN: %v", err)
	}

	// Rendered, not just inspected field by field: a future field would be
	// invisible to an enumeration written today, and the rendered file is what
	// actually reaches another person.
	out := views.Generate(views.Input{
		GeneratedAt:    time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Version:        "test",
		ArchiveSources: []string{"/a/bintrail_id=x"},
		LiveIndex:      li,
	})
	if strings.Contains(out, pw) {
		t.Fatal("the index password reached the generated file, which is meant to be shared")
	}

	// The non-secret half must survive, or the file cannot be used at all.
	if li.Host != "db.internal" || li.Port != 3307 || li.Database != "bintrail_index" || li.User != "bintrail" {
		t.Errorf("connection facts lost: %+v", li)
	}
}

// TestLiveIndexFromDSN_refusesWithoutADatabase: a DSN with no database would
// render an ATTACH naming nothing, which fails in the operator's DuckDB with an
// error about a catalog rather than about their flag.
func TestLiveIndexFromDSN_refusesWithoutADatabase(t *testing.T) {
	if _, err := liveIndexFromDSN("bintrail:pw@tcp(db.internal:3307)/"); err == nil {
		t.Error("expected a refusal for a DSN that names no database")
	}
}
