package console

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// TestViewsAPI_bothShapesOfOneSnapshotKeepTheirCasts is the guard for a bug the
// pointer introduced through a seam neither half looked at.
//
// The decimal memo is keyed by SNAPSHOT and applied back by exact PATH. Rewrite
// the paths to the pointer before the memo is populated and the two spellings
// of one snapshot poison each other: whichever request lands first fills the
// cache, and the other matches nothing, so every DECIMAL column ships uncast.
// Nothing errors, nothing is logged, and the file's own note blames a footer
// that was never unreadable. The SQL panel calls this on every query, so in
// practice the followed shape wins the cache and the operator who ticks "Pin to
// the backup that exists now" is the one who gets the broken file.
//
// Both orders are exercised: a fix that only works one way round is not a fix.
func TestViewsAPI_bothShapesOfOneSnapshotKeepTheirCasts(t *testing.T) {
	const createSQL = "CREATE TABLE `orders` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `total` decimal(10,2) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		");\n"
	const wantCast = `CAST("total" AS DECIMAL(10,2))`

	for _, tc := range []struct {
		name  string
		first string // query string of the request that populates the cache
		then  string
	}{
		{"followed first, then pinned", "", "?pin_snapshot=1"},
		{"pinned first, then followed", "?pin_snapshot=1", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeRealBaselineFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "orders", createSQL)
			if err := baseline.PublishCurrentPointer(dir + "/2026-06-10T12-00-00Z"); err != nil {
				t.Fatal(err)
			}
			srv := newViewsServer(t, dir, false)

			for _, q := range []string{tc.first, tc.then} {
				rec, body := doServersReq(t, srv, "GET", "/api/views.sql"+q, "")
				if rec.Code != 200 {
					t.Fatalf("%q: code = %d, body = %s", q, rec.Code, body)
				}
				if !strings.Contains(string(body), wantCast) {
					t.Fatalf("%q lost the money column's cast (%s missing); "+
						"the other shape of this snapshot poisoned the memo:\n%s", q, wantCast, body)
				}
			}
		})
	}
}

// TestSQLPanelInput_pins pins the panel's side of the same seam, and a second
// property besides. The panel EXECUTES what it builds, so following buys it
// nothing (it re-discovers the newest snapshot on every request anyway) and
// costs it per-statement consistency: a join over two state views resolves two
// read_parquet paths, and a pointer swap between them returns a join of two
// snapshots that never coexisted. The download can disclose that window in its
// header; the panel has no header to disclose it in.
func TestSQLPanelInput_pins(t *testing.T) {
	const createSQL = "CREATE TABLE `orders` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `total` decimal(10,2) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		");\n"
	dir := t.TempDir()
	writeRealBaselineFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "orders", createSQL)
	if err := baseline.PublishCurrentPointer(dir + "/2026-06-10T12-00-00Z"); err != nil {
		t.Fatal(err)
	}
	srv := newViewsServer(t, dir, false)

	in, err := srv.buildViewsInput(t.Context(), srv.cm.boot, false, false, true)
	if err != nil {
		t.Fatalf("buildViewsInput: %v", err)
	}
	if in.FollowsSnapshot {
		t.Fatal("the SQL panel's input follows the pointer; it executes its views, so it must pin")
	}
	for _, b := range in.Baselines {
		if strings.Contains(b.Path, baseline.CurrentLinkName) {
			t.Fatalf("panel path goes through the pointer: %q", b.Path)
		}
	}
}
