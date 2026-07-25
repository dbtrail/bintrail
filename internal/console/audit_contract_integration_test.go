//go:build integration

package console

import (
	"net/http"
	"testing"

	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationAuditContract_Console is the half of the console's #945 audit
// contract that needs a live index: the two endpoints whose fixtures are a
// real MySQL index plus a baseline snapshot (reconstruct) or a real
// foreign-key topology (recover-cascade).
//
// It reuses the seeds the existing endpoint tests already build
// (seedReconstruct / seedCascadeConsole) and asserts on emissions rather than
// on source text, so an emission that is deleted or moved onto a branch the
// request never takes fails here. CI runs the integration matrix on every
// pull request as a required check, so this gates merges like the unit tier
// does.
//
// No t.Parallel(): ext's sink is process-wide (audittest.Install).
func TestIntegrationAuditContract_Console(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	rec := audittest.Install(t)

	cases := []struct {
		name   string
		action string
		table  string
		call   func(t *testing.T)
	}{
		{
			name:   "reconstruct",
			action: "reconstruct.run",
			table:  "users",
			call: func(t *testing.T) {
				srv := seedReconstruct(t)
				reconstructAt(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2013:30:00&allow_gaps=true")
			},
		},
		{
			name:   "recover-cascade",
			action: "recover.cascade",
			table:  "parent",
			call: func(t *testing.T) {
				srv, dbName := seedCascadeConsole(t, nil)
				w, body := doReq(t, srv, "POST", "/api/recover-cascade",
					`{"schema":"`+dbName+`","table":"parent"}`)
				if w.Code != http.StatusOK {
					t.Fatalf("recover-cascade: code=%d body=%s", w.Code, body)
				}
			},
		},
	}

	var observed []audittest.Pair
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec.Reset()
			tc.call(t)
			evs := rec.Events()
			if len(evs) != 1 {
				t.Fatalf("recorded %d audit events, want exactly 1: %+v", len(evs), evs)
			}
			ev := evs[0]
			if ev.Surface != "console" || ev.Action != tc.action {
				t.Errorf("event = %s/%s, want console/%s", ev.Surface, ev.Action, tc.action)
			}
			if ev.Table != tc.table {
				t.Errorf("table = %q, want %q", ev.Table, tc.table)
			}
			if ev.Actor == "" {
				t.Error("actor must not be empty")
			}
			observed = append(observed, audittest.Pair{Surface: ev.Surface, Action: ev.Action})
		})
	}

	audittest.CheckCoverage(t, audittest.OwnerConsoleIntegration, observed)
}
