package views

import (
	"strings"
	"testing"
	"time"
)

// The ATTACH is the one statement in this file that depends on reaching another
// machine, and `duckdb -init` aborts the session at the first error. Emitted
// ahead of the views it cost the reader everything, including views that read
// only Parquet and needed nothing from the index (#1536). Verified in DuckDB
// separately: statements that ran before an aborting one stay committed, so the
// ordering below is what turns a total loss into a degrade.

// orderedInput is the shape the ordering matters for: a cold leg AND a baseline
// AND an index, so the file carries all three kinds of statement.
func orderedInput() Input {
	in := liveInput(liveIdx())
	in.BaselineSource = "/baselines"
	in.BaselineSnapshot = time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC)
	in.Baselines = []BaselineTable{{Schema: "shop", Table: "orders", Path: "/baselines/2026-04-30/shop/orders.parquet"}}
	return in
}

func TestLiveLeg_parquetOnlyViewsPrecedeTheAttach(t *testing.T) {
	out := Generate(orderedInput())

	attach := strings.Index(out, "\nATTACH ")
	if attach < 0 {
		t.Fatalf("no ATTACH in a file generated with an index:\n%s", out)
	}
	events := strings.Index(out, `CREATE OR REPLACE VIEW "events"`)
	state := strings.Index(out, `CREATE OR REPLACE VIEW "state_`)

	if events < 0 || state < 0 {
		t.Fatalf("expected both an events view and a state view:\n%s", out)
	}
	if state > attach {
		t.Error("a state view is defined after the ATTACH. State views read baseline " +
			"Parquet and never touch the index, so a failed ATTACH must not take them")
	}
	if events < attach {
		t.Error("the two-leg events view is defined before the ATTACH it reads through")
	}

	// EXACTLY ONE definition, counted rather than located. The events view is
	// the expensive statement in the file — union_by_name opens a Parquet
	// footer per archived file at CREATE VIEW time (#1535), measured at ~7s
	// over 120 files with a second definition costing ~3.6s more whether it
	// repeats the literal or references the first view. A change that reverts
	// to defining it archives-only and CREATE OR REPLACEing it after the ATTACH
	// still satisfies every ordering assertion above, so only the count catches
	// it.
	if n := strings.Count(out, `CREATE OR REPLACE VIEW "events"`); n != 1 {
		t.Errorf("the events view is defined %d times, want 1: each definition binds "+
			"the whole archive file list", n)
	}

	// The catalog name must appear only on the far side of the ATTACH that
	// creates it. This is the assertion that would catch a reorder that moved
	// the statements but left a view reading a catalog that does not exist yet.
	if i := strings.Index(out, `"bintrail_live"."binlog_events"`); i >= 0 && i < attach {
		t.Error("a view reads the attached catalog before the ATTACH creates it")
	}
}

// TestLiveLeg_saysWhatAFailedAttachCosts: the file is the only thing a reader
// whose ATTACH failed still has. It must state the trade the ordering makes —
// the state views survive, the events view does not — because that reader
// cannot otherwise tell an intended degrade from a truncated download.
//
// The second half is the older guarantee: this render is the same code path
// that serves a file with NO index, where it tells the reader to regenerate
// with --include-live. In a file that already has a live leg, that advice asks
// for something already done.
func TestLiveLeg_saysWhatAFailedAttachCosts(t *testing.T) {
	in := orderedInput()
	in.LiveLegHowTo = "tick the Include the live index box"
	out := Generate(in)

	for _, unwanted := range []string{"--include-live", "tick the Include the live index box"} {
		if strings.Contains(out, unwanted) {
			t.Errorf("the file tells the reader to add a live leg (%q) in a file that "+
				"already has one", unwanted)
		}
	}
	for _, want := range []string{
		"cannot reach",
		"state_ view still usable",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the file never says %q, so a reader whose ATTACH failed cannot "+
				"tell what they are left holding:\n%s", want, out)
		}
	}
}

// TestLiveOnly_definesEventsOnce guards the OTHER shape the single definition
// has to hold for: an index with nothing archived, where the archives-only
// render would emit a "(skipped: no archive sources are registered)" comment.
// Emitted alongside a live-only definition, that comment sits directly above a
// CREATE OR REPLACE VIEW "events" and flatly contradicts it.
func TestLiveOnly_definesEventsOnce(t *testing.T) {
	in := orderedInput()
	in.ArchiveSources = nil
	out := Generate(in)

	if n := strings.Count(out, "-- events:"); n != 1 {
		t.Errorf("a live-only file has %d events blocks, want 1:\n%s", n, out)
	}
	if n := strings.Count(out, `CREATE OR REPLACE VIEW "events"`); n != 1 {
		t.Errorf("a live-only file defines the events view %d times, want 1", n)
	}
	if strings.Contains(out, "skipped:") {
		t.Errorf("a file that DOES define the events view also says it was skipped:\n%s", out)
	}
}

// TestLiveLeg_singleLabelHostWarns covers the shape that produced #1536: a
// console running under Docker Compose emits its compose service name, which
// resolves for containers on that network and nowhere else.
func TestLiveLeg_singleLabelHostWarns(t *testing.T) {
	const marker = "bare name with no domain"
	cases := []struct {
		name string
		host string
		want bool
	}{
		{"compose service name", "index-mysql", true},
		{"kubernetes short name", "bintrail-index", true},
		{"fully qualified", "index.example.com", false},
		{"trailing dot is still qualified", "index.example.com.", false},
		{"ipv4 literal", "10.0.0.5", false},
		{"ipv6 literal has no dots but is an address", "[2001:db8::1]", false},
		// localhost is single-label too, and it gets the LOUDER warning: it
		// resolves everywhere and answers with a different index.
		{"localhost keeps the loopback warning", "localhost", false},
		{"loopback literal keeps the loopback warning", "127.0.0.1", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			li := liveIdx()
			li.Host = tc.host
			out := Generate(liveInput(li))
			if got := strings.Contains(out, marker); got != tc.want {
				t.Errorf("single-label warning present = %v, want %v for host %q",
					got, tc.want, tc.host)
			}
			// Exactly one warning, never both: they name different failures and
			// a reader handed both cannot tell which one they are in.
			if strings.Contains(out, marker) && strings.Contains(out, "is a loopback address") {
				t.Errorf("host %q got both the loopback and the single-label warning", tc.host)
			}
		})
	}
}

func TestIsSingleLabelHost(t *testing.T) {
	for host, want := range map[string]bool{
		"index-mysql": true,
		"db":          true,
		// The trailing dot is the only case TrimSuffix discriminates: without it
		// "db." reads as qualified because it contains a dot.
		"db.":                true,
		"index.example.com":  false,
		"index.example.com.": false,
		"10.0.0.5":           false,
		"2001:db8::1":        false,
		"[2001:db8::1]":      false,
		"::1":                false,
		"":                   false,
	} {
		if got := isSingleLabelHost(host); got != want {
			t.Errorf("isSingleLabelHost(%q) = %v, want %v", host, got, want)
		}
	}
}
