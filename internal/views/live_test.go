package views

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func liveInput(li *LiveIndex) Input {
	return Input{
		GeneratedAt:    time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Version:        "test",
		ArchiveSources: []string{"/archives/bintrail_id=aaaa"},
		LiveIndex:      li,
	}
}

// TestLiveLeg_carriesNoCredential is a structural guard on the artifact's
// central promise: this file is meant to be SHARED, and its header says it
// holds no credentials.
//
// LiveIndex has no password field today, so nothing can leak one right now.
// That is exactly why this exists. The cheapest future change is adding the
// field "so the file just runs", and it would be a one-line diff that no
// behavioural test would notice. This asserts on the TYPE, so it fails when the
// field appears rather than when someone later renders it.
func TestLiveLeg_carriesNoCredential(t *testing.T) {
	rt := reflect.TypeOf(LiveIndex{})
	for i := 0; i < rt.NumField(); i++ {
		name := strings.ToLower(rt.Field(i).Name)
		for _, banned := range []string{"pass", "secret", "token", "credential", "key"} {
			if strings.Contains(name, banned) {
				t.Errorf("LiveIndex.%s: this struct is rendered into a file meant to be shared, "+
					"whose header states it carries no credentials. The password belongs in the "+
					"operator's own DuckDB session, via the empty slot the preamble emits.",
					rt.Field(i).Name)
			}
		}
	}

	out := Generate(liveInput(&LiveIndex{Host: "db.internal", Port: 3306, Database: "idx", User: "reader"}))
	// The empty slot must be there, or the promise costs usability for nothing:
	// the file has to be runnable after ONE edit, not require the operator to
	// reconstruct the secret statement from the docs.
	if !strings.Contains(out, "PASSWORD ''") {
		t.Error("no empty PASSWORD slot in the preamble")
	}
	// The location IS configuration and must be present, or a reader on another
	// machine cannot use the file at all.
	for _, want := range []string{"'db.internal'", "PORT 3306", "'idx'", "'reader'"} {
		if !strings.Contains(out, want) {
			t.Errorf("preamble is missing %s, which the reader needs to connect", want)
		}
	}
}

// TestLiveLeg_shape pins the parts of the union whose absence is silent.
//
// Each assertion below corresponds to something that produced a WRONG ANSWER
// rather than an error when it was missing, which is why they are pinned
// individually instead of by one golden comparison.
func TestLiveLeg_shape(t *testing.T) {
	out := Generate(liveInput(&LiveIndex{
		Host: "h", Port: 3306, Database: "d", User: "u", BintrailID: "the-id",
	}))

	for _, want := range []string{
		// Without the cast the union of a BIGINT UNSIGNED index column and a
		// signed Parquet one widens to HUGEINT, silently, in every downstream join.
		`CAST("event_id" AS BIGINT) AS "event_id"`,
		// Without the anti-join, a partition that is archived but not yet
		// dropped is counted twice.
		`WHERE NOT EXISTS (SELECT 1 FROM hot WHERE hot.event_id = cold.event_id)`,
		// Live rows carry no partition path, so the hot leg has to produce the
		// Hive columns or a filter on them silently drops every live row.
		`strftime("event_timestamp", '%Y-%m-%d') AS "event_date"`,
		`'the-id' AS "bintrail_id"`,
		`ATTACH '' AS "bintrail_live"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("generated SQL is missing:\n  %s", want)
		}
	}

	// pk_hash exists on the live table and NOT in the archives. Selecting it
	// would give the two legs different shapes.
	if strings.Contains(out, "pk_hash") {
		t.Error(`the hot leg selects pk_hash, which the archives do not carry: ` +
			`the two legs must have the same shape`)
	}
}

// TestLiveLeg_unattributedSaysSo: with more than one source registered, the
// command leaves BintrailID empty because a live row carries no identity of its
// own. The view must then say NULL rather than inheriting a neighbour's id.
func TestLiveLeg_unattributedSaysSo(t *testing.T) {
	out := Generate(liveInput(&LiveIndex{Host: "h", Port: 3306, Database: "d", User: "u"}))
	if !strings.Contains(out, `NULL AS "bintrail_id"`) {
		t.Error("unattributed hot rows must select NULL for bintrail_id")
	}
}

// TestNoLiveIndex_saysWhatIsMissing: the default file is archives-only, and the
// whole point of #1480 is that a reader cannot otherwise tell an absent row
// from a row that does not exist.
func TestNoLiveIndex_saysWhatIsMissing(t *testing.T) {
	out := Generate(liveInput(nil))
	if strings.Contains(out, "bintrail_live") {
		t.Error("no LiveIndex was given, but the output references the live catalog")
	}
	if !strings.Contains(out, "ARCHIVED events only") {
		t.Error("the archives-only file must state its scope: without it, the most recent " +
			"window reads as if nothing happened")
	}
}
