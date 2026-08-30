package views

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

// archiveID is the identity the fixture's archive PATH carries. The hot leg's
// id has to match it or the cross-check refuses to assert either one, which is
// the behaviour TestLiveLeg_disagreementAssertsNeither covers.
const archiveID = "aaaa"

func liveInput(li *LiveIndex) Input {
	return Input{
		GeneratedAt:    time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Version:        "test",
		ArchiveSources: []string{"/archives/bintrail_id=" + archiveID},
		LiveIndex:      li,
	}
}

func liveIdx() *LiveIndex {
	return &LiveIndex{Host: "db.internal", Port: 3306, Database: "idx", User: "reader"}
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

	out := Generate(liveInput(liveIdx()))
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
	li := liveIdx()
	li.BintrailID = archiveID
	out := Generate(liveInput(li))

	for _, want := range []string{
		// Without the cast the union of a BIGINT UNSIGNED index column and a
		// signed Parquet one widens to HUGEINT, silently, in every downstream join.
		`CAST("event_id" AS BIGINT) AS "event_id"`,
		// Without the anti-join, a partition that is archived but not yet
		// dropped is counted twice. Its direction is load-bearing and has its
		// own test: the ARCHIVES win the overlap.
		`WHERE NOT EXISTS (SELECT 1 FROM cold WHERE cold.event_id = hot.event_id)`,
		// Live rows carry no partition path, so the hot leg has to produce the
		// Hive columns or a filter on them silently drops every live row.
		`CAST(strftime("event_timestamp", '%Y-%m-%d') AS DATE) AS "event_date"`,
		`'aaaa' AS "bintrail_id"`,
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

// TestLiveLeg_archivesWinTheOverlap pins the direction of the dedup.
//
// The index leg is the one that can be missing information: it derives
// event_date/event_hour and may have no bintrail_id at all, while an archived
// row carries all three from its path. Letting it win the overlap replaces a
// known source with NULL for every event in it, so a WHERE bintrail_id = ...
// stops returning rows the archives hold. TestTwoLegs_executeInDuckDB measures
// that; this pins the shape that prevents it.
func TestLiveLeg_archivesWinTheOverlap(t *testing.T) {
	out := Generate(liveInput(liveIdx()))
	union := out[strings.Index(out, "  SELECT * FROM "):]
	if !strings.HasPrefix(union, "  SELECT * FROM cold\n  UNION ALL BY NAME\n  SELECT * FROM hot\n") {
		t.Errorf("the archives must be the winning side of the union, got:\n%s", union)
	}
	if strings.Contains(out, "SELECT 1 FROM hot WHERE hot.event_id = cold.event_id") {
		t.Error("the anti-join excludes archived rows on account of index rows: " +
			"that hands the overlap to the leg with less information")
	}
}

// TestLiveLeg_attributionSaysOnlyWhatWasObserved is the test whose absence let
// one sentence stand in for four different observations.
//
// The old code collapsed every outcome of one COUNT(*) — several sources, none
// registered, no such table, no permission, a dead connection — into "index
// serves more than one source", which was false for all but the first.
func TestLiveLeg_attributionSaysOnlyWhatWasObserved(t *testing.T) {
	// Every sentence any branch can emit. Each case asserts its own appears
	// AND that no other one does, so a branch cannot quietly borrow another's
	// claim.
	const (
		multi        = "more than one source is registered"
		unregistered = "this index registers no source id"
		unreadable   = "could not be read"
		disagree     = "unattributed rather than assert either"
	)
	all := []string{multi, unregistered, unreadable, disagree}

	cases := []struct {
		name string
		li   func() *LiveIndex
		id   string // the literal the hot leg must select, "" for NULL
		want string
	}{
		{
			name: "attributed by a single registered source",
			li: func() *LiveIndex {
				li := liveIdx()
				li.BintrailID = archiveID
				return li
			},
			id: `'aaaa' AS "bintrail_id"`,
		},
		{
			name: "several sources registered",
			li: func() *LiveIndex {
				li := liveIdx()
				li.Attribution = AttributionMultiSource
				return li
			},
			want: multi,
		},
		{
			// A file-mode index (bintrail index --binlog-dir) registers no
			// server and serves exactly ONE source. Reporting it as
			// multi-source was the inversion.
			name: "no source registered",
			li: func() *LiveIndex {
				li := liveIdx()
				li.Attribution = AttributionUnregistered
				return li
			},
			want: unregistered,
		},
		{
			name: "the registry could not be read",
			li: func() *LiveIndex {
				li := liveIdx()
				li.Attribution = AttributionUndetermined
				return li
			},
			want: unreadable,
		},
		{
			// The zero value must be the one that claims nothing: a producer
			// that fills no attribution must not make the file assert.
			name: "zero value claims nothing",
			li:   liveIdx,
			want: unreadable,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := Generate(liveInput(tc.li()))
			if tc.id != "" {
				if !strings.Contains(out, tc.id) {
					t.Errorf("missing the attributed literal %s", tc.id)
				}
				for _, s := range all {
					if strings.Contains(out, s) {
						t.Errorf("an attributed leg still explains itself with %q", s)
					}
				}
				return
			}
			if !strings.Contains(out, `NULL AS "bintrail_id"`) {
				t.Error("unattributed rows must select NULL for bintrail_id")
			}
			if !strings.Contains(out, tc.want) {
				t.Errorf("the file does not say what was observed (%q):\n%s", tc.want, eventsComment(out))
			}
			for _, s := range all {
				if s != tc.want && strings.Contains(out, s) {
					t.Errorf("states %q, which this branch did not observe", s)
				}
			}
		})
	}
}

// TestLiveLeg_disagreementAssertsNeither covers the cross-check between the two
// places an identity comes from.
//
// The hot leg's id is read from bintrail_servers; the cold leg's comes from the
// `bintrail_id=` path segment, which rotate takes verbatim from --bintrail-id
// and never checks against the registry. When they differ, asserting the probed
// one makes ONE source appear as TWO servers inside a single view, and a
// WHERE bintrail_id = ... silently returns half the rows.
func TestLiveLeg_disagreementAssertsNeither(t *testing.T) {
	li := liveIdx()
	li.BintrailID = "from-the-registry"
	out := Generate(liveInput(li))

	if strings.Contains(out, `'from-the-registry' AS "bintrail_id"`) {
		t.Error("the probed id was asserted over archives written under a different one")
	}
	if !strings.Contains(out, `NULL AS "bintrail_id"`) {
		t.Error("a disagreement must fall back to unattributed")
	}
	// Naming both is the point: the operator cannot fix a mismatch they cannot
	// see, and neither value is knowable from the other side.
	for _, want := range []string{"from-the-registry", archiveID} {
		if !strings.Contains(out, want) {
			t.Errorf("the comment does not name %q, so the disagreement is invisible", want)
		}
	}
}

// TestLiveLeg_agreementIsNotADisagreement: the cross-check must not fire when
// the ids match, or --include-live never attributes anything.
func TestLiveLeg_agreementIsNotADisagreement(t *testing.T) {
	li := liveIdx()
	li.BintrailID = archiveID
	in := liveInput(li)
	// One archive per source is the multi-archive shape: the id being present
	// at all is what matters, not it being alone.
	in.ArchiveSources = append(in.ArchiveSources, "/archives/bintrail_id=bbbb")
	if out := Generate(in); !strings.Contains(out, `'aaaa' AS "bintrail_id"`) {
		t.Error("an id that IS among the archives' must still be asserted")
	}
}

// TestLiveLeg_missingColumnsBecomeNull covers an index migrated to an earlier
// point than this build's schema — the console sets EnsureSchema: false and
// never migrates registry servers, so it is reachable rather than theoretical.
//
// Naming a column the table does not have does not degrade the view: DuckDB
// refuses the statement with a binder error and creates NO events view at all.
func TestLiveLeg_missingColumnsBecomeNull(t *testing.T) {
	li := liveIdx()
	li.BintrailID = archiveID
	// Everything except the four columns EnsureSchema adds after the original
	// schema.
	for _, c := range []string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp", "gtid",
		"schema_name", "table_name", "event_type", "pk_values", "changed_columns",
		"row_before", "row_after",
	} {
		li.TableColumns = append(li.TableColumns, c)
	}
	out := Generate(liveInput(li))
	hot := out[strings.Index(out, "), hot AS ("):]

	for _, missing := range []string{"connection_id", "query_text", "query_hash", "commit_ts_us"} {
		if !strings.Contains(hot, `NULL AS "`+missing+`"`) {
			t.Errorf("the hot leg names %s, which this index does not have: the file fails to bind", missing)
		}
	}
	// commit_ts_us renders TWO output columns. Dropping only one of them
	// changes the leg's shape, which is a different failure with the same
	// cause.
	if !strings.Contains(hot, `NULL AS "commit_time"`) {
		t.Error("commit_time disappeared from the hot leg, so the two legs no longer line up")
	}
	// A column that IS present must still be selected, or an observed column
	// set becomes a way to lose data.
	if !strings.Contains(hot, `"pk_values"`) {
		t.Error("a column the index does have was dropped")
	}
	// The cold leg has union_by_name for exactly this and must be untouched.
	cold := out[strings.Index(out, "WITH cold AS ("):strings.Index(out, "), hot AS (")]
	if strings.Contains(cold, "NULL AS") {
		t.Error("the archives' leg lost a column: union_by_name already handles its absences")
	}
}

// TestLiveLeg_unobservedColumnsAreAllNamed: an empty TableColumns means NOT
// OBSERVED, not "the table has no columns". Emitting NULL for everything would
// generate a view of nothing but NULLs.
func TestLiveLeg_unobservedColumnsAreAllNamed(t *testing.T) {
	li := liveIdx()
	li.BintrailID = archiveID
	out := Generate(liveInput(li))
	if strings.Contains(out, "NULL AS \"pk_values\"") {
		t.Error("an unobserved column set blanked a column instead of naming it")
	}
}

// TestLiveOnly_whenNothingIsArchived: a fresh install has archived nothing, and
// that is exactly when the live leg is worth most. The archives-only early
// return used to swallow the whole view, so the file attached the index
// read-only, asked for a password, and then defined nothing (#1485).
func TestLiveOnly_whenNothingIsArchived(t *testing.T) {
	in := liveInput(liveIdx())
	in.ArchiveSources = nil
	out := Generate(in)

	if !strings.Contains(out, `CREATE OR REPLACE VIEW "events" AS`) {
		t.Fatalf("no events view at all with an index and no archives:\n%s", out)
	}
	if !strings.Contains(out, `FROM "bintrail_live"."binlog_events"`) {
		t.Error("the view does not read the index it just attached")
	}
	if strings.Contains(out, "read_parquet(") {
		t.Error("a view over archives that do not exist")
	}
	// No archive path means no id to cross-check against, so the leg keeps
	// whatever the registry said.
	if strings.Contains(eventsComment(out), "(skipped:") {
		t.Error("the events view was skipped despite having a source to read")
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
	// The remediation has to name the flag. "Regenerate with a reachable
	// index" describes what an operator who already passed --index-dsn just
	// did, and doing it again returns a byte-identical file.
	if !strings.Contains(out, "--include-live") {
		t.Error("the note does not name the flag that adds the live leg")
	}
}

// TestNoLiveIndex_unavailableSaysSomethingTrue: a producer with no route to
// the index at all (the console download for a server whose index connection
// is not open, or whose DSN names a unix socket) must say why, not point at a
// flag its reader cannot pass.
func TestNoLiveIndex_unavailableSaysSomethingTrue(t *testing.T) {
	in := liveInput(nil)
	in.LiveLegUnavailable = true
	out := Generate(in)

	if !strings.Contains(out, "ARCHIVED events only") {
		t.Error("the scope statement must survive: it is what #1480 is about")
	}
	if !strings.Contains(out, "it has no way to reach the index") {
		t.Error("the file must say why it has no live leg, not point at a flag it cannot pass")
	}
	if strings.Contains(out, "--include-live` from a host") && strings.Contains(out, "Add a leg over the index by") {
		t.Error("both remediations rendered; the reader gets two different instructions")
	}
}

// TestNoLiveIndex_producerRouteReplacesTheFlag: the console CAN offer the hot
// leg (#1480 + the download checkbox), so its archives-only file must name the
// control that adds it. Naming the CLI flag instead sends a reader who is
// looking at a page to a command line they may not have.
func TestNoLiveIndex_producerRouteReplacesTheFlag(t *testing.T) {
	in := liveInput(nil)
	in.LiveLegHowTo = `Tick "Include the live index" on the Query in DuckDB card and download again.`
	out := Generate(in)

	if !strings.Contains(out, `-- Tick "Include the live index" on the Query in DuckDB card and download again.`) {
		t.Errorf("the producer's own route is not in the file:\n%s", out)
	}
	if strings.Contains(out, "--include-live") {
		t.Error("the CLI flag is still named alongside the producer's route")
	}
	if !strings.Contains(out, "ARCHIVED events only") {
		t.Error("the scope statement must survive whichever route is named")
	}
}

// A hint carrying a newline must not end its comment and turn the rest into
// statement text. Same rule as every other interpolated string in this file.
func TestLiveLegHowTo_staysInsideItsComment(t *testing.T) {
	in := liveInput(nil)
	in.LiveLegHowTo = "tick the box\nDROP TABLE orders;"
	out := Generate(in)
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "DROP TABLE") {
			t.Fatalf("a newline in the hint escaped its comment:\n%s", out)
		}
	}
}

// LiveLegUnavailable wins: a producer that cannot reach the index has no route
// to name, and rendering both would contradict the sentence above it.
func TestLiveLegHowTo_ignoredWhenUnavailable(t *testing.T) {
	in := liveInput(nil)
	in.LiveLegUnavailable = true
	in.LiveLegHowTo = "tick the box"
	out := Generate(in)
	if strings.Contains(out, "-- tick the box") {
		t.Error("a route was named for a producer that has none")
	}
}

// TestLiveLeg_costNote: the shape of this view has a cost the operator will
// otherwise measure and blame on the view. Every query streams the whole live
// table; a predicate on the derived Hive columns cannot become an index filter.
func TestLiveLeg_costNote(t *testing.T) {
	// BOTH live shapes. The preamble points forward at this note ("see COST
	// below") and is emitted whenever the index is attached, so a shape whose
	// cost note went missing would leave the file with a dangling reference.
	// Covering only the two-leg shape let that happen unnoticed.
	for name, in := range map[string]Input{
		"archives and index": liveInput(liveIdx()),
		"index alone":        func() Input { in := liveInput(liveIdx()); in.ArchiveSources = nil; return in }(),
	} {
		t.Run(name, func(t *testing.T) {
			out := eventsComment(Generate(in))
			for _, want := range []string{"COST:", "streams the whole live binlog_events"} {
				if !strings.Contains(out, want) {
					t.Errorf("the events view does not state its cost (%q)", want)
				}
			}
		})
	}
}

// TestGenerateViews_dropsTheLiveLeg: this entry point emits no preamble, so it
// emits no ATTACH. A two-leg view rendered through it would reference a catalog
// that does not exist and fail at CREATE VIEW, in the console's SQL panel.
func TestGenerateViews_dropsTheLiveLeg(t *testing.T) {
	out := GenerateViews(liveInput(liveIdx()))
	if strings.Contains(out, "bintrail_live") {
		t.Error("GenerateViews emitted a leg over a catalog it never attaches")
	}
	if !strings.Contains(out, "read_parquet(") {
		t.Error("GenerateViews dropped the archives too")
	}
}

// TestLivePreamble_loopbackIsFlagged: a loopback host is the driver's default
// for a DSN that names no address, and this file's whole justification for
// carrying host and port is that a reader on ANOTHER machine needs them. A
// recipient who also runs an index attaches successfully to the wrong one.
func TestLivePreamble_loopbackIsFlagged(t *testing.T) {
	for _, host := range []string{"127.0.0.1", "localhost", "::1"} {
		li := liveIdx()
		li.Host = host
		out := Generate(liveInput(li))
		if !strings.Contains(out, "loopback address") {
			t.Errorf("host %q is emitted as a location with no warning", host)
		}
	}
	// And not on a host that travels, or the warning becomes noise everyone
	// learns to skip.
	if strings.Contains(Generate(liveInput(liveIdx())), "loopback address") {
		t.Error("a routable host was flagged as loopback")
	}
}

// TestLivePreamble_declaresEveryExtensionItUses.
//
// The file runs in a DuckDB this repo does not control, which is why the
// preamble INSTALLs and LOADs what it reads through rather than assuming. The
// index leg reads event_timestamp AT TIME ZONE 'UTC' — that is ICU, and an
// undeclared dependency on it fails the same way a missing column does: a
// binder error, and no events view created at all.
func TestLivePreamble_declaresEveryExtensionItUses(t *testing.T) {
	out := Generate(liveInput(liveIdx()))
	for _, use := range []struct{ sql, ext string }{
		{`."binlog_events"`, "mysql"},
		{"AT TIME ZONE", "icu"},
	} {
		if !strings.Contains(out, use.sql) {
			continue
		}
		if !strings.Contains(out, "INSTALL "+use.ext) || !strings.Contains(out, "LOAD "+use.ext) {
			t.Errorf("the file uses %s but never declares the %s extension", use.sql, use.ext)
		}
	}
}

// eventsComment returns the events view's comment block, so an assertion about
// what the file SAYS cannot pass on a sentence somewhere else in it.
func eventsComment(out string) string {
	start := strings.Index(out, "-- events:")
	if start < 0 {
		return out
	}
	end := strings.Index(out[start:], "CREATE OR REPLACE VIEW")
	if end < 0 {
		return out[start:]
	}
	return out[start : start+end]
}

// livePreamble returns the block between the live setup title and the ATTACH
// that closes it, so an assertion about what the preamble SAYS cannot pass on a
// sentence somewhere else in the file.
//
// It returns the block as PROSE: comment markers stripped and the lines joined,
// so an assertion is about the sentence and not about where the copy happens to
// wrap. Asserting on raw bytes makes every reflow a test failure and quietly
// invites the next person to soften the sentence to fit a line.
func livePreamble(out string) string {
	start := strings.Index(out, "-- Live index setup")
	if start < 0 {
		return ""
	}
	block := out[start:]
	if end := strings.Index(block, "ATTACH '' AS"); end >= 0 {
		block = block[:end]
	}
	var words []string
	for _, line := range strings.Split(block, "\n") {
		words = append(words, strings.Fields(strings.TrimPrefix(strings.TrimSpace(line), "--"))...)
	}
	return strings.Join(words, " ")
}

// TestLivePreamble_saysTheReadLandsOnTheCaptureIndex is #1483.
//
// The cold leg opens Parquet on disk or in an object store; this one opens a
// connection to the index a running bintrail is writing captured events into.
// The file presented the two as halves of one view, and an operator who ran an
// analytical query over the hot leg had no signal that it lands on the server
// capture depends on.
func TestLivePreamble_saysTheReadLandsOnTheCaptureIndex(t *testing.T) {
	shapes := map[string]Input{
		"archives and index": liveInput(liveIdx()),
		"index alone":        func() Input { in := liveInput(liveIdx()); in.ArchiveSources = nil; return in }(),
	}
	for name, in := range shapes {
		t.Run(name, func(t *testing.T) {
			pre := livePreamble(Generate(in))
			for _, want := range []string{
				// Whose server this is. Without it the two legs read as
				// interchangeable halves of one view.
				"SHARED WITH CAPTURE:",
				"the index bintrail writes captured events",
				// Why a query here is never small.
				"full scan of binlog_events",
				// What it does to capture, which is the whole point.
				"competes with capture",
				// The way out. Naming only the cost and no remedy leaves the
				// reader with a warning they cannot act on, and the remedy has
				// to be one that WORKS: this same block says a filter on the
				// view does not reach the index, so "filter it down" would
				// contradict it. The direct read is the one that does.
				"\"binlog_events\" directly with your own WHERE",
				"read replica",
			} {
				if !strings.Contains(pre, want) {
					t.Errorf("the preamble does not say %q:\n%s", want, pre)
				}
			}
		})
	}
}

// TestLivePreamble_capturePriceIsTrueAfter1482: the daemon restarts on an index
// write deadline now instead of exiting, and only consoleapp/mainstream.go
// re-arms. Saying capture "stops" would overstate it for `bintrail-console
// watch`; saying it always recovers would overstate it for `bintrail stream`.
func TestLivePreamble_capturePriceIsTrueAfter1482(t *testing.T) {
	pre := livePreamble(Generate(liveInput(liveIdx())))
	for _, want := range []string{
		// The whole standalone class, not one member of it: `bintrail up` and
		// `bintrail-pg stream` reach indexer.InsertBatch through the same
		// unwrapped streamrun/pgstreamrun call, so naming only `bintrail
		// stream` lets those two operators read themselves out of it.
		"`bintrail stream`",
		"`bintrail up`",
		"`bintrail-pg stream`",
		// And it does not end until something restarts it. "capture falls
		// behind while that happens" framed a process that exits and stays
		// exited as a bounded dip.
		"stays down until something restarts it",
		"`bintrail-console watch` restarts from its last checkpoint",
	} {
		if !strings.Contains(pre, want) {
			t.Errorf("the preamble does not name %q, so it does not say which binary recovers:\n%s", want, pre)
		}
	}
}

// TestNoLiveIndex_noCaptureNote: the archives-only file attaches nothing and
// reads no index, so a warning about competing with capture there would be
// false. This is what keeps the note out of a block shared with the cold leg.
func TestNoLiveIndex_noCaptureNote(t *testing.T) {
	for name, out := range map[string]string{
		"archives only": Generate(liveInput(nil)),
		"GenerateViews": GenerateViews(liveInput(liveIdx())),
	} {
		if strings.Contains(out, "SHARED WITH CAPTURE") {
			t.Errorf("%s: warns about a capture index it never reads:\n%s", name, out)
		}
	}
}
