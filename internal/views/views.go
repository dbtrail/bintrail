// Package views generates DuckDB view definitions over a bintrail archive
// layout.
//
// The output is SQL TEXT and nothing else: this package never opens DuckDB, and
// `bintrail views` never executes what it prints. That is the whole design
// stance — the Parquet tier is an open layout the operator already owns, so the
// product's job is to hand them a correct schema for it, not to become the
// engine that reads it. Whatever DuckDB they already have (CLI, a notebook, an
// embedded process) runs the result, on their machine, with no result caps, no
// server and no new surface to secure.
//
// Everything here is a pure function of its Input, which is what makes the
// generated SQL golden-file testable and what keeps the discovery (index reads,
// baseline listing) in the command layer where it can be seen.
package views

import (
	"fmt"
	"net"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// BaselineTable names one table's Parquet file inside a baseline snapshot.
type BaselineTable struct {
	Schema string
	Table  string
	Path   string // local path or s3:// URL of the table's .parquet file
}

// LiveIndex names the index whose `binlog_events` the events view unions in as
// its hot leg, so a query is fresh to capture lag instead of to the archive
// retention window (#1480).
//
// The archives only hold partitions rotation has already archived. On a
// deployment measured for #1465 that left the most recent ~12 hours visible
// nowhere in the Parquet tier, which a reader of an archives-only view cannot
// distinguish from those rows not existing.
//
// It carries HOST, PORT, DATABASE and USER, which are configuration, and NEVER
// a password: this file is meant to be shared, and the header says it holds no
// credentials. The generated preamble emits the password slot empty for the
// operator to fill in their own session.
type LiveIndex struct {
	Host     string
	Port     int
	Database string
	User     string

	// BintrailID attributes the hot leg's rows, and is the ONLY signal that
	// they are attributed at all. Live binlog_events has no per-row source
	// column (the archives get theirs from the Hive path), so this can only be
	// filled when exactly one source was observed. Empty = the hot rows carry
	// NULL, and Attribution below says what was observed instead.
	//
	// One field, not a pair: an "it is attributed" flag alongside the value is
	// a pair that can desync, and the desynced state would emit an
	// attribution nobody established.
	BintrailID string

	// Attribution is consulted ONLY when BintrailID is empty, to say what was
	// observed. Its zero value is the undetermined one on purpose: a producer
	// that fills nothing must not make the file claim anything.
	Attribution LiveAttribution

	// TableColumns is the live binlog_events column set as OBSERVED on the
	// index. A column named here is selected; one that is absent is emitted as
	// NULL, because naming it would make DuckDB refuse the whole file with a
	// binder error and define no view at all.
	//
	// This is the hot leg's union_by_name: the cold leg tolerates archives
	// written before a column existed, and an index migrated to a different
	// point (the console sets EnsureSchema: false and never migrates registry
	// servers) needs exactly the same tolerance.
	//
	// Empty means NOT OBSERVED, and every column is named — the behaviour of a
	// producer that cannot probe. `bintrail views` always probes.
	TableColumns []string
}

// LiveAttribution records what the producer OBSERVED about which source the
// index serves, for the one line of the generated file that must not guess.
//
// The rule this type exists to keep is the same one Input.ArchiveDiscoveryFailed
// and Input.RegionAmbiguous keep: never state a cause the caller does not know.
// A single "unattributed" value collapsed four different observations —
// several sources, none registered, an unreadable list, a disagreement — into
// one sentence that was false for three of them.
//
// It names the OBSERVATION, never the inference. "No source is registered" is
// what the count reports; that this is a file-mode or legacy index is a guess
// about why, and it stays out of the artifact.
type LiveAttribution int

const (
	// AttributionUndetermined: nothing was established. The zero value, so an
	// Input that fills none of this says so rather than asserting.
	AttributionUndetermined LiveAttribution = iota
	// AttributionMultiSource: more than one source is registered, and a live
	// row carries no identity of its own to tell them apart by.
	AttributionMultiSource
	// AttributionUnregistered: the index registers no source id — a zero count,
	// a NULL id, or no such table at all. Every one of those is "there is no id
	// to attribute a row to", which is what the file says.
	AttributionUnregistered
)

// Input is everything Generate needs. The command layer resolves it; nothing in
// this package performs IO.
type Input struct {
	// GeneratedAt stamps the header. Passed in rather than read from the clock
	// so the output is reproducible under test.
	GeneratedAt time.Time
	// Version is the bintrail build that generated the file, for the header.
	Version string

	// ArchiveSources are the archive base paths discovered from archive_state
	// (each ending in the `bintrail_id=<id>` segment), local or s3://.
	ArchiveSources []string
	// PortableRouting is true when ArchiveSources were resolved by
	// query.PortableArchiveSources (S3 wherever the registry has one), which
	// is the only case in which the header may state that rule. It names the
	// ROUTING, not the provenance: the console SQL panel's sources also come
	// out of the registry, local-first, and leave this false. An operator who
	// named the roots with --archive-dir/--archive-s3 gets exactly what they
	// named, both of them if they passed both (#1456).
	PortableRouting bool
	// ArchiveDiscoveryFailed is set when the registry could not be read at
	// all. The file then says so instead of "none registered", which would
	// state a cause the caller does not know. A bool, not the error: the
	// error text names the index host and the DB user (a dial error, a 1142),
	// and this file is meant to be shared. The text goes to the console log,
	// and to the 502 body when there is no baseline half to serve; `bintrail
	// views` fails the command instead of setting this.
	ArchiveDiscoveryFailed bool
	// ArchiveRegion pins REGION in the S3 secret. Empty = let the credential
	// chain resolve it, which is what every other bintrail S3 read does by
	// default.
	ArchiveRegion string

	// RegionAmbiguous is set when two buckets were each DETECTED in a
	// different region, so ArchiveRegion was left empty rather than pinned to
	// one of them: one secret and one s3_region cannot describe two. The file
	// states that as a fact, so the producer must not set it for a bucket that
	// merely could not be asked — an undetectable bucket is not evidence that
	// the regions differ, and claiming it would send the reader off to split a
	// file that is fine. Same rule as ArchiveDiscoveryFailed above: never state
	// a cause the caller does not know.
	RegionAmbiguous bool
	// S3Endpoint is the S3-compatible store the paths live in, when the
	// generating process was configured with one (#1454). It is a location,
	// not a credential, and belongs in the file: a reader on another machine
	// has no other way to learn that s3:// here does not mean AWS.
	S3Endpoint storage.S3Endpoint

	// BaselineSource is the root the snapshot below was discovered under, for
	// the header. BaselineSnapshot is that snapshot's timestamp.
	BaselineSource   string
	BaselineSnapshot time.Time
	// Baselines are the tables of the NEWEST discoverable snapshot only. An
	// older snapshot's rows are a different point in time, and a view union-ing
	// two of them would silently mix states that never coexisted.
	Baselines []BaselineTable

	// LiveIndex, when set, adds the hot leg to the events view. Nil keeps the
	// archives-only view, which is what `bintrail views` emitted before #1480
	// and what a file generated with no reachable index must still emit.
	LiveIndex *LiveIndex

	// LiveLegUnavailable is set by a producer that has no way to offer the hot
	// leg at all, so the archives-only note does not send its reader to a flag
	// that surface cannot pass. The console download is that producer: it has
	// no --include-live, and telling an operator to "regenerate" from a page
	// served BY the index gets them a byte-identical file.
	LiveLegUnavailable bool

	// ExcludeEventColumns drops the named columns (matched case-insensitively
	// against archive.BinlogEventColumns) from the `events` view. Purely
	// mechanical here — the views package names no policy. The console SQL panel
	// (#1177) uses it to withhold the paid forensics columns (connection_id,
	// query_text, query_hash) it must not serve, the same set eventDTO omits.
	// Empty for the downloadable `bintrail views` file, which describes the
	// operator's OWN Parquet in full.
	ExcludeEventColumns []string
}

// Generate renders the complete .sql file: the explanatory header, the S3
// credential preamble when needed, and the view definitions. This is the
// artifact an operator downloads and runs in their OWN DuckDB, so the preamble
// creates a credential_chain secret and INSTALLs httpfs inline.
func Generate(in Input) string {
	var b strings.Builder
	writeHeader(&b, in)
	if in.NeedsS3() {
		region := in.ArchiveRegion
		if in.RegionAmbiguous {
			// Enforced here, not only trusted from the producer: the file
			// STATES that no region is pinned, so emitting one anyway would
			// make the artifact contradict itself in the one place a reader
			// looks to understand why their read failed.
			region = ""
		}
		writeS3Preamble(&b, region, in.S3Endpoint, in.RegionAmbiguous)
	}
	if in.LiveIndex != nil {
		writeLivePreamble(&b, in.LiveIndex)
	}
	writeEventsView(&b, in)
	writeStateViews(&b, in)
	return b.String()
}

// GenerateViews renders ONLY the view definitions — no header, no S3 preamble.
// It is for a caller that runs the views in a DuckDB session it already set up:
// bintrail's own S3 credential wiring (duckdbutil.EnableS3CredentialChain) is
// best-effort and tolerates an unresolved credential chain, whereas the
// download preamble's `CREATE SECRET` aborts the whole script when no
// credentials resolve. The console SQL panel (#1177) uses this so its session
// setup does not hinge on the human-facing preamble.
func GenerateViews(in Input) string {
	var b strings.Builder
	// The hot leg is dropped here rather than documented as unsupported. This
	// entry point emits no preamble, so it emits no ATTACH, so a two-leg view
	// rendered through it would reference a catalog that does not exist and
	// fail at CREATE VIEW. Nilling it makes that divergence impossible instead
	// of leaving Generate and GenerateViews with different preconditions on
	// the same Input; the caller gets the archives-only view, which is what
	// this entry point can actually back.
	in.LiveIndex = nil
	writeEventsView(&b, in)
	writeStateViews(&b, in)
	return b.String()
}

// NeedsS3 reports whether the rendered file will read any s3:// path. Callers
// use it to decide whether a broken S3 endpoint configuration is worth
// refusing over: a layout that is entirely local reads nothing through httpfs,
// so failing it on an unrelated environment variable blocks a render that
// would have been correct.
func (in Input) NeedsS3() bool {
	for _, s := range in.ArchiveSources {
		if isS3(s) {
			return true
		}
	}
	for _, t := range in.Baselines {
		if isS3(t.Path) {
			return true
		}
	}
	return false
}

func isS3(p string) bool { return strings.HasPrefix(p, "s3://") }

func writeHeader(b *strings.Builder, in Input) {
	fmt.Fprintf(b, "-- DuckDB views over a bintrail archive layout.\n")
	fmt.Fprintf(b, "-- Generated by bintrail %s at %s.\n",
		orUnknown(in.Version), in.GeneratedAt.UTC().Format(time.RFC3339))
	b.WriteString("--\n")
	b.WriteString("-- THIS FILE IS A SNAPSHOT OF THE LAYOUT, NOT A LIVE BINDING. The globs below\n")
	b.WriteString("-- keep picking up newly rotated partitions on their own, but the baseline\n")
	b.WriteString("-- state views point at ONE snapshot. Re-run `bintrail views` (or download the\n")
	b.WriteString("-- file again from the console) after taking or refreshing a baseline, and\n")
	b.WriteString("-- whenever archive sources are added or removed.\n")
	b.WriteString("--\n")
	b.WriteString("-- Nothing here writes: every view is a read over Parquet files you already own.\n")
	b.WriteString("--\n")

	// The path choice is stated where the paths are listed, and only when a
	// choice was made over a real listing: portable routing names an archive
	// registered with both a local path and an S3 location by the S3 one,
	// because this file is meant to run on a machine that is not the one the
	// local path belongs to (#1456). Explicitly named roots are listed as
	// named, and a failed read lists nothing, so neither gets the sentence.
	// Baseline state views are out of it on purpose: they point wherever the
	// baseline root points.
	if in.PortableRouting && !in.ArchiveDiscoveryFailed {
		b.WriteString("-- Archive sources (an archive registered with both a local path and an S3\n")
		b.WriteString("-- location is listed by its S3 location, so those reads work from another\n")
		b.WriteString("-- machine; a local path below means the registry holds no S3 location this\n")
		b.WriteString("-- file can use):\n")
	} else {
		b.WriteString("-- Archive sources:\n")
	}
	switch {
	case in.ArchiveDiscoveryFailed:
		b.WriteString("--   (could not be read from archive_state; the console log has the error)\n")
	case len(in.ArchiveSources) == 0:
		b.WriteString("--   (none registered in archive_state: no rotated partitions have been archived yet)\n")
	}
	for _, s := range in.ArchiveSources {
		fmt.Fprintf(b, "--   %s\n", s)
	}
	b.WriteString("-- Baseline snapshot:\n")
	switch {
	case len(in.Baselines) == 0 && in.BaselineSource == "":
		b.WriteString("--   (no baseline source given: pass --baseline-dir or --baseline-s3)\n")
	case len(in.Baselines) == 0:
		fmt.Fprintf(b, "--   (none discoverable under %s)\n", in.BaselineSource)
	default:
		fmt.Fprintf(b, "--   %s at %s (%d table(s))\n",
			in.BaselineSource, in.BaselineSnapshot.UTC().Format(time.RFC3339), len(in.Baselines))
	}
	b.WriteString("\n")
}

func orUnknown(s string) string {
	if strings.TrimSpace(s) == "" {
		return "(unknown version)"
	}
	return s
}

// writeS3Preamble mirrors what duckdbutil.EnableS3CredentialChain configures on
// every DuckDB session bintrail opens, so a query that works through `bintrail
// query --archive-s3` works the same way pasted into a bare DuckDB.
//
// The credential-chain form is deliberate and is the reason this file is safe to
// paste into a notebook or share with a colleague: it resolves whatever the
// environment already has (instance role, SSO profile, env vars) and puts NO key
// material in the generated text.
func writeS3Preamble(b *strings.Builder, region string, ep storage.S3Endpoint, ambiguousRegion bool) {
	b.WriteString("-- S3 setup, mirroring what bintrail's own DuckDB sessions configure.\n")
	if ep.Set() {
		fmt.Fprintf(b, "-- s3:// paths here live in an S3-compatible store at %s, not in AWS.\n", ep.URL)
	}
	if ambiguousRegion {
		b.WriteString("-- No region is pinned below: two of the buckets this file reads were\n")
		b.WriteString("-- each detected in a DIFFERENT region, and one secret cannot name two.\n")
		b.WriteString("-- Your own AWS configuration resolves one of them; the other rejects a\n")
		b.WriteString("-- request signed for it. Split those reads into one file per region, or\n")
		b.WriteString("-- add a second scoped secret for the odd bucket out.\n")
	}
	b.WriteString("INSTALL httpfs; LOAD httpfs;\n")
	b.WriteString("INSTALL aws; LOAD aws;\n")
	// Ahead of the secret, and not only inside it: an interactive DuckDB
	// CONTINUES past a failed statement, so a session where the credential
	// chain resolves nothing (an offline laptop, expired SSO, no aws
	// extension) would skip the secret and read AWS with whatever ambient keys
	// it finds — the same hole the daemon's own routing closes. The two
	// mechanisms are independent: a secret's ENDPOINT does not set these, and
	// these do not populate the secret.
	//
	// From duckdbutil's own list, not a copy of it: the copy drifted once
	// already, losing s3_region.
	for _, stmt := range duckdbutil.S3SettingStatements(region, ep) {
		fmt.Fprintf(b, "%s;\n", stmt)
	}
	secret := "CREATE OR REPLACE SECRET bintrail_s3_chain (TYPE s3, PROVIDER credential_chain" +
		duckdbutil.S3SecretClauses(region, ep) + ");\n"
	b.WriteString(secret)
	// The secret is TEMPORARY on purpose, and the file says so where the
	// operator will look when a reopened database file cannot read S3. The
	// PERSISTENT form is not offered: DuckDB resolves the credential chain at
	// creation and writes the resulting keys to ~/.duckdb/stored_secrets, which
	// would make a file that promises "no credentials" plant them on disk
	// (#1456).
	b.WriteString("-- This secret lives only in this DuckDB session. Views persist in a database\n")
	b.WriteString("-- file; secrets do not. Reopening that file later and querying S3 fails with\n")
	b.WriteString("-- \"No credentials are provided\": run this file again in every session that\n")
	b.WriteString("-- reads S3 (`.read views.sql`, or `duckdb -init views.sql your.db`).\n")
	b.WriteString("-- Do not make it PERSISTENT: DuckDB would resolve your credential chain now\n")
	b.WriteString("-- and store the resulting keys on disk.\n")
	b.WriteString("-- No credentials appear in this file by design. If the credential chain is not\n")
	b.WriteString("-- available where you run this, replace the secret above with explicit keys:\n")
	b.WriteString("--   CREATE OR REPLACE SECRET bintrail_s3_chain (\n")
	if ep.Set() {
		b.WriteString("--     TYPE s3, KEY_ID '…', SECRET '…', REGION '…',\n")
		fmt.Fprintf(b, "--     ENDPOINT %s, URL_STYLE %s, USE_SSL %t);\n",
			sqlString(ep.Host()), sqlString(map[bool]string{true: "path", false: "vhost"}[ep.PathStyle]), ep.UseSSL())
	} else {
		b.WriteString("--     TYPE s3, KEY_ID '…', SECRET '…', REGION '…');\n")
	}
	b.WriteString("\n")
}

// eventTypeCase renders the numeric event_type as its name. The mapping is
// event.EventInsert/Update/Delete; an unknown code renders as its own number
// rather than NULL, so a future event type shows up as an unfamiliar value
// instead of silently disappearing from a filtered query.
const eventTypeCase = "CASE \"event_type\"\n" +
	"      WHEN 1 THEN 'INSERT'\n" +
	"      WHEN 2 THEN 'UPDATE'\n" +
	"      WHEN 3 THEN 'DELETE'\n" +
	"      ELSE CAST(\"event_type\" AS VARCHAR)\n" +
	"    END AS \"event_type\""

// writeEventsView emits the union view over every archived partition.
//
// The projected column list is derived from archive.BinlogEventColumns — the
// same slice the archiver writes with — so a column added or removed there
// changes this output and breaks the golden test, rather than leaving the
// generated schema quietly behind the files it describes.
// liveAttachAlias is the DuckDB catalog name the index is attached under. It is
// deliberately not "index": that is close enough to a reserved word in enough
// dialects that an operator pasting these views elsewhere hits an avoidable
// parse error, and a distinctive name makes the hot leg obvious in a plan.
const liveAttachAlias = "bintrail_live"

// liveSecretName is the DuckDB secret the ATTACH reads credentials from.
const liveSecretName = "bintrail_index"

// writeLivePreamble emits the mysql extension setup for the hot leg.
//
// The password slot is left EMPTY on purpose. Everything else here (host, port,
// database, user) is configuration that a reader on another machine genuinely
// needs, but this file is meant to be shared and its header says it carries no
// credentials — so the one field that is a credential is the one the operator
// fills in their own session. Same stance as the S3 preamble, which reaches for
// a credential chain rather than writing keys into the file; MySQL has no such
// chain, so the slot is explicit instead.
func writeLivePreamble(b *strings.Builder, li *LiveIndex) {
	b.WriteString("-- Live index setup, for the hot leg of the events view.\n")
	b.WriteString("-- FILL IN THE PASSWORD BELOW before running: this file is shareable and\n")
	b.WriteString("-- carries none. Everything else is the index location, which is not secret.\n")
	b.WriteString("-- Read-only: nothing in this file writes, and the ATTACH enforces it.\n")
	b.WriteString("INSTALL mysql; LOAD mysql;\n")
	// icu is declared for the same reason mysql and httpfs are: this file runs
	// in a DuckDB nobody here controls, and it must not depend on an extension
	// that merely happens to be loaded. The index leg reads event_timestamp
	// AT TIME ZONE 'UTC', which is ICU's. Where ICU is built in, both
	// statements are a no-op that reports it already installed.
	b.WriteString("INSTALL icu; LOAD icu;\n")
	fmt.Fprintf(b, "CREATE OR REPLACE SECRET %s (\n", quoteIdent(liveSecretName))
	b.WriteString("    TYPE mysql,\n")
	fmt.Fprintf(b, "    HOST %s,\n", sqlString(li.Host))
	fmt.Fprintf(b, "    PORT %d,\n", li.Port)
	fmt.Fprintf(b, "    DATABASE %s,\n", sqlString(li.Database))
	fmt.Fprintf(b, "    USER %s,\n", sqlString(li.User))
	b.WriteString("    PASSWORD ''  -- <- your index password\n")
	b.WriteString(");\n")
	if isLoopbackHost(li.Host) {
		// Observed: the host IS a loopback address. NOT stated: why it is one
		// (an omitted address in the DSN, an SSH tunnel, a sidecar). The file
		// says what it can see, and what that costs a reader elsewhere.
		b.WriteString("-- HOST above is a loopback address, which names only the machine this file\n")
		b.WriteString("-- was generated on. Run this file somewhere else and the ATTACH resolves to\n")
		b.WriteString("-- whatever that machine runs on the same port. That may answer, with\n")
		b.WriteString("-- entirely plausible rows from a different index. Change HOST to a name that\n")
		b.WriteString("-- resolves from where you run this.\n")
	}
	fmt.Fprintf(b, "ATTACH '' AS %s (TYPE mysql, SECRET %s, READ_ONLY);\n\n",
		quoteIdent(liveAttachAlias), quoteIdent(liveSecretName))
}

// isLoopbackHost reports whether the generated ATTACH points at the generating
// machine itself. "localhost" is included by name: it is not an IP literal, but
// it resolves to the loopback everywhere it resolves at all.
func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

func writeEventsView(b *strings.Builder, in Input) {
	live := in.LiveIndex != nil
	// A failed discovery leaves no usable archive list whatever ArchiveSources
	// holds, so it decides the cold leg here rather than in a second switch.
	cold := !in.ArchiveDiscoveryFailed && len(in.ArchiveSources) > 0

	switch {
	case live && !cold:
		b.WriteString("-- events: every binlog event the index still holds.\n")
	case live:
		b.WriteString("-- events: every binlog event, from the archives and from the index.\n")
	default:
		b.WriteString("-- events: every archived binlog event, across all archive sources.\n")
	}

	if !cold && !live {
		if in.ArchiveDiscoveryFailed {
			// The header already names the failure; the body must not
			// contradict it with a cause nobody verified.
			b.WriteString("-- (skipped: archive_state could not be read; see the header)\n\n")
		} else {
			b.WriteString("-- (skipped: no archive sources are registered in archive_state)\n\n")
		}
		return
	}

	if cold {
		b.WriteString("--\n")
		b.WriteString("-- union_by_name is required, not cosmetic: archives written before a column\n")
		b.WriteString("-- existed simply lack it, and those files must read back with NULLs rather\n")
		b.WriteString("-- than failing the whole scan. A column absent from EVERY archived file is\n")
		b.WriteString("-- still an error: drop it from the SELECT if you hit that on an old archive.\n")
	}

	switch {
	case cold && live:
		b.WriteString("--\n")
		b.WriteString("-- Two legs: the Parquet for everything rotation has archived, the index for\n")
		b.WriteString("-- events it has not. A partition that has been archived but not yet dropped\n")
		b.WriteString("-- exists on BOTH sides, so the index leg excludes any event_id the archives\n")
		b.WriteString("-- already returned.\n")
		b.WriteString("--\n")
		b.WriteString("-- The ARCHIVES win that overlap, which is the one place this differs from\n")
		b.WriteString("-- bintrail's own merge in Go. There the two copies of an event carry the same\n")
		b.WriteString("-- fields and the winner is immaterial; here they do not. An archived row\n")
		b.WriteString("-- knows its bintrail_id, event_date and event_hour from its path, and an\n")
		b.WriteString("-- index row has to derive or forgo them. Letting the index win would\n")
		b.WriteString("-- replace a known source with NULL for every event in the overlap, and a\n")
		b.WriteString("-- WHERE bintrail_id = ... would then miss rows the archives hold.\n")
		writeLiveCostNote(b, true)
		b.WriteString("CREATE OR REPLACE VIEW \"events\" AS\n")
		b.WriteString("  WITH cold AS (\n")
		writeEventSelect(b, in, false, "    ")
		b.WriteString("\n  ), hot AS (\n")
		writeEventSelect(b, in, true, "    ")
		b.WriteString("\n  )\n")
		b.WriteString("  SELECT * FROM cold\n")
		b.WriteString("  UNION ALL BY NAME\n")
		b.WriteString("  SELECT * FROM hot\n")
		b.WriteString("   WHERE NOT EXISTS (SELECT 1 FROM cold WHERE cold.event_id = hot.event_id);\n\n")
	case live:
		b.WriteString("--\n")
		// "No archive source in this file", not "nothing has been archived":
		// this branch is also where a registry that could not be READ lands,
		// and the header is the one place that already distinguishes the two.
		b.WriteString("-- The index alone: this file names no archive source to read from, and the\n")
		b.WriteString("-- header above says why. It covers whatever the index still holds, and\n")
		b.WriteString("-- rotation dropping a partition removes those events from it. Archive and\n")
		b.WriteString("-- take a baseline before that matters, then regenerate this file.\n")
		writeLiveCostNote(b, false)
		b.WriteString("CREATE OR REPLACE VIEW \"events\" AS\n")
		writeEventSelect(b, in, true, "  ")
		b.WriteString(";\n\n")
	default:
		b.WriteString("--\n")
		b.WriteString("-- SCOPE: these are the ARCHIVED events only. Partitions rotation has not\n")
		b.WriteString("-- archived yet exist solely in the index, so the most recent window is\n")
		b.WriteString("-- absent here and reads as if nothing happened.\n")
		if in.LiveLegUnavailable {
			b.WriteString("-- This download covers the archives; it has no way to reach the index.\n")
			b.WriteString("-- To add a leg over the index, run `bintrail views --index-dsn ...\n")
			b.WriteString("-- --include-live` from a host that can connect to it.\n")
		} else {
			b.WriteString("-- Add a leg over the index by regenerating with --include-live:\n")
			b.WriteString("--   bintrail views --index-dsn ... --include-live\n")
		}
		b.WriteString("CREATE OR REPLACE VIEW \"events\" AS\n")
		writeEventSelect(b, in, false, "  ")
		b.WriteString(";\n\n")
	}
}

// writeLiveCostNote states what a query against the two-leg view actually
// reads, because the operator otherwise measures it and blames the view.
//
// Measured, not assumed: event_date and event_hour are DERIVED on the index leg
// (the index has no partition path), so a predicate on them is evaluated above
// that scan and cannot become a partition filter the way it does on Parquet.
func writeLiveCostNote(b *strings.Builder, withCold bool) {
	b.WriteString("--\n")
	b.WriteString("-- COST: a filter on this view does not become a filter on the index. The\n")
	b.WriteString("-- index leg derives event_date and event_hour from event_timestamp, so a\n")
	b.WriteString("-- predicate on them is applied after the rows are read")
	if withCold {
		b.WriteString(", and the anti-join\n-- needs every archived event_id regardless of what the query asked for")
	}
	b.WriteString(".\n")
	b.WriteString("-- Every query therefore streams the whole live binlog_events (row_before,\n")
	b.WriteString("-- row_after and query_text included)")
	if withCold {
		b.WriteString(", and the archive scan behind the\n-- anti-join is not pruned by your filter either")
	}
	b.WriteString(".\n")
	fmt.Fprintf(b, "-- For a narrow read of recent events, query %s.\"binlog_events\" directly\n",
		quoteIdent(liveAttachAlias))
	b.WriteString("-- with your own WHERE: that one does reach the index, which is indexed on\n")
	b.WriteString("-- event_timestamp and on schema_name/table_name.\n")
}

// writeEventSelect renders one leg of the events view. Both legs go through
// here so the derived columns (the event_type label, commit_time) cannot drift
// between them: two hand-written copies of one projection is how a UNION starts
// reporting different things for the same event depending on its age.
func writeEventSelect(b *strings.Builder, in Input, live bool, indent string) {
	exclude := make(map[string]bool, len(in.ExcludeEventColumns))
	for _, c := range in.ExcludeEventColumns {
		exclude[strings.ToLower(c)] = true
	}
	b.WriteString(indent + "SELECT\n")
	col := indent + "  "

	// The three hive_partitioning columns. The cold leg gets them synthesized
	// by DuckDB from the path; the hot leg has to produce them itself, because
	// live binlog_events carries no source column and no partition path.
	if live {
		id, note := liveSourceID(in)
		if id != "" {
			fmt.Fprintf(b, "%s%s AS \"bintrail_id\",\n", col, sqlString(id))
		} else {
			fmt.Fprintf(b, "%sNULL AS \"bintrail_id\", %s\n", col, note)
		}
		// Derived so the hot leg filters on the same predicates as the cold
		// one. These mirror the Hive path rotation writes, which is UTC.
		//
		// Run on the NAIVE column, before the UTC cast below: the index stores
		// DATETIME in UTC and rotation names the path from that same value, so
		// these strftimes agree with the archives exactly as they stand. The
		// CAST to DATE is what keeps event_date a DATE in the union — DuckDB
		// types the archives' hive column DATE and strftime returns VARCHAR, and
		// the widened VARCHAR silently breaks date_trunc, `- INTERVAL 1 DAY` and
		// any comparison against current_date on the SAME file regenerated with
		// this flag. event_hour needs no cast: the hive column reads back
		// VARCHAR ('03'), which is what strftime already produces.
		b.WriteString(col + "CAST(strftime(\"event_timestamp\", '%Y-%m-%d') AS DATE) AS \"event_date\",\n")
		b.WriteString(col + "strftime(\"event_timestamp\", '%H') AS \"event_hour\",\n")
	} else {
		b.WriteString(col + "\"bintrail_id\", \"event_date\", \"event_hour\",\n")
	}

	has := liveColumnSet(in, live)
	for _, c := range archive.BinlogEventColumns {
		if exclude[strings.ToLower(c.Name)] {
			continue
		}
		if has != nil && !has[strings.ToLower(c.Name)] {
			// The hot leg's union_by_name. This index does not have the column
			// (it was migrated to an older point than this build's schema);
			// naming it would fail the whole file with a binder error and
			// define no view at all, so it reads back NULL the same way an old
			// archive's missing column does.
			writeMissingLiveColumn(b, col, c.Name)
			continue
		}
		switch c.Name {
		case "event_id":
			// Cast on BOTH legs. The index column is BIGINT UNSIGNED and the
			// Parquet one is signed, and DuckDB reconciles that pair by
			// widening the union to HUGEINT — a 128-bit surprise in every
			// downstream join for a value that never leaves BIGINT range.
			b.WriteString(col + "CAST(\"event_id\" AS BIGINT) AS \"event_id\",\n")
		case "event_timestamp":
			if live {
				// The one cast that decides whether this view answers the
				// question it was asked. DuckDB reads the archives' Parquet
				// timestamp as TIMESTAMP WITH TIME ZONE and the index's
				// DATETIME as a naive TIMESTAMP; UNION reconciles that pair by
				// interpreting the naive value in the READER's session
				// timezone. The index stores UTC, so on any non-UTC session
				// every hot row lands off by that offset — the same event
				// returning two different instants depending on which leg
				// served it, and a UTC range filter returning nothing for
				// events that are in it. Correct on a UTC box, which is how it
				// goes unnoticed.
				b.WriteString(col + "\"event_timestamp\" AT TIME ZONE 'UTC' AS \"event_timestamp\",\n")
			} else {
				b.WriteString(col + "\"event_timestamp\",\n")
			}
		case "event_type":
			// INTEGER, not the narrower TINYINT the index stores it in: the
			// archives read back INTEGER, so this is also the type the
			// archives-only file has always produced. A narrowing cast would
			// make an unknown future event type fail the whole query, which
			// contradicts the ELSE branch below, whose entire purpose is that
			// such a code shows up as an unfamiliar value instead.
			b.WriteString(col + "CAST(\"event_type\" AS INTEGER) AS \"event_type_code\",\n")
			b.WriteString(col + eventTypeCase + ",\n")
		case "schema_version":
			b.WriteString(col + "CAST(\"schema_version\" AS INTEGER) AS \"schema_version\",\n")
		case "commit_ts_us":
			b.WriteString(col + "\"commit_ts_us\",\n")
			// Stored as epoch MICROSECONDS (#18); make the usable form the one
			// that reads like a timestamp next to event_timestamp.
			// The CAST is load-bearing: commit_ts_us is UNSIGNED (UBIGINT in the
			// Parquet schema) and make_timestamp only binds BIGINT, so the
			// uncast form is a binder error that would reach the operator's
			// DuckDB, not ours. Epoch microseconds stay far below 2^63 (year
			// 294247), so the narrowing cannot lose a real value.
			b.WriteString(col + "CASE WHEN \"commit_ts_us\" IS NULL THEN NULL\n")
			b.WriteString(col + "     ELSE make_timestamp(CAST(\"commit_ts_us\" AS BIGINT)) END AS \"commit_time\",\n")
		default:
			fmt.Fprintf(b, "%s%s,\n", col, quoteIdent(c.Name))
		}
	}
	trimTrailingComma(b)

	if live {
		// Only the columns above: live binlog_events also has pk_hash, a
		// generated column the archives do not carry, and selecting it would
		// give the two legs different shapes.
		fmt.Fprintf(b, "\n%sFROM %s.\"binlog_events\"", indent, quoteIdent(liveAttachAlias))
		return
	}
	b.WriteString("\n" + indent + "FROM read_parquet(\n")
	b.WriteString(indent + "  [\n")
	for i, src := range in.ArchiveSources {
		sep := ","
		if i == len(in.ArchiveSources)-1 {
			sep = ""
		}
		fmt.Fprintf(b, "%s    %s%s\n", indent, sqlString(archiveGlob(src)), sep)
	}
	b.WriteString(indent + "  ],\n")
	b.WriteString(indent + "  hive_partitioning = true,\n")
	b.WriteString(indent + "  union_by_name = true\n")
	b.WriteString(indent + ")")
}

// liveSourceID decides what the hot leg may claim about its rows' source, and
// when it may claim nothing, the end-of-line comment that says WHY — stating
// only what was observed, never a cause nobody established.
//
// It returns ("", note) far more often than it returns an id. That is the
// point: one sentence about several sources used to cover a file-mode index
// (which registers none and serves exactly one), an index too old to have the
// table, an account without SELECT on it, and a dropped connection.
func liveSourceID(in Input) (id, note string) {
	li := in.LiveIndex
	if li.BintrailID == "" {
		switch li.Attribution {
		case AttributionMultiSource:
			return "", "-- more than one source is registered in this index, and an index row carries none of its own"
		case AttributionUnregistered:
			return "", "-- this index registers no source id, so there is none to attribute an index row to"
		default:
			return "", "-- the index's registered sources could not be read, so these rows are left unattributed"
		}
	}
	// Cross-check against the identity the COLD leg will actually carry. The
	// two come from unrelated places — this one from bintrail_servers, the
	// other from the `bintrail_id=` path segment, which `rotate` takes verbatim
	// from --bintrail-id and never validates against the registry. When they
	// disagree, one source appears in the view as two servers and a
	// WHERE bintrail_id = ... returns half its rows, so assert neither.
	archived := archiveIDs(in.ArchiveSources)
	if len(archived) > 0 && !slices.Contains(archived, li.BintrailID) {
		return "", fmt.Sprintf(
			"-- the index reports source %s, these archives are written under %s: unattributed rather than assert either",
			commentSafe(li.BintrailID), commentSafe(strings.Join(archived, ", ")))
	}
	return li.BintrailID, ""
}

// archiveIDs pulls the `bintrail_id=<id>` identity out of each archive base
// path — the same segment archive.ParseArchivePath reads back, and the only
// place the cold leg's bintrail_id comes from.
func archiveIDs(sources []string) []string {
	var ids []string
	for _, s := range sources {
		const marker = "bintrail_id="
		i := strings.LastIndex(s, marker)
		if i < 0 {
			continue
		}
		id := strings.TrimRight(s[i+len(marker):], "/")
		if id = strings.SplitN(id, "/", 2)[0]; id != "" && !slices.Contains(ids, id) {
			ids = append(ids, id)
		}
	}
	return ids
}

// commentSafe keeps interpolated text inside the SQL comment it is written in.
// A bintrail_id is an operator-chosen string on the archive side, and a newline
// in one would end the comment and make everything after it statement text.
func commentSafe(s string) string {
	return strings.NewReplacer("\n", " ", "\r", " ").Replace(s)
}

// liveColumnSet returns the lookup for which columns the hot leg may name, or
// nil when every column is nameable — the cold leg (whose union_by_name handles
// its own absences) and a producer that observed no column set.
func liveColumnSet(in Input, live bool) map[string]bool {
	if !live || len(in.LiveIndex.TableColumns) == 0 {
		return nil
	}
	has := make(map[string]bool, len(in.LiveIndex.TableColumns))
	for _, c := range in.LiveIndex.TableColumns {
		has[strings.ToLower(c)] = true
	}
	return has
}

// writeMissingLiveColumn emits the NULL placeholders for one column this index
// does not have. It follows the projection's own shape: the two columns that
// render as two outputs must produce two NULLs, or the legs stop lining up.
func writeMissingLiveColumn(b *strings.Builder, col, name string) {
	const why = " -- not a column of this index's binlog_events\n"
	switch name {
	case "event_type":
		b.WriteString(col + "NULL AS \"event_type_code\"," + why)
		b.WriteString(col + "NULL AS \"event_type\",\n")
	case "commit_ts_us":
		b.WriteString(col + "NULL AS \"commit_ts_us\"," + why)
		b.WriteString(col + "NULL AS \"commit_time\",\n")
	default:
		fmt.Fprintf(b, "%sNULL AS %s,%s", col, quoteIdent(name), why)
	}
}

// archiveGlob turns an archive base path (…/bintrail_id=<id>) into the glob that
// matches every partition file under it — the same three-level Hive layout
// rotation writes and archive.ParseArchivePath reads back.
func archiveGlob(base string) string {
	return strings.TrimRight(base, "/") + "/event_date=*/event_hour=*/*.parquet"
}

// writeStateViews emits one view per table in the newest baseline snapshot.
func writeStateViews(b *strings.Builder, in Input) {
	b.WriteString("-- state_<schema>_<table>: each table's full contents as of the baseline snapshot.\n")
	b.WriteString("--\n")
	b.WriteString("-- These are the SNAPSHOT's rows, not the table's current state: changes after\n")
	b.WriteString("-- the snapshot live in `events` above. To materialize a later point in time,\n")
	b.WriteString("-- use `bintrail reconstruct`. Folding the deltas back onto a baseline is what\n")
	b.WriteString("-- that command does, and it is not expressible as a view.\n")
	if len(in.Baselines) == 0 {
		b.WriteString("-- (skipped: no baseline snapshot was discovered)\n")
		return
	}

	tables := append([]BaselineTable(nil), in.Baselines...)
	sort.Slice(tables, func(i, j int) bool {
		if tables[i].Schema != tables[j].Schema {
			return tables[i].Schema < tables[j].Schema
		}
		return tables[i].Table < tables[j].Table
	})

	used := map[string]bool{}
	for _, t := range tables {
		name := stateViewName(t.Schema, t.Table, used)
		fmt.Fprintf(b, "CREATE OR REPLACE VIEW %s AS\n", quoteIdent(name))
		fmt.Fprintf(b, "  SELECT * FROM read_parquet(%s);\n", sqlString(t.Path))
	}
	b.WriteString("\n")
}

// stateViewName builds the view identifier for a table and guarantees it is
// unique within the file.
//
// Sanitizing collapses distinct tables onto one name — `a_b`.`c` and `a`.`b_c`
// both want state_a_b_c — and since every statement is CREATE OR REPLACE, a
// collision would silently leave only the last one, with the earlier table's
// view pointing at the wrong Parquet file. Suffixing is not pretty; a view that
// reads someone else's table is worse.
func stateViewName(schema, table string, used map[string]bool) string {
	base := "state_" + sanitizeIdent(schema) + "_" + sanitizeIdent(table)
	name := base
	for i := 2; used[name]; i++ {
		name = fmt.Sprintf("%s_%d", base, i)
	}
	used[name] = true
	return name
}

// sanitizeIdent reduces a MySQL identifier to a bare word so the generated view
// name stays typeable without quoting in an interactive DuckDB session.
func sanitizeIdent(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + ('a' - 'A'))
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// quoteIdent renders a SQL identifier with double quotes, doubling any embedded
// quote.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// sqlString renders a SQL string literal, doubling any embedded quote. Every
// path this package interpolates goes through it — a bucket or directory name
// containing an apostrophe is unusual but entirely legal, and the generated file
// is meant to be executed.
func sqlString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// trimTrailingComma removes the "," a projection loop just wrote, so the last
// column does not need a special case at every call site.
func trimTrailingComma(b *strings.Builder) {
	s := b.String()
	if strings.HasSuffix(s, ",\n") {
		b.Reset()
		b.WriteString(strings.TrimSuffix(s, ",\n"))
	}
}
