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
		b.WriteString("--   (none registered in archive_state — no rotated partitions have been archived yet)\n")
	}
	for _, s := range in.ArchiveSources {
		fmt.Fprintf(b, "--   %s\n", s)
	}
	b.WriteString("-- Baseline snapshot:\n")
	switch {
	case len(in.Baselines) == 0 && in.BaselineSource == "":
		b.WriteString("--   (no baseline source given — pass --baseline-dir or --baseline-s3)\n")
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
func writeEventsView(b *strings.Builder, in Input) {
	sources := in.ArchiveSources
	exclude := make(map[string]bool, len(in.ExcludeEventColumns))
	for _, c := range in.ExcludeEventColumns {
		exclude[strings.ToLower(c)] = true
	}
	b.WriteString("-- events: every archived binlog event, across all archive sources.\n")
	switch {
	case in.ArchiveDiscoveryFailed:
		// The header already names the failure; the body must not contradict
		// it with a cause nobody verified.
		b.WriteString("-- (skipped: archive_state could not be read; see the header)\n\n")
		return
	case len(sources) == 0:
		b.WriteString("-- (skipped: no archive sources are registered in archive_state)\n\n")
		return
	}
	b.WriteString("--\n")
	b.WriteString("-- union_by_name is required, not cosmetic: archives written before a column\n")
	b.WriteString("-- existed simply lack it, and those files must read back with NULLs rather\n")
	b.WriteString("-- than failing the whole scan. A column absent from EVERY archived file is\n")
	b.WriteString("-- still an error — drop it from the SELECT if you hit that on an old archive.\n")
	b.WriteString("CREATE OR REPLACE VIEW \"events\" AS\n")
	b.WriteString("  SELECT\n")
	// The three hive_partitioning columns, synthesized by DuckDB from the path.
	// bintrail_id is the one that matters: it is what keeps two servers' events
	// distinguishable inside a single view.
	b.WriteString("    \"bintrail_id\", \"event_date\", \"event_hour\",\n")
	for _, col := range archive.BinlogEventColumns {
		if exclude[strings.ToLower(col.Name)] {
			continue
		}
		switch col.Name {
		case "event_type":
			b.WriteString("    \"event_type\" AS \"event_type_code\",\n")
			b.WriteString("    " + eventTypeCase + ",\n")
		case "commit_ts_us":
			b.WriteString("    \"commit_ts_us\",\n")
			// Stored as epoch MICROSECONDS (#18); make the usable form the one
			// that reads like a timestamp next to event_timestamp.
			// The CAST is load-bearing: commit_ts_us is UNSIGNED (UBIGINT in the
			// Parquet schema) and make_timestamp only binds BIGINT, so the
			// uncast form is a binder error that would reach the operator's
			// DuckDB, not ours. Epoch microseconds stay far below 2^63 (year
			// 294247), so the narrowing cannot lose a real value.
			b.WriteString("    CASE WHEN \"commit_ts_us\" IS NULL THEN NULL\n")
			b.WriteString("         ELSE make_timestamp(CAST(\"commit_ts_us\" AS BIGINT)) END AS \"commit_time\",\n")
		default:
			fmt.Fprintf(b, "    %s,\n", quoteIdent(col.Name))
		}
	}
	trimTrailingComma(b)
	b.WriteString("\n  FROM read_parquet(\n")
	b.WriteString("    [\n")
	for i, s := range sources {
		sep := ","
		if i == len(sources)-1 {
			sep = ""
		}
		fmt.Fprintf(b, "      %s%s\n", sqlString(archiveGlob(s)), sep)
	}
	b.WriteString("    ],\n")
	b.WriteString("    hive_partitioning = true,\n")
	b.WriteString("    union_by_name = true\n")
	b.WriteString("  );\n\n")
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
	b.WriteString("-- use `bintrail reconstruct` — folding the deltas back onto a baseline is what\n")
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
