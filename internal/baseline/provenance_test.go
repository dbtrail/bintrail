package baseline

import (
	"testing"
	"time"
)

func ts(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

func TestProvenanceOf(t *testing.T) {
	const snap = "2026-06-10T12:00:00Z"
	older := ts("2026-06-03T12:00:00Z")

	cases := []struct {
		name     string
		dir      time.Time
		md       DumpMetadata
		want     string
		from     time.Time
		fromPath string
	}{
		{
			name: "dump stamps itself",
			dir:  ts(snap),
			md:   DumpMetadata{Producer: ProducerDump, SnapshotTimestamp: ts(snap), MydumperFormat: "csv"},
			want: ProducedByDump,
		},
		{
			name: "fold stamps itself and names its ancestor",
			dir:  ts(snap),
			md: DumpMetadata{Producer: ProducerReconstruct, SnapshotTimestamp: ts(snap),
				DerivedFrom: older, DerivedFromPath: "/b/2026-06-03T12-00-00Z/shop/orders.parquet"},
			// A fold DOES name its ancestor's file, and it is the same snapshot
			// From names. That pairing is what makes the carried case below a
			// real assertion rather than "nothing is ever set".
			want:     ProducedByFold,
			from:     older,
			fromPath: "/b/2026-06-03T12-00-00Z/shop/orders.parquet",
		},
		{
			// The case the footer CANNOT be stamped for: carry-forward hard
			// links the previous file, so its footer is the ancestor's, and
			// rewriting it would edit the older snapshot through the same
			// inode. The disagreement between the footer's instant and the
			// directory it now sits in is the only honest signal.
			name: "a file whose footer predates its directory was carried forward",
			dir:  ts(snap),
			md: DumpMetadata{Producer: ProducerReconstruct, SnapshotTimestamp: older,
				DerivedFromPath: "/b/2026-05-27T12-00-00Z/shop/orders.parquet"},
			want: ProducedByCarriedForward,
			from: older,
		},
		{
			// And it wins over the producer key, which for a carried file is
			// the ancestor's and would name the wrong operation for THIS
			// snapshot. A carried dump is the same story.
			name: "a carried DUMP is still carried, not a dump",
			dir:  ts(snap),
			md:   DumpMetadata{Producer: ProducerDump, SnapshotTimestamp: older, MydumperFormat: "csv"},
			want: ProducedByCarriedForward,
			from: older,
		},
		{
			name: "a legacy mysql dump is dated by its mydumper format",
			dir:  ts(snap),
			md:   DumpMetadata{SnapshotTimestamp: ts(snap), MydumperFormat: "csv"},
			want: ProducedByDump,
		},
		{
			name: "a legacy postgres dump is dated by its WAL floor",
			dir:  ts(snap),
			md:   DumpMetadata{SnapshotTimestamp: ts(snap), LSN: 42},
			want: ProducedByDump,
		},
		{
			// Refused rather than guessed. An old fold carries no producer key
			// either, so absence names nothing.
			name: "a snapshot with no signal at all is unknown",
			dir:  ts(snap),
			md:   DumpMetadata{SnapshotTimestamp: ts(snap)},
			want: ProducedByUnknown,
		},
		{
			// A footer with no timestamp cannot be compared, so the carried
			// check must not fire on the zero value and call every such file
			// carried from year zero.
			name: "a footer with no timestamp falls through to the producer",
			dir:  ts(snap),
			md:   DumpMetadata{Producer: ProducerReconstruct},
			want: ProducedByFold,
		},
		{
			// Same, from the other side: a caller that does not know the
			// snapshot's directory time must not have every file called
			// carried.
			name: "an unknown directory time falls through to the producer",
			md:   DumpMetadata{Producer: ProducerDump, SnapshotTimestamp: ts(snap)},
			want: ProducedByDump,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ProvenanceOf(tc.dir, tc.md)
			if got.ProducedBy != tc.want {
				t.Errorf("ProducedBy = %q, want %q", got.ProducedBy, tc.want)
			}
			// Unconditional, like FromPath below. A guard that only fires when a
			// non-zero From is EXPECTED cannot see a verdict that grew one it
			// must not have: `unknown` says the file records nothing, and
			// rendering an ancestor date beside that is the confident wrong
			// answer this whole type exists to refuse.
			if !got.From.Equal(tc.from) {
				t.Errorf("From = %v, want %v — the ancestor is what makes the verdict actionable", got.From, tc.from)
			}
			// FromPath must name FROM's file or nothing. On a carried table the
			// footer's derived_from_path belongs to whoever wrote the bytes,
			// which for a folded ancestor is one link FURTHER back than From —
			// so returning it would hand an operator a timestamp and a path that
			// describe two different snapshots.
			if got.FromPath != tc.fromPath {
				t.Errorf("FromPath = %q, want %q", got.FromPath, tc.fromPath)
			}
		})
	}
}

// The values are load-bearing on disk. Every snapshot already written carries
// them, and a rename silently drops provenance on exactly the historical files
// this feature exists to explain.
func TestProvenanceKeysAndValuesAreFrozen(t *testing.T) {
	for k, want := range map[string]string{
		MetaKeySnapshotProducer:  "bintrail.snapshot_producer",
		MetaKeyDerivedFrom:       "bintrail.derived_from_snapshot",
		MetaKeyDerivedFromPath:   "bintrail.derived_from_path",
		MetaKeySnapshotTimestamp: "bintrail.snapshot_timestamp",
		MetaKeyMydumperFormat:    "bintrail.mydumper_format",
		ProducerReconstruct:      "reconstruct",
	} {
		if k != want {
			t.Errorf("a footer key/value moved: got %q, want %q — files already on disk carry the old spelling", k, want)
		}
	}
	// Pairs, not a map. ProducerDump and ProducedByDump share the spelling
	// "dump" and Go rejects a duplicate constant key in a map literal, so the
	// map above structurally cannot hold all of them — which is how the four
	// ProducedBy values ended up unpinned.
	//
	// Those four are WIRE values: app.js keys MADE_BY by the literals and looks
	// up MADE_BY[t.produced_by]. Rename one and every Go assertion still passes
	// (they all compare constant to constant) while the backup row renders
	// nothing at all.
	for _, p := range []struct{ got, want string }{
		{ProducerDump, "dump"},
		{ProducedByDump, "dump"},
		{ProducedByFold, "fold"},
		{ProducedByCarriedForward, "carried_forward"},
		{ProducedByUnknown, "unknown"},
	} {
		if p.got != p.want {
			t.Errorf("a value moved: got %q, want %q — the console keys its labels by these "+
				"literals, so a rename renders an empty cell with every test green", p.got, p.want)
		}
	}
}

// TestProvenanceOf_unrecognisedProducerIsUnknown covers the forward-compat
// region the table above leaves open: a value a NEWER build stamped.
//
// The switch has two cases and falls through to the legacy sniff, so a future
// "refresh" or "carry" producer would grade a confident `dump` on any file that
// also carries a mydumper format or an LSN. Unknown is the answer: this build
// genuinely does not know.
func TestProvenanceOf_unrecognisedProducerIsUnknown(t *testing.T) {
	at := ts("2026-06-10T12:00:00Z")
	for _, md := range []DumpMetadata{
		{Producer: "refresh", SnapshotTimestamp: at, MydumperFormat: "csv"},
		{Producer: "refresh", SnapshotTimestamp: at, LSN: 42},
	} {
		if got := ProvenanceOf(at, md); got.ProducedBy != ProducedByUnknown {
			t.Errorf("producer %q with %+v graded %q; a value this build does not know is not "+
				"evidence of a dump", md.Producer, md, got.ProducedBy)
		}
	}
}

// TestParseFooterTime_corruptIsZeroNotNow pins the failure mode of a damaged
// stamp.
//
// The carried check is direction-blind (a footer disagreeing with its directory
// is carried, either way), so a corrupt stamp resolving to "now" would grade an
// ordinary file carried_forward with a present-day ancestor. Zero is what makes
// the guard above skip it and fall through to the producer.
func TestParseFooterTime_corruptIsZeroNotNow(t *testing.T) {
	if got := parseFooterTime("/b/x.parquet", MetaKeySnapshotTimestamp, "not-a-time"); !got.IsZero() {
		t.Errorf("a corrupt stamp parsed to %v; anything but the zero time makes the carried "+
			"check fire on a file whose stamp was merely damaged", got)
	}
}
