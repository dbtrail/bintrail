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
		name string
		dir  time.Time
		md   DumpMetadata
		want string
		from time.Time
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
			want: ProducedByFold,
			from: older,
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
			if !tc.from.IsZero() && !got.From.Equal(tc.from) {
				t.Errorf("From = %v, want %v — the ancestor is what makes the verdict actionable", got.From, tc.from)
			}
			if tc.want == ProducedByDump && !got.From.IsZero() {
				t.Errorf("From = %v on a dump, which is derived from nothing", got.From)
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
}
