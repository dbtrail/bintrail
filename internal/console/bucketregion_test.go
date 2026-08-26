package console

import (
	"context"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/views"
)

// TestLayoutBuckets: both halves of a layout count, because archives and
// baselines can sit in different buckets and the generated views read both.
// Missing the baseline half was how the region could look unanimous while one
// of the two reads pointed elsewhere.
func TestLayoutBuckets(t *testing.T) {
	in := views.Input{
		ArchiveSources: []string{
			"s3://archives/events/bintrail_id=aaa",
			"/local/archives/bintrail_id=bbb", // not S3: contributes no bucket
			"s3://archives/events/bintrail_id=ccc",
		},
		Baselines: []views.BaselineTable{
			{Path: "s3://baselines/2026-06-10T12-00-00Z/shop/orders.parquet"},
			{Path: "/local/baselines/x.parquet"},
		},
	}
	got := layoutBuckets(in)
	want := []string{"archives", "baselines"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("layoutBuckets = %v, want %v", got, want)
	}
	if b := layoutBuckets(views.Input{ArchiveSources: []string{"/only/local"}}); len(b) != 0 {
		t.Errorf("a local-only layout yielded buckets: %v", b)
	}
}

// TestArchiveRegion_agreementDecidesWhetherAnythingIsPinned drives the decision
// this feature turns on, with detection pre-seeded so the test needs no network
// and no AWS. Pinning a region the layout does not unanimously live in is worse
// than pinning none: the reader's own chain resolves one either way, and a
// wrong pin sends EVERY bucket to the wrong place instead of just the odd one.
func TestArchiveRegion_agreementDecidesWhetherAnythingIsPinned(t *testing.T) {
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	layout := views.Input{
		ArchiveSources: []string{"s3://archives/events/bintrail_id=aaa"},
		Baselines:      []views.BaselineTable{{Path: "s3://baselines/x/shop/orders.parquet"}},
	}
	cases := []struct {
		name          string
		seed          map[string]string
		wantRegion    string
		wantAmbiguous bool
	}{
		{"both buckets in one region", map[string]string{"archives": "eu-central-1", "baselines": "eu-central-1"}, "eu-central-1", false},
		{"buckets disagree", map[string]string{"archives": "eu-central-1", "baselines": "us-west-2"}, "", true},
		{"one bucket unresolvable, the other pins", map[string]string{"archives": "", "baselines": "us-west-2"}, "us-west-2", false},
		{"nothing resolvable", map[string]string{"archives": "", "baselines": ""}, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &Server{bucketRegions: tc.seed}
			region, ambiguous := s.archiveRegion(context.Background(), layout)
			if region != tc.wantRegion || ambiguous != tc.wantAmbiguous {
				t.Fatalf("got (%q, %v), want (%q, %v)", region, ambiguous, tc.wantRegion, tc.wantAmbiguous)
			}
		})
	}

	// A local-only layout asks nothing of AWS at all.
	s := &Server{}
	if r, amb := s.archiveRegion(context.Background(), views.Input{ArchiveSources: []string{"/local"}}); r != "" || amb {
		t.Errorf("a local layout resolved a region: (%q, %v)", r, amb)
	}
}

// The cache is what keeps this off the SQL panel's hot path: buildViewsInput
// runs per query, so a second call must not re-detect.
func TestArchiveRegion_detectsEachBucketOnce(t *testing.T) {
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	s := &Server{bucketRegions: map[string]string{"archives": "eu-central-1"}}
	in := views.Input{ArchiveSources: []string{"s3://archives/events/bintrail_id=aaa"}}
	for range 3 {
		if r, _ := s.archiveRegion(context.Background(), in); r != "eu-central-1" {
			t.Fatalf("region = %q; a re-detection would have overwritten the cached value", r)
		}
	}
	if len(s.bucketRegions) != 1 {
		t.Errorf("cache grew to %d entries for one bucket: %v", len(s.bucketRegions), s.bucketRegions)
	}
}
