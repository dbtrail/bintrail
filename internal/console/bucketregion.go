package console

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/dbtrail/dbtrail/internal/storage"
	"github.com/dbtrail/dbtrail/internal/views"
)

// archiveRegion resolves the region to pin in a generated views.sql, and in the
// SQL panel's own DuckDB session, for the layout in `in`.
//
// It exists because those two used to pin NOTHING while the daemon's own
// archive reads pin a DETECTED bucket region (#511): the file described a
// different read than the one this process performs, and a store that checks
// the signing region answers 403 (Ceph) or 301 PermanentRedirect (a
// cross-region AWS bucket) to the recipient of a file that works here.
//
// Returns ("", true) when the layout's buckets do NOT agree on one region. One
// secret and one s3_region cannot describe two regions, so pinning either would
// be worse than pinning none: with none, the reader's own credential chain
// still resolves a region, and only the odd bucket out fails. The caller
// surfaces that so the file says which case it is instead of looking unpinned
// by accident.
//
// Cached because buildViewsInput runs on every SQL panel query, and this would
// otherwise add a GetBucketLocation round trip to a latched hot path. A
// bucket's region is fixed for its lifetime, so the entry never needs to
// expire.
func (s *Server) archiveRegion(ctx context.Context, in views.Input) (region string, ambiguous bool) {
	buckets := layoutBuckets(in)
	if len(buckets) == 0 {
		return "", false
	}
	cfg, err := storage.LoadAWSConfig(ctx, "")
	if err != nil {
		// Best-effort: a region is a hint, and the read paths each load their
		// own config anyway. Rendering no region leaves today's behavior.
		return "", false
	}
	seen := ""
	for _, b := range buckets {
		r := s.cachedBucketRegion(ctx, cfg, b)
		if r == "" {
			continue
		}
		if seen == "" {
			seen = r
			continue
		}
		if r != seen {
			return "", true
		}
	}
	return seen, false
}

func (s *Server) cachedBucketRegion(ctx context.Context, cfg aws.Config, bucket string) string {
	s.bucketRegionMu.Lock()
	defer s.bucketRegionMu.Unlock()
	if s.bucketRegions == nil {
		s.bucketRegions = map[string]string{}
	}
	if r, ok := s.bucketRegions[bucket]; ok {
		return r
	}
	r := storage.DetectBucketRegion(ctx, cfg, bucket)
	s.bucketRegions[bucket] = r
	return r
}

// layoutBuckets returns the distinct S3 buckets the rendered file will read,
// in a stable order. Both halves count: archives and baselines can live in
// different buckets, and the views read both.
func layoutBuckets(in views.Input) []string {
	var out []string
	seen := map[string]bool{}
	add := func(p string) {
		bucket, _, err := storage.ParseS3URL(p)
		if err != nil || bucket == "" || seen[bucket] {
			return
		}
		seen[bucket] = true
		out = append(out, bucket)
	}
	for _, src := range in.ArchiveSources {
		add(src)
	}
	for _, b := range in.Baselines {
		add(b.Path)
	}
	return out
}
