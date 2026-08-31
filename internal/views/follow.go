package views

import (
	"strings"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// ApplyFollow decides how the state views will reach a snapshot published after
// this file is generated, and records it on in.
//
// One function for both producers on purpose. The CLI and the console download
// serve the same file to the same reader, and they used to carry a byte-identical
// copy of this decision each; a mode added to one of them and not the other is a
// file whose header describes a following its own views do not do.
//
// pin is the operator asking for today's rows to stay today's, and it is the only
// decision made here. Every other reason to refuse belongs to the mechanism:
// baseline.RewriteToPointer owns the pointer's conditions, and the S3 arm below
// owns its own.
func ApplyFollow(in *Input, root string, pin bool) {
	if pin || len(in.Baselines) == 0 {
		return
	}
	paths := make([]string, len(in.Baselines))
	for i, t := range in.Baselines {
		paths[i] = t.Path
	}
	if rewritten, ok := baseline.RewriteToPointer(root, paths); ok {
		for i := range in.Baselines {
			in.Baselines[i].Path = rewritten[i]
		}
		in.Follow = FollowPointer
		return
	}
	// A local root that refused the rewrite stops here rather than falling
	// through: RewriteToPointer refuses when the pointer names a DIFFERENT
	// snapshot than the one discovered, and answering that by following
	// something else is how a file comes to serve rows its header does not
	// describe. FollowNewest is the S3 answer to having no pointer at all.
	if !isS3(root) {
		return
	}
	rels := make([]string, len(in.Baselines))
	for i, t := range in.Baselines {
		rel, ok := snapshotRel(root, t.Path)
		if !ok {
			// All or nothing. A file where some state views follow and others
			// are pinned reads exactly like one where they all do, and the two
			// halves would drift apart at the first refresh.
			return
		}
		rels[i] = rel
	}
	for i := range in.Baselines {
		in.Baselines[i].Rel = rels[i]
	}
	in.Follow = FollowNewest
}

// snapshotRel cuts a table's path down to its position inside a snapshot
// directory: "s3://bucket/baselines/2026-08-31T06-00-45Z/shop/orders.parquet"
// under root "s3://bucket/baselines/" gives "shop/orders.parquet".
//
// String operations, never filepath: filepath.Clean collapses the "//" in a
// scheme, so every path helper in the standard library turns "s3://bucket/x"
// into "s3:/bucket/x". Rel happens to survive that because it mangles both
// sides identically, which is a coincidence and not a contract.
func snapshotRel(root, path string) (string, bool) {
	base := strings.TrimSuffix(root, "/") + "/"
	rel, ok := strings.CutPrefix(path, base)
	if !ok {
		return "", false
	}
	_, rest, found := strings.Cut(rel, "/")
	if !found || rest == "" {
		return "", false
	}
	return rest, true
}
