package archive

import (
	"database/sql"
	"testing"
	"time"
)

var (
	tNow      = time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC)
	tOld      = tNow.Add(-24 * time.Hour)
	tModified = time.Date(2026, 6, 5, 9, 30, 0, 0, time.UTC)
)

func nInt(v int64) sql.NullInt64    { return sql.NullInt64{Int64: v, Valid: true} }
func nStr(v string) sql.NullString  { return sql.NullString{String: v, Valid: true} }
func nTime(v time.Time) sql.NullTime { return sql.NullTime{Time: v, Valid: true} }

func localFile(part, id, path string, size, rows int64) ScannedFile {
	return ScannedFile{PartitionName: part, BintrailID: id, Backend: BackendLocal,
		LocalPath: path, SizeBytes: size, RowCount: nInt(rows), LastModified: tModified}
}

func s3File(part, id, bucket, key string, size int64) ScannedFile {
	return ScannedFile{PartitionName: part, BintrailID: id, Backend: BackendS3,
		S3Bucket: bucket, S3Key: key, SizeBytes: size, LastModified: tModified}
}

func changes(a Action) map[string]any {
	m := make(map[string]any, len(a.Changes))
	for _, c := range a.Changes {
		m[c.Column] = c.Value
	}
	return m
}

func bothScanned() DiffOptions {
	return DiffOptions{ScannedLocal: true, ScannedS3: true, PruneMinAge: time.Hour, Now: tNow}
}

// TestDiffInsert covers files-without-rows: the post-index-loss rebuild.
func TestDiffInsert(t *testing.T) {
	files := []ScannedFile{
		localFile("p_2026060510", "abc", "/a/bintrail_id=abc/event_date=2026-06-05/event_hour=10/events.parquet", 100, 42),
		s3File("p_2026060510", "abc", "bkt", "pre/bintrail_id=abc/event_date=2026-06-05/event_hour=10/events.parquet", 100),
	}
	rep := Diff(files, nil, bothScanned())
	if rep.Inserts != 1 || len(rep.Actions) != 1 {
		t.Fatalf("want 1 insert, got %+v", rep)
	}
	ch := changes(rep.Actions[0])
	if ch["local_path"] != files[0].LocalPath || ch["s3_bucket"] != "bkt" || ch["file_size_bytes"] != int64(100) || ch["row_count"] != int64(42) {
		t.Errorf("insert changes incomplete: %v", ch)
	}
	// The drop-block trap: an S3-confirmed insert MUST stamp s3_uploaded_at —
	// rotate's hasPendingS3Upload reads bucket-set + stamp-NULL as a pending
	// upload and refuses to drop the partition forever.
	if _, ok := ch["s3_uploaded_at"]; !ok {
		t.Error("insert with confirmed S3 object must stamp s3_uploaded_at (hasPendingS3Upload drop-block trap)")
	}
	if err := rep.Err(); err == nil {
		t.Error("drift must make Err() non-nil (cron exit-code contract)")
	}
}

// TestDiffInSync: matching row and file → no action, Err() nil.
func TestDiffInSync(t *testing.T) {
	f := localFile("p_2026060510", "abc", "/a/x.parquet", 100, 42)
	rows := []StateRow{{
		PartitionName: "p_2026060510", BintrailID: "abc",
		LocalPath: nStr("/a/x.parquet"), FileSizeBytes: nInt(100), RowCount: nInt(42), ArchivedAt: tOld,
	}}
	rep := Diff([]ScannedFile{f}, rows, bothScanned())
	if len(rep.Actions) != 0 || rep.InSync != 1 {
		t.Fatalf("want clean in-sync report, got %+v", rep)
	}
	if err := rep.Err(); err != nil {
		t.Errorf("in-sync must exit clean, got %v", err)
	}
}

// TestDiffBackendScopedUpdates pins the per-backend repair rules.
func TestDiffBackendScopedUpdates(t *testing.T) {
	t.Run("S3 object present but row lacks it → add columns + stamp", func(t *testing.T) {
		f := s3File("p_2026060510", "abc", "bkt", "k/events.parquet", 100)
		rows := []StateRow{{PartitionName: "p_2026060510", BintrailID: "abc",
			LocalPath: nStr("/gone.parquet"), FileSizeBytes: nInt(100), ArchivedAt: tOld}}
		rep := Diff([]ScannedFile{f}, rows, bothScanned())
		if rep.Updates != 1 {
			t.Fatalf("want 1 update, got %+v", rep)
		}
		ch := changes(rep.Actions[0])
		if ch["s3_bucket"] != "bkt" {
			t.Errorf("missing s3 columns: %v", ch)
		}
		// Local was scanned and the registered local file is gone, while
		// the S3 copy holds the data → the stale local_path is cleared.
		if v, ok := ch["local_path"]; !ok || v != nil {
			t.Errorf("stale local_path should be cleared (SET NULL), got %v present=%v", v, ok)
		}
	})

	t.Run("local NOT scanned → stale local_path untouched", func(t *testing.T) {
		f := s3File("p_2026060510", "abc", "bkt", "k/events.parquet", 100)
		rows := []StateRow{{PartitionName: "p_2026060510", BintrailID: "abc",
			LocalPath: nStr("/maybe-fine.parquet"), FileSizeBytes: nInt(100),
			S3Bucket: nStr("bkt"), S3Key: nStr("k/events.parquet"), S3UploadedAt: nTime(tModified), ArchivedAt: tOld}}
		rep := Diff([]ScannedFile{f}, rows, DiffOptions{ScannedS3: true, PruneMinAge: time.Hour, Now: tNow})
		for _, a := range rep.Actions {
			if _, touched := changes(a)["local_path"]; touched {
				t.Errorf("local_path must not be touched when local was not scanned: %+v", a)
			}
		}
	})

	t.Run("missing s3_uploaded_at stamp on confirmed object", func(t *testing.T) {
		f := s3File("p_2026060510", "abc", "bkt", "k/events.parquet", 100)
		rows := []StateRow{{PartitionName: "p_2026060510", BintrailID: "abc",
			FileSizeBytes: nInt(100),
			S3Bucket:      nStr("bkt"), S3Key: nStr("k/events.parquet"), ArchivedAt: tOld}}
		rep := Diff([]ScannedFile{f}, rows, DiffOptions{ScannedS3: true, PruneMinAge: time.Hour, Now: tNow})
		if rep.Updates != 1 {
			t.Fatalf("want 1 update, got %+v", rep)
		}
		if _, ok := changes(rep.Actions[0])["s3_uploaded_at"]; !ok {
			t.Error("confirmed S3 object with NULL stamp must be stamped")
		}
	})

	t.Run("size drift always; row_count drift only under Deep", func(t *testing.T) {
		f := localFile("p_2026060510", "abc", "/a/x.parquet", 999, 42)
		rows := []StateRow{{PartitionName: "p_2026060510", BintrailID: "abc",
			LocalPath: nStr("/a/x.parquet"), FileSizeBytes: nInt(100), RowCount: nInt(7), ArchivedAt: tOld}}

		shallow := Diff([]ScannedFile{f}, rows, bothScanned())
		if shallow.Updates != 1 {
			t.Fatalf("want size-drift update, got %+v", shallow)
		}
		ch := changes(shallow.Actions[0])
		if ch["file_size_bytes"] != int64(999) {
			t.Errorf("size drift not repaired: %v", ch)
		}
		if _, ok := ch["row_count"]; ok {
			t.Error("row_count drift must be gated behind Deep")
		}

		deepOpts := bothScanned()
		deepOpts.Deep = true
		deep := Diff([]ScannedFile{f}, rows, deepOpts)
		if _, ok := changes(deep.Actions[0])["row_count"]; !ok {
			t.Error("Deep must repair row_count drift")
		}
	})
}

// TestDiffPruneSafety pins the three prune gates: backend coverage
// (partial-scan misclassification — the registry-data-loss trap), the age
// margin, and the genuinely-orphaned happy path.
func TestDiffPruneSafety(t *testing.T) {
	s3OnlyRow := StateRow{PartitionName: "p_2026060510", BintrailID: "abc",
		S3Bucket: nStr("bkt"), S3Key: nStr("k/events.parquet"), S3UploadedAt: nTime(tModified), ArchivedAt: tOld}

	t.Run("partial scan: S3-referenced row, local-only scan → unverified, never pruned", func(t *testing.T) {
		rep := Diff(nil, []StateRow{s3OnlyRow}, DiffOptions{ScannedLocal: true, PruneMinAge: time.Hour, Now: tNow})
		if rep.Prunes != 0 || rep.SkippedUnverified != 1 {
			t.Fatalf("partial scan must yield skip-unverified, got %+v", rep)
		}
	})

	t.Run("full scan with testimony, target's files gone → prune candidate", func(t *testing.T) {
		// An unrelated S3 file proves the scan can see the layout
		// (testimony); the target row's own files are gone → prunable.
		witness := s3File("p_2026060409", "other-id", "bkt", "w/events.parquet", 1)
		rep := Diff([]ScannedFile{witness}, []StateRow{s3OnlyRow}, bothScanned())
		if rep.Prunes != 1 {
			t.Fatalf("want prune candidate, got %+v", rep)
		}
	})

	t.Run("blind scanner: zero files seen in the referenced backend → never pruned", func(t *testing.T) {
		// The scan ran (flag present) but saw NOTHING — wrong directory,
		// or a scanner blind spot. It cannot distinguish "all orphaned"
		// from "I am blind", so pruning on its testimony is forbidden
		// (the registry-wipe trap from the #392 adversarial review).
		rep := Diff(nil, []StateRow{s3OnlyRow}, bothScanned())
		if rep.Prunes != 0 || rep.SkippedUnverified != 1 {
			t.Fatalf("empty-scan prune must be refused, got %+v", rep)
		}
	})

	t.Run("recent row inside the margin → skip-recent", func(t *testing.T) {
		recent := s3OnlyRow
		recent.ArchivedAt = tNow.Add(-10 * time.Minute)
		witness := s3File("p_2026060409", "other-id", "bkt", "w/events.parquet", 1)
		rep := Diff([]ScannedFile{witness}, []StateRow{recent}, bothScanned())
		if rep.Prunes != 0 || rep.SkippedRecent != 1 {
			t.Fatalf("recent row must be skip-recent, got %+v", rep)
		}
	})

	t.Run("row referencing NOTHING (phantom coverage) → prune candidate", func(t *testing.T) {
		phantom := StateRow{PartitionName: "p_2026060511", BintrailID: "abc", ArchivedAt: tOld}
		rep := Diff(nil, []StateRow{phantom}, DiffOptions{ScannedLocal: true, PruneMinAge: time.Hour, Now: tNow})
		// References nothing → the (empty) referenced set is covered by any
		// scan; the row only feeds phantom planner coverage.
		if rep.Prunes != 1 {
			t.Fatalf("phantom row must be a prune candidate, got %+v", rep)
		}
	})
}

// TestDiffDeterministicOrder: actions sort by (partition, bintrail_id).
func TestDiffDeterministicOrder(t *testing.T) {
	files := []ScannedFile{
		localFile("p_2026060512", "zzz", "/a/2.parquet", 1, 1),
		localFile("p_2026060510", "aaa", "/a/1.parquet", 1, 1),
		localFile("p_2026060510", "bbb", "/a/3.parquet", 1, 1),
	}
	rep := Diff(files, nil, bothScanned())
	got := make([]string, len(rep.Actions))
	for i, a := range rep.Actions {
		got[i] = a.PartitionName + "/" + a.BintrailID
	}
	want := []string{"p_2026060510/aaa", "p_2026060510/bbb", "p_2026060512/zzz"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}
}
