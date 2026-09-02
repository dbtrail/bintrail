package baseline

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The snapshot's views file (#1583) is the one file the upload must not copy:
// its bodies spell the producing machine's absolute paths. These pin the two
// arms — armed, it is REGENERATED against the destination's own s3://
// spelling under the same key; unarmed, it is SKIPPED, because a file whose
// every path is wrong is worse than no file.

func uploadViewsFixture(t *testing.T) (outputDir string) {
	t.Helper()
	outputDir = t.TempDir()
	snap := filepath.Join(outputDir, "2025-01-01T00-00-00Z")
	if err := os.MkdirAll(filepath.Join(snap, "shop"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{
		filepath.Join("shop", "orders.parquet"): "x",
		SuccessMarker:                           "",
		SnapshotViewsName:                       "-- LOCAL spelling",
	} {
		if err := os.WriteFile(filepath.Join(snap, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return outputDir
}

func TestUploadWithOps_respellsTheViewsFile(t *testing.T) {
	outputDir := uploadViewsFixture(t)
	SetSnapshotViewsRespeller(func(_ context.Context, snapshotDir, root string) (string, bool, error) {
		return "-- RESPELLED under " + root + " from " + filepath.Base(snapshotDir), true, nil
	})
	t.Cleanup(func() { SetSnapshotViewsRespeller(nil) })

	uploaded := map[string]string{}
	ops := s3UploadOps{
		putEmpty: func(_ context.Context, _ string) error { return nil },
		uploadFile: func(_ context.Context, path, key string) error {
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			uploaded[key] = string(b)
			return nil
		},
		objectExists: func(_ context.Context, _ string) (bool, error) { return false, nil },
		deleteObject: func(_ context.Context, _ string) error { return nil },
		objectURL:    func(key string) string { return "s3://bkt/" + key },
	}
	n, err := uploadWithOps(context.Background(), outputDir, "p", false, ops)
	if err != nil {
		t.Fatalf("uploadWithOps: %v", err)
	}
	if n != 3 { // data file + views file + _SUCCESS
		t.Fatalf("uploaded %d objects, want 3", n)
	}
	key := "p/2025-01-01T00-00-00Z/" + SnapshotViewsName
	got, ok := uploaded[key]
	switch {
	case !ok:
		t.Fatalf("no %s uploaded; keys = %v", key, keys(uploaded))
	case got == "-- LOCAL spelling":
		t.Fatal("the LOCAL views file's bytes were copied to S3; every path in them names the producing machine")
	case !strings.Contains(got, "s3://bkt/p/2025-01-01T00-00-00Z"):
		t.Errorf("respelled content does not carry the destination root: %q", got)
	}
}

func TestUploadWithOps_skipsTheViewsFileUnarmed(t *testing.T) {
	outputDir := uploadViewsFixture(t)
	SetSnapshotViewsRespeller(nil)

	uploaded := map[string]string{}
	ops := s3UploadOps{
		putEmpty: func(_ context.Context, _ string) error { return nil },
		uploadFile: func(_ context.Context, path, key string) error {
			uploaded[key] = path
			return nil
		},
		objectExists: func(_ context.Context, _ string) (bool, error) { return false, nil },
		deleteObject: func(_ context.Context, _ string) error { return nil },
	}
	if _, err := uploadWithOps(context.Background(), outputDir, "p", false, ops); err != nil {
		t.Fatalf("uploadWithOps: %v", err)
	}
	for key := range uploaded {
		if strings.HasSuffix(key, "/"+SnapshotViewsName) {
			t.Fatalf("the views file was uploaded with no respeller armed; its local bytes are wrong for every reader of the bucket (key %s)", key)
		}
	}
}

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
