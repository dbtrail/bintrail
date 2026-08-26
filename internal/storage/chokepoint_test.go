package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestS3ClientConstructionGoesThroughOnePlace pins what makes a custom S3
// endpoint work everywhere (#1453): every S3 client in the tree is built by
// NewS3ClientFromConfig, which carries the path-style option that no AWS
// environment variable can set. A client built with a bare s3.NewFromConfig
// reaches MinIO at a virtual-hosted URL and fails on DNS, and it would do so
// only for the one surface that got it wrong, which is the kind of partial
// breakage this guard exists to prevent.
//
// Source-level rather than type-level: the SDK constructor is a function, so
// nothing in the type system can stop a new call site from appearing.
func TestS3ClientConstructionGoesThroughOnePlace(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	// The one legitimate call, inside the shared constructor.
	allowed := filepath.Join(root, "internal", "storage", "s3url.go")

	var offenders []string
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "vendor", "out":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || path == allowed {
			return nil
		}
		b, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if strings.Contains(string(b), "s3.NewFromConfig(") {
			rel, _ := filepath.Rel(root, path)
			offenders = append(offenders, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(offenders) > 0 {
		t.Fatalf("these files build an S3 client directly instead of through storage.NewS3ClientFromConfig, "+
			"so a custom S3 endpoint (BINTRAIL_S3_ENDPOINT) would not apply to them: %v", offenders)
	}
	// The guard covers nothing if the constructor stopped calling it.
	b, err := os.ReadFile(allowed)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "s3.NewFromConfig(") {
		t.Fatal("s3url.go no longer constructs the client: this guard covers nothing")
	}
}
