package storage

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// aliasedS3Import matches an import of the SDK's s3 package under any name,
// which would rename every call and slip past a substring check.
var aliasedS3Import = regexp.MustCompile(`(?m)^\s*(\w+)\s+"github\.com/aws/aws-sdk-go-v2/service/s3"`)

// TestS3ClientConstructionGoesThroughOnePlace pins what makes a custom S3
// endpoint work everywhere (#1453): every S3 client in the tree is built by
// NewS3ClientFromConfig, which carries the path-style option that no AWS
// environment variable can set. A client built with a bare s3.NewFromConfig
// reaches MinIO at a virtual-hosted URL and fails on DNS, and it would do so
// only for the one surface that got it wrong, which is the kind of partial
// breakage this guard exists to prevent.
//
// Source-level rather than type-level: these are functions, so nothing in the
// type system can stop a new call site from appearing. Four things are
// scanned for in non-test .go files:
//
//   - `s3.NewFromConfig(`, the SDK constructor that skips the shared options
//   - an ALIASED import of the SDK's s3 package, which would rename that call
//   - `LoadDefaultConfig(`, which skips the endpoint, the region default and
//     the IMDS fallback that LoadAWSConfig applies
//   - `LoadHTTPFS(` in a file that never configures S3, whose s3:// reads go
//     wherever DuckDB defaults point, which is AWS
//
// What it still cannot see, since it reads text and not types: a dot-import,
// or a wrapper in a third package that re-exports the constructor. Neither
// exists today, which is when a limit is worth writing down.
func TestS3ClientConstructionGoesThroughOnePlace(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	// The shared constructor itself: exempt from all four checks, since it is
	// the one place that legitimately makes these calls.
	allowed := filepath.Join(root, "internal", "storage", "s3url.go")

	// A second, narrower guard: an S3 read path that loads httpfs must also
	// configure S3 in the same file, or its reads go wherever DuckDB's
	// defaults point — AWS — no matter how the endpoint is configured.
	var unrouted []string

	var offenders []string
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// Any dot-directory: .git, and .claude/worktrees, which holds
			// checkouts of OTHER branches. Walking those made this guard fail
			// from the main checkout for code that is not in this tree.
			if name := d.Name(); name != "." && strings.HasPrefix(name, ".") {
				return filepath.SkipDir
			}
			switch d.Name() {
			case "node_modules", "vendor", "out":
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
		src := string(b)
		rel, _ := filepath.Rel(root, path)
		if strings.Contains(src, "s3.NewFromConfig(") {
			offenders = append(offenders, rel+" (s3.NewFromConfig)")
		}
		// An aliased import would rename the call and evade the check above.
		if m := aliasedS3Import.FindStringSubmatch(src); m != nil {
			offenders = append(offenders, rel+" (aliased s3 import: "+m[1]+")")
		}
		// LoadDefaultConfig directly skips LoadAWSConfig, which is where the
		// endpoint, the region default and the IMDS fallback are applied.
		if strings.Contains(src, "LoadDefaultConfig(") {
			offenders = append(offenders, rel+" (awsconfig.LoadDefaultConfig)")
		}
		if strings.Contains(src, "duckdbutil.LoadHTTPFS(") && !strings.Contains(src, "EnableS3CredentialChain") {
			unrouted = append(unrouted, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(offenders) > 0 {
		t.Errorf("these files reach S3 outside storage.NewS3ClientFromConfig / storage.LoadAWSConfig, "+
			"so a custom S3 endpoint (BINTRAIL_S3_ENDPOINT) would not apply to them: %v", offenders)
	}
	if len(unrouted) > 0 {
		t.Errorf("these files open DuckDB httpfs without calling duckdbutil.EnableS3CredentialChain*, "+
			"so their s3:// reads go to AWS regardless of the configured endpoint: %v", unrouted)
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
