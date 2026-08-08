// baselineintegrity — s3.go: the S3 half of the at-rest integrity story
// (#698, follow-up to #636's local validation).
//
// The manifest's CRC-32C is over the raw Parquet object BYTES, and no S3 read
// path keeps a byte-identical local copy to hash against — two stream directly
// via DuckDB parquet_scan, one re-encodes via DuckDB COPY to a temp. So the S3
// validation is a PRE-PASS: before any reader touches the object, the ORIGINAL
// object is streamed once through CRC-32C via the AWS SDK and compared against
// the snapshot's _MANIFEST (also fetched from S3). This validates exact bytes
// on all three read paths without forcing a download-to-disk (the DuckDB read
// that follows keeps whatever streaming mode the caller chose), at the cost of
// one extra full read of the object — memoized per process, see s3VerdictCache.
//
// Scope is unchanged from #636: bit-rot / truncated or partial writes, NOT
// tamper-evidence — an attacker who rewrites the object can rewrite the
// manifest too.
package baselineintegrity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"path"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// OpenS3Object fetches an S3 object as a byte stream. It is a package variable
// ONLY so the read-path wiring tests can stub S3 without a mock server;
// production code never reassigns it. The default implementation uses the AWS
// SDK default credential chain with the shared IMDS region fallback
// (storage.NewS3Client) — the same resolution every other SDK-side S3 caller
// in this codebase uses.
var OpenS3Object = sdkOpenS3Object

// s3VerdictCache memoizes TERMINAL validation verdicts per s3:// path
// (map[string]error; nil = validated or not-verifiable). ReadBaselineRows runs
// in tight loops (cascade Phase-2 child scans, the shim `_snapshot` per
// request), and without this cache every call would re-download the whole
// object just to hash it. Caching per process is safe because snapshots are
// immutable once their _SUCCESS marker exists. Transient failures (a manifest
// or object GET that errored) are deliberately NOT cached, so a network blip
// does not disable validation for the rest of the process.
var s3VerdictCache sync.Map

// ValidateS3File checks an s3:// baseline Parquet object against its
// snapshot's _MANIFEST — the S3 mirror of ValidateLocalFile, with the same
// outcomes: nil on match, ErrIntegrity (wrapped) on a CRC mismatch, and a
// degrade-to-skip (nil) whenever the check CANNOT run — no manifest (legacy
// snapshot), unreadable/unparseable manifest, unrecognized version/algo, file
// not listed, or a layout that isn't <snapshot>/<db>/<table>.parquet. All of
// those mean "unverified", never "corrupt"; a rotted sidecar must not brick a
// good baseline.
//
// One DELIBERATE divergence from ValidateLocalFile: a failure to READ the data
// object here degrades to a logged skip instead of an error. Locally, an
// unopenable file means the subsequent read fails on the same syscall path
// anyway. On S3 the validator (AWS SDK default chain) and the reader (DuckDB
// httpfs credential_chain) are DIFFERENT clients that can diverge on region
// resolution or IAM path, so failing here could deny recovery of intact data
// that DuckDB can perfectly well read. Only a completed hash that mismatches
// the manifest is proof of corruption — and note a truncated-at-rest object
// still produces exactly that (a complete, shorter read → CRC mismatch), so
// the skip does not hide the partial-write case this exists to catch.
func ValidateS3File(ctx context.Context, s3Path string) error {
	if v, ok := s3VerdictCache.Load(s3Path); ok {
		if v == nil {
			return nil
		}
		return v.(error)
	}
	bucket, key, err := storage.ParseS3URL(s3Path)
	if err != nil {
		return nil // not an interpretable s3:// URL — nothing to locate a manifest by
	}
	// Snapshot layout is <prefix>/<timestamp>/<schema>/<table>.parquet with the
	// _MANIFEST directly under <timestamp>/ — the object's grandparent, exactly
	// like the local layout. Fewer than two segments above the object means the
	// path is not under a snapshot directory; skip like ValidateLocalFile's
	// filepath.Rel failure.
	tableDir := path.Dir(key)
	snapKey := path.Dir(tableDir)
	if tableDir == "." || snapKey == "." {
		return nil
	}
	rel := strings.TrimPrefix(key, snapKey+"/")
	snapshotLabel := "s3://" + bucket + "/" + snapKey

	m, ok, err := loadManifestS3(ctx, bucket, snapKey)
	if err != nil {
		// Unreadable / unparseable manifest = cannot verify, not data corruption
		// (same degrade as the local path). Not cached: it may be transient.
		slog.Warn("S3 integrity manifest unreadable; treating baseline as integrity-not-verified",
			"snapshot", snapshotLabel, "error", err)
		return nil
	}
	if !ok {
		// Legacy snapshot written before #636 — no manifest will ever appear
		// (snapshots are immutable), so the skip verdict is cacheable.
		s3VerdictCache.Store(s3Path, error(nil))
		return nil
	}
	want, verify := m.digestFor(rel, snapshotLabel)
	if !verify {
		s3VerdictCache.Store(s3Path, error(nil))
		return nil
	}
	got, err := crc32cS3Object(ctx, bucket, key)
	if err != nil {
		// The deliberate divergence documented above: validator-side read
		// failure ≠ corruption. Loud (once per attempt), then proceed unverified.
		slog.Warn("could not re-read S3 baseline for integrity check; treating as integrity-not-verified",
			"path", s3Path, "error", err)
		return nil
	}
	if got != want {
		verr := fmt.Errorf("%w: %s (crc32c %s, manifest %s)", ErrIntegrity, s3Path, got, want)
		s3VerdictCache.Store(s3Path, verr)
		return verr
	}
	s3VerdictCache.Store(s3Path, error(nil))
	return nil
}

// loadManifestS3 is LoadManifest over S3. ok=false with a nil error means the
// manifest object is ABSENT (NoSuchKey/NotFound — a legacy snapshot); any
// other fetch or parse failure is returned for the caller to degrade on, since
// an AccessDenied/throttle/network error hides a manifest that may exist
// (same discrimination as the restore-index sidecar download).
func loadManifestS3(ctx context.Context, bucket, snapKey string) (*Manifest, bool, error) {
	rc, err := OpenS3Object(ctx, bucket, path.Join(snapKey, ManifestName))
	if err != nil {
		var noKey *s3types.NoSuchKey
		var notFound *s3types.NotFound
		if errors.As(err, &noKey) || errors.As(err, &notFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		return nil, false, err
	}
	var parsed Manifest
	if err := json.Unmarshal(b, &parsed); err != nil {
		return nil, false, fmt.Errorf("parse %s: %w", ManifestName, err)
	}
	return &parsed, true, nil
}

// crc32cS3Object streams the object through CRC-32C (Castagnoli) and returns
// the 8-char lowercase hex digest — CRC32CFile over a GET body.
func crc32cS3Object(ctx context.Context, bucket, key string) (string, error) {
	rc, err := OpenS3Object(ctx, bucket, key)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	h := crc32.New(crc32cTable)
	if _, err := io.Copy(h, rc); err != nil {
		return "", err
	}
	return fmt.Sprintf("%08x", h.Sum32()), nil
}

var (
	s3ClientMu sync.Mutex
	s3Client   *s3.Client
)

// sharedS3Client lazily builds one process-wide S3 client. A failed build is
// not cached, so a transient config error doesn't pin validation off forever.
func sharedS3Client(ctx context.Context) (*s3.Client, error) {
	s3ClientMu.Lock()
	defer s3ClientMu.Unlock()
	if s3Client != nil {
		return s3Client, nil
	}
	c, err := storage.NewS3Client(ctx, "")
	if err != nil {
		return nil, err
	}
	s3Client = c
	return c, nil
}

func sdkOpenS3Object(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	client, err := sharedS3Client(ctx)
	if err != nil {
		return nil, err
	}
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}
