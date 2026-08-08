package baselineintegrity

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"testing"
	"time"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// stubS3 serves objects from a map keyed "bucket/key" and counts GETs, so the
// tests can assert both the verdict and the fetch behavior (cache hits, absent
// keys). Absent keys return NoSuchKey — the shape the real SDK produces for a
// missing object.
type stubS3 struct {
	objects map[string][]byte
	errs    map[string]error // takes precedence over objects
	gets    map[string]int
}

func (s *stubS3) open(_ context.Context, bucket, key string) (io.ReadCloser, error) {
	k := bucket + "/" + key
	s.gets[k]++
	if err, ok := s.errs[k]; ok {
		return nil, err
	}
	b, ok := s.objects[k]
	if !ok {
		return nil, &s3types.NoSuchKey{}
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

// installStub swaps OpenS3Object for a stub for the duration of the test.
// Each test must use a UNIQUE bucket/path: the package-level verdict cache is
// keyed by full s3:// path and deliberately survives across calls.
func installStub(t *testing.T) *stubS3 {
	t.Helper()
	s := &stubS3{objects: map[string][]byte{}, errs: map[string]error{}, gets: map[string]int{}}
	orig := OpenS3Object
	OpenS3Object = s.open
	t.Cleanup(func() { OpenS3Object = orig })
	return s
}

func crcHex(b []byte) string {
	return fmt.Sprintf("%08x", crc32.Checksum(b, crc32cTable))
}

func manifestJSON(t *testing.T, files map[string]string) []byte {
	t.Helper()
	b, err := json.Marshal(Manifest{Version: manifestVersion, Algo: "crc32c", Files: files})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestValidateS3File_matchAndCache(t *testing.T) {
	s := installStub(t)
	data := []byte("baseline parquet bytes")
	s.objects["bkt-match/base/20260101000000/_MANIFEST"] = manifestJSON(t, map[string]string{"db/t.parquet": crcHex(data)})
	s.objects["bkt-match/base/20260101000000/db/t.parquet"] = data

	p := "s3://bkt-match/base/20260101000000/db/t.parquet"
	if err := ValidateS3File(context.Background(), p); err != nil {
		t.Fatalf("clean S3 baseline must validate, got %v", err)
	}
	// Verdict is memoized: a second call must not re-download the object
	// (ReadBaselineRows runs in tight loops — cascade Phase-2, shim _snapshot).
	if err := ValidateS3File(context.Background(), p); err != nil {
		t.Fatalf("cached verdict must stay nil, got %v", err)
	}
	if got := s.gets["bkt-match/base/20260101000000/db/t.parquet"]; got != 1 {
		t.Errorf("object fetched %d times, want 1 (verdict cache)", got)
	}
}

func TestValidateS3File_mismatchFailsLoud(t *testing.T) {
	s := installStub(t)
	s.objects["bkt-corrupt/base/20260101000000/_MANIFEST"] = manifestJSON(t, map[string]string{"db/t.parquet": crcHex([]byte("original bytes"))})
	s.objects["bkt-corrupt/base/20260101000000/db/t.parquet"] = []byte("CORRUPTED bytes — bit-rot")

	p := "s3://bkt-corrupt/base/20260101000000/db/t.parquet"
	if err := ValidateS3File(context.Background(), p); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("corrupt S3 baseline must fail loud with ErrIntegrity, got %v", err)
	}
	// The mismatch verdict is terminal and cached too.
	if err := ValidateS3File(context.Background(), p); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("cached mismatch must persist, got %v", err)
	}
	if got := s.gets["bkt-corrupt/base/20260101000000/db/t.parquet"]; got != 1 {
		t.Errorf("object fetched %d times, want 1 (verdict cache)", got)
	}
}

func TestValidateS3File_truncatedObjectFailsLoud(t *testing.T) {
	// A truncated-at-rest object (partial write / bad multipart stitch) reads
	// completely but SHORT — that must land as a CRC mismatch, not a skip.
	s := installStub(t)
	full := []byte("full parquet object bytes")
	s.objects["bkt-trunc/base/20260101000000/_MANIFEST"] = manifestJSON(t, map[string]string{"db/t.parquet": crcHex(full)})
	s.objects["bkt-trunc/base/20260101000000/db/t.parquet"] = full[:10]

	p := "s3://bkt-trunc/base/20260101000000/db/t.parquet"
	if err := ValidateS3File(context.Background(), p); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("truncated S3 baseline must fail loud with ErrIntegrity, got %v", err)
	}
}

func TestValidateS3File_legacyNoManifest(t *testing.T) {
	s := installStub(t)
	s.objects["bkt-legacy/base/20260101000000/db/t.parquet"] = []byte("legacy baseline, no manifest")

	p := "s3://bkt-legacy/base/20260101000000/db/t.parquet"
	if err := ValidateS3File(context.Background(), p); err != nil {
		t.Fatalf("legacy snapshot (no _MANIFEST) must skip, got %v", err)
	}
	// Legacy skip is terminal (snapshots are immutable) → cached.
	if err := ValidateS3File(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if got := s.gets["bkt-legacy/base/20260101000000/_MANIFEST"]; got != 1 {
		t.Errorf("manifest probed %d times, want 1 (legacy verdict cached)", got)
	}
}

func TestValidateS3File_degradeToSkip(t *testing.T) {
	// Every "cannot verify" shape must degrade to a skip (nil), never deny the
	// read: unparseable manifest, unrecognized version, unlisted file, and a
	// path too shallow for the <snapshot>/<db>/<table>.parquet layout.
	s := installStub(t)
	s.objects["bkt-skip/rotted/20260101000000/_MANIFEST"] = []byte("{not json")
	s.objects["bkt-skip/rotted/20260101000000/db/t.parquet"] = []byte("x")
	v2, _ := json.Marshal(Manifest{Version: 99, Algo: "crc32c", Files: map[string]string{"db/t.parquet": "00000000"}})
	s.objects["bkt-skip/v99/20260101000000/_MANIFEST"] = v2
	s.objects["bkt-skip/v99/20260101000000/db/t.parquet"] = []byte("x")
	s.objects["bkt-skip/unlisted/20260101000000/_MANIFEST"] = manifestJSON(t, map[string]string{"db/other.parquet": "00000000"})
	s.objects["bkt-skip/unlisted/20260101000000/db/t.parquet"] = []byte("x")

	for _, p := range []string{
		"s3://bkt-skip/rotted/20260101000000/db/t.parquet",
		"s3://bkt-skip/v99/20260101000000/db/t.parquet",
		"s3://bkt-skip/unlisted/20260101000000/db/t.parquet",
		"s3://bkt-skip/shallow.parquet",
	} {
		if err := ValidateS3File(context.Background(), p); err != nil {
			t.Errorf("%s: cannot-verify must degrade to skip, got %v", p, err)
		}
	}
}

// setTTL overrides failureVerdictTTL for the test. 0 makes every failure
// verdict expire immediately, exposing the re-check path.
func setTTL(t *testing.T, d time.Duration) {
	t.Helper()
	orig := failureVerdictTTL
	failureVerdictTTL = d
	t.Cleanup(func() { failureVerdictTTL = orig })
}

func TestValidateS3File_failureVerdictsCachedWithinTTL(t *testing.T) {
	// A validator-side read failure degrades to skip AND is cached for
	// failureVerdictTTL: the hot paths (shim `_snapshot` per client query,
	// cascade Phase-2 per parent PK) must not pay S3 round-trips plus a warn
	// per read against a persistently unreachable validator — the
	// WarnS3IntegrityNotValidated this replaced was once-per-process for
	// exactly that reason.
	s := installStub(t)
	data := []byte("object bytes")
	s.objects["bkt-blip/base/20260101000000/_MANIFEST"] = manifestJSON(t, map[string]string{"db/t.parquet": crcHex(data)})
	s.errs["bkt-blip/base/20260101000000/db/t.parquet"] = errors.New("AccessDenied: simulated")

	p := "s3://bkt-blip/base/20260101000000/db/t.parquet"
	if err := ValidateS3File(context.Background(), p); err != nil {
		t.Fatalf("object read failure must degrade to skip, got %v", err)
	}
	if err := ValidateS3File(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if got := s.gets["bkt-blip/base/20260101000000/db/t.parquet"]; got != 1 {
		t.Errorf("object attempted %d times, want 1 (failure verdict cached within TTL)", got)
	}
}

func TestValidateS3File_failureVerdictExpiresAndHeals(t *testing.T) {
	// Once the TTL lapses, validation re-attempts — a transient blip must not
	// disable validation for the rest of the process.
	setTTL(t, 0)
	s := installStub(t)
	data := []byte("object bytes")
	s.objects["bkt-heal/base/20260101000000/_MANIFEST"] = manifestJSON(t, map[string]string{"db/t.parquet": crcHex(data)})
	objKey := "bkt-heal/base/20260101000000/db/t.parquet"
	s.errs[objKey] = errors.New("throttled: simulated")

	p := "s3://bkt-heal/base/20260101000000/db/t.parquet"
	if err := ValidateS3File(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if err := ValidateS3File(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if got := s.gets[objKey]; got != 2 {
		t.Fatalf("object attempted %d times, want 2 (expired verdict must re-check)", got)
	}
	// Blip clears → the re-check reaches the bytes, validates, and the match
	// verdict is terminal: no further GETs.
	delete(s.errs, objKey)
	s.objects[objKey] = data
	if err := ValidateS3File(context.Background(), p); err != nil {
		t.Fatalf("healed object must validate, got %v", err)
	}
	if err := ValidateS3File(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if got := s.gets[objKey]; got != 3 {
		t.Errorf("object attempted %d times, want 3 (match verdict is terminal)", got)
	}
}

func TestValidateS3File_repairedObjectUnbricksWithoutRestart(t *testing.T) {
	// The mismatch verdict also expires: an operator who repairs a corrupt
	// object by re-uploading a good copy to the SAME key must not stay bricked
	// until a daemon restart — mid-incident, restarting capture is the last
	// thing wanted.
	setTTL(t, 0)
	s := installStub(t)
	good := []byte("original bytes")
	s.objects["bkt-repair/base/20260101000000/_MANIFEST"] = manifestJSON(t, map[string]string{"db/t.parquet": crcHex(good)})
	objKey := "bkt-repair/base/20260101000000/db/t.parquet"
	s.objects[objKey] = []byte("CORRUPTED bytes")

	p := "s3://bkt-repair/base/20260101000000/db/t.parquet"
	if err := ValidateS3File(context.Background(), p); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("corrupt object must fail loud, got %v", err)
	}
	s.objects[objKey] = good // repaired in place
	if err := ValidateS3File(context.Background(), p); err != nil {
		t.Errorf("repaired object must validate after the TTL, got %v", err)
	}
}

func TestValidateS3File_manifestFetchedOncePerSnapshot(t *testing.T) {
	// The manifest cache is per SNAPSHOT, not per object: an N-table snapshot
	// must not GET the identical _MANIFEST N times (full-table reconstruct
	// iterates tables, each through its own ValidateS3File).
	s := installStub(t)
	a, b := []byte("table a bytes"), []byte("table b bytes")
	s.objects["bkt-multi/base/20260101000000/_MANIFEST"] = manifestJSON(t, map[string]string{
		"db/a.parquet": crcHex(a), "db/b.parquet": crcHex(b),
	})
	s.objects["bkt-multi/base/20260101000000/db/a.parquet"] = a
	s.objects["bkt-multi/base/20260101000000/db/b.parquet"] = b

	for _, p := range []string{
		"s3://bkt-multi/base/20260101000000/db/a.parquet",
		"s3://bkt-multi/base/20260101000000/db/b.parquet",
	} {
		if err := ValidateS3File(context.Background(), p); err != nil {
			t.Fatalf("%s: %v", p, err)
		}
	}
	if got := s.gets["bkt-multi/base/20260101000000/_MANIFEST"]; got != 1 {
		t.Errorf("manifest fetched %d times, want 1 (per-snapshot manifest cache)", got)
	}
}
