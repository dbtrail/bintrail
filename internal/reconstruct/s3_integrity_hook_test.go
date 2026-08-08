package reconstruct

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
)

// These tests pin the #698 S3 read hooks the way integrity_hook_test.go pins
// the local ones: the WIRING, not just the validator — a corrupt s3:// object
// must fail loud with ErrIntegrity from the real production entry points.
// Both hooks run BEFORE DuckDB's httpfs is loaded, so stubbing
// baselineintegrity.OpenS3Object is enough: no S3, no network, no extension
// install. Only the corrupt path is exercised here (the clean path would
// proceed into a real S3 read); the validator's clean path is covered by the
// baselineintegrity unit tests.
//
// Each test uses a unique bucket: the verdict cache is keyed by full path and
// survives across calls within the process.

func stubS3Corrupt(t *testing.T, bucket string) string {
	t.Helper()
	crc := fmt.Sprintf("%08x", crc32.Checksum([]byte("original bytes"), crc32.MakeTable(crc32.Castagnoli)))
	manifest, err := json.Marshal(baselineintegrity.Manifest{
		Version: 1, Algo: "crc32c", Files: map[string]string{"db/orders.parquet": crc},
	})
	if err != nil {
		t.Fatal(err)
	}
	objects := map[string][]byte{
		bucket + "/base/20260101000000/" + baselineintegrity.ManifestName: manifest,
		bucket + "/base/20260101000000/db/orders.parquet":                 []byte("CORRUPTED bytes — bit-rot"),
	}
	orig := baselineintegrity.OpenS3Object
	baselineintegrity.OpenS3Object = func(_ context.Context, b, k string) (io.ReadCloser, error) {
		data, ok := objects[b+"/"+k]
		if !ok {
			return nil, errors.New("unexpected key " + b + "/" + k)
		}
		return io.NopCloser(bytes.NewReader(data)), nil
	}
	t.Cleanup(func() { baselineintegrity.OpenS3Object = orig })
	return "s3://" + bucket + "/base/20260101000000/db/orders.parquet"
}

func TestReadBaselineRows_s3ValidatesIntegrity(t *testing.T) {
	path := stubS3Corrupt(t, "wire-rbr")
	if _, err := ReadBaselineRows(context.Background(), path, nil, 1); !errors.Is(err, baselineintegrity.ErrIntegrity) {
		t.Errorf("a corrupt S3 baseline must fail loud with ErrIntegrity, got %v", err)
	}
}

func TestMaterializeBaselineLocal_s3ValidatesIntegrity(t *testing.T) {
	path := stubS3Corrupt(t, "wire-mbl")
	if _, _, err := materializeBaselineLocal(context.Background(), path, duckdbutil.Tuning{}); !errors.Is(err, baselineintegrity.ErrIntegrity) {
		t.Errorf("a corrupt S3 baseline must fail loud with ErrIntegrity before the COPY re-encode, got %v", err)
	}
}
