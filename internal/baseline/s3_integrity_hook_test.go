package baseline

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
)

// TestReadParquetMetadataAny_s3ValidatesIntegrity pins the FOURTH #698 S3
// wiring site: the footer read (binlog anchor, CreateTableSQL) acts on the
// same object bytes the row paths validate, and in baseline-anchored `verify`
// the newest baseline's footer can be consumed without any row read of that
// object ever running — so deleting the ValidateS3File call here must turn a
// test red (the repo's mutation standard). The hook runs before the DuckDB
// open, so a stubbed OpenS3Object suffices — no S3, no network.
func TestReadParquetMetadataAny_s3ValidatesIntegrity(t *testing.T) {
	crc := fmt.Sprintf("%08x", crc32.Checksum([]byte("original bytes"), crc32.MakeTable(crc32.Castagnoli)))
	manifest, err := json.Marshal(baselineintegrity.Manifest{
		Version: 1, Algo: "crc32c", Files: map[string]string{"db/orders.parquet": crc},
	})
	if err != nil {
		t.Fatal(err)
	}
	objects := map[string][]byte{
		"wire-meta/base/20260101000000/" + baselineintegrity.ManifestName: manifest,
		"wire-meta/base/20260101000000/db/orders.parquet":                 []byte("CORRUPTED bytes — bit-rot"),
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

	_, err = ReadParquetMetadataAny(context.Background(),
		"s3://wire-meta/base/20260101000000/db/orders.parquet")
	if !errors.Is(err, baselineintegrity.ErrIntegrity) {
		t.Errorf("the footer read must fail loud on a corrupt S3 baseline, got %v", err)
	}
}
