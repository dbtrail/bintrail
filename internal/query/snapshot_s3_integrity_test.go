package query

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

// TestFetchSnapshot_s3ValidatesIntegrity pins the #698 S3 read hook on the
// third baseline read path, mirroring TestFetchSnapshot_validatesIntegrity:
// `query --include-snapshot` over an s3:// baseline must fail loud on a CRC
// mismatch. The hook runs before httpfs is loaded, so a stubbed
// baselineintegrity.OpenS3Object suffices — no S3, no network.
func TestFetchSnapshot_s3ValidatesIntegrity(t *testing.T) {
	crc := fmt.Sprintf("%08x", crc32.Checksum([]byte("original bytes"), crc32.MakeTable(crc32.Castagnoli)))
	manifest, err := json.Marshal(baselineintegrity.Manifest{
		Version: 1, Algo: "crc32c", Files: map[string]string{"shop/orders.parquet": crc},
	})
	if err != nil {
		t.Fatal(err)
	}
	objects := map[string][]byte{
		"wire-snap/base/20260101000000/" + baselineintegrity.ManifestName: manifest,
		"wire-snap/base/20260101000000/shop/orders.parquet":               []byte("CORRUPTED bytes — bit-rot"),
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

	_, err = FetchSnapshot(context.Background(),
		"s3://wire-snap/base/20260101000000/shop/orders.parquet",
		Options{Schema: "shop", Table: "orders"})
	if !errors.Is(err, baselineintegrity.ErrIntegrity) {
		t.Errorf("query --include-snapshot must fail loud on a corrupt S3 baseline, got %v", err)
	}
}
