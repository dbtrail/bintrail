package cli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// TestRunViews_explicitRootsCarryNoRoutingSentence pins the producer side of
// the header's honesty (#1456): roots the operator named are listed as named,
// both of them when both flags are passed, and the file must not claim the
// S3-over-local routing that only registry discovery applies. A future edit
// that sets PortableRouting unconditionally in runViews fails here.
func TestRunViews_explicitRootsCarryNoRoutingSentence(t *testing.T) {
	saved := []*string{&vIndexDSN, &vArchiveDir, &vArchiveS3, &vBintrailID, &vBaselineDir, &vBaselineS3, &vOut}
	vals := make([]string, len(saved))
	for i, p := range saved {
		vals[i] = *p
	}
	savedNoBaselines, savedIncludeEvents := vNoBaselines, vIncludeEvents
	t.Cleanup(func() {
		for i, p := range saved {
			*p = vals[i]
		}
		vNoBaselines, vIncludeEvents = savedNoBaselines, savedIncludeEvents
	})
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv(storage.EnvS3Endpoint, "http://minio:9000")
	vIndexDSN, vBaselineDir, vBaselineS3 = "", "", ""
	vArchiveDir, vArchiveS3, vBintrailID = t.TempDir(), "s3://bkt/events/", "aaaa"
	// This test is about the ARCHIVE half, so it asks for the view that reads
	// it: --no-baselines alone now defines nothing and is refused.
	vNoBaselines, vIncludeEvents, vOut = true, true, "-"

	var out bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&out)
	cmd.SetContext(context.Background())
	if err := runViews(cmd, nil); err != nil {
		t.Fatal(err)
	}
	sql := out.String()
	if !strings.Contains(sql, "ENDPOINT 'minio:9000'") {
		// The producer half of #1453: the generator is tested with a populated
		// Input, so without this nothing pins that runViews fills it.
		t.Errorf("generated file does not name the configured store:\n%s", sql)
	}
	if strings.Contains(sql, "listed by its S3 location") {
		t.Errorf("explicitly named roots got the registry routing sentence:\n%s", sql)
	}
	for _, want := range []string{"bintrail_id=aaaa/event_date=*", "s3://bkt/events/bintrail_id=aaaa/event_date=*"} {
		if !strings.Contains(sql, want) {
			t.Errorf("a named root is missing (%q):\n%s", want, sql)
		}
	}
}

// The CLI half of the same rule: a purely local layout renders fine with a
// broken BINTRAIL_S3_ENDPOINT, because it never reads through httpfs. Before
// the endpoint was resolved lazily this exited 1 on a command that touches no
// S3 at all.
func TestRunViews_localOnlyLayoutIgnoresEndpointTypo(t *testing.T) {
	saved := []*string{&vIndexDSN, &vArchiveDir, &vArchiveS3, &vBintrailID, &vBaselineDir, &vBaselineS3, &vOut}
	vals := make([]string, len(saved))
	for i, p := range saved {
		vals[i] = *p
	}
	savedNoBaselines, savedIncludeEvents := vNoBaselines, vIncludeEvents
	t.Cleanup(func() {
		for i, p := range saved {
			*p = vals[i]
		}
		vNoBaselines, vIncludeEvents = savedNoBaselines, savedIncludeEvents
	})
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv(storage.EnvS3Endpoint, "minio:9000") // no scheme: rejected when it matters
	vIndexDSN, vArchiveS3, vBaselineDir, vBaselineS3 = "", "", "", ""
	vArchiveDir, vBintrailID = t.TempDir(), "aaaa"
	// This test is about the ARCHIVE half, so it asks for the view that reads
	// it: --no-baselines alone now defines nothing and is refused.
	vNoBaselines, vIncludeEvents, vOut = true, true, "-"

	var out bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&out)
	cmd.SetContext(context.Background())
	if err := runViews(cmd, nil); err != nil {
		t.Fatalf("a local-only render was refused over an S3 variable: %v", err)
	}
	if sql := out.String(); strings.Contains(sql, "s3://") || strings.Contains(sql, "httpfs") {
		t.Errorf("a layout treated as local reads S3 after all:\n%s", sql)
	}
}

// And it still refuses when the file DOES carry s3:// paths, where the
// alternative is a file that silently sends the reader's DuckDB to AWS.
func TestRunViews_invalidEndpointRefusedWhenS3IsInPlay(t *testing.T) {
	saved := []*string{&vIndexDSN, &vArchiveDir, &vArchiveS3, &vBintrailID, &vBaselineDir, &vBaselineS3, &vOut}
	vals := make([]string, len(saved))
	for i, p := range saved {
		vals[i] = *p
	}
	savedNoBaselines, savedIncludeEvents := vNoBaselines, vIncludeEvents
	t.Cleanup(func() {
		for i, p := range saved {
			*p = vals[i]
		}
		vNoBaselines, vIncludeEvents = savedNoBaselines, savedIncludeEvents
	})
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv(storage.EnvS3Endpoint, "minio:9000")
	vIndexDSN, vArchiveDir, vBaselineDir, vBaselineS3 = "", "", "", ""
	vArchiveS3, vBintrailID = "s3://bkt/events/", "aaaa"
	// This test is about the ARCHIVE half, so it asks for the view that reads
	// it: --no-baselines alone now defines nothing and is refused.
	vNoBaselines, vIncludeEvents, vOut = true, true, "-"

	var out bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&out)
	cmd.SetContext(context.Background())
	err := runViews(cmd, nil)
	if err == nil {
		t.Fatal("an S3 layout was rendered with an unusable endpoint: the file would describe AWS")
	}
	if !errors.Is(err, storage.ErrS3EndpointConfig) {
		t.Errorf("error does not wrap ErrS3EndpointConfig: %v", err)
	}
}
