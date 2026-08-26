package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/spf13/cobra"
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
	savedNoBaselines := vNoBaselines
	t.Cleanup(func() {
		for i, p := range saved {
			*p = vals[i]
		}
		vNoBaselines = savedNoBaselines
	})
	vIndexDSN, vBaselineDir, vBaselineS3 = "", "", ""
	vArchiveDir, vArchiveS3, vBintrailID = t.TempDir(), "s3://bkt/events/", "aaaa"
	vNoBaselines, vOut = true, "-"

	var out bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&out)
	cmd.SetContext(context.Background())
	if err := runViews(cmd, nil); err != nil {
		t.Fatal(err)
	}
	sql := out.String()
	if strings.Contains(sql, "listed by its S3 location") {
		t.Errorf("explicitly named roots got the registry routing sentence:\n%s", sql)
	}
	for _, want := range []string{"bintrail_id=aaaa/event_date=*", "s3://bkt/events/bintrail_id=aaaa/event_date=*"} {
		if !strings.Contains(sql, want) {
			t.Errorf("a named root is missing (%q):\n%s", want, sql)
		}
	}
}
