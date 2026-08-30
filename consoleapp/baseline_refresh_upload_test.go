package consoleapp

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// uploadCall records one upload the loop asked for.
type uploadCall struct{ outputDir, dest string }

// stubS3Fold points the fold's listing at a bucket and records what the loop
// asked to upload, so a whole refresh can run for an S3-backed server without
// a bucket. Only s3:// sources are answered here; a local source still runs
// the real listing, which is what the on-disk cases depend on.
func stubS3Fold(t *testing.T, bucketTables []string, uploadErr error) (*[]uploadCall, *[]string) {
	t.Helper()
	realList, realUpload := newestSnapshotTables, uploadSnapshot
	t.Cleanup(func() { newestSnapshotTables, uploadSnapshot = realList, realUpload })

	var listed []string
	newestSnapshotTables = func(ctx context.Context, src string) ([]string, error) {
		if !strings.HasPrefix(src, "s3://") {
			return realList(ctx, src)
		}
		listed = append(listed, src)
		return bucketTables, nil
	}
	var uploads []uploadCall
	uploadSnapshot = func(_ context.Context, outputDir, dest, _ string, _ bool) (int, error) {
		uploads = append(uploads, uploadCall{outputDir, dest})
		return 1, uploadErr
	}
	return &uploads, &listed
}

// runRefreshFor drives one whole refresh for a request and returns what it
// logged at Warn.
func runRefreshFor(t *testing.T, req refreshRequest) string {
	t.Helper()
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	sup.refreshes[req.ServerID] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(req, refreshAt, time.Minute)
	return buf.String()
}

// The whole of #1539 end to end: on a server whose backups go to S3, one
// scheduled refresh reads its previous snapshot from the BUCKET, folds into
// the local directory, and sends the result to the destination a full backup
// would have written to.
//
// The local directory is staged EMPTY on purpose. That is the real shape of an
// S3-backed server the daemon has not folded for yet — every backup it has was
// uploaded — and it is the shape that used to make this refuse with "no
// baseline snapshot to refresh" while the console listed dozens.
func TestRunRefresh_S3BackedServerFoldsFromTheBucketAndUploads(t *testing.T) {
	local := t.TempDir()
	uploads, listed := stubS3Fold(t, []string{"shop.orders"}, nil)
	injectFold(t, 0, nil)

	out := runRefreshFor(t, refreshRequest{
		ServerID: "s", ServerName: "s", IndexDSN: "d",
		BaselineDir: local, BaselineS3: "s3://bucket/backups/",
	})

	if out != "" {
		t.Fatalf("the refresh warned: %s", out)
	}
	if len(*listed) != 1 || (*listed)[0] != "s3://bucket/backups/" {
		t.Fatalf("looked for the previous snapshot in %v, want the bucket once", *listed)
	}
	stamp := reconstruct.SnapshotDirName(refreshAt)
	want := uploadCall{filepath.Join(local, stamp), "s3://bucket/backups/" + stamp}
	if len(*uploads) != 1 || (*uploads)[0] != want {
		t.Fatalf("uploads = %+v, want exactly %+v", *uploads, want)
	}
}

// The daemon-wide --baseline-refresh-interval names no destination, so its
// requests carry no S3 and nothing is uploaded. Keeping that untouched is the
// point of gating on the request's own field rather than on a mode: the flag
// documents that its snapshots stay local.
func TestRunRefresh_withoutAnS3DestinationUploadsNothing(t *testing.T) {
	uploads, _ := stubS3Fold(t, nil, nil)
	injectFold(t, 0, nil)

	out := runRefreshFor(t, refreshRequest{
		ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: stageBaselineRoot(t),
	})

	if out != "" {
		t.Fatalf("the refresh warned: %s", out)
	}
	if len(*uploads) != 0 {
		t.Fatalf("uploaded %+v, want nothing", *uploads)
	}
}

// A refresh is not published until the snapshot is where this server's backups
// live, so a failed upload fails the run. Two things must survive that: the
// local snapshot (it is finished, and it is the operator's whole remaining
// result), and a report that says the fold worked and the sending did not.
func TestRunRefresh_failedUploadFailsTheRunAndKeepsTheSnapshot(t *testing.T) {
	local := t.TempDir()
	_, _ = stubS3Fold(t, []string{"shop.orders"}, errors.New("AccessDenied"))
	injectFold(t, 0, nil)

	out := runRefreshFor(t, refreshRequest{
		ServerID: "s", ServerName: "s", IndexDSN: "d",
		BaselineDir: local, BaselineS3: "s3://bucket/backups/",
	})

	if !strings.Contains(out, "AccessDenied") {
		t.Fatalf("the failed upload was not reported: %s", out)
	}
	if !strings.Contains(out, "only sending it to the backup destination failed") {
		t.Fatalf("the report does not say the fold itself worked: %s", out)
	}
	snap := filepath.Join(local, reconstruct.SnapshotDirName(refreshAt))
	if _, err := os.Stat(filepath.Join(snap, baseline.SuccessMarker)); err != nil {
		t.Fatalf("the finished snapshot was reclaimed after the upload failed: %v", err)
	}
}
