package baseline

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// Upload walks outputDir and uploads every file to the S3 URL, preserving the
// relative directory structure under the prefix. region is optional — if empty,
// the AWS SDK resolves it from AWS_REGION or ~/.aws/config. When retry is true,
// files that already exist in S3 are skipped (checked via HeadObject). Returns
// the number of files uploaded.
//
// The upload mirrors the local Run marker contract (#467) so a mid-upload death
// leaves a snapshot that S3 discovery treats as INCOMPLETE, not complete:
//
//  1. _INCOMPLETE FIRST, per snapshot dir (a zero-byte object — the local
//     _INCOMPLETE marker was already removed once Run succeeded, so there is no
//     local file to walk).
//  2. every data file.
//  3. _SUCCESS LAST. "_SUCCESS" can sort before sibling schema dirs depending
//     on the database name's first byte ('_' is 0x5F — before lowercase letters
//     but after digits and uppercase), so a single-pass lexical WalkDir could
//     publish it before all data is up. We defer it UNCONDITIONALLY, which keeps
//     the S3 snapshot un-marked-complete until its data is fully present.
//  4. best-effort _INCOMPLETE delete. s3IncompleteSnapshots only flags a
//     snapshot incomplete when _INCOMPLETE is present AND _SUCCESS is absent, so
//     a leftover _INCOMPLETE next to a published _SUCCESS is harmless — a failed
//     delete never demotes a completed snapshot.
//
// Extracted from cmd/bintrail (#613) so the bintrail-console daemon can run the
// dump→convert→upload pipeline in-process, without a docker socket.
func Upload(ctx context.Context, outputDir, s3URL, region string, retry bool) (int, error) {
	bucket, prefix, err := storage.ParseS3URL(s3URL)
	if err != nil {
		return 0, fmt.Errorf("invalid upload URL: %w", err)
	}

	client, err := storage.NewS3Client(ctx, region)
	if err != nil {
		return 0, err
	}

	// Route the four S3 operations through an injectable seam so the ordering
	// invariant can be unit-tested with a recording mock (#524 review).
	ops := s3UploadOps{
		putEmpty: func(ctx context.Context, key string) error { return storage.PutEmptyObject(ctx, client, bucket, key) },
		uploadFile: func(ctx context.Context, path, key string) error {
			return storage.UploadFile(ctx, client, path, bucket, key)
		},
		objectExists: func(ctx context.Context, key string) (bool, error) {
			return storage.S3ObjectExists(ctx, client, bucket, key)
		},
		deleteObject: func(ctx context.Context, key string) error { return storage.DeleteObject(ctx, client, bucket, key) },
	}
	return uploadWithOps(ctx, outputDir, prefix, retry, ops)
}

// s3UploadOps abstracts the four S3 operations the baseline upload performs, so
// the crash-safe ordering invariant (_INCOMPLETE first → data files → _SUCCESS
// last → best-effort _INCOMPLETE delete) can be pinned by a recording mock
// without a live client (#524 review).
type s3UploadOps struct {
	putEmpty     func(ctx context.Context, key string) error
	uploadFile   func(ctx context.Context, path, key string) error
	objectExists func(ctx context.Context, key string) (bool, error)
	deleteObject func(ctx context.Context, key string) error
}

// uploadWithOps performs the crash-safe upload ordering against ops. See the
// Upload doc for the four-step contract it guarantees.
func uploadWithOps(ctx context.Context, outputDir, prefix string, retry bool, ops s3UploadOps) (int, error) {
	upload := func(path string) error {
		key, err := storage.BuildS3Key(outputDir, path, prefix)
		if err != nil {
			return err
		}
		if retry {
			exists, err := ops.objectExists(ctx, key)
			if err != nil {
				return err
			}
			if exists {
				slog.Info("skipping existing S3 object (--retry)", "key", key)
				return nil
			}
		}
		if err := ops.uploadFile(ctx, path, key); err != nil {
			return err
		}
		slog.Debug("uploaded", "file", path, "key", key)
		return nil
	}

	// Snapshot dirs to upload, identified by a local _SUCCESS marker — only
	// completed snapshots reach here post-Run. Each one's _INCOMPLETE marker is
	// keyed off the snapshot dir, NOT off a walked file (Run already removed it).
	snapDirs, err := snapshotDirsWithSuccess(outputDir)
	if err != nil {
		return 0, err
	}
	// Steps 1 and 4 are the only readers of snapDirs; the walk that uploads the
	// data and the deferred _SUCCESS are not gated on it. So an empty snapDirs
	// does not upload nothing — it uploads EVERYTHING except the crash-safety
	// bracket, and a remote snapshot carrying neither marker is complete by
	// default (#467). An upload interrupted at three tables of twelve would
	// then be discoverable and readable and wrong.
	//
	// Every caller reaches here right after a successful Run or fold, so a
	// completed snapshot is always present and this costs them nothing. It is
	// the assertion they were already relying on, now stated.
	if len(snapDirs) == 0 {
		return 0, fmt.Errorf("refusing to upload %q: no completed snapshot was found in it or under it, so the "+
			"%s marker cannot be written and an interrupted upload would read as a complete backup",
			outputDir, IncompleteMarker)
	}
	incompleteKey := func(snapDir string) (string, error) {
		return storage.BuildS3Key(outputDir, filepath.Join(snapDir, IncompleteMarker), prefix)
	}

	// 1. Publish _INCOMPLETE FIRST so an interrupted upload reads as incomplete.
	for _, snapDir := range snapDirs {
		key, err := incompleteKey(snapDir)
		if err != nil {
			return 0, err
		}
		if err := ops.putEmpty(ctx, key); err != nil {
			return 0, err
		}
	}

	// 2 & 3. Upload data files; defer the _SUCCESS marker(s) to the very end.
	var count int
	var successMarkers []string
	err = filepath.WalkDir(outputDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return walkErr
		}
		// The baselines root's `current` pointer (see pointer.go) is a symlink
		// to a directory. WalkDir does not follow symlinks, so it arrives here
		// with IsDir() false and would be handed to the file uploader, which
		// opens the path, follows it, and fails with "is a directory" — taking
		// the whole upload down. It is a local convenience that means nothing
		// in S3, so skip it by name.
		if isPointerArtifact(outputDir, path, d) || isPointerLock(outputDir, path) {
			return nil
		}
		// Every OTHER non-regular entry is RESOLVED, not skipped. An operator
		// who symlinks one large table's Parquet onto another volume had it
		// uploaded correctly before the pointer existed, and must keep having
		// it: skipping silently would publish _SUCCESS over a snapshot missing
		// a table, and the loss would surface mid-recovery. A link to a
		// directory stays a refusal, as it always was, but now says so.
		if !d.Type().IsRegular() {
			info, serr := os.Stat(path) // follows the link
			if serr != nil {
				return fmt.Errorf("cannot resolve %s in the snapshot: %w "+
					"(refusing to publish this snapshot as complete while a file it holds is unreadable)", path, serr)
			}
			if !info.Mode().IsRegular() {
				return fmt.Errorf("%s in the snapshot resolves to a %s, not a file; "+
					"refusing to publish the snapshot as complete while it cannot be uploaded whole", path, info.Mode().Type())
			}
		}
		if d.Name() == SuccessMarker {
			successMarkers = append(successMarkers, path) // defer to the end
			return nil
		}
		if err := upload(path); err != nil {
			return err
		}
		count++
		return nil
	})
	if err != nil {
		return count, err
	}
	for _, path := range successMarkers {
		if err := upload(path); err != nil {
			return count, err
		}
		count++
	}

	// 4. Best-effort _INCOMPLETE cleanup — harmless to leave (see the func doc).
	for _, snapDir := range snapDirs {
		key, err := incompleteKey(snapDir)
		if err != nil {
			slog.Warn("could not build _INCOMPLETE marker key for cleanup", "snapshot", snapDir, "error", err)
			continue
		}
		if err := ops.deleteObject(ctx, key); err != nil {
			slog.Warn("could not remove S3 _INCOMPLETE marker after upload (harmless; _SUCCESS decides completeness)",
				"key", key, "error", err)
		}
	}
	return count, nil
}

// snapshotDirsWithSuccess returns the completed snapshot directories under
// outputDir. The baseline layout is <output>/<timestamp>/..., so only one
// level of children is scanned.
//
// outputDir may also BE a single snapshot directory, which is how the
// scheduled refresh uploads the one snapshot it just folded (#1539) instead of
// re-walking every snapshot the server ever wrote. Without this branch that
// call found no snapshot, so steps 1 and 4 silently did nothing: the data and
// _SUCCESS still landed in the right keys, and only the crash-safety marker
// went missing — an interrupted upload would then read as COMPLETE, because a
// snapshot with neither marker is complete-by-default (#467).
func snapshotDirsWithSuccess(outputDir string) ([]string, error) {
	switch _, err := os.Stat(filepath.Join(outputDir, SuccessMarker)); {
	case err == nil:
		return []string{outputDir}, nil
	case !errors.Is(err, fs.ErrNotExist):
		// Anything but "it is not there" is a real IO answer, and swallowing it
		// would send a single-snapshot upload down the children scan, which
		// finds no snapshot and reports none — the markerless upload the
		// caller's guard exists to refuse.
		return nil, fmt.Errorf("look for the %s marker in %q: %w", SuccessMarker, outputDir, err)
	}
	entries, err := os.ReadDir(outputDir)
	if err != nil {
		return nil, fmt.Errorf("read output directory %q: %w", outputDir, err)
	}
	var dirs []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		snapDir := filepath.Join(outputDir, e.Name())
		if _, err := os.Stat(filepath.Join(snapDir, SuccessMarker)); err == nil {
			dirs = append(dirs, snapDir)
		}
	}
	return dirs, nil
}

// isPointerArtifact reports whether path is the baselines root's `current`
// pointer or a staging link left by an interrupted publish (see pointer.go).
// Both are symlinks directly under the root, both are local conveniences that
// mean nothing in S3, and neither is part of any snapshot.
//
// The staging half matters as much as the pointer: a crash between the symlink
// and the rename leaves a dangling `.current.tmp.<pid>.<nanos>`, and treating
// that as a broken snapshot file would make every later upload refuse until an
// operator found and deleted a hidden link they never created.
func isPointerArtifact(root, path string, d fs.DirEntry) bool {
	if d.Type()&fs.ModeSymlink == 0 || filepath.Dir(path) != filepath.Clean(root) {
		return false
	}
	name := d.Name()
	return name == CurrentLinkName || strings.HasPrefix(name, currentLinkTmp)
}

// isPointerLock reports whether path is the pointer's flock file. Unlike the
// pointer and its staging links it is a REGULAR file directly under the root,
// so the symlink test above cannot see it and it would otherwise be uploaded as
// snapshot data. It is a local mutex; it means nothing in S3.
func isPointerLock(root, path string) bool {
	return filepath.Dir(path) == filepath.Clean(root) && filepath.Base(path) == pointerLockName
}
