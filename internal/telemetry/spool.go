package telemetry

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	spoolDirName = "telemetry-spool"
	spoolSuffix  = ".ndjson"
	claimedMark  = ".claimed-"

	// maxSpoolFileBytes caps a single day's spool. A box that never reaches the
	// network must not grow unbounded; past the cap events are dropped, which is
	// the correct tradeoff for aggregate usage data.
	maxSpoolFileBytes = 5 << 20 // 5 MiB

	// maxSpoolAge bounds how long an undelivered event lingers on disk.
	maxSpoolAge = 7 * 24 * time.Hour

	// claimReclaimAfter is how long a claimed file may sit untouched before a
	// later run adopts it. It only ever fires for a drainer that died
	// mid-flight — which is the COMMON case, not an exotic one: a sub-100ms
	// command's process exits while its detached drain goroutine is still in
	// its HTTP call. Comfortably longer than drainDeadline, so it can never
	// steal a file from a drainer that is genuinely still working.
	claimReclaimAfter = 5 * time.Minute
)

// SpoolDir returns the spool directory inside dir.
func SpoolDir(dir string) string { return filepath.Join(dir, spoolDirName) }

// appendEvent writes one NDJSON line. Plain O_APPEND with no fsync: this runs
// at the end of every command, and an fsync per invocation is exactly the kind
// of hot-path tax telemetry must never impose. A single write of a short line
// is atomic enough that concurrent appenders interleave whole lines rather
// than corrupting each other.
//
// There is deliberately NO network call on this path, ever.
func appendEvent(spoolDir string, e Event, now time.Time) error {
	line, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal telemetry event: %w", err)
	}
	if err := os.MkdirAll(spoolDir, 0o700); err != nil {
		return fmt.Errorf("create spool directory: %w", err)
	}
	path := filepath.Join(spoolDir, now.UTC().Format("2006-01-02")+spoolSuffix)

	if fi, err := os.Stat(path); err == nil && fi.Size() >= maxSpoolFileBytes {
		return nil // over cap: drop rather than grow
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open spool file: %w", err)
	}
	defer f.Close()
	if _, err := f.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("append telemetry event: %w", err)
	}
	return nil
}

// drain delivers every spooled file and removes it, whether or not the send
// succeeded.
//
// Concurrency is handled by claim-by-rename: the drainer atomically renames a
// spool file to a unique name and works on that immutable snapshot. Two
// drainers racing (a cron run and a human run sharing $HOME) cannot both claim
// the same file, so no event is sent twice and none is lost to a
// read-then-truncate window. No lock is held across network I/O.
//
// Delivery failures drop the batch rather than retrying: an offline box must
// not accumulate a backlog it will one day flush all at once, and losing
// aggregate usage counts costs nothing worth a retry queue.
func drain(spoolDir string, now time.Time, send func([]byte) error) {
	entries, err := os.ReadDir(spoolDir)
	if err != nil {
		return // no spool yet, or unreadable — nothing to do, never an error
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		path := filepath.Join(spoolDir, name)
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if now.Sub(info.ModTime()) > maxSpoolAge {
			os.Remove(path)
			continue
		}

		var claimed string
		switch {
		case strings.Contains(name, claimedMark):
			// Left behind by a drainer that died mid-flight. Adopt it once it is
			// old enough that no live drainer could still be working on it.
			if now.Sub(info.ModTime()) < claimReclaimAfter {
				continue
			}
			claimed = path
		case strings.HasSuffix(name, spoolSuffix):
			claimed = path + claimedMark + fmt.Sprintf("%d-%d", os.Getpid(), now.UnixNano())
			if err := os.Rename(path, claimed); err != nil {
				continue // another drainer claimed it first
			}
		default:
			continue
		}

		data, readErr := os.ReadFile(claimed)
		if readErr != nil || len(data) == 0 {
			os.Remove(claimed)
			continue
		}
		sendErr := send(data)
		// Removed only AFTER the send attempt: if this process exits mid-send
		// the file survives as a claim and a later run adopts it, instead of the
		// events vanishing into a delete that already happened.
		os.Remove(claimed)
		if sendErr != nil {
			// Drop-on-fail by design — no retry queue, so an offline box never
			// builds a backlog it flushes all at once. Stop here rather than
			// hammering an endpoint that just refused us.
			return
		}
	}
}
