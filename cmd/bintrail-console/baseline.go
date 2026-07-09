package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
)

// baselineSupervisor implements console.BaselineController by running the
// dump→convert→upload pipeline IN-PROCESS (#613): the console image bundles
// mydumper, so a baseline never starts a sibling container and the daemon never
// mounts the docker socket. One job at a time per server, tracked in-memory —
// the durable record is the snapshot itself (listed by /api/baselines).
type baselineSupervisor struct {
	ctx        context.Context // daemon lifecycle; cancels an in-flight dump on shutdown
	stagingDir string          // base dir for temp dump + staged Parquet (S3-destined runs)

	mu   sync.Mutex
	jobs map[string]*console.BaselineStatus
}

// newBaselineSupervisor builds a supervisor bound to the daemon context. The
// staging dir is created lazily per run.
func newBaselineSupervisor(ctx context.Context, stagingDir string) *baselineSupervisor {
	return &baselineSupervisor{
		ctx:        ctx,
		stagingDir: stagingDir,
		jobs:       make(map[string]*console.BaselineStatus),
	}
}

// Trigger starts a baseline in the background; returns console.ErrBaselineRunning
// if one is already in flight for this server.
func (s *baselineSupervisor) Trigger(req console.BaselineRequest) error {
	s.mu.Lock()
	if st, ok := s.jobs[req.ServerID]; ok && st.State == "running" {
		s.mu.Unlock()
		return console.ErrBaselineRunning
	}
	s.jobs[req.ServerID] = &console.BaselineStatus{State: "running", Since: nowStamp()}
	s.mu.Unlock()

	slog.Info("baseline: starting in-process snapshot", "server", req.ServerName, "id", req.ServerID)
	go s.run(req)
	return nil
}

// Status returns a copy of the latest known job state (idle if never run here).
func (s *baselineSupervisor) Status(serverID string) console.BaselineStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.jobs[serverID]; ok {
		return *st
	}
	return console.BaselineStatus{State: "idle"}
}

func (s *baselineSupervisor) run(req console.BaselineRequest) {
	stats, uploaded, err := s.execute(req)

	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.jobs[req.ServerID]
	if st == nil { // defensive: never overwritten away under lock, but don't panic
		st = &console.BaselineStatus{}
		s.jobs[req.ServerID] = st
	}
	st.FinishedAt = nowStamp()
	if err != nil {
		st.State = "failed"
		st.LastError = err.Error()
		slog.Error("baseline: snapshot failed", "server", req.ServerName, "id", req.ServerID, "error", err)
		return
	}
	st.State = "succeeded"
	st.LastError = ""
	st.Tables = stats.TablesProcessed
	st.Rows = stats.RowsWritten
	st.Uploaded = uploaded
	slog.Info("baseline: snapshot complete", "server", req.ServerName, "id", req.ServerID,
		"tables", stats.TablesProcessed, "rows", stats.RowsWritten, "uploaded", uploaded)
}

// execute runs the full pipeline: mydumper → baseline.Run → (S3) baseline.Upload.
// For a local-dir destination the Parquet is written there persistently and not
// uploaded; for an S3 destination it is staged under a fresh temp dir, uploaded,
// and the staging removed (so a re-run never re-uploads an old snapshot).
func (s *baselineSupervisor) execute(req console.BaselineRequest) (baseline.Stats, int, error) {
	if err := os.MkdirAll(s.stagingDir, 0o755); err != nil {
		return baseline.Stats{}, 0, fmt.Errorf("create staging dir: %w", err)
	}

	dumpDir, err := os.MkdirTemp(s.stagingDir, "dump-")
	if err != nil {
		return baseline.Stats{}, 0, fmt.Errorf("create dump dir: %w", err)
	}
	defer os.RemoveAll(dumpDir)

	// Captured immediately before invoking mydumper: since this pipeline runs
	// mydumper and baseline.Run in the same process, we can pass our own UTC
	// wall-clock time straight through as the snapshot anchor instead of
	// letting baseline.Run re-parse mydumper's "Started dump at" metadata
	// line — which is written in the dump host's LOCAL time and would
	// otherwise be misread as UTC verbatim, skewing the replay window by the
	// host's UTC offset (#768).
	dumpStartedAt := time.Now().UTC()
	if err := runMydumper(s.ctx, req.SourceDSN, req.Schemas, dumpDir); err != nil {
		return baseline.Stats{}, 0, fmt.Errorf("dump: %w", err)
	}

	outputDir := req.LocalDir
	if outputDir == "" { // S3-only: stage then upload, discard staging
		outputDir, err = os.MkdirTemp(s.stagingDir, "baseline-")
		if err != nil {
			return baseline.Stats{}, 0, fmt.Errorf("create baseline staging dir: %w", err)
		}
		defer os.RemoveAll(outputDir)
	}

	stats, err := baseline.Run(s.ctx, baseline.Config{
		InputDir:    dumpDir,
		OutputDir:   outputDir,
		Compression: "zstd",
		Timestamp:   dumpStartedAt,
	})
	if err != nil {
		return baseline.Stats{}, 0, fmt.Errorf("convert: %w", err)
	}

	var uploaded int
	if req.S3 != "" {
		// Region/credentials come from the ambient AWS chain (env / ~/.aws / IAM
		// role), like every other S3 read the console does.
		uploaded, err = baseline.Upload(s.ctx, outputDir, req.S3, "", false)
		if err != nil {
			return baseline.Stats{}, 0, fmt.Errorf("upload: %w", err)
		}
	}
	return stats, uploaded, nil
}

// runMydumper invokes the bundled mydumper binary against the source DSN, writing
// a consistent dump (with binlog coordinates in its metadata, which baseline.Run
// reads) into dumpDir. The image pins the SAME mydumper version the compose
// baseline-dump pipeline uses, so a console-created baseline matches a CLI/compose
// one exactly.
func runMydumper(ctx context.Context, sourceDSN string, schemas []string, dumpDir string) error {
	host, port, user, password, err := config.ParseSourceDSN(sourceDSN)
	if err != nil {
		return err
	}

	args := buildConsoleMydumperArgs(host, port, user, schemas, dumpDir)
	cmd := exec.CommandContext(ctx, "mydumper", args...)
	// Deliver the source password out of band via MYSQL_PWD (honored by the
	// MySQL client library mydumper links against) so it never lands on argv,
	// where it would be world-readable in `ps aux` / /proc/<pid>/cmdline. The
	// child's /proc/<pid>/environ is mode 0400 (#811).
	if password != "" {
		cmd.Env = append(os.Environ(), "MYSQL_PWD="+password)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		if msg := strings.TrimSpace(string(out)); msg != "" {
			return fmt.Errorf("mydumper failed: %w; output: %s", err, msg)
		}
		return fmt.Errorf("mydumper failed: %w", err)
	}
	return nil
}

// systemSchemaExcludeRegex dumps every USER schema but excludes the MySQL system
// schemas, matching the compose baseline-dump pipeline (#612). A least-privilege
// capture user (REPLICATION + SELECT, no SHOW VIEW) cannot read the sys views, so
// an unfiltered mydumper dies with "SHOW VIEW command denied … sys.host_summary";
// the system schemas are useless as a baseline anyway. mydumper uses PCRE, so the
// negative lookahead drops a system db both bare and as <db>.<table>.
const systemSchemaExcludeRegex = `^(?!(mysql|sys|performance_schema|information_schema)($|\.))`

// buildConsoleMydumperArgs builds the mydumper argument slice for the console's
// in-process dump. It mirrors `bintrail dump` / the compose baseline-dump
// invocation: a CONSISTENT lock-free snapshot (--sync-thread-lock-mode NO_LOCK
// --trx-tables — no global FTWRL, so a least-privilege replication user WITHOUT
// RELOAD/FLUSH_TABLES can dump; verified against a real Percona 8.0 source).
// Schema selection: single → --database; multiple → an anchored --regex; none →
// every user schema with the system schemas excluded. Extracted for unit testing
// without a live mydumper.
//
// The source password is NEVER placed on argv (world-readable via `ps aux` /
// /proc/<pid>/cmdline); runMydumper delivers it via MYSQL_PWD in the child env
// (#811).
func buildConsoleMydumperArgs(host string, port uint16, user string, schemas []string, dumpDir string) []string {
	args := []string{
		"--host", host,
		"--port", strconv.Itoa(int(port)),
		"--user", user,
		"--threads", "4",
		"--compress-protocol",
		"--complete-insert",
		"--sync-thread-lock-mode", "NO_LOCK", "--trx-tables",
	}
	switch {
	case len(schemas) == 1:
		args = append(args, "--database", schemas[0])
	case len(schemas) > 1:
		args = append(args, "--regex", "^("+strings.Join(schemas, "|")+")\\.")
	default:
		args = append(args, "--regex", systemSchemaExcludeRegex)
	}
	// --outputdir last: docker wrapper scripts read the last arg for the mount.
	args = append(args, "--outputdir", dumpDir)
	return args
}

// nowStamp is the RFC3339 timestamp used in job status fields.
func nowStamp() string { return time.Now().UTC().Format(time.RFC3339) }
