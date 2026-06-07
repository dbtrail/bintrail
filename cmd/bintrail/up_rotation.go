package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/bintrail/internal/config"
)

// upRotationSettings is the parsed configuration of `up`'s built-in rotation —
// the loop that keeps an unattended index from growing until the volume fills
// and takes the forensic record with it (#420).
type upRotationSettings struct {
	enabled   bool
	retain    time.Duration
	retainRaw string
	interval  time.Duration
	addFuture int
}

// upRotationCfg carries the parsed settings from runUp's validation to the
// phase-3 start sites, following the package-global flag style of this file's
// neighbors (populateStreamFlags et al.).
var upRotationCfg upRotationSettings

// parseUpRotation validates the --rotate-* flag values. retain accepts the
// rotate command's Nd/Nh forms, plus "off", "0", or "" to disable the
// built-in rotation entirely.
func parseUpRotation(retain, interval string, addFuture int) (upRotationSettings, error) {
	switch retain {
	case "off", "0", "":
		return upRotationSettings{}, nil
	}
	dur, err := parseRetain(retain)
	if err != nil {
		return upRotationSettings{}, fmt.Errorf("--rotate-retain: %w (or \"off\" to disable)", err)
	}
	iv, err := time.ParseDuration(interval)
	if err != nil {
		return upRotationSettings{}, fmt.Errorf("--rotate-interval: %w", err)
	}
	if iv <= 0 {
		return upRotationSettings{}, fmt.Errorf("--rotate-interval must be positive, got %q", interval)
	}
	if addFuture < 0 {
		return upRotationSettings{}, fmt.Errorf("--rotate-add-future cannot be negative, got %d", addFuture)
	}
	return upRotationSettings{
		enabled:   true,
		retain:    dur,
		retainRaw: retain,
		interval:  iv,
		addFuture: addFuture,
	}, nil
}

// startUpRotation announces and launches the built-in rotation loop: one
// cycle immediately, then one every interval, each cycle rotating the boot
// index database plus every DSN the provider returns (the control plane's
// per-source databases). Rotation is the secondary job — failures are logged,
// never fatal to the stream. Returns immediately; the loop stops when ctx is
// cancelled.
func startUpRotation(ctx context.Context, s upRotationSettings, dsns func() []string) {
	if !s.enabled {
		fmt.Fprintln(os.Stderr, "Built-in rotation: off — the index grows until you rotate it yourself (see `bintrail rotate`)")
		slog.Info("built-in rotation disabled")
		return
	}
	fmt.Fprintf(os.Stderr,
		"Built-in rotation: dropping index partitions older than %s every %s, keeping %d future partitions ready.\n"+
			"  Tune with --rotate-retain / --rotate-interval (or BINTRAIL_ROTATE_RETAIN); disable with --rotate-retain off.\n",
		s.retainRaw, s.interval, s.addFuture)
	slog.Info("built-in rotation enabled",
		"retain", s.retainRaw, "interval", s.interval.String(), "add_future", s.addFuture)

	// performRotation reads the rot* package globals (the rotate command's
	// flag set). Fan them out once before the goroutine starts — same pattern
	// as populateStreamFlags; nothing else writes them in an `up` process.
	rotRetain = s.retainRaw
	rotAddFuture = s.addFuture
	rotNoReplace = false
	rotArchiveDir = ""
	rotArchiveS3 = ""
	rotBintrailID = ""
	rotRetry = false
	// "json" suppresses performRotation's per-partition stdout chatter (it
	// never emits JSON itself — that is runRotate's wrapper); slog carries
	// the per-cycle signal instead.
	rotFormat = "json"
	rotProtectUnarchived = true

	go func() {
		runUpRotationCycle(ctx, s, dsns)
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runUpRotationCycle(ctx, s, dsns)
			}
		}
	}()
}

// runUpRotationCycle rotates each index database once. Errors are logged and
// the cycle moves to the next DSN — a transient failure self-heals on the
// next tick.
func runUpRotationCycle(ctx context.Context, s upRotationSettings, dsns func() []string) {
	for _, dsn := range dedupeDSNs(dsns()) {
		rotateOneIndex(ctx, dsn, s.retain)
	}
}

// dedupeDSNs drops empty strings and duplicates, preserving order.
func dedupeDSNs(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, dsn := range in {
		if dsn == "" || seen[dsn] {
			continue
		}
		seen[dsn] = true
		out = append(out, dsn)
	}
	return out
}

// rotateOneIndex runs one performRotation cycle against a single index DSN.
// Log messages are scrubbed: DSNs (and their passwords) never reach the log.
func rotateOneIndex(ctx context.Context, dsn string, retain time.Duration) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil || cfg.DBName == "" {
		slog.Warn("built-in rotation: skipping index DSN without a database name")
		return
	}
	db, err := config.Connect(dsn)
	if err != nil {
		slog.Warn("built-in rotation: cannot connect to index database",
			"db", cfg.DBName, "error", scrubMonitorErrText(err.Error(), dsn))
		return
	}
	defer db.Close()
	if _, _, err := performRotation(ctx, db, cfg.DBName, retain); err != nil && ctx.Err() == nil {
		slog.Warn("built-in rotation cycle failed",
			"db", cfg.DBName, "error", scrubMonitorErrText(err.Error(), dsn))
	}
}
