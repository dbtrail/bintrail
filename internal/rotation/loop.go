package rotation

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/bintrail/internal/cliutil"
	"github.com/dbtrail/bintrail/internal/config"
	"github.com/dbtrail/bintrail/internal/indexer"
)

// Settings is the parsed configuration of `up`'s built-in rotation —
// the loop that keeps an unattended index from growing until the volume fills
// and takes the forensic record with it (#420).
type Settings struct {
	Enabled   bool
	Retain    time.Duration
	RetainRaw string
	Interval  time.Duration
	AddFuture int
	// Explicit records whether the operator configured --rotate-retain
	// themselves (flag or env — bindCommandEnv marks env-set flags Changed).
	// When false (running on the built-in default), the upgrade guard
	// refuses to drop pre-existing deep history: an operator who never chose
	// a retention must not lose months of forensic record to a binary
	// upgrade.
	Explicit bool
}

// ParseSettings validates the --rotate-* flag values and is the sole
// author of every Settings field. retain accepts the rotate
// command's Nd/Nh forms, plus "off", "0", or "" to disable the built-in
// rotation entirely. explicit is whether the operator set --rotate-retain
// themselves (cmd.Flags().Changed — true for flag and env alike).
func ParseSettings(retain, interval string, addFuture int, explicit bool) (Settings, error) {
	switch retain {
	case "off", "0", "":
		return Settings{}, nil
	}
	dur, err := cliutil.ParseRetain(retain)
	if err != nil {
		return Settings{}, fmt.Errorf("--rotate-retain: %w (or \"off\" to disable)", err)
	}
	iv, err := time.ParseDuration(interval)
	if err != nil {
		return Settings{}, fmt.Errorf("--rotate-interval: %w", err)
	}
	if iv <= 0 {
		return Settings{}, fmt.Errorf("--rotate-interval must be positive, got %q", interval)
	}
	if addFuture < 0 {
		return Settings{}, fmt.Errorf("--rotate-add-future cannot be negative, got %d", addFuture)
	}
	return Settings{
		Enabled:   true,
		Retain:    dur,
		RetainRaw: retain,
		Interval:  iv,
		AddFuture: addFuture,
		Explicit:  explicit,
	}, nil
}

// StartLoop announces and launches the built-in rotation loop: one
// cycle immediately, then one every interval, each cycle rotating the boot
// index database plus every DSN the provider returns (the control plane's
// per-source databases). Rotation is the secondary job — failures are logged,
// never fatal to the stream. Returns immediately; the loop stops when ctx is
// cancelled, and the returned channel closes when it has fully exited (used
// by tests for deterministic shutdown; production callers may ignore it).
func StartLoop(ctx context.Context, s Settings, dsns func() []string) <-chan struct{} {
	done := make(chan struct{})
	if !s.Enabled {
		fmt.Fprintln(os.Stderr, "Built-in rotation: off — the index grows until you rotate it yourself (see `bintrail rotate`)")
		slog.Info("built-in rotation disabled")
		close(done)
		return done
	}
	fmt.Fprintf(os.Stderr,
		"Built-in rotation: dropping index partitions older than %s every %s, keeping %d future partitions ready.\n"+
			"  Tune with --rotate-retain / --rotate-interval (or BINTRAIL_ROTATE_RETAIN); disable with --rotate-retain off.\n",
		s.RetainRaw, s.Interval, s.AddFuture)
	slog.Info("built-in rotation enabled",
		"retain", s.RetainRaw, "interval", s.Interval.String(), "add_future", s.AddFuture)

	go func() {
		defer close(done)
		// Consecutive-unhealthy-cycle streak for escalation: an hourly Warn
		// blends into noise, but a rotation that makes no progress it should
		// have — failing, deferring to a stalled archiving flow, or any
		// alternation of the two — for hours means the index is growing
		// unbounded, the exact condition this loop exists to detect. ONE
		// streak over (failed || deferred>0), not per-reason counters: split
		// counters each reset while the other condition fires, so an index
		// alternating defer/fail would never escalate.
		var unhealthyStreak int
		cycle := func() {
			// Rotation is the secondary job: a panic here must never take
			// down the stream (the primary forensic capture) — same
			// principle as the console goroutine in runUpStreamWithConsole.
			defer func() {
				if r := recover(); r != nil {
					slog.Error("built-in rotation cycle panicked; rotation continues next tick", "panic", r)
				}
			}()
			deferred, failed := runCycle(ctx, s, dsns)
			if failed || deferred > 0 {
				unhealthyStreak++
			} else {
				unhealthyStreak = 0
			}
			if unhealthyStreak >= escalateAfter {
				slog.Error("built-in rotation made no progress for consecutive cycles — the index is growing unbounded (rotation is failing and/or deferring unarchived partitions to a stalled archiving flow; archive the partitions, fix the failure, or set --rotate-retain off and rotate manually)",
					"consecutive_cycles", unhealthyStreak,
					"deferred_last_cycle", deferred,
					"failed_last_cycle", failed)
			}
		}
		cycle()
		ticker := time.NewTicker(s.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cycle()
			}
		}
	}()
	return done
}

// escalateAfter is how many consecutive unhealthy cycles (failed
// or deferring) the loop tolerates before escalating from Warn to Error. A
// var, not a const, so tests can shrink it.
var escalateAfter = 3

// runCycle rotates each index database once. Errors are logged and
// the cycle moves to the next DSN — a transient failure self-heals on the
// next tick. Returns the total number of partitions the protect-unarchived
// guard deferred this cycle, and whether any database's rotation failed.
func runCycle(ctx context.Context, s Settings, dsns func() []string) (deferred int, failed bool) {
	for _, dsn := range dedupeDSNs(dsns()) {
		d, err := rotateOneIndex(ctx, dsn, s)
		deferred += d
		if err != nil {
			failed = true
		}
	}
	return deferred, failed
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

// loopOptions builds the Perform Options for one built-in-rotation cycle.
// Built-in rotation never archives (ArchiveDir empty) — it drops and tops up
// only — and ALWAYS arms ProtectUnarchived so it can never be the first to
// destroy data an external archiving flow would preserve; Format "json"
// suppresses per-partition stdout (slog carries the per-cycle signal). The
// data-loss safety of `up`'s rotation reduces to this one constructor, so it is
// kept pure and unit-tested (it replaces the old rot*-global fan-out).
func loopOptions(retain time.Duration, s Settings) Options {
	return Options{
		RetainDur:         retain,
		RetainRaw:         s.RetainRaw,
		AddFuture:         s.AddFuture,
		NoReplace:         false,
		Format:            "json",
		ProtectUnarchived: true,
	}
}

// rotateOneIndex runs one Perform cycle against a single index DSN,
// returning the guard-deferred partition count and any failure. Log messages
// are scrubbed: DSNs (and their passwords) never reach the log.
func rotateOneIndex(ctx context.Context, dsn string, s Settings) (int, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		slog.Warn("built-in rotation: skipping unparseable index DSN",
			"error", config.ScrubDSNText(err.Error(), dsn))
		return 0, fmt.Errorf("parse index DSN: %w", err)
	}
	if cfg.DBName == "" {
		slog.Warn("built-in rotation: skipping index DSN without a database name")
		return 0, fmt.Errorf("index DSN without a database name")
	}
	db, err := config.Connect(dsn)
	if err != nil {
		slog.Warn("built-in rotation: cannot connect to index database",
			"db", cfg.DBName, "error", config.ScrubDSNText(err.Error(), dsn))
		return 0, err
	}
	defer db.Close()

	retain := s.Retain
	if !s.Explicit {
		// Upgrade guard: an operator running on the IMPLICIT default never
		// chose a retention. If the oldest partition extends far beyond the
		// default window — the signature of a pre-existing deployment that
		// predates built-in rotation — refuse to drop it and demand an
		// explicit choice. Fresh installs never trip this (they can't
		// accumulate beyond the window while the loop runs), and a restart
		// after ordinary downtime stays under the 2× threshold.
		guarded, oldest, err := upgradeGuardTrips(ctx, db, cfg.DBName, s.Retain)
		if err != nil {
			slog.Warn("built-in rotation: could not evaluate the upgrade guard; skipping drops this cycle",
				"db", cfg.DBName, "error", config.ScrubDSNText(err.Error(), dsn))
			return 0, err
		}
		if guarded {
			slog.Error("built-in rotation: existing history extends far beyond the default retention — refusing to drop it without an explicit choice",
				"db", cfg.DBName,
				"oldest_partition", oldest.UTC().Format("2006-01-02 15:04"),
				"default_retain", s.RetainRaw,
				"action", "set --rotate-retain explicitly (e.g. 30d to confirm, 90d to keep more, off to disable) or BINTRAIL_ROTATE_RETAIN")
			retain = 0 // still top up future partitions; no drops
		}
	}

	res, err := Perform(ctx, db, cfg.DBName, loopOptions(retain, s))
	if err != nil && ctx.Err() == nil {
		slog.Warn("built-in rotation cycle failed",
			"db", cfg.DBName, "error", config.ScrubDSNText(err.Error(), dsn))
		return res.Deferred, err
	}
	return res.Deferred, nil
}

// upgradeGuardTrips reports whether the implicit-default upgrade guard should
// block drops: true when the oldest named hourly partition is more than twice
// the retain window old. Returns the oldest partition hour for the log line.
func upgradeGuardTrips(ctx context.Context, db *sql.DB, dbName string, retain time.Duration) (bool, time.Time, error) {
	partitions, err := listPartitions(ctx, db, dbName)
	if err != nil {
		return false, time.Time{}, err
	}
	trips, oldest := guardTrips(partitions, retain, time.Now())
	return trips, oldest, nil
}

// guardTrips is the pure decision behind the upgrade guard: strict-> on twice
// the retain window, measured against the oldest named hourly partition.
// p_future and malformed names are ignored; no named partitions (a fresh
// install) never trips.
func guardTrips(partitions []partitionInfo, retain time.Duration, now time.Time) (bool, time.Time) {
	var oldest time.Time
	for _, p := range partitions {
		d, ok := indexer.PartitionDate(p.Name)
		if !ok {
			continue
		}
		if oldest.IsZero() || d.Before(oldest) {
			oldest = d
		}
	}
	if oldest.IsZero() {
		return false, time.Time{}
	}
	return now.Sub(oldest) > 2*retain, oldest
}
