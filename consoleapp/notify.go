package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/notify"
)

// continuityPollInterval is how often the continuity watcher re-reads each
// index's stream_state. gap_lost is a permanent, stamped condition — a few
// minutes of notification latency is fine; hammering every index every few
// seconds is not.
const continuityPollInterval = 5 * time.Minute

// eventSender is the one seam watchNotifier needs from the delivery layer —
// an interface so tests can capture events without HTTP.
type eventSender interface {
	Notify(ev notify.Event)
}

// watchNotifier maps the watch daemon's health signals onto webhook events
// with edge-triggering: one notification on the transition into a bad state,
// a daily reminder while it persists, one on recovery. Wired only when
// --notify-webhook is set.
type watchNotifier struct {
	send eventSender
	edge *notify.Edge
	// lastGapDetail remembers each index's gap_lost_detail so a DIFFERENT gap
	// arriving while the first is still active reads as a new transition.
	// Touched only by the continuity poller goroutine — no lock.
	lastGapDetail map[string]string
}

func newWatchNotifier(ctx context.Context, url string) *watchNotifier {
	return &watchNotifier{send: notify.NewWebhook(ctx, url), edge: notify.NewEdge(0), lastGapDetail: make(map[string]string)}
}

// newWatchNotifierFromFlags builds the notifier when --notify-webhook is set;
// nil otherwise — every hook then stays disconnected. The URL is validated
// here so a typo refuses startup instead of surfacing as a buried delivery
// warning at the moment of the first real incident.
func newWatchNotifierFromFlags(ctx context.Context) (*watchNotifier, error) {
	if upNotifyWebhook == "" {
		return nil, nil
	}
	u, err := url.Parse(upNotifyWebhook)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("--notify-webhook: %q is not a valid http(s) URL", upNotifyWebhook)
	}
	slog.Info("webhook notifications enabled")
	return newWatchNotifier(ctx, upNotifyWebhook), nil
}

// rotationNotifyHooks adapts an optional notifier to rotation.StartLoop's
// variadic onCycle parameter.
func rotationNotifyHooks(n *watchNotifier) []func(failed bool, deferred int) {
	if n == nil {
		return nil
	}
	return []func(bool, int){n.RotationCycle}
}

// VerifyFinished is the verifySupervisor.onFinish hook: it fires on runs that
// found mismatches (critical — the data recover would produce is wrong) or
// could not verify cleanly (warning), and resolves once a clean run lands.
// Skip records never notify — the skip is bookkeeping, not a health signal.
func (n *watchNotifier) VerifyFinished(rec console.VerifyRunRecord) {
	if rec.State == "skipped" {
		return
	}
	s := rec.Summary
	// A run that verified nothing must neither alarm as a mismatch nor
	// reassure. Two shapes land here with zero verified tables: Total == 0
	// ("only one baseline yet", an empty tables filter) and all-inconclusive
	// (baseline/archive unreachable). The CLI's Report.ExitError treats
	// all-inconclusive as a failure, so it fires here too — but never as the
	// "clean" that would auto-close a real mismatch alert.
	allInconclusive := s.Total > 0 && s.Inconclusive == s.Total
	problem := rec.State == "failed" || s.Mismatch > 0 || s.Error > 0 || allInconclusive
	clean := rec.State == "succeeded" && !problem && s.Match > 0
	sev := "warning"
	if s.Mismatch > 0 {
		sev = "critical"
	}
	// The edge key carries the severity class so an escalation (a
	// warning-grade run followed by a critical mismatch) is a new transition,
	// never suppressed by the lower tier's repeat window.
	key := "verify:" + sev + ":" + rec.ServerID
	if clean {
		resolved := n.edge.Resolve("verify:critical:" + rec.ServerID)
		if n.edge.Resolve("verify:warning:" + rec.ServerID) {
			resolved = true
		}
		if resolved {
			n.send.Notify(notify.Event{
				Event: "verify_problem", Severity: "info", Server: rec.ServerName, Resolved: true,
				Summary: fmt.Sprintf("verification is clean again: %d/%d tables match", s.Match, s.Total),
			})
		}
		return
	}
	if !problem || !n.edge.Fire(key) {
		return
	}
	summary := fmt.Sprintf("verification found problems: %d mismatch, %d error, %d inconclusive (%d match)",
		s.Mismatch, s.Error, s.Inconclusive, s.Match)
	switch {
	case rec.State == "failed":
		summary = "verification run failed: " + rec.LastError
	case allInconclusive:
		summary = fmt.Sprintf("verification could not verify any table: all %d inconclusive", s.Total)
	}
	n.send.Notify(notify.Event{
		Event: "verify_problem", Severity: sev, Server: rec.ServerName, Summary: summary,
		Details: map[string]string{
			"mode": string(rec.Mode), "trigger": rec.Trigger, "state": rec.State,
			"mismatch": strconv.Itoa(s.Mismatch), "error": strconv.Itoa(s.Error), "match": strconv.Itoa(s.Match),
		},
	})
}

// RotationCycle is the rotation.StartLoop onCycle hook. Unhealthy mirrors the
// loop's own escalation condition — failed OR deferring unarchived partitions
// — because either way the index is not shrinking when it should.
func (n *watchNotifier) RotationCycle(failed bool, deferred int) {
	const key = "rotation"
	if !failed && deferred == 0 {
		if n.edge.Resolve(key) {
			n.send.Notify(notify.Event{
				Event: "rotation_unhealthy", Severity: "info", Resolved: true,
				Summary: "built-in rotation is healthy again",
			})
		}
		return
	}
	if !n.edge.Fire(key) {
		return
	}
	n.send.Notify(notify.Event{
		Event: "rotation_unhealthy", Severity: "warning",
		Summary: "built-in rotation made no progress — the index keeps growing (see the daemon log)",
		Details: map[string]string{"failed": strconv.FormatBool(failed), "deferred_partitions": strconv.Itoa(deferred)},
	})
}

// Continuity reports one index's stream continuity. gap_lost is critical:
// events in the gap are permanently unrecoverable. edgeKey identifies the
// index (the target DSN — unique after dedup, unlike display names, which a
// registry entry could share with the reserved boot label).
func (n *watchNotifier) Continuity(server, edgeKey string, gapLost bool, detail string) {
	key := "continuity:" + edgeKey
	if !gapLost {
		delete(n.lastGapDetail, key)
		if n.edge.Resolve(key) {
			n.send.Notify(notify.Event{
				Event: "continuity_gap_lost", Severity: "info", Server: server, Resolved: true,
				Summary: "capture continuity restored (the stream was re-baselined)",
			})
		}
		return
	}
	// A DIFFERENT gap while the first is still active (re-baseline plus a
	// second loss inside one repeat window) is a new data-loss event, not a
	// repeat — a changed detail bypasses the suppression window.
	changed := detail != "" && n.lastGapDetail[key] != "" && detail != n.lastGapDetail[key]
	n.lastGapDetail[key] = detail
	if !n.edge.Fire(key) && !changed {
		return
	}
	ev := notify.Event{
		Event: "continuity_gap_lost", Severity: "critical", Server: server,
		Summary: "capture continuity lost — events in the gap are PERMANENTLY unrecoverable; re-baseline to resume trustworthy coverage",
	}
	if detail != "" {
		ev.Details = map[string]string{"detail": detail}
	}
	n.send.Notify(ev)
}

// continuityTarget is one index DB the watcher polls.
type continuityTarget struct {
	name string
	dsn  string
}

// startContinuityWatch polls each index's stream_state for a stamped
// gap_lost_at and edge-notifies transitions. Its own loop — deliberately NOT
// piggybacked on rotation (rotation can be off) or the metrics scraper
// (metrics can be off, and it lives in the capture plane).
func startContinuityWatch(ctx context.Context, n *watchNotifier, registry *console.Registry, bootDSN string) {
	go func() {
		runCycle := func() {
			defer func() {
				if r := recover(); r != nil {
					slog.Error("continuity watch cycle panicked; watching continues next tick", "panic", r)
				}
			}()
			for _, t := range continuityTargets(registry, bootDSN) {
				if ctx.Err() != nil {
					return
				}
				gapLost, detail, err := readGapLost(ctx, t.dsn)
				if err != nil {
					// Unknown is never "no gap" — and never silent either: a
					// watcher that cannot watch is itself a coverage hole. The
					// edge keeps this to one warning per day per index.
					if n.edge.Fire("continuity-unknown:" + t.dsn) {
						slog.Warn("continuity watch cannot evaluate this index (unreachable, or a legacy schema without the gap columns) — gap_lost will NOT be detected for it",
							"server", t.name, "error", err)
					}
					continue
				}
				n.edge.Resolve("continuity-unknown:" + t.dsn)
				n.Continuity(t.name, t.dsn, gapLost, detail)
			}
		}
		if ctx.Err() == nil {
			runCycle()
		}
		tick := time.NewTicker(continuityPollInterval)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				runCycle()
			}
		}
	}()
}

// continuityTargets enumerates the boot index (when watch streams one) plus
// every registry server, deduplicated by DSN — unlike scheduled verify, the
// continuity check is cheap enough to cover the command-line boot stream too.
func continuityTargets(registry *console.Registry, bootDSN string) []continuityTarget {
	var out []continuityTarget
	seen := make(map[string]bool)
	if bootDSN != "" {
		out = append(out, continuityTarget{name: "cli index", dsn: bootDSN})
		seen[bootDSN] = true
	}
	if registry != nil {
		for _, e := range registry.List() {
			if seen[e.DSN] {
				continue
			}
			seen[e.DSN] = true
			out = append(out, continuityTarget{name: e.Name, dsn: e.DSN})
		}
	}
	return out
}

// readGapLost reads the gap_lost stamp from one index's stream_state. A
// non-nil error means the state is unknowable right now (index unreachable,
// or a legacy schema without the gap columns) — the caller must treat it as
// unknown, never as "no gap", and must say so.
func readGapLost(ctx context.Context, dsn string) (gapLost bool, detail string, err error) {
	db, err := config.Connect(dsn)
	if err != nil {
		return false, "", err
	}
	defer db.Close()
	var lost bool
	var d sql.NullString
	err = db.QueryRowContext(ctx,
		"SELECT gap_lost_at IS NOT NULL, gap_lost_detail FROM stream_state WHERE id = 1").Scan(&lost, &d)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// Empty stream_state = no capture ran = genuinely no continuity to
		// break (same rule as verify's gap evaluation).
		return false, "", nil
	case err != nil:
		return false, "", err
	}
	return lost, d.String, nil
}
