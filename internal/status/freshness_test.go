package status

import (
	"database/sql"
	"errors"
	"testing"
	"time"
)

func at(t time.Time) sql.NullTime { return sql.NullTime{Time: t, Valid: true} }

func TestFreshnessStatus(t *testing.T) {
	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)
	fresh := now.Add(-20 * time.Second)
	old := now.Add(-2 * time.Hour)

	tests := []struct {
		name   string
		stream *StreamStateInfo
		err    error
		want   string
		why    string
	}{
		{
			name: "checkpointing and indexing recent events",
			stream: &StreamStateInfo{
				LastCheckpoint: fresh,
				LastEventTime:  at(fresh),
			},
			want: FreshnessCurrent,
		},
		{
			name: "checkpoint fresh but nothing recent indexed",
			stream: &StreamStateInfo{
				LastCheckpoint: fresh,
				LastEventTime:  at(old),
			},
			want: FreshnessIdle,
			why:  "no traffic and capture-far-behind are indistinguishable offline; idle says so instead of guessing",
		},
		{
			name: "checkpoint fresh, no event ever recorded",
			stream: &StreamStateInfo{
				LastCheckpoint: fresh,
			},
			want: FreshnessIdle,
			why:  "a daemon that has checkpointed but indexed nothing is idle, not stalled",
		},
		{
			name: "checkpoint stale — the daemon is not writing",
			stream: &StreamStateInfo{
				LastCheckpoint: old,
				LastEventTime:  at(fresh),
			},
			want: FreshnessStalled,
			why:  "the checkpoint ticker runs WITHOUT traffic, so a stale one is the daemon, not the workload",
		},
		{
			name: "stale checkpoint wins over recent events",
			stream: &StreamStateInfo{
				LastCheckpoint: old,
				LastEventTime:  at(now),
			},
			want: FreshnessStalled,
			why:  "events can be recent because someone else wrote them; the checkpoint is the liveness signal",
		},
		{
			name:   "stream row with no checkpoint at all",
			stream: &StreamStateInfo{LastEventTime: at(fresh)},
			want:   FreshnessUnknown,
			why:    "a zero checkpoint judged against now would date to the epoch and report a permanent false stall",
		},
		{
			name: "no stream row — file-mode index",
			want: FreshnessNone,
			why:  "no capture ran, so there is no freshness to claim; this is a no-claim, not a fault",
		},
		{
			name: "stream_state unreadable",
			err:  errors.New("boom"),
			want: FreshnessUnavailable,
			why:  "a read failure must degrade, never fabricate a healthy verdict",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := FreshnessStatus(tc.stream, tc.err, now, 0, 0)
			if got != tc.want {
				t.Errorf("FreshnessStatus = %q, want %q\n%s", got, tc.want, tc.why)
			}
		})
	}
}

// The three non-claims must never read as a pass. This is the exact fold the
// continuity vocabulary warns about, re-pinned here so an alerting consumer
// cannot quietly treat "could not evaluate" as healthy.
func TestFreshnessEvaluated_nonClaimsAreNotAPass(t *testing.T) {
	for _, v := range []string{FreshnessUnknown, FreshnessUnavailable, FreshnessNone} {
		if FreshnessEvaluated(v) {
			t.Errorf("FreshnessEvaluated(%q) = true — a non-claim must never count as evaluated", v)
		}
	}
	for _, v := range []string{FreshnessCurrent, FreshnessIdle, FreshnessStalled} {
		if !FreshnessEvaluated(v) {
			t.Errorf("FreshnessEvaluated(%q) = false, want true", v)
		}
	}
}

func TestFreshnessStatus_thresholdsAreHonoured(t *testing.T) {
	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)
	s := &StreamStateInfo{
		LastCheckpoint: now.Add(-30 * time.Second),
		LastEventTime:  at(now.Add(-30 * time.Second)),
	}

	if got := FreshnessStatus(s, nil, now, time.Minute, time.Minute); got != FreshnessCurrent {
		t.Errorf("inside both thresholds = %q, want current", got)
	}
	if got := FreshnessStatus(s, nil, now, 10*time.Second, time.Minute); got != FreshnessStalled {
		t.Errorf("checkpoint past its threshold = %q, want stalled", got)
	}
	if got := FreshnessStatus(s, nil, now, time.Minute, 10*time.Second); got != FreshnessIdle {
		t.Errorf("events past their threshold = %q, want idle", got)
	}
	// A zero threshold must fall back to the default, never behave as "0 means
	// everything is instantly stale".
	if got := FreshnessStatus(s, nil, now, 0, 0); got != FreshnessCurrent {
		t.Errorf("zero thresholds = %q, want current (defaults applied)", got)
	}
}

func TestCheckpointAgeAndNewestEventAge(t *testing.T) {
	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)

	if _, ok := CheckpointAge(nil, now); ok {
		t.Error("CheckpointAge(nil) reported a knowable age")
	}
	if _, ok := CheckpointAge(&StreamStateInfo{}, now); ok {
		t.Error("a zero checkpoint must not report a confident 0s age")
	}
	if age, ok := CheckpointAge(&StreamStateInfo{LastCheckpoint: now.Add(-90 * time.Second)}, now); !ok || age != 90*time.Second {
		t.Errorf("CheckpointAge = %v, %v; want 1m30s, true", age, ok)
	}

	if _, ok := NewestEventAge(&StreamStateInfo{}, now); ok {
		t.Error("an absent LastEventTime must not report an age")
	}
	if age, ok := NewestEventAge(&StreamStateInfo{LastEventTime: at(now.Add(-5 * time.Minute))}, now); !ok || age != 5*time.Minute {
		t.Errorf("NewestEventAge = %v, %v; want 5m, true", age, ok)
	}
	// A source clock ahead of ours must clamp, not report a negative age.
	if age, _ := NewestEventAge(&StreamStateInfo{LastEventTime: at(now.Add(time.Minute))}, now); age != 0 {
		t.Errorf("NewestEventAge with a source clock ahead = %v, want 0", age)
	}
	if age, _ := CheckpointAge(&StreamStateInfo{LastCheckpoint: now.Add(time.Minute)}, now); age != 0 {
		t.Errorf("CheckpointAge with a daemon clock ahead = %v, want 0", age)
	}
}
