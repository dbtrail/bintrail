package pgstreamrun

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

func TestSlotHealthResult(t *testing.T) {
	tests := []struct {
		name              string
		h                 pgcapture.SlotHealth
		wantStatus        doctor.CheckStatus
		wantInDetailOrRem string
	}{
		{"absent", pgcapture.SlotHealth{Exists: false}, doctor.StatusSkip, "does not exist"},
		{"reserved", pgcapture.SlotHealth{Exists: true, WalStatus: "reserved", Active: true}, doctor.StatusPass, "wal_status=reserved"},
		{"extended", pgcapture.SlotHealth{Exists: true, WalStatus: "extended"}, doctor.StatusWarn, "approaching"},
		{"unreserved", pgcapture.SlotHealth{Exists: true, WalStatus: "unreserved"}, doctor.StatusWarn, "approaching"},
		{"lost", pgcapture.SlotHealth{Exists: true, WalStatus: "lost"}, doctor.StatusFail, "re-baseline"},
		{"unknown-status-passes", pgcapture.SlotHealth{Exists: true, WalStatus: "weird"}, doctor.StatusPass, "wal_status=weird"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := slotHealthResult(tt.h, "myslot")
			if got.Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q", got.Status, tt.wantStatus)
			}
			hay := got.Detail + " " + got.Remediation
			if !strings.Contains(hay, tt.wantInDetailOrRem) {
				t.Errorf("detail/remediation %q missing %q", hay, tt.wantInDetailOrRem)
			}
		})
	}
}

// TestSlotHealthResult_lostHasRecoveryPath pins the load-bearing #532 acceptance: a
// lost slot is a loud FAIL whose remediation names the manual re-baseline steps and
// reassures that recovery (which is index-only) is unaffected.
func TestSlotHealthResult_lostHasRecoveryPath(t *testing.T) {
	got := slotHealthResult(pgcapture.SlotHealth{Exists: true, WalStatus: "lost"}, "s1")
	if got.Status != doctor.StatusFail {
		t.Fatalf("lost slot must FAIL, got %q", got.Status)
	}
	for _, want := range []string{"pg_drop_replication_slot('s1')", "DELETE FROM stream_state", "recovery never needs the slot"} {
		if !strings.Contains(got.Remediation, want) {
			t.Errorf("lost remediation missing %q:\n%s", want, got.Remediation)
		}
	}
}

func TestSlotHealthResult_safeWalSizeRendering(t *testing.T) {
	withSafe := slotHealthResult(pgcapture.SlotHealth{
		Exists: true, WalStatus: "reserved", RetainedBytes: 2048,
		SafeWalSize: sql.NullInt64{Int64: 1048576, Valid: true},
	}, "s")
	if !strings.Contains(withSafe.Detail, "retained_wal=2.0 KiB") {
		t.Errorf("missing humanized retained_wal: %q", withSafe.Detail)
	}
	if !strings.Contains(withSafe.Detail, "safe_wal=1.0 MiB") {
		t.Errorf("missing safe_wal when valid: %q", withSafe.Detail)
	}
	// NULL safe_wal_size (unlimited retention) → safe_wal omitted entirely.
	noSafe := slotHealthResult(pgcapture.SlotHealth{Exists: true, WalStatus: "reserved"}, "s")
	if strings.Contains(noSafe.Detail, "safe_wal=") {
		t.Errorf("safe_wal must be omitted when NULL: %q", noSafe.Detail)
	}
}

func TestFormatBytes(t *testing.T) {
	cases := map[int64]string{
		0:          "0 B",
		512:        "512 B",
		1024:       "1.0 KiB",
		1536:       "1.5 KiB",
		1048576:    "1.0 MiB",
		1073741824: "1.0 GiB",
	}
	for in, want := range cases {
		if got := formatBytes(in); got != want {
			t.Errorf("formatBytes(%d) = %q, want %q", in, got, want)
		}
	}
}
