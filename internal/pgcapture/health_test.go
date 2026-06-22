package pgcapture

import (
	"database/sql"
	"testing"

	"github.com/jackc/pglogrepl"
)

func nullStr(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }
func nullInt(i int64) sql.NullInt64   { return sql.NullInt64{Int64: i, Valid: true} }

func TestParseNullLSN(t *testing.T) {
	want, err := pglogrepl.ParseLSN("16/B374D848")
	if err != nil {
		t.Fatalf("setup ParseLSN: %v", err)
	}
	tests := []struct {
		name    string
		in      sql.NullString
		want    pglogrepl.LSN
		wantErr bool
	}{
		{"valid", nullStr("16/B374D848"), want, false},
		{"null", sql.NullString{}, 0, false},
		{"empty-but-valid", nullStr(""), 0, false},
		{"garbage", nullStr("not-an-lsn"), 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseNullLSN(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseNullLSN(%v) err=%v, wantErr=%v", tt.in, err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("parseNullLSN(%v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestSlotHealthRow_toHealth(t *testing.T) {
	// A healthy slot under unlimited retention (max_slot_wal_keep_size=-1): all LSNs
	// present, safe_wal_size NULL.
	t.Run("healthy-unlimited-retention", func(t *testing.T) {
		r := slotHealthRow{
			active:            true,
			walStatus:         nullStr("reserved"),
			restartLSN:        nullStr("0/1000000"),
			confirmedFlushLSN: nullStr("0/1000060"),
			currentWalLSN:     nullStr("0/2000000"),
			retainedBytes:     nullInt(16777216),
			safeWalSize:       sql.NullInt64{}, // NULL when retention is unlimited
		}
		h, err := r.toHealth()
		if err != nil {
			t.Fatalf("toHealth: %v", err)
		}
		if !h.Exists || !h.Active || h.WalStatus != "reserved" {
			t.Errorf("basic fields wrong: %+v", h)
		}
		if h.RetainedBytes != 16777216 {
			t.Errorf("RetainedBytes = %d, want 16777216", h.RetainedBytes)
		}
		if h.SafeWalSize.Valid {
			t.Errorf("SafeWalSize should be NULL (invalid) under unlimited retention, got %v", h.SafeWalSize)
		}
		want, _ := pglogrepl.ParseLSN("0/2000000")
		if h.CurrentWalLSN != want {
			t.Errorf("CurrentWalLSN = %v, want %v", h.CurrentWalLSN, want)
		}
	})

	// A lost slot: wal_status=lost; PostgreSQL returns safe_wal_size NULL for a lost
	// slot (as it does under unlimited retention), not 0.
	t.Run("lost-safe-wal-null", func(t *testing.T) {
		r := slotHealthRow{
			active:        false,
			walStatus:     nullStr(WalStatusLost),
			restartLSN:    nullStr("0/1000000"),
			currentWalLSN: nullStr("0/9000000"),
			retainedBytes: nullInt(0),
			safeWalSize:   sql.NullInt64{}, // NULL for a lost slot
		}
		h, err := r.toHealth()
		if err != nil {
			t.Fatalf("toHealth: %v", err)
		}
		if h.WalStatus != WalStatusLost {
			t.Errorf("WalStatus = %q, want %q", h.WalStatus, WalStatusLost)
		}
		if h.SafeWalSize.Valid {
			t.Errorf("SafeWalSize = %v, want NULL (invalid) for a lost slot", h.SafeWalSize)
		}
	})

	// A just-created slot can have NULL restart_lsn → RetainedBytes 0, RestartLSN 0.
	t.Run("null-lsns", func(t *testing.T) {
		r := slotHealthRow{
			active:        false,
			walStatus:     nullStr("reserved"),
			restartLSN:    sql.NullString{},
			currentWalLSN: nullStr("0/3000000"),
			retainedBytes: sql.NullInt64{}, // NULL when restart_lsn is NULL
		}
		h, err := r.toHealth()
		if err != nil {
			t.Fatalf("toHealth: %v", err)
		}
		if h.RestartLSN != 0 || h.RetainedBytes != 0 {
			t.Errorf("NULL restart_lsn should give 0/0, got RestartLSN=%v RetainedBytes=%d", h.RestartLSN, h.RetainedBytes)
		}
	})

	t.Run("bad-lsn-errors", func(t *testing.T) {
		r := slotHealthRow{walStatus: nullStr("reserved"), restartLSN: nullStr("bogus")}
		if _, err := r.toHealth(); err == nil {
			t.Fatal("expected an error for a malformed LSN")
		}
	})
}
