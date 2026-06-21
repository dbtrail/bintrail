package pgcapture_test

import (
	"testing"

	"github.com/jackc/pglogrepl"

	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

func TestLSN_RoundTrip(t *testing.T) {
	cases := []pglogrepl.LSN{
		0,
		1,
		0x19DF9E8,
		0x16B374D848,
		0xFFFFFFFFFFFFFFFF, // max uint64
	}
	for _, want := range cases {
		s := pgcapture.EncodeLSN(want)
		got, err := pgcapture.DecodeLSN(s)
		if err != nil {
			t.Errorf("DecodeLSN(%q): unexpected error: %v", s, err)
			continue
		}
		if got != want {
			t.Errorf("round-trip %d: EncodeLSN=%q DecodeLSN=%d, want %d", uint64(want), s, uint64(got), uint64(want))
		}
	}
}

func TestEncodeLSN_CanonicalFormat(t *testing.T) {
	// EncodeLSN must produce the "X/Y" upper-case-hex form PostgreSQL itself uses,
	// so the stored checkpoint is the same string a DBA sees in pg_replication_slots.
	if got := pgcapture.EncodeLSN(pglogrepl.LSN(0x19DF9E8)); got != "0/19DF9E8" {
		t.Errorf("EncodeLSN(0x19DF9E8) = %q, want %q", got, "0/19DF9E8")
	}
}

func TestDecodeLSN_Invalid(t *testing.T) {
	for _, s := range []string{"", "notanlsn", "0/", "/19DF9E8", "ZZ/ZZ"} {
		if _, err := pgcapture.DecodeLSN(s); err == nil {
			t.Errorf("DecodeLSN(%q): expected error, got nil", s)
		}
	}
}
