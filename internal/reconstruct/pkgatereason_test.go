package reconstruct

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// TestPKTypeGateReason_discriminatesPGShape pins the two branches of the
// shared discriminator (#1198, lifted from internal/verify's #1009 fix): an
// empty DataType is the PostgreSQL snapshot shape and gets the wrong-path
// verdict naming the caller's surface/action; a real MySQL type keeps the
// honest per-type canonicalizer message.
func TestPKTypeGateReason_discriminatesPGShape(t *testing.T) {
	pg := metadata.ColumnMeta{Name: "id", IsPK: true, DataType: ""}
	got := PKTypeGateReason(pg, "full-table _snapshot", "materialize")
	for _, want := range []string{
		"PostgreSQL snapshot shape",
		`stream_state flavor did not read "postgres"`,
		"full-table _snapshot took its MySQL path",
		"cannot materialize a PostgreSQL-sourced table",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("empty DataType reason lacks %q: %s", want, got)
		}
	}
	if strings.Contains(got, "unsupported by the baseline canonicalizer") {
		t.Errorf("empty DataType must not get the misleading PK-type blame: %s", got)
	}

	typed := metadata.ColumnMeta{Name: "id", IsPK: true, DataType: "float"}
	got = PKTypeGateReason(typed, "verify", "verify")
	if want := `primary-key column "id" has type "float" unsupported by the baseline canonicalizer`; got != want {
		t.Errorf("typed reason = %q, want %q", got, want)
	}
}

// TestFullTablePKTypeRefusal_discriminatesPGShape pins fullTablePKTypeRefusal,
// the helper ReconstructTable's PK-type gate (fulltable.go) delegates to — the
// gate itself needs a DB + baseline, so this test covers the message contract,
// not the delegation: a PG-shaped snapshot that reached the full-table MySQL
// path gets the wrong-path verdict; a genuinely unsupported MySQL type keeps
// the pre-#1198 supported-set refusal verbatim.
func TestFullTablePKTypeRefusal_discriminatesPGShape(t *testing.T) {
	pg := metadata.ColumnMeta{Name: "id", IsPK: true, DataType: ""}
	err := fullTablePKTypeRefusal("app", "orders", pg)
	if err == nil {
		t.Fatal("fullTablePKTypeRefusal returned nil for a PG-shaped column")
	}
	if !strings.Contains(err.Error(), "PostgreSQL snapshot shape") {
		t.Errorf("want the PostgreSQL-shape wrong-path reason, got: %v", err)
	}
	if strings.Contains(err.Error(), "not in the supported PK type set") {
		t.Errorf("empty DataType must not get the misleading PK-type blame: %v", err)
	}

	typed := metadata.ColumnMeta{Name: "id", IsPK: true, DataType: "bit"}
	err = fullTablePKTypeRefusal("app", "orders", typed)
	want := `full-table reconstruct: app.orders PK column "id" has type "bit" which is not in the supported PK type set; ` +
		"file a follow-up issue if you need this type"
	if err == nil || err.Error() != want {
		t.Errorf("typed refusal = %v, want %q", err, want)
	}
	if strings.Contains(err.Error(), "PostgreSQL") {
		t.Errorf("a real MySQL type must not get the PostgreSQL verdict: %v", err)
	}
}
