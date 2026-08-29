package reconstruct

import (
	"errors"
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
// path gets the wrong-path verdict; a genuinely unsupported MySQL type gets
// the SHARED per-type sentence (#1461).
//
// The empty-DataType negative below is anchored on "unsupported by the
// baseline canonicalizer" — the sentence a PG-shaped column must NOT get. It
// used to be anchored on the typed branch's old private wording, which #1461
// deleted from the tree: that assertion would now pass for a string that no
// longer exists anywhere, which is no assertion at all.
func TestFullTablePKTypeRefusal_discriminatesPGShape(t *testing.T) {
	pg := metadata.ColumnMeta{Name: "id", IsPK: true, DataType: ""}
	err := fullTablePKTypeRefusal("app", "orders", pg)
	if err == nil {
		t.Fatal("fullTablePKTypeRefusal returned nil for a PG-shaped column")
	}
	if !strings.Contains(err.Error(), "PostgreSQL snapshot shape") {
		t.Errorf("want the PostgreSQL-shape wrong-path reason, got: %v", err)
	}
	if strings.Contains(err.Error(), "unsupported by the baseline canonicalizer") {
		t.Errorf("empty DataType must not get the misleading PK-type blame: %v", err)
	}

	// #1461: one limitation, one sentence. The full-table path keeps its
	// `full-table reconstruct: <schema>.<table>: ` frame — the same frame the
	// empty branch above already used — and the refusal itself is now the
	// exact string verify, single-row reconstruct and the shim render, so an
	// operator who meets two of those surfaces can tell it is one limit.
	typed := metadata.ColumnMeta{Name: "id", IsPK: true, DataType: "bit"}
	err = fullTablePKTypeRefusal("app", "orders", typed)
	want := "full-table reconstruct: app.orders: " + PKTypeGateReason(typed, "full-table reconstruct", "reconstruct")
	if err == nil || err.Error() != want {
		t.Errorf("typed refusal = %v, want %q", err, want)
	}
	// Spelled out once, so a change to PKTypeGateReason that silently rewrites
	// the sentence above still has to be typed here on purpose.
	if literal := `full-table reconstruct: app.orders: primary-key column "id" has type "bit" unsupported by the baseline canonicalizer`; err == nil || err.Error() != literal {
		t.Errorf("typed refusal = %v, want the shared sentence %q", err, literal)
	}
	if strings.Contains(err.Error(), "PostgreSQL") {
		t.Errorf("a real MySQL type must not get the PostgreSQL verdict: %v", err)
	}
	// ErrUnsupportedPKType's own doc says EVERY error-typed refusal of this
	// shape carries the sentinel, and its sibling fullTableGeneratedPKRefusal
	// routes through GeneratedPKRefusalError for exactly that reason. A bare
	// fmt.Errorf here leaves the invariant written down and unenforced, and
	// cliapp/baseline_refresh.go classifies failures by sentinel with an
	// unrecognized error falling into a generic "refused" bucket.
	if !errors.Is(err, ErrUnsupportedPKType) {
		t.Errorf("the full-table refusal must carry ErrUnsupportedPKType: %v", err)
	}
	// The PG-shaped branch carries it too: it is the same gate refusing the
	// same column, and a caller keying on the sentinel must not have to also
	// know which branch of the reason rendered.
	if !errors.Is(fullTablePKTypeRefusal("app", "orders", pg), ErrUnsupportedPKType) {
		t.Errorf("the PG-shaped full-table refusal must carry ErrUnsupportedPKType too")
	}
}
