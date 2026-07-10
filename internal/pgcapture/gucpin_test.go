package pgcapture

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// TestPinRenderGUCs_exactSet pins the five canonical values — the identity
// contract between the baseline COPY text and the pgoutput delta text (#593
// slice D). Changing any value silently breaks the text join against data
// captured under the old pin, so this test is a change detector on purpose.
func TestPinRenderGUCs_exactSet(t *testing.T) {
	params := map[string]string{}
	PinRenderGUCs(params)
	want := map[string]string{
		"TimeZone":           "UTC",
		"DateStyle":          "ISO",
		"extra_float_digits": "3",
		"bytea_output":       "hex",
		"IntervalStyle":      "postgres",
	}
	if len(params) != len(want) {
		t.Fatalf("pinned %d GUCs, want %d: %v", len(params), len(want), params)
	}
	for k, v := range want {
		if params[k] != v {
			t.Errorf("params[%q] = %q, want %q", k, params[k], v)
		}
	}
}

// TestPinRenderGUCs_overridesOperatorValue: a same-key value the operator put
// in the DSN must LOSE to the pin (an unpinned rendering session is the
// corruption class the pin kills), while unrelated params survive.
func TestPinRenderGUCs_overridesOperatorValue(t *testing.T) {
	cfg, err := pgconn.ParseConfig("postgres://u@h/db?TimeZone=America/New_York&application_name=myapp")
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	PinRenderGUCs(cfg.RuntimeParams)
	if got := cfg.RuntimeParams["TimeZone"]; got != "UTC" {
		t.Errorf("TimeZone = %q, want the pinned UTC (operator value must lose)", got)
	}
	if got := cfg.RuntimeParams["application_name"]; got != "myapp" {
		t.Errorf("application_name = %q, want myapp (unrelated params must survive)", got)
	}
}

// TestPinRenderGUCs_caseInsensitiveCollision: PostgreSQL GUC names are
// case-insensitive but Go map keys are not — an operator DSN `?timezone=...`
// lands under a different map key than the pinned "TimeZone", and BOTH would
// reach the startup packet in random map order (server applies last-writer-
// wins at equal priority → ~half of all connections silently unpinned). The
// pin must purge every case-variant.
func TestPinRenderGUCs_caseInsensitiveCollision(t *testing.T) {
	cfg, err := pgconn.ParseConfig("postgres://u@h/db?timezone=America/New_York&datestyle=German")
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	PinRenderGUCs(cfg.RuntimeParams)
	if v, ok := cfg.RuntimeParams["timezone"]; ok {
		t.Errorf("lowercase 'timezone' key survived the pin (=%q) — both case-variants in the startup packet apply last-writer-wins in random order", v)
	}
	if v, ok := cfg.RuntimeParams["datestyle"]; ok {
		t.Errorf("lowercase 'datestyle' key survived the pin (=%q)", v)
	}
	if got := cfg.RuntimeParams["TimeZone"]; got != "UTC" {
		t.Errorf("TimeZone = %q, want UTC", got)
	}
	if got := cfg.RuntimeParams["DateStyle"]; got != "ISO" {
		t.Errorf("DateStyle = %q, want ISO", got)
	}
}

// TestRenderGUCsStamp_canonical cross-pins the stamp text against the read
// layer's copy (baseline.RenderGUCsPinned — the read layer cannot import
// pgcapture, so it carries the canonical value as a constant). The stamp is
// persisted into baseline Parquet metadata (baseline.MetaKeyRenderGUCs); a
// divergence would make every pinned baseline read as differently-pinned.
func TestRenderGUCsStamp_canonical(t *testing.T) {
	if got := RenderGUCsStamp(); got != baseline.RenderGUCsPinned {
		t.Errorf("RenderGUCsStamp() = %q, want baseline.RenderGUCsPinned %q — the capture-side stamp and the read-side constant have diverged", got, baseline.RenderGUCsPinned)
	}
}

// TestConnectPinned_badDSNFailsWithoutDialing: an unparseable DSN errors at
// parse time (no network).
func TestConnectPinned_badDSNFailsWithoutDialing(t *testing.T) {
	if _, err := ConnectReplPinned(context.Background(), "://not-a-dsn"); err == nil {
		t.Error("ConnectReplPinned: expected a parse error for a bad DSN")
	}
	if _, err := ConnectQueryPinned(context.Background(), "://not-a-dsn"); err == nil {
		t.Error("ConnectQueryPinned: expected a parse error for a bad DSN")
	}
}
