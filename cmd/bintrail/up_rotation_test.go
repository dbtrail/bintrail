package main

import (
	"strings"
	"testing"
	"time"
)

func TestParseUpRotation_enabled(t *testing.T) {
	s, err := parseUpRotation("30d", "1h", 3)
	if err != nil {
		t.Fatalf("parseUpRotation: %v", err)
	}
	if !s.enabled {
		t.Fatal("expected enabled")
	}
	if s.retain != 30*24*time.Hour {
		t.Errorf("retain = %v, want 720h", s.retain)
	}
	if s.interval != time.Hour {
		t.Errorf("interval = %v, want 1h", s.interval)
	}
	if s.addFuture != 3 {
		t.Errorf("addFuture = %d, want 3", s.addFuture)
	}
	if s.retainRaw != "30d" {
		t.Errorf("retainRaw = %q, want 30d", s.retainRaw)
	}
}

func TestParseUpRotation_disabledForms(t *testing.T) {
	for _, retain := range []string{"off", "0", ""} {
		s, err := parseUpRotation(retain, "1h", 3)
		if err != nil {
			t.Errorf("parseUpRotation(%q): unexpected error %v", retain, err)
			continue
		}
		if s.enabled {
			t.Errorf("parseUpRotation(%q): expected disabled", retain)
		}
	}
}

func TestParseUpRotation_invalidRetain(t *testing.T) {
	_, err := parseUpRotation("1x", "1h", 3)
	if err == nil {
		t.Fatal("expected error for invalid retain unit")
	}
	if !strings.Contains(err.Error(), "off") {
		t.Errorf("error should mention the \"off\" escape hatch, got: %v", err)
	}
}

func TestParseUpRotation_invalidInterval(t *testing.T) {
	if _, err := parseUpRotation("7d", "soon", 3); err == nil {
		t.Fatal("expected error for unparseable interval")
	}
	if _, err := parseUpRotation("7d", "-1h", 3); err == nil {
		t.Fatal("expected error for non-positive interval")
	}
}

func TestParseUpRotation_negativeAddFuture(t *testing.T) {
	if _, err := parseUpRotation("7d", "1h", -1); err == nil {
		t.Fatal("expected error for negative add-future")
	}
}

func TestDedupeDSNs(t *testing.T) {
	in := []string{"a", "", "b", "a", "c", "b"}
	got := dedupeDSNs(in)
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("dedupeDSNs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("dedupeDSNs = %v, want %v", got, want)
		}
	}
}
