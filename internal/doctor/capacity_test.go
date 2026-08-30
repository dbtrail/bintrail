package doctor

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

func capHour(now time.Time, hoursAgo int) time.Time {
	return now.UTC().Truncate(time.Hour).Add(-time.Duration(hoursAgo) * time.Hour)
}

func TestProjectCapacity_math(t *testing.T) {
	now := time.Date(2026, 6, 7, 12, 30, 0, 0, time.UTC)
	// 6 completed hours, 1000 rows / 1 MB each → 24k events/day, 1000 B/event.
	var parts []CapacityPartition
	for i := 1; i <= 6; i++ {
		parts = append(parts, CapacityPartition{Hour: capHour(now, i), Rows: 1000, Bytes: 1_000_000})
	}
	p, ok := projectCapacity(parts, 30*24*time.Hour, now)
	if !ok {
		t.Fatal("expected a measurement")
	}
	if p.eventsPerDay != 24000 {
		t.Errorf("eventsPerDay = %v, want 24000", p.eventsPerDay)
	}
	if p.bytesPerEvent != 1000 {
		t.Errorf("bytesPerEvent = %v, want 1000", p.bytesPerEvent)
	}
	want := 24000.0 * 1000 * 30 // events/day × bytes/event × retain days
	if p.projectedBytes != want {
		t.Errorf("projectedBytes = %v, want %v", p.projectedBytes, want)
	}
	if p.currentBytes != 6_000_000 {
		t.Errorf("currentBytes = %d, want 6000000", p.currentBytes)
	}
	if p.sampleHours != 6 {
		t.Errorf("sampleHours = %d, want 6", p.sampleHours)
	}
}

func TestProjectCapacity_zeroRetainProjectsNothing(t *testing.T) {
	now := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)
	var parts []CapacityPartition
	for i := 1; i <= 4; i++ {
		parts = append(parts, CapacityPartition{Hour: capHour(now, i), Rows: 1000, Bytes: 1_000_000})
	}
	p, ok := projectCapacity(parts, 0, now)
	if !ok {
		t.Fatal("expected a measurement")
	}
	if p.projectedBytes != 0 {
		t.Errorf("projectedBytes = %v, want 0 for retain=0", p.projectedBytes)
	}
	if p.eventsPerDay == 0 || p.bytesPerEvent == 0 {
		t.Error("rates must still be measured under retain=0")
	}
}

func TestProjectCapacity_windowFiltering(t *testing.T) {
	now := time.Date(2026, 6, 7, 12, 30, 0, 0, time.UTC)
	parts := []CapacityPartition{
		{Hour: capHour(now, 0), Rows: 9999, Bytes: 9_999_999},  // current partial hour — excluded
		{Hour: capHour(now, 1), Rows: 1000, Bytes: 1_000_000},  // in window
		{Hour: capHour(now, 2), Rows: 1000, Bytes: 1_000_000},  // in window
		{Hour: capHour(now, 3), Rows: 1000, Bytes: 1_000_000},  // in window
		{Hour: capHour(now, 4), Rows: 0, Bytes: 65536},         // empty — excluded from rate
		{Hour: capHour(now, 48), Rows: 8888, Bytes: 8_888_888}, // older than 24h — excluded
	}
	p, ok := projectCapacity(parts, 30*24*time.Hour, now)
	if !ok {
		t.Fatal("expected a measurement")
	}
	if p.sampleHours != 3 {
		t.Errorf("sampleHours = %d, want 3 (current hour, empty, and old partitions excluded)", p.sampleHours)
	}
	if p.eventsPerDay != 24000 {
		t.Errorf("eventsPerDay = %v, want 24000 — excluded partitions leaked into the rate", p.eventsPerDay)
	}
	// currentBytes still sums EVERYTHING — it is the table's footprint today.
	wantCurrent := uint64(9_999_999 + 3_000_000 + 65536 + 8_888_888)
	if p.currentBytes != wantCurrent {
		t.Errorf("currentBytes = %d, want %d", p.currentBytes, wantCurrent)
	}
}

func TestProjectCapacity_insufficientHistory(t *testing.T) {
	now := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)

	// Too few sampled hours.
	few := []CapacityPartition{
		{Hour: capHour(now, 1), Rows: 5000, Bytes: 5_000_000},
		{Hour: capHour(now, 2), Rows: 5000, Bytes: 5_000_000},
	}
	if _, ok := projectCapacity(few, time.Hour, now); ok {
		t.Error("2 sampled hours must be insufficient (capMinSampleHours=3)")
	}

	// Enough hours, too few rows.
	sparse := []CapacityPartition{
		{Hour: capHour(now, 1), Rows: 100, Bytes: 100_000},
		{Hour: capHour(now, 2), Rows: 100, Bytes: 100_000},
		{Hour: capHour(now, 3), Rows: 100, Bytes: 100_000},
	}
	if _, ok := projectCapacity(sparse, time.Hour, now); ok {
		t.Error("300 total rows must be insufficient (capMinSampleRows=1000)")
	}

	// Empty.
	if _, ok := projectCapacity(nil, time.Hour, now); ok {
		t.Error("no partitions must be insufficient")
	}
}

func TestCapacityVerdict_noRetention(t *testing.T) {
	p := capacityProjection{eventsPerDay: 24000, bytesPerEvent: 1000, currentBytes: 10_000_000}

	r := capacityVerdict(p, 0, 0, false, CapacityFreeMountUnset)
	if r.Status != StatusWarn {
		t.Fatalf("status = %s, want warn for retain=0", r.Status)
	}
	if !strings.Contains(r.Detail, "unbounded") {
		t.Errorf("detail should name the unbounded growth, got: %s", r.Detail)
	}
	if r.Remediation == "" {
		t.Error("warn must carry remediation")
	}

	// With free space known, the detail carries days-until-full:
	// 24 MB/day against 240 MB free ≈ 10 days.
	r = capacityVerdict(p, 0, 240_000_000, true, CapacityFreeFromDatadir)
	if !strings.Contains(r.Detail, "days until the volume fills") {
		t.Errorf("detail should estimate days until full, got: %s", r.Detail)
	}
}

func TestCapacityVerdict_thresholds(t *testing.T) {
	retain := 30 * 24 * time.Hour

	cases := []struct {
		desc    string
		current uint64
		free    uint64
		want    CheckStatus
	}{
		// Fresh index (currentBytes 0): remaining growth == full projection.
		{"fresh: projection exceeds free space", 0, 700_000_000, StatusFail},
		{"fresh: projection at exactly free space", 0, 720_000_000, StatusFail},
		{"fresh: projection over 70% of free", 0, 1_000_000_000, StatusWarn}, // 72%
		{"fresh: comfortable headroom", 0, 2_000_000_000, StatusPass},        // 36%

		// Mature index at steady state: the table already occupies most of
		// its projection — comparing the TOTAL against free would
		// double-count and spuriously FAIL a healthy deployment on restart.
		{"steady state: tiny remaining growth, modest free", 700_000_000, 680_000_000, StatusPass}, // remaining 20 MB
		{"mature: remaining growth exceeds free", 300_000_000, 400_000_000, StatusFail},            // remaining 420 MB
		{"mature: remaining growth over 70% of free", 300_000_000, 500_000_000, StatusWarn},        // 420/500 = 84%

		// Free-space floor (growthPerDay = 24 MB/day → floor = 72 MB): the
		// remaining-growth thresholds go quiet at steady state, but a
		// nearly-full volume still deserves a WARN — and the floor replaces
		// the nonsensical "~0 B EXCEEDS 0 B free" FAIL on a full disk.
		{"steady state: volume nearly full", 720_000_000, 10_000_000, StatusWarn},
		{"steady state: rate dropped, under 3 days of free", 800_000_000, 50_000_000, StatusWarn},
		{"steady state: disk completely full", 720_000_000, 0, StatusWarn},
	}
	for _, tc := range cases {
		// Projection: 24000 events/day × 1000 B × 30d = 720 MB total.
		p := capacityProjection{eventsPerDay: 24000, bytesPerEvent: 1000, projectedBytes: 720_000_000, currentBytes: tc.current}
		r := capacityVerdict(p, retain, tc.free, true, CapacityFreeFromDatadir)
		if r.Status != tc.want {
			t.Errorf("%s: status = %s, want %s (detail: %s)", tc.desc, r.Status, tc.want, r.Detail)
		}
		if (tc.want == StatusFail || tc.want == StatusWarn) && r.Remediation == "" {
			t.Errorf("%s: %s must carry remediation", tc.desc, tc.want)
		}
	}
}

func TestSameHostname(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"db01", "db01", true},
		{"DB01", "db01", true},
		{"db01.local", "db01", true},
		{"db01", "db01.internal.corp", true},
		{"db01", "db02", false},
		{"index-mysql", "MacBook", false},
	}
	for _, tc := range cases {
		if got := sameHostname(tc.a, tc.b); got != tc.want {
			t.Errorf("sameHostname(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestDoctorReportErrExcluding(t *testing.T) {
	r := &Report{}
	r.add(CheckResult{Name: "Index disk capacity", Status: StatusFail})
	r.add(CheckResult{Name: "log_bin enabled", Status: StatusPass})

	if r.Err() == nil {
		t.Error("Err must report the capacity failure")
	}
	if err := r.ErrExcluding(CapacityCheckName); err != nil {
		t.Errorf("ErrExcluding(capacity) = %v, want nil — capacity is advisory in up's preflight", err)
	}

	// A non-advisory failure still blocks.
	r.add(CheckResult{Name: "binlog_format=ROW", Status: StatusFail})
	if r.ErrExcluding(CapacityCheckName) == nil {
		t.Error("ErrExcluding must still report non-advisory failures")
	}
}

func TestCapacityVerdict_freeUnknownSkipsWithGuidance(t *testing.T) {
	p := capacityProjection{eventsPerDay: 24000, bytesPerEvent: 1000, projectedBytes: 720_000_000}
	r := capacityVerdict(p, 30*24*time.Hour, 0, false, CapacityFreeMountUnset)
	// #948: a volume this process cannot see (no read-only mount of the index
	// datadir, as on a compose stack that predates that wiring) must SKIP, not
	// PASS, so the check does not read green when its FAIL/WARN thresholds
	// could never fire.
	if r.Status != StatusSkip {
		t.Fatalf("status = %s, want skip when free space is unknown", r.Status)
	}
	if !strings.Contains(r.Detail, "not measurable") || !strings.Contains(r.Detail, "headroom") {
		t.Errorf("detail must carry the projection plus headroom guidance, got: %s", r.Detail)
	}
}

func TestDSNTargetsLocalhost(t *testing.T) {
	cases := []struct {
		dsn  string
		want bool
	}{
		{"root:x@tcp(127.0.0.1:3306)/db", true},
		{"root:x@tcp(localhost:3306)/db", true},
		{"root:x@tcp([::1]:3306)/db", true},
		{"root:x@unix(/var/run/mysqld/mysqld.sock)/db", true},
		{"root:x@tcp(index-mysql:3306)/db", false},
		{"root:x@tcp(10.0.0.5:3306)/db", false},
		{"root:x@tcp(db.example.com:3306)/db", false},
	}
	for _, tc := range cases {
		cfg, err := mysql.ParseDSN(tc.dsn)
		if err != nil {
			t.Fatalf("ParseDSN(%s): %v", tc.dsn, err)
		}
		if got := dsnTargetsLocalhost(cfg); got != tc.want {
			t.Errorf("dsnTargetsLocalhost(%s) = %v, want %v", tc.dsn, got, tc.want)
		}
	}
}

// TestIndexDatadirFreeFromEnv covers the #948 compose short-circuit in
// isolation: env-var-set + real directory measures successfully, and a
// stale/missing path degrades gracefully instead of panicking.
func TestIndexDatadirFreeFromEnv(t *testing.T) {
	t.Run("set and valid dir", func(t *testing.T) {
		t.Setenv("BINTRAIL_INDEX_DATADIR_RO", t.TempDir())
		free, ok := indexDatadirFreeFromEnv()
		if !ok {
			t.Fatal("expected freeKnown=true for a valid directory")
		}
		if free == 0 {
			t.Error("expected a nonzero free byte count for a real filesystem")
		}
	})

	t.Run("set but path does not exist", func(t *testing.T) {
		t.Setenv("BINTRAIL_INDEX_DATADIR_RO", "/nonexistent/path/for/948/test")
		if _, ok := indexDatadirFreeFromEnv(); ok {
			t.Error("expected freeKnown=false when the mounted path does not exist")
		}
	})

	t.Run("set but points at a file, not a directory", func(t *testing.T) {
		f := filepath.Join(t.TempDir(), "not-a-dir")
		if err := os.WriteFile(f, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("BINTRAIL_INDEX_DATADIR_RO", f)
		if _, ok := indexDatadirFreeFromEnv(); ok {
			t.Error("expected freeKnown=false when the path is a file, not a directory")
		}
	})

	t.Run("unset", func(t *testing.T) {
		t.Setenv("BINTRAIL_INDEX_DATADIR_RO", "")
		if _, ok := indexDatadirFreeFromEnv(); ok {
			t.Error("expected freeKnown=false when the env var is unset")
		}
	})
}

// TestIndexDatadirFree_envShortCircuitsHostnameCheck confirms the env-var
// path in indexDatadirFree runs BEFORE — and independent of — the
// loopback/hostname-match logic: a non-loopback DSN and a nil *sql.DB (which
// would panic if any of the hostname-matching queries ran) still measure
// successfully once the env var is set. This is the exact scenario #948
// describes (bundled compose reaches the index over tcp(index-mysql:3306),
// never loopback).
func TestIndexDatadirFree_envShortCircuitsHostnameCheck(t *testing.T) {
	t.Setenv("BINTRAIL_INDEX_DATADIR_RO", t.TempDir())
	free, ok, _ := indexDatadirFree(context.Background(), nil, "root:x@tcp(index-mysql:3306)/bintrail_index")
	if !ok {
		t.Fatal("expected freeKnown=true via the env-var short-circuit, independent of the DSN/db")
	}
	if free == 0 {
		t.Error("expected a nonzero free byte count")
	}
}

func TestHumanBytes(t *testing.T) {
	cases := []struct {
		in   float64
		want string
	}{
		{512, "512 B"},
		{2048, "2.0 KB"},
		{720_000_000, "686.6 MB"},
		{52_000_000_000, "48.4 GB"},
	}
	for _, tc := range cases {
		if got := humanBytes(tc.in); got != tc.want {
			t.Errorf("humanBytes(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
