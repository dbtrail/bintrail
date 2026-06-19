package streamrun

import (
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

// The detectMariaDBGTIDGap unit tests sqlmock the three MariaDB gap-detection
// queries — SHOW BINARY LOGS, BINLOG_GTID_POS(<earliest>, 4) (the purge floor;
// MariaDB has no @@gtid_purged), and @@gtid_binlog_pos (the executed set) — and
// assert the decision for every branch: caught-up, fillable, unfillable, an empty
// floor (nothing purged), multi-domain, and the post-failover cross-server case
// that proves the domain-aware coverage check (not go-mysql's server-keyed
// MariadbGTIDSet.Contain) is in use.

// expectMariaDBGapQueries primes the ordered three-query sequence used by
// detectMariaDBGTIDGap.
func expectMariaDBGapQueries(mock sqlmock.Sqlmock, earliestFile, floor, executed string) {
	mock.ExpectQuery("SHOW BINARY LOGS").WillReturnRows(
		sqlmock.NewRows([]string{"Log_name", "File_size"}).
			AddRow(earliestFile, 1024).
			AddRow("mariadb-bin.999999", 2048))
	mock.ExpectQuery("SELECT BINLOG_GTID_POS").WithArgs(earliestFile).WillReturnRows(
		sqlmock.NewRows([]string{"BINLOG_GTID_POS"}).AddRow(floor))
	mock.ExpectQuery("SELECT @@gtid_binlog_pos").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_binlog_pos"}).AddRow(executed))
}

func TestDetectMariaDBGTIDGap_noGapNothingPurged(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// Empty floor = the first binlog still exists, nothing purged; checkpoint
	// equals the executed set, so the stream is caught up.
	expectMariaDBGapQueries(mock, "mariadb-bin.000001", "", "0-2-75")

	gap, err := detectMariaDBGTIDGap(db, "0-2-75", 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gap.HasGap {
		t.Errorf("expected no gap when checkpoint equals executed and nothing is purged, got %+v", gap)
	}
}

// TestDetectMariaDBGTIDGap_nullFloorNothingPurged pins the SQL-NULL purge floor.
// On a source whose first-ever binlog still exists, BINLOG_GTID_POS returns NULL
// (not an empty string); the detector must read that through a NullString and
// treat it as "nothing purged", not error. A regression to a plain-string Scan
// fails here — and that path is the common no-purge happy case.
func TestDetectMariaDBGTIDGap_nullFloorNothingPurged(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW BINARY LOGS").WillReturnRows(
		sqlmock.NewRows([]string{"Log_name", "File_size"}).AddRow("mariadb-bin.000001", 1024))
	mock.ExpectQuery("SELECT BINLOG_GTID_POS").WithArgs("mariadb-bin.000001").WillReturnRows(
		sqlmock.NewRows([]string{"BINLOG_GTID_POS"}).AddRow(nil)) // SQL NULL
	mock.ExpectQuery("SELECT @@gtid_binlog_pos").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_binlog_pos"}).AddRow("0-2-75"))

	gap, err := detectMariaDBGTIDGap(db, "0-2-75", 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error on NULL purge floor (nothing purged): %v", err)
	}
	if gap.HasGap {
		t.Errorf("expected no gap when the purge floor is NULL and checkpoint equals executed, got %+v", gap)
	}
}

func TestDetectMariaDBGTIDGap_fillableNothingPurged(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// Nothing purged but the checkpoint is behind — fillable, just replay.
	expectMariaDBGapQueries(mock, "mariadb-bin.000001", "", "0-2-200")

	gap, err := detectMariaDBGTIDGap(db, "0-2-75", 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !gap.HasGap {
		t.Fatal("expected a gap when checkpoint is behind executed")
	}
	if !gap.Fillable {
		t.Errorf("expected fillable gap when nothing is purged, got %+v", gap)
	}
}

func TestDetectMariaDBGTIDGap_unfillable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// Purge floor 0-2-71 is beyond the checkpoint 0-2-50 — GTIDs 51..71 are gone.
	expectMariaDBGapQueries(mock, "mariadb-bin.000005", "0-2-71", "0-2-200")

	gap, err := detectMariaDBGTIDGap(db, "0-2-50", 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !gap.HasGap {
		t.Fatal("expected a gap when the purge floor is beyond the checkpoint")
	}
	if gap.Fillable {
		t.Error("expected an UNFILLABLE gap when purged GTIDs precede the checkpoint")
	}
	if gap.PurgedGTIDSet != "0-2-71" {
		t.Errorf("expected PurgedGTIDSet=floor %q, got %q", "0-2-71", gap.PurgedGTIDSet)
	}
}

// TestDetectMariaDBGTIDGap_boundaryAtFloor pins the strict `cpMax < floorMax`
// coverage boundary one unit on each side. Every other unfillable case sits far
// from the boundary, so an off-by-one regression (e.g. `cpMax+1 < floorMax`)
// would pass them all yet flip cp=floorMax-1 from unfillable to fillable — a real
// unfillable gap reported as fillable, the silent-data-loss direction.
func TestDetectMariaDBGTIDGap_boundaryAtFloor(t *testing.T) {
	// cp = floorMax - 1 (70 vs 71): the checkpoint still needs the purged GTID 71
	// → UNFILLABLE.
	t.Run("justBelowFloor_unfillable", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		expectMariaDBGapQueries(mock, "mariadb-bin.000005", "0-2-71", "0-2-200")
		gap, err := detectMariaDBGTIDGap(db, "0-2-70", 10*time.Second)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !gap.HasGap || gap.Fillable {
			t.Errorf("cp=70 / floor=71: still needs purged GTID 71 → want UNFILLABLE, got %+v", gap)
		}
	})

	// cp = floorMax exactly (71 vs 71): the checkpoint has seen everything up to the
	// floor; the next GTID it needs is 72, which survives → FILLABLE (behind the
	// executed set 200). The safe complement of the boundary.
	t.Run("atFloor_fillable", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		expectMariaDBGapQueries(mock, "mariadb-bin.000005", "0-2-71", "0-2-200")
		gap, err := detectMariaDBGTIDGap(db, "0-2-71", 10*time.Second)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !gap.HasGap {
			t.Fatal("cp=71 / floor=71: behind executed 200 → expected a gap")
		}
		if !gap.Fillable {
			t.Errorf("cp=71 / floor=71: checkpoint covers the floor exactly → want FILLABLE, got %+v", gap)
		}
	})
}

func TestDetectMariaDBGTIDGap_fillableCheckpointPastFloor(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// Floor 0-2-50 is purged but the checkpoint 0-2-100 is already past it, and
	// still behind executed 0-2-200 — fillable.
	expectMariaDBGapQueries(mock, "mariadb-bin.000003", "0-2-50", "0-2-200")

	gap, err := detectMariaDBGTIDGap(db, "0-2-100", 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !gap.HasGap {
		t.Fatal("expected a gap when checkpoint is behind executed")
	}
	if !gap.Fillable {
		t.Errorf("expected fillable gap when checkpoint is past the purge floor, got %+v", gap)
	}
}

func TestDetectMariaDBGTIDGap_caughtUpWithPurged(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// Floor purged, but the checkpoint is past it AND equal to executed — no gap.
	expectMariaDBGapQueries(mock, "mariadb-bin.000003", "0-2-50", "0-2-200")

	gap, err := detectMariaDBGTIDGap(db, "0-2-200", 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gap.HasGap {
		t.Errorf("expected no gap when checkpoint is past the floor and equals executed, got %+v", gap)
	}
}

func TestDetectMariaDBGTIDGap_multiDomainUnfillable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// Two domains. Domain 0 is covered (200 >= 50) but domain 1's checkpoint (20)
	// is behind its purge floor (30) — multi-domain gap detection must catch it.
	expectMariaDBGapQueries(mock, "mariadb-bin.000004", "0-1-50,1-2-30", "0-1-200,1-2-100")

	gap, err := detectMariaDBGTIDGap(db, "0-1-200,1-2-20", 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !gap.HasGap || gap.Fillable {
		t.Errorf("expected an unfillable gap when one domain is behind its floor, got %+v", gap)
	}
}

func TestDetectMariaDBGTIDGap_multiDomainFillable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// Both domains past their floors but behind executed — fillable.
	expectMariaDBGapQueries(mock, "mariadb-bin.000004", "0-1-50,1-2-30", "0-1-200,1-2-100")

	gap, err := detectMariaDBGTIDGap(db, "0-1-150,1-2-80", 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !gap.HasGap {
		t.Fatal("expected a gap when checkpoint is behind executed")
	}
	if !gap.Fillable {
		t.Errorf("expected fillable gap when both domains cover their floors, got %+v", gap)
	}
}

// TestDetectMariaDBGTIDGap_crossServerFailover is the discriminator for the
// domain-aware coverage check. The purge floor was written by server 1
// (0-1-71) but after a failover the checkpoint is server 2 at a higher sequence
// (0-2-100). Because MariaDB sequence numbers are domain-global, 100 >= 71 means
// the checkpoint HAS seen past the floor — so this must be a fillable gap, not an
// unfillable one. go-mysql's server-keyed MariadbGTIDSet.Contain would wrongly
// flag it unfillable (server 1 absent from the checkpoint); this test fails if we
// regress to that.
func TestDetectMariaDBGTIDGap_crossServerFailover(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	expectMariaDBGapQueries(mock, "mariadb-bin.000005", "0-1-71", "0-2-200")

	gap, err := detectMariaDBGTIDGap(db, "0-2-100", 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !gap.HasGap {
		t.Fatal("expected a gap when checkpoint is behind executed")
	}
	if !gap.Fillable {
		t.Errorf("expected a FILLABLE gap: checkpoint seq 100 >= floor seq 71 in domain 0 "+
			"despite a different server_id (domain-aware coverage), got %+v", gap)
	}
}

// TestMariaDBCheckpointCoversFloor_multiServerPerDomain pins the one path the
// detect-level tests cannot reach: mariadbDomainMaxSeq's max() aggregating MORE
// THAN ONE server within a SINGLE domain. The cross-server detect test keeps its
// two servers in separate sets (one per domain each), so the multi-server max()
// never runs there — and the floor-side aggregation is the silent-data-loss
// direction: under-computing the floor frontier reports a real unfillable gap as
// fillable, and would stay green on every detect-level test. So assert it
// directly and order-independently.
func TestMariaDBCheckpointCoversFloor_multiServerPerDomain(t *testing.T) {
	mustParse := func(s string) *gomysql.MariadbGTIDSet {
		set, err := parseMariadbSet(s)
		if err != nil {
			t.Fatalf("parseMariadbSet(%q): %v", s, err)
		}
		return set
	}

	// Checkpoint-side max(): a realistic post-failover accumulated checkpoint with
	// two servers in domain 0. The domain frontier is max(50,100)=100, which
	// covers floor 71 → fillable. A first-wins/min regression makes this fail
	// regardless of map iteration order (100 must win).
	if !mariadbCheckpointCoversFloor(mustParse("0-1-50,0-2-100"), mustParse("0-1-71")) {
		t.Error("multi-server checkpoint: domain-0 max(50,100)=100 >= floor 71 → want covered (fillable)")
	}

	// Floor-side max() — the silent-data-loss direction. The domain-0 purge floor
	// is max(71,30)=71; a checkpoint at seq 50 has NOT seen purged 51..71 →
	// unfillable.
	if mariadbCheckpointCoversFloor(mustParse("0-2-50"), mustParse("0-1-71,0-2-30")) {
		t.Error("multi-server floor: cp seq 50 does not cover domain-0 frontier max(71,30)=71 → want NOT covered (unfillable)")
	}

	// Pin the floor-side aggregation directly. On a multi-server domain ANY
	// "pick one server" regression (first-wins, drop-higher) equals the true max
	// whenever map iteration happens to start on the max element, so a single
	// assertion is ~50/50 against it — Go re-randomizes map order per range. Turn
	// that randomization AGAINST the bug: assert the frontier over many iterations
	// so a first-wins regression (P(miss) = (1/2)^N) is caught with overwhelming
	// probability, while min/drop-higher are caught on the first iteration.
	floor := mustParse("0-1-71,0-2-30")
	for i := range 64 {
		if got := mariadbDomainMaxSeq(floor.Sets[0]); got != 71 {
			t.Fatalf("mariadbDomainMaxSeq(domain 0 of {0-1-71,0-2-30}) = %d, want 71 (the per-domain "+
				"frontier) on iteration %d — the aggregation must be order-independent", got, i)
		}
	}

	// Also assert the defining invariant order-independently: the frontier is >=
	// every server's sequence in the domain (catches any under-computing
	// aggregation, the silent-data-loss direction).
	for srv, g := range floor.Sets[0] {
		if mariadbDomainMaxSeq(floor.Sets[0]) < g.SequenceNumber {
			t.Errorf("mariadbDomainMaxSeq < server %d seq %d — frontier must cover every server", srv, g.SequenceNumber)
		}
	}
}

// TestDetectMariaDBGTIDGap_domainNeverSeen covers a purge floor in a domain the
// checkpoint never indexed at all — unreachable, so unfillable.
func TestDetectMariaDBGTIDGap_domainNeverSeen(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	expectMariaDBGapQueries(mock, "mariadb-bin.000005", "1-2-10", "0-2-200,1-2-50")

	gap, err := detectMariaDBGTIDGap(db, "0-2-200", 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !gap.HasGap || gap.Fillable {
		t.Errorf("expected an unfillable gap for a purged domain the checkpoint never saw, got %+v", gap)
	}
}

// TestDetectMariaDBGTIDGap_emptyCheckpointGuard mirrors detectGTIDGap's guard:
// with a non-empty purge floor we cannot reason about an empty checkpoint.
func TestDetectMariaDBGTIDGap_emptyCheckpointGuard(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	expectMariaDBGapQueries(mock, "mariadb-bin.000005", "0-2-71", "0-2-200")

	_, err = detectMariaDBGTIDGap(db, "", 10*time.Second)
	if err == nil {
		t.Fatal("expected an error for an empty checkpoint against a non-empty purge floor")
	}
}

// TestNormalizeGTIDForFlavor pins the byte-identical-for-MySQL guarantee of the
// flavor-aware auto-advance refactor. The unfillable-gap auto-advance was rewired
// from a hardcoded NormalizeGTIDSet call to normalizeGTIDForFlavor; for a MySQL
// source the two must be indistinguishable, and a MariaDB domain-server-seq set
// (no UUID to pad) must pass through untouched.
func TestNormalizeGTIDForFlavor(t *testing.T) {
	const mysqlSet = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
	if got, want := normalizeGTIDForFlavor(gomysql.MySQLFlavor, mysqlSet), NormalizeGTIDSet(mysqlSet); got != want {
		t.Errorf("normalizeGTIDForFlavor(mysql, %q) = %q, want NormalizeGTIDSet = %q (byte-identical for MySQL)", mysqlSet, got, want)
	}
	if got := normalizeGTIDForFlavor(gomysql.MariaDBFlavor, "0-2-71"); got != "0-2-71" {
		t.Errorf("normalizeGTIDForFlavor(mariadb, %q) = %q, want unchanged (no UUID to pad)", "0-2-71", got)
	}
}

// advancedTestState is a minimal streamState for persistGapAutoAdvance tests.
func advancedTestState() *streamState {
	return &streamState{mode: "gtid", binlogFile: "mariadb-bin.000005", gtidSet: "0-2-71", flavor: gomysql.MariaDBFlavor, serverID: 99}
}

// TestPersistGapAutoAdvance_stampsBeforeAdvance pins the data-loss-safety ordering
// invariant: the gap_lost_at stamp (UPDATE) must be written BEFORE the advanced
// checkpoint (the INSERT...ON DUPLICATE KEY upsert). sqlmock matches expectations
// in order by default, so declaring UPDATE-then-INSERT fails if the code advances
// the checkpoint first.
func TestPersistGapAutoAdvance_stampsBeforeAdvance(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectExec("UPDATE stream_state").
		WithArgs("events lost").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO stream_state").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := persistGapAutoAdvance(db, advancedTestState(), "events lost"); err != nil {
		t.Fatalf("persistGapAutoAdvance: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("stamp must precede checkpoint advance: %v", err)
	}
}

// TestPersistGapAutoAdvance_abortsOnStampFailure is the core of the #402 fix: if
// the gap_lost_at stamp fails, the function must return an error and must NOT
// advance the checkpoint — otherwise the next restart would see no gap with no
// durable trace of the loss. The INSERT is deliberately NOT expected; sqlmock
// errors if the code issues it after the failed UPDATE.
func TestPersistGapAutoAdvance_abortsOnStampFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectExec("UPDATE stream_state").
		WillReturnError(errors.New("index DB transient error"))
	// No ExpectExec for the INSERT — the checkpoint must not be advanced.

	if err := persistGapAutoAdvance(db, advancedTestState(), "events lost"); err == nil {
		t.Fatal("expected an error when the gap-loss stamp fails (checkpoint must not advance)")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("checkpoint advance must be skipped when the stamp fails: %v", err)
	}
}
