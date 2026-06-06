package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/bintrail/internal/config"
	"github.com/dbtrail/bintrail/internal/console"
)

// Replica / duplicate detection (#402): adding a replica of an already-
// monitored server double-indexes the same logical writes silently. GTID
// lineage makes the relationship detectable without touching the other
// sources: a replica's @@gtid_executed contains transactions originated at
// its primary's server_uuid, and each monitored entry's per-source index DB
// already stores that entry's server_uuid (bintrail_servers) and accumulated
// executed set (stream_state.gtid_set). Warn-only per the approved decision —
// an amber card in the add-server doctor flow, never a hard block.

// replicaOverlapTimeout bounds the whole check — it runs inside the
// interactive doctor flow and must not hang on a dead peer index DB.
const replicaOverlapTimeout = 15 * time.Second

// replicaOverlapCheck compares the candidate entry's source against every
// other monitored registry entry. Returns nil when there is nothing to
// compare (no registry, no peers) — no card is shown then.
func (m *monitorSupervisor) replicaOverlapCheck(ctx context.Context, e console.ServerEntry) *console.DoctorCheck {
	const checkName = "Replica / duplicate detection"
	if m.registry == nil {
		return nil
	}
	var peers []console.ServerEntry
	for _, p := range m.registry.List() {
		if p.ID != e.ID && p.SourceDSN != "" && p.DSN != "" {
			peers = append(peers, p)
		}
	}
	if len(peers) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, replicaOverlapTimeout)
	defer cancel()

	srcDB, err := config.Connect(e.SourceDSN)
	if err != nil {
		return &console.DoctorCheck{Name: checkName, Status: "skip",
			Detail: "could not connect to the source to compare GTID lineage: " + err.Error()}
	}
	defer srcDB.Close()

	var gtidMode string
	if err := srcDB.QueryRowContext(ctx, "SELECT @@gtid_mode").Scan(&gtidMode); err != nil {
		return &console.DoctorCheck{Name: checkName, Status: "skip",
			Detail: "could not read @@gtid_mode: " + err.Error()}
	}
	if !strings.EqualFold(gtidMode, "ON") {
		return &console.DoctorCheck{Name: checkName, Status: "skip",
			Detail: fmt.Sprintf("gtid_mode is %s on this source — replica detection needs GTID; a position-mode duplicate cannot be detected", gtidMode)}
	}

	var candUUID, candExecuted string
	if err := srcDB.QueryRowContext(ctx,
		"SELECT @@server_uuid, @@global.gtid_executed").Scan(&candUUID, &candExecuted); err != nil {
		return &console.DoctorCheck{Name: checkName, Status: "skip",
			Detail: "could not read server_uuid / gtid_executed: " + err.Error()}
	}

	var findings []string
	unverified := 0
	for _, p := range peers {
		peerUUID, peerExecuted, err := loadPeerIdentity(ctx, p.DSN)
		if err != nil {
			// Best-effort: a peer whose index DB is unreadable (never
			// started, dropped, server down) is unverified, not a failure.
			unverified++
			continue
		}
		if rel := classifyReplicaOverlap(candUUID, candExecuted, peerUUID, peerExecuted); rel != "" {
			findings = append(findings, fmt.Sprintf("%s %q", rel, p.Name))
		}
	}

	if len(findings) > 0 {
		return &console.DoctorCheck{
			Name:   checkName,
			Status: "warn",
			Detail: "this server " + strings.Join(findings, "; "),
			Remediation: "Monitoring a primary and its replica (or the same server twice) indexes every\n" +
				"row change once per entry — duplicate history, duplicate storage.\n\n" +
				"If that is not intentional, monitor only one of them (usually the primary).\n" +
				"This is a WARN, not a hard fail — press Start again to proceed anyway.",
		}
	}
	detail := fmt.Sprintf("no replica relationship detected among %d monitored source(s)", len(peers))
	if unverified > 0 {
		detail += fmt.Sprintf(" (%d could not be verified)", unverified)
	}
	return &console.DoctorCheck{Name: checkName, Status: "pass", Detail: detail}
}

// loadPeerIdentity reads a monitored entry's recorded server_uuid and
// accumulated executed GTID set from its per-source index database.
func loadPeerIdentity(ctx context.Context, indexDSN string) (peerUUID, peerExecuted string, err error) {
	db, err := config.Connect(indexDSN)
	if err != nil {
		return "", "", err
	}
	defer db.Close()

	if err := db.QueryRowContext(ctx, `
		SELECT server_uuid FROM bintrail_servers
		WHERE decommissioned_at IS NULL
		ORDER BY updated_at DESC LIMIT 1`).Scan(&peerUUID); err != nil {
		return "", "", err
	}
	// stream_state may not exist yet (monitoring never started) or hold no
	// GTID set (position mode) — both leave peerExecuted empty, which only
	// disables the primary-of-monitored-replica direction.
	var gtidSet sql.NullString
	if err := db.QueryRowContext(ctx,
		`SELECT gtid_set FROM stream_state WHERE id = 1`).Scan(&gtidSet); err == nil && gtidSet.Valid {
		peerExecuted = gtidSet.String
	}
	return peerUUID, peerExecuted, nil
}

// classifyReplicaOverlap describes the GTID-lineage relationship between a
// candidate server and one monitored peer, or "" when none is detected.
// Pure — unit-testable without MySQL.
func classifyReplicaOverlap(candUUID, candExecuted, peerUUID, peerExecuted string) string {
	if strings.EqualFold(candUUID, peerUUID) {
		return "is the same server as already-monitored"
	}
	// The peer's UUID appears in the candidate's executed set: the candidate
	// has applied transactions originated on the peer — it is (or was) a
	// replica of it.
	if gtidSetContainsUUID(candExecuted, peerUUID) {
		return "appears to be a replica of already-monitored"
	}
	// The candidate's UUID appears in the monitored peer's executed set: the
	// peer replicates from the candidate — the candidate is its primary.
	if gtidSetContainsUUID(peerExecuted, candUUID) {
		return "appears to be the primary of already-monitored replica"
	}
	return ""
}

// gtidSetContainsUUID reports whether the GTID set contains any transactions
// originated at the given server UUID. Malformed input is never a match —
// the caller treats this as best-effort detection. Comparison is
// case-insensitive (go-mysql lowercases UUIDs).
func gtidSetContainsUUID(gtidSet, serverUUID string) bool {
	if gtidSet == "" || serverUUID == "" {
		return false
	}
	parsed, err := gomysql.ParseMysqlGTIDSet(normalizeGTIDSet(gtidSet))
	if err != nil {
		return false
	}
	set, ok := parsed.(*gomysql.MysqlGTIDSet)
	if !ok {
		return false
	}
	want := strings.ToLower(serverUUID)
	for sid := range set.Sets {
		if strings.ToLower(sid) == want {
			return true
		}
	}
	return false
}
