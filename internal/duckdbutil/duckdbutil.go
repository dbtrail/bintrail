// Package duckdbutil holds small helpers shared by the DuckDB sessions
// bintrail opens (baseline reads, snapshot queries, S3 footer probes).
package duckdbutil

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
)

// EnableS3CredentialChain gives a DuckDB session AWS-SDK credentials for
// s3:// access: the aws extension's credential_chain provider resolves the
// AWS SDK default chain — env keys, config profiles, and EC2/ECS/EKS IAM
// roles (SSO-session profiles have open gaps upstream: duckdb-aws#125).
// Without it, plain httpfs resolves static env keys at best, so role-only
// environments failed exactly on the DuckDB read paths while every
// SDK-backed upload worked (#459).
//
// Best-effort BY DESIGN, but never silent where it matters: when the aws
// extension cannot install/load (offline hosts — it is cached in ~/.duckdb,
// per DuckDB version and platform, after one connected run) the session
// proceeds with plain httpfs env-key resolution. That fallback is logged at
// debug when AWS env keys exist (it genuinely works) and at WARN when they
// don't — the upcoming S3 read is then doomed to a generic 403 that never
// mentions the chain, so this warn is the only diagnostic the operator gets.
// A failed CREATE SECRET (the chain resolved no usable credentials — broken
// profile, expired SSO, unreachable IMDS) always warns: the SDK upload paths
// report that state loudly and the read paths must not bury it.
//
// BINTRAIL_DUCKDB_NO_AWS_EXT=1 skips the setup entirely. Escape hatch for
// proxies that BLACKHOLE (rather than refuse) the DuckDB extension registry:
// there the INSTALL attempt can stall for minutes, ignores context
// cancellation, and recurs every session because failures are never cached.
//
// The secret resolves credentials at CREATE time, not per request — fine
// here because every caller opens a short-lived session per operation; do
// not reuse this on long-lived pooled sessions under expiring roles.
//
// Call it after `INSTALL httpfs; LOAD httpfs;` on sessions that will touch
// s3:// paths.
func EnableS3CredentialChain(ctx context.Context, db *sql.DB) {
	if os.Getenv("BINTRAIL_DUCKDB_NO_AWS_EXT") != "" {
		return
	}
	if _, err := db.ExecContext(ctx, "INSTALL aws; LOAD aws;"); err != nil {
		if os.Getenv("AWS_ACCESS_KEY_ID") != "" {
			slog.Debug("duckdb: aws extension unavailable; S3 reads fall back to env-key resolution",
				"error", err)
		} else {
			slog.Warn("duckdb: aws extension unavailable and no AWS env keys set — profile/role credentials will NOT apply to this S3 read; expect an authentication failure",
				"error", err)
		}
		return
	}
	if _, err := db.ExecContext(ctx,
		"CREATE OR REPLACE SECRET bintrail_s3_chain (TYPE s3, PROVIDER credential_chain)"); err != nil {
		slog.Warn("duckdb: AWS credential chain resolved no usable credentials for S3 reads",
			"error", err)
	}
}
