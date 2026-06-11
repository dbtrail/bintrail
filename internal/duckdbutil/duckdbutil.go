// Package duckdbutil holds small helpers shared by the DuckDB sessions
// bintrail opens (baseline reads, snapshot queries, S3 footer probes).
package duckdbutil

import (
	"context"
	"database/sql"
	"log/slog"
)

// EnableS3CredentialChain gives a DuckDB session AWS-SDK-parity credentials
// for s3:// access: the aws extension's credential_chain provider resolves
// the same chain as aws-sdk-go-v2 — env keys, shared profile (incl. SSO),
// and EC2/ECS/EKS IAM roles. Without it, plain httpfs resolves env keys at
// best, so role-/SSO-only environments failed exactly on the DuckDB read
// paths while every SDK-backed upload worked (#459).
//
// Best-effort BY DESIGN: INSTALL may need to download the extension once
// (cached in ~/.duckdb afterwards), and offline/airgapped hosts must keep
// working wherever env keys — or no S3 at all — suffice. A failure therefore
// logs at debug and the session proceeds with plain httpfs resolution; if
// credentials are genuinely absent, the S3 read that follows fails loudly on
// its own.
//
// Call it after `INSTALL httpfs; LOAD httpfs;` on sessions that will touch
// s3:// paths.
func EnableS3CredentialChain(ctx context.Context, db *sql.DB) {
	if _, err := db.ExecContext(ctx,
		"INSTALL aws; LOAD aws; CREATE OR REPLACE SECRET bintrail_s3_chain (TYPE s3, PROVIDER credential_chain);"); err != nil {
		slog.Debug("duckdb: aws credential_chain unavailable; s3 access falls back to httpfs env-key resolution",
			"error", err)
	}
}
